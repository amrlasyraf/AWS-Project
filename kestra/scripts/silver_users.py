import os
import datetime
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, 
    StringType, 
    TimestampType, 
    DoubleType, 
    IntegerType, 
    NestedField
)

def main():
    # Setup AWS credentials from Kestra environment variables
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')

    # Path for the Bronze layer partitioned by date
    now = datetime.datetime.now(datetime.timezone.utc)
    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    s3_path = f"s3://ewallet-storage/bronze/partner=*/table=users/{year}/{month}/{day}/*.parquet"

    # Initialize pyiceberg catalog using AWS Glue
    catalog = load_catalog(
        "glue_catalog",
        **{
            "type": "glue",
            "s3.region": aws_region,
            "s3.access-key-id": aws_access_key,
            "s3.secret-access-key": aws_secret_key,
            "warehouse": "s3://ewallet-storage/silver/tables"
        }
    )

    # Initialize DuckDB with S3 support
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{aws_region}';")
    con.execute(f"SET s3_access_key_id='{aws_access_key}';")
    con.execute(f"SET s3_secret_access_key='{aws_secret_key}';")

    try:
        # Check if files exist before processing
        input_count_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}', hive_partitioning=1)"
        input_rows = con.execute(input_count_query).fetchone()[0]
        
        if input_rows == 0:
            print(f"No records found in path: {s3_path}. Exiting.")
            return
    except Exception as e:
        print(f"Error accessing S3 path: {e}")
        return

    # Transformation Query: Cast types and Deduplicate using window functions
    # Using 'clean' aliases to avoid DuckDB Binder Errors
    query = f"""
        WITH deduped AS (
            SELECT 
                TRY_CAST(user_id AS BIGINT) AS user_id,
                TRY_CAST(current_age AS INTEGER) AS age_clean,
                TRY_CAST(yearly_income AS DOUBLE) AS income_clean,
                TRY_CAST(source_partner AS VARCHAR) AS partner_clean,
                NOW() AS ingest_ts_clean, 
                ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_id) as rn
            FROM read_parquet('{s3_path}', hive_partitioning=1)
        )
        SELECT 
            user_id,
            age_clean AS age,
            income_clean AS income,
            partner_clean AS partner,
            ingest_ts_clean AS ingest_ts
        FROM deduped 
        WHERE rn = 1
    """

    # Convert Result to PyArrow Table
    arrow_table = con.execute(query).arrow()
    output_rows = len(arrow_table)
    print(f"Successfully processed {input_rows} raw records into {output_rows} unique records.")

    # Load or Create the Iceberg Table in Glue
    table_identifier = "silver.users"
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        print(f"Table {table_identifier} not found. Initializing new Iceberg table.")
        schema = Schema(
            NestedField(field_id=1, name="user_id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="age", field_type=IntegerType(), required=False),
            NestedField(field_id=3, name="income", field_type=DoubleType(), required=False),
            NestedField(field_id=4, name="partner", field_type=StringType(), required=False),
            NestedField(field_id=5, name="ingest_ts", field_type=TimestampType(), required=False)
        )
        table = catalog.create_table(table_identifier, schema=schema)

    # Upsert data into Silver Layer
    print(f"Upserting data into {table_identifier}...")
    table.upsert(arrow_table)
    print("Silver layer update successful.")

if __name__ == "__main__":
    main()