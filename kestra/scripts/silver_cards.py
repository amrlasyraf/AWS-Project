import os
import datetime
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, 
    StringType, 
    TimestampType, 
    NestedField
)

def main():
    # Setup AWS credentials
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')

    # Path for Bronze Cards layer
    now = datetime.datetime.now(datetime.timezone.utc)
    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    s3_path = f"s3://ewallet-storage/bronze/partner=*/table=cards/{year}/{month}/{day}/*.parquet"

    # Initialize pyiceberg catalog
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

    # Initialize DuckDB
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{aws_region}';")
    con.execute(f"SET s3_access_key_id='{aws_access_key}';")
    con.execute(f"SET s3_secret_access_key='{aws_secret_key}';")

    try:
        input_count_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}', hive_partitioning=1)"
        input_rows = con.execute(input_count_query).fetchone()[0]
        if input_rows == 0:
            print("No card records found. Exiting.")
            return
    except Exception as e:
        print(f"S3 Access Error: {e}")
        return

    # SQL Transformation: Cast ingest_ts to naive TIMESTAMP and use distinct aliases
    query = f"""
        WITH deduped AS (
            SELECT 
                TRY_CAST(card_id AS BIGINT) AS card_id,
                TRY_CAST(user_id AS BIGINT) AS user_id_clean,
                TRY_CAST(card_status AS VARCHAR) AS status_clean,
                TRY_CAST(card_type AS VARCHAR) AS type_clean,
                TRY_CAST(source_partner AS VARCHAR) AS partner_clean,
                CAST(NOW() AS TIMESTAMP) AS ingest_ts_clean, 
                ROW_NUMBER() OVER(PARTITION BY card_id ORDER BY ingest_ts_clean DESC) as rn
            FROM read_parquet('{s3_path}', hive_partitioning=1)
        )
        SELECT 
            card_id,
            user_id_clean AS user_id,
            status_clean AS card_status,
            type_clean AS card_type,
            partner_clean AS partner,
            ingest_ts_clean AS ingest_ts
        FROM deduped 
        WHERE rn = 1
    """

    # Create Arrow Table
    arrow_table = con.execute(query).arrow()
    
    # Force card_id to be non-nullable to satisfy Iceberg identifier rules
    card_id_idx = arrow_table.schema.get_field_index("card_id")
    new_field = arrow_table.schema.field(card_id_idx).with_nullable(False)
    updated_schema = arrow_table.schema.set(card_id_idx, new_field)
    arrow_table = arrow_table.cast(updated_schema)

    output_rows = len(arrow_table)
    print(f"Processed {input_rows} raw to {output_rows} unique card records.")

    # Load or Create Table
    table_identifier = "silver.cards"
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        print(f"Initializing {table_identifier}...")
        schema = Schema(
            NestedField(field_id=1, name="card_id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="user_id", field_type=LongType(), required=False),
            NestedField(field_id=3, name="card_status", field_type=StringType(), required=False),
            NestedField(field_id=4, name="card_type", field_type=StringType(), required=False),
            NestedField(field_id=5, name="partner", field_type=StringType(), required=False),
            NestedField(field_id=6, name="ingest_ts", field_type=TimestampType(), required=False),
            identifier_field_ids=[1]
        )
        table = catalog.create_table(table_identifier, schema=schema)

    # Final Upsert
    print(f"Upserting into {table_identifier}...")
    table.upsert(arrow_table)
    print("Silver Cards layer update successful.")

if __name__ == "__main__":
    main()