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
    DoubleType, 
    DecimalType,
    NestedField
)

def main():
    # Setup AWS credentials
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')

    # Path for Bronze Transactions layer
    now = datetime.datetime.now(datetime.timezone.utc)
    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    s3_path = f"s3://ewallet-storage/bronze/partner=*/table=transactions/{year}/{month}/{day}/*.parquet"

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
            print("No transaction records found. Exiting.")
            return
    except Exception as e:
        print(f"S3 Access Error: {e}")
        return

    # Transformation Query: Using 'clean' aliases and naive TIMESTAMP casting
    query = f"""
        WITH deduped AS (
            SELECT 
                TRY_CAST(transaction_id AS VARCHAR) AS transaction_id,
                TRY_CAST(user_id AS BIGINT) AS user_id_clean,
                TRY_CAST(card_id AS BIGINT) AS card_id_clean,
                TRY_CAST(amount AS DECIMAL(18,2)) AS amount_clean,
                TRY_CAST(status AS VARCHAR) AS status_clean,
                CAST(TRY_CAST(transaction_time AS TIMESTAMP) AS TIMESTAMP) AS tx_time_clean,
                TRY_CAST(use_chip AS VARCHAR) AS use_chip_clean,
                TRY_CAST(merchant_id AS BIGINT) AS merchant_id_clean,
                TRY_CAST(merchant_name AS VARCHAR) AS merchant_name_clean,
                TRY_CAST(mcc AS BIGINT) AS mcc_clean,
                TRY_CAST(risk_score AS DOUBLE) AS risk_score_clean,
                TRY_CAST(source_partner AS VARCHAR) AS partner_clean,
                TRY_CAST(amount AS DECIMAL(18,2)) * 4.70 AS amount_myr_clean,
                CAST(NOW() AS TIMESTAMP) AS ingest_ts_clean,
                ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_id) as rn
            FROM read_parquet('{s3_path}', hive_partitioning=1)
        )
        SELECT 
            transaction_id,
            user_id_clean AS user_id,
            card_id_clean AS card_id,
            amount_clean AS amount,
            status_clean AS status,
            tx_time_clean AS transaction_time,
            use_chip_clean AS use_chip,
            merchant_id_clean AS merchant_id,
            merchant_name_clean AS merchant_name,
            mcc_clean AS mcc,
            risk_score_clean AS risk_score,
            partner_clean AS partner,
            amount_myr_clean AS amount_myr,
            ingest_ts_clean AS ingest_ts
        FROM deduped 
        WHERE rn = 1
    """

    # Create Arrow Table
    arrow_table = con.execute(query).arrow()
    
    # FIX: Force transaction_id to be non-nullable for Iceberg Identifier field requirements
    tx_id_idx = arrow_table.schema.get_field_index("transaction_id")
    new_field = arrow_table.schema.field(tx_id_idx).with_nullable(False)
    updated_schema = arrow_table.schema.set(tx_id_idx, new_field)
    arrow_table = arrow_table.cast(updated_schema)

    output_rows = len(arrow_table)
    print(f"Processed {input_rows} raw to {output_rows} unique transaction records.")

    # Load or Create Table
    table_identifier = "silver.transactions"
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        print(f"Initializing {table_identifier} with identifier field...")
        schema = Schema(
            NestedField(field_id=1, name="transaction_id", field_type=StringType(), required=True),
            NestedField(field_id=2, name="user_id", field_type=LongType(), required=False),
            NestedField(field_id=3, name="card_id", field_type=LongType(), required=False),
            NestedField(field_id=4, name="amount", field_type=DecimalType(18, 2), required=False),
            NestedField(field_id=5, name="status", field_type=StringType(), required=False),
            NestedField(field_id=6, name="transaction_time", field_type=TimestampType(), required=False),
            NestedField(field_id=7, name="use_chip", field_type=StringType(), required=False),
            NestedField(field_id=8, name="merchant_id", field_type=LongType(), required=False),
            NestedField(field_id=9, name="merchant_name", field_type=StringType(), required=False),
            NestedField(field_id=10, name="mcc", field_type=LongType(), required=False),
            NestedField(field_id=11, name="risk_score", field_type=DoubleType(), required=False),
            NestedField(field_id=12, name="partner", field_type=StringType(), required=False),
            NestedField(field_id=13, name="amount_myr", field_type=DecimalType(18, 2), required=False),
            NestedField(field_id=14, name="ingest_ts", field_type=TimestampType(), required=False),
            identifier_field_ids=[1]
        )
        table = catalog.create_table(table_identifier, schema=schema)

    # Final Upsert
    print(f"Upserting into {table_identifier}...")
    table.upsert(arrow_table)
    print("Silver Transactions layer update successful.")

if __name__ == "__main__":
    main()