import os
import datetime
import duckdb
from pyiceberg.catalog import load_catalog

def main():
    # Setup AWS credentials
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')

    # Calculate current day's path for the Bronze layer extraction
    now = datetime.datetime.now(datetime.timezone.utc)
    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    s3_path = f"s3://ewallet-storage/bronze/partner=*/table=transactions/{year}/{month}/{day}/*.parquet"

    # Initialize pyiceberg catalog using AWS Glue and S3 warehouse
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
        # Get input row counts for Kestra logging
        input_count_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}', hive_partitioning=1)"
        input_rows = con.execute(input_count_query).fetchone()[0]
        
        if input_rows == 0:
            print(f"No records found in path: {s3_path}. Exiting gracefully.")
            return

    except Exception as e:
        print(f"No files found or error reading from path: {s3_path}. Error: {e}")
        return

    # Batch of Truth query: cast to proper types, enrich, deduplicate, filter for rn = 1
    query = f"""
        WITH deduped AS (
            SELECT 
                TRY_CAST(transaction_id AS VARCHAR) AS transaction_id,
                TRY_CAST(user_id AS BIGINT) AS user_id,
                TRY_CAST(card_id AS BIGINT) AS card_id,
                TRY_CAST(amount AS DECIMAL(18,2)) AS amount,
                TRY_CAST(status AS VARCHAR) AS status,
                TRY_CAST(transaction_time AS TIMESTAMP) AS transaction_time,
                TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
                TRY_CAST(use_chip AS VARCHAR) AS use_chip,
                TRY_CAST(merchant_id AS BIGINT) AS merchant_id,
                TRY_CAST(merchant_name AS VARCHAR) AS merchant_name,
                TRY_CAST(mcc AS BIGINT) AS mcc,
                TRY_CAST(risk_score AS DOUBLE) AS risk_score,
                TRY_CAST(source_partner AS VARCHAR) AS partner,
                TRY_CAST(amount AS DECIMAL(18,2)) * 4.70 AS amount_myr,
                date_diff('second', TRY_CAST(transaction_time AS TIMESTAMP), TRY_CAST(updated_at AS TIMESTAMP)) AS processing_latency,
                TRY_CAST(ingest_ts AS TIMESTAMP) AS ingest_ts,
                ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY ingest_ts DESC) as rn
            FROM read_parquet('{s3_path}', hive_partitioning=1)
        )
        SELECT * EXCLUDE (rn) 
        FROM deduped 
        WHERE rn = 1
    """

    # Convert the result to a PyArrow table
    arrow_table = con.execute(query).arrow()
    output_rows = len(arrow_table)

    print(f"Input records: {input_rows}")
    print(f"Deduplicated Output records: {output_rows}")

    # Load Iceberg table from Glue catalog
    table = catalog.load_table("silver.transactions")

    # Use table.upsert() to merge into the silver.transactions Iceberg table
    table.upsert(arrow_table)

if __name__ == "__main__":
    main()
