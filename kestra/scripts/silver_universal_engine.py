import os
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import warnings

def main():
    # 1. Get Metadata from Kestra Env Vars
    TABLE = os.environ.get('TABLE_NAME')
    PK = os.environ.get('PRIMARY_KEY')
    S3_SOURCE = os.environ.get('S3_SOURCE_PATH')
    
    print(f"INFO: Starting Dynamic Processing for: {TABLE}")

    # 2. Initialize DuckDB & Load Bronze
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # --- ADDED THESE TWO LINES TO FIX S3 403 FORBIDDEN ---
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    # -----------------------------------------------------
    
    # Universal Deduplication Logic
    # Qualify handles the 'latest record' logic regardless of schema
    sql = f"""
        SELECT * FROM read_parquet('{S3_SOURCE}*.parquet')
        QUALIFY ROW_NUMBER() OVER(PARTITION BY {PK} ORDER BY updated_at DESC) = 1
    """
    arrow_table = con.execute(sql).arrow()

    # 3. Iceberg Write (The "Universal" Handshake)
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    table_identifier = f"silver.{TABLE}"
    
    try:
        iceberg_table = catalog.load_table(table_identifier)
        print(f"INFO: Upserting into {table_identifier}...")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            iceberg_table.overwrite(arrow_table)
    except Exception:
        print(f"INFO: Creating new table {table_identifier}...")
        catalog.create_table(table_identifier, schema=arrow_table.schema)

    print(f"INFO: {TABLE} processing complete.")

if __name__ == "__main__":
    main()