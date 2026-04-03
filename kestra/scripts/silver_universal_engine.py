import os
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import warnings

def main():
    # 1. Get Metadata from Kestra Env Vars
    TABLE = os.environ.get('TABLE_NAME')
    PK = os.environ.get('PRIMARY_KEY')
    
    print(f"INFO: Starting Dynamic Processing for: {TABLE}")

    # 2. Initialize DuckDB & Load Bronze
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    
    con.execute(f"SET s3_region='{os.environ.get('AWS_DEFAULT_REGION')}';")
    con.execute(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID')}';")
    con.execute(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY')}';")
    
    # 3. Dynamic Path Construction for Hive Partitioning
    # This automatically searches through all partners and date folders
    s3_wildcard_path = f"s3://ewallet-storage/bronze/*/table={TABLE}/**/*.parquet"
    
    # Enable hive_partitioning=true to let DuckDB parse the folder structure correctly
    sql = f"""
        SELECT * FROM read_parquet('{s3_wildcard_path}', hive_partitioning=true)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY {PK} ORDER BY updated_at DESC) = 1
    """
    arrow_table = con.execute(sql).arrow()

    # 4. Iceberg Write (The "Universal" Handshake)
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