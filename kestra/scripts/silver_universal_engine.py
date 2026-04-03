import os
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
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
    s3_wildcard_path = f"s3://ewallet-storage/bronze/*/table={TABLE}/**/*.parquet"
    
    sql = f"""
        SELECT * FROM read_parquet('{s3_wildcard_path}', hive_partitioning=true)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY {PK} ORDER BY updated_at DESC) = 1
    """
    arrow_table = con.execute(sql).arrow()

    # 4. Iceberg Write (The "Universal" Handshake)
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    table_identifier = f"silver.{TABLE}"
    
    # Explicitly define where new tables should live in S3
    s3_location = f"s3://ewallet-storage/silver/{TABLE}"
    
    try:
        iceberg_table = catalog.load_table(table_identifier)
        print(f"INFO: Upserting into {table_identifier}...")
        
        # CRITICAL FIX 1: Auto-evolve the Iceberg schema to accept new columns
        with iceberg_table.update_schema() as update:
            update.union_by_name(arrow_table.schema)
            
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            iceberg_table.overwrite(arrow_table)
            
    except NoSuchTableError:
        # Better error handling: Only create if it actually doesn't exist
        print(f"INFO: Creating new table {table_identifier}...")
        
        # CRITICAL FIX 2: Pass the explicit S3 location to Glue
        catalog.create_table(
            identifier=table_identifier, 
            schema=arrow_table.schema,
            location=s3_location
        )

    print(f"INFO: {TABLE} processing complete.")

if __name__ == "__main__":
    main()