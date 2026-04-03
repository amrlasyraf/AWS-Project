import os
import json
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from datetime import datetime, timezone
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, StringType, LongType, NestedField

def main():
    # 1. Get Metadata
    payload_str = os.environ.get('TASK_PAYLOAD')
    if not payload_str:
        print("No payload found. Exiting audit.")
        return
        
    payload = json.loads(payload_str)
    TABLE = payload['table']
    PK = payload['pk']
    
    print(f"INFO: Starting Audit for: {TABLE}")

    # 2. Initialize DuckDB
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    
    con.execute(f"SET s3_region='{os.environ.get('AWS_DEFAULT_REGION')}';")
    con.execute(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID')}';")
    con.execute(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY')}';")
    
    # 3. Query Bronze Metrics
    s3_wildcard_path = f"s3://ewallet-storage/bronze/*/table={TABLE}/**/*.parquet"
    raw_stats_sql = f"""
        SELECT 
            COUNT(*) as bronze_raw_count,
            SUM(CASE WHEN {PK} IS NULL THEN 1 ELSE 0 END) as null_pk_count
        FROM read_parquet('{s3_wildcard_path}', hive_partitioning=true)
    """
    raw_stats = con.execute(raw_stats_sql).fetchone()
    bronze_raw_count = raw_stats[0]
    null_pk_count = raw_stats[1] or 0

    # 4. Query Silver Metrics via Iceberg
    silver_stats_sql = f"SELECT COUNT(*) FROM iceberg_scan('silver.{TABLE}', allow_moved_paths=true)"
    try:
        silver_deduped_count = con.execute(silver_stats_sql).fetchone()[0]
    except Exception as e:
        print(f"WARNING: Could not read Silver table. Setting count to 0. Error: {e}")
        silver_deduped_count = 0

    # 5. Write to Audit Table
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    
    audit_data = pa.table({
        'execution_time': pa.array([datetime.now(timezone.utc)], type=pa.timestamp('us', tz='UTC')),
        'table_name': pa.array([TABLE], type=pa.string()),
        'bronze_raw_count': pa.array([bronze_raw_count], type=pa.int64()),
        'silver_deduped_count': pa.array([silver_deduped_count], type=pa.int64()),
        'null_pk_count': pa.array([null_pk_count], type=pa.int64())
    })

    audit_table_identifier = "silver.pipeline_audit"
    audit_location = "s3://ewallet-storage/silver/pipeline_audit"

    try:
        audit_iceberg_table = catalog.load_table(audit_table_identifier)
        audit_iceberg_table.append(audit_data)
    except NoSuchTableError:
        print(f"INFO: Initializing audit table {audit_table_identifier}...")
        audit_schema = Schema(
            NestedField(field_id=1, name="execution_time", field_type=TimestampType(), required=True),
            NestedField(field_id=2, name="table_name", field_type=StringType(), required=True),
            NestedField(field_id=3, name="bronze_raw_count", field_type=LongType(), required=False),
            NestedField(field_id=4, name="silver_deduped_count", field_type=LongType(), required=False),
            NestedField(field_id=5, name="null_pk_count", field_type=LongType(), required=False)
        )
        audit_iceberg_table = catalog.create_table(
            identifier=audit_table_identifier,
            schema=audit_schema,
            location=audit_location
        )
        audit_iceberg_table.append(audit_data)

    print(f"INFO: Audit logged successfully for {TABLE}.")

if __name__ == "__main__":
    main()