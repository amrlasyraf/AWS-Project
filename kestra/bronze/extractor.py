import argparse
import os
import psycopg2
import pandas as pd
import sys
import boto3
import json
from datetime import datetime

def get_db_creds():
    """Fetches database credentials from AWS Secrets Manager."""
    secret_name = os.environ.get('AWS_SECRET_NAME', 'ProjectShield/RDS/Postgres')
    region_name = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')

    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Error fetching secret {secret_name}: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    """
    Extracts data from a PostgreSQL table based on a watermark and saves it to a Parquet file.
    Prints the maximum updated_at value to stdout for Kestra state management.
    """
    parser = argparse.ArgumentParser(description='Extract data from RDS to Parquet')
    parser.add_argument('--table', required=True, help='Table name to extract')
    parser.add_argument('--partner', required=True, help='Partner name')
    parser.add_argument('--watermark', required=True, help='Watermark timestamp')
    args = parser.parse_args()

    # Fetch DB credentials from AWS Secrets Manager
    db_creds = get_db_creds()
    
    db_host = db_creds.get('host')
    db_password = db_creds.get('password')
    db_user = db_creds.get('username', 'postgres')
    db_name = db_creds.get('dbname', 'postgres')
    db_port = db_creds.get('port', '5432')

    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            port=db_port
        )
        
        # Execute extraction query as requested
        # Note: Casting IDs to TEXT for consistency
        query = f"SELECT *, transaction_id::TEXT, user_id::TEXT FROM {args.table} WHERE updated_at > '{args.watermark}'"
        
        # Load into Pandas for Parquet conversion
        df = pd.read_sql_query(query, conn)

        if not df.empty:
            # Save to extract.parquet
            df.to_parquet('extract.parquet', index=False, compression='snappy')

            # Find and print the maximum updated_at for Kestra watermark capture
            try:
                max_updated_at = df['updated_at'].max()
                
                # Format datetime to string if necessary
                if isinstance(max_updated_at, (datetime, pd.Timestamp)):
                    print(f"NEW_WATERMARK: {max_updated_at.strftime('%Y-%m-%d %H:%M:%S.%f')}")
                else:
                    print(f"NEW_WATERMARK: {max_updated_at}")
            except (KeyError, ValueError):
                # Fallback to current watermark if updated_at is not found in results
                print(f"NEW_WATERMARK: {args.watermark}")
        else:
            # If no new data, print the existing watermark
            print(f"NEW_WATERMARK: {args.watermark}")

    except Exception as e:
        print(f"Error during extraction: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
