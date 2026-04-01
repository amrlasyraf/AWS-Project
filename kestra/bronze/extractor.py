import argparse
import os
import psycopg2
import csv
import sys
from datetime import datetime

def main():
    """
    Extracts data from a PostgreSQL table based on a watermark and saves it to a CSV.
    Prints the maximum updated_at value to stdout for Kestra state management.
    """
    parser = argparse.ArgumentParser(description='Extract data from RDS to CSV')
    parser.add_argument('--table', required=True, help='Table name to extract')
    parser.add_argument('--partner', required=True, help='Partner name')
    parser.add_argument('--watermark', required=True, help='Watermark timestamp')
    args = parser.parse_args()

    # DB Connection params from environment variables
    # The prompt explicitly mentioned DB_HOST and DB_PASSWORD
    db_host = os.environ.get('DB_HOST')
    db_password = os.environ.get('DB_PASSWORD')
    # Defaulting others if not provided, though they are usually required
    db_user = os.environ.get('DB_USER', 'postgres')
    db_name = os.environ.get('DB_NAME', 'postgres')
    db_port = os.environ.get('DB_PORT', '5432')

    if not db_host or not db_password:
        print("Error: DB_HOST and DB_PASSWORD environment variables must be set.", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            port=db_port
        )
        cursor = conn.cursor()

        # Execute extraction query as requested
        # Note: Casting transaction_id and user_id to TEXT to ensure consistency in the CSV
        query = f"SELECT *, transaction_id::TEXT, user_id::TEXT FROM {args.table} WHERE updated_at > '{args.watermark}'"
        
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        # Save to extract.csv
        with open('extract.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            if rows:
                writer.writerows(rows)

        # Logic to find and print the maximum updated_at for Kestra watermark capture
        if rows:
            try:
                updated_at_idx = columns.index('updated_at')
                max_updated_at = max(row[updated_at_idx] for row in rows)
                
                # Format datetime to string if necessary
                if isinstance(max_updated_at, datetime):
                    print(f"NEW_WATERMARK: {max_updated_at.strftime('%Y-%m-%d %H:%M:%S.%f')}")
                else:
                    print(f"NEW_WATERMARK: {max_updated_at}")
            except (ValueError, IndexError):
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
