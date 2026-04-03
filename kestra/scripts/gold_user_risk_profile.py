import os
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, 
    TimestampType, 
    DoubleType, 
    BooleanType, 
    NestedField
)

def main():
    # Setup AWS credentials
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')

    # Initialize pyiceberg catalog
    catalog = load_catalog(
        "glue_catalog",
        **{
            "type": "glue",
            "s3.region": aws_region,
            "s3.access-key-id": aws_access_key,
            "s3.secret-access-key": aws_secret_key,
            "warehouse": "s3://ewallet-storage/gold/tables"
        }
    )

    print("Fetching Silver tables...")
    users_table = catalog.load_table("silver.users")
    cards_table = catalog.load_table("silver.cards")
    transactions_table = catalog.load_table("silver.transactions")

    print("Converting to Arrow...")
    users_arrow = users_table.scan().to_arrow()
    cards_arrow = cards_table.scan().to_arrow()
    transactions_arrow = transactions_table.scan().to_arrow()

    print("Executing Gold transformation logic in DuckDB...")
    con = duckdb.connect()
    con.register("users", users_arrow)
    con.register("cards", cards_arrow)
    con.register("transactions", transactions_arrow)

    query = """
        WITH tx_agg AS (
            SELECT 
                user_id,
                SUM(CASE WHEN status = 'SUCCESS' AND transaction_time >= CAST(NOW() - INTERVAL 7 DAYS AS TIMESTAMP) THEN CAST(amount AS DOUBLE) ELSE 0 END) AS total_spend_7d,
                AVG(CASE WHEN status = 'SUCCESS' AND transaction_time >= CAST(NOW() - INTERVAL 7 DAYS AS TIMESTAMP) THEN CAST(amount AS DOUBLE) ELSE NULL END) AS avg_spend_7d,
                COUNT(CASE WHEN status = 'FAILED' AND transaction_time >= CAST(NOW() - INTERVAL 7 DAYS AS TIMESTAMP) THEN 1 ELSE NULL END) AS failed_tx_7d,
                MAX(CAST(risk_score AS DOUBLE)) AS max_risk_score
            FROM transactions
            WHERE user_id IS NOT NULL
            GROUP BY user_id
        ),
        user_cards AS (
            SELECT DISTINCT t.user_id, t.card_id
            FROM transactions t
            WHERE t.user_id IS NOT NULL AND t.card_id IS NOT NULL
        ),
        compromised_users AS (
            SELECT DISTINCT uc.user_id
            FROM user_cards uc
            JOIN cards c ON uc.card_id = c.card_id
            WHERE c.is_compromised = true
        ),
        gold_profile AS (
            SELECT
                TRY_CAST(u.user_id AS BIGINT) AS user_id,
                COALESCE(t.total_spend_7d, 0.0) AS total_spend_7d,
                t.avg_spend_7d AS avg_spend_7d,
                CASE 
                    WHEN cu.user_id IS NOT NULL THEN true
                    WHEN t.failed_tx_7d > 3 THEN true
                    WHEN t.max_risk_score > 0.8 THEN true
                    ELSE false
                END AS high_risk,
                CAST(NOW() AS TIMESTAMP) AS profile_updated_at
            FROM users u
            LEFT JOIN tx_agg t ON u.user_id = t.user_id
            LEFT JOIN compromised_users cu ON u.user_id = cu.user_id
        )
        SELECT * FROM gold_profile
    """

    gold_arrow = con.execute(query).arrow()

    # Enforce schema constraints for Iceberg identifier
    user_id_idx = gold_arrow.schema.get_field_index("user_id")
    new_field = gold_arrow.schema.field(user_id_idx).with_nullable(False)
    updated_schema = gold_arrow.schema.set(user_id_idx, new_field)
    gold_arrow = gold_arrow.cast(updated_schema)

    output_rows = len(gold_arrow)
    print(f"Processed {output_rows} User Risk Profiles.")

    # Self-Healing Namespace Check
    try:
        catalog.load_namespace_properties('gold')
        print("INFO: Namespace 'gold' verified/created.")
    except NoSuchNamespaceError:
        catalog.create_namespace('gold')
        print("INFO: Namespace 'gold' verified/created.")

    # Load or Create Table
    table_identifier = "gold.user_risk_profile"
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        print(f"Initializing {table_identifier} with identifier field...")
        schema = Schema(
            NestedField(field_id=1, name="user_id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="total_spend_7d", field_type=DoubleType(), required=False),
            NestedField(field_id=3, name="avg_spend_7d", field_type=DoubleType(), required=False),
            NestedField(field_id=4, name="high_risk", field_type=BooleanType(), required=False),
            NestedField(field_id=5, name="profile_updated_at", field_type=TimestampType(), required=False),
            identifier_field_ids=[1]
        )
        table = catalog.create_table(table_identifier, schema=schema)

    # Full Refresh Upsert via Overwrite
    print(f"Overwriting {table_identifier}...")
    table.overwrite(gold_arrow)
    print("Gold User Risk Profile layer update successful.")

if __name__ == "__main__":
    main()
