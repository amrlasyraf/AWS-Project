-- Athena DDL to create the Silver Unified Wallet Data table
-- Note: Replace <your-bucket-name> with the actual S3 bucket name

CREATE EXTERNAL TABLE IF NOT EXISTS risk_db.unified_wallet_data (
    transaction_id STRING,
    user_id INT,
    card_id INT,
    amount DOUBLE,
    status STRING,
    transaction_time TIMESTAMP,
    updated_at TIMESTAMP,
    use_chip STRING,
    merchant_id INT,
    merchant_name STRING,
    mcc INT,
    risk_score DOUBLE,
    partner STRING, -- Dynamically discovered via Hive partitioning
    current_age INT,
    yearly_income DOUBLE,
    credit_score INT,
    card_brand STRING,
    credit_limit DOUBLE,
    card_on_dark_web BOOLEAN,
    amount_myr DOUBLE,
    processing_latency BIGINT
)
STORED AS PARQUET
LOCATION 's3://<your-bucket-name>/silver/unified_wallet_data/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
