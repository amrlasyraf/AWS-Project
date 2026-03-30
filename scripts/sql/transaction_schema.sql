-- Transaction Table Schema (Matches Refined Lambda Producer)
-- Focus: 12 Core and Metadata Columns for E-Wallet Simulation

DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    transaction_id UUID NOT NULL,
    user_id INT NOT NULL,
    card_id INT NOT NULL,
    amount NUMERIC(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    use_chip VARCHAR(10),
    merchant_id INT,
    merchant_name VARCHAR(100),
    mcc INT,
    risk_score NUMERIC(3, 2),

    -- Primary Key: Allows 3 records per transaction_id (one per status)
    PRIMARY KEY (transaction_id, status)
);

CREATE INDEX idx_trans_id ON transactions (transaction_id);
CREATE INDEX idx_user_id ON transactions (user_id);
CREATE INDEX idx_time ON transactions (transaction_time);
