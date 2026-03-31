# 🛡️ Project Shield-Stream: System Design Document
**Lead Data Engineer: Antigravity**

---

## 🏢 Business Context
**Department**: Merchant Risk & Fraud Operations  
**Objectives**: 
- Centralize high-volume e-wallet transaction feeds from multiple siloed partners.
- Provide a unified, enriched dataset for real-time risk scoring and fraud pattern detection.
- Minimize engineering overhead when onboarding new e-wallet partners.

---

## 🏗️ Modular Architecture
Project Shield-Stream implements a **Metadata-Driven** ingestion architecture, moving away from hardcoded logic to a scalable configuration model.

### 🔌 Multi-Partner Scalability
The system is designed to scale horizontally across any number of partners (A, B, C... N) with **Zero Code Changes** to the core transformation logic:
- **Lambda Producer**: Uses a centralized `PARTNERS_CONFIG` metadata dictionary to map partner IDs to their respective RDS table schemas.
- **Kestra Orchestration**: Employs dynamic `ForEach` task patterns to iterate through partner lists for extraction.
- **Auto-Discovery**: The Silver layer uses wildcard S3 path detection (`partner=*`) and Hive-style partitioning to automatically ingest new partner data as soon as it exists in the Bronze layer.

---

## 🔄 The Medallion Flow

### 🧱 Bronze (The Vault)
**Storage**: `s3://{{vars.s3_bucket}}/bronze/`  
**Logic**: 
- Parallel extraction from RDS siloed tables (`transactions`, `users`, `cards`).
- Data is persisted in **Snappy-compressed Parquet** format.
- Folders are structured using **Hive Partitioning**: `bronze/partner={id}/{table}/data.parquet`.

### 🥈 Silver (The Big Wallet)
**Storage**: `s3://{{vars.s3_bucket}}/silver/unified_wallet_data/`  
**Logic**:
- **Unified Merge**: Performs a `UNION ALL` across all discovered partner partitions via DuckDB.
- **Denormalization**: Joins transaction data with `users` and `cards` dimension tables using both `user_id` and the metadata-injected `partner` column.
- **Business Enrichment**:
    - **Currency Normalization**: All transaction amounts are converted to MYR (Exchange Rate: 4.70).
    - **Latency Profiling**: Calculates `processing_latency` (in seconds) between `transaction_time` and `updated_at` to detect gateway delays.

### 🥇 Gold (The Showcase)
**Access**: AWS Athena / BI (Tableau/Power BI)  
**Logic**:
- Exposes a unified External Table (`risk_db.unified_wallet_data`) for the Risk Department.
- Optimized for analytical queries, allowing for rapid slicing by `partner`, `mcc`, or `risk_score`.

---

## 📖 Data Dictionary

| Column | Type | Origin | Description |
|---|---|---|---|
| `transaction_id` | STRING (UUID) | Core | Unique identifier across all 3 lifecycle states. |
| `user_id` | INT | Core | Unique user identifier. |
| `card_id` | INT | Core | Unique card identifier. |
| `amount` | DOUBLE | Core | Raw transaction amount. |
| `status` | STRING | Core | Transaction state (PENDING, APPROVED, SUCCESS, FAILED). |
| `transaction_time`| TIMESTAMP | Core | Time of transaction initiation. |
| `updated_at` | TIMESTAMP | Core | Time of the latest state update. |
| `use_chip` | STRING | Core | Method of transaction (Swipe, Chip, Online). |
| `merchant_id` | INT | Core | Target merchant identifier. |
| `merchant_name` | STRING | Core | Display name of the merchant. |
| `mcc` | INT | Core | Merchant Category Code (1000–9999). |
| `risk_score` | DOUBLE | Core | ML-generated risk probability (0.01 to 0.99). |
| `partner` | STRING | Injected | **Metadata Partition**: Identifies the e-wallet partner source. |
| `amount_myr` | DOUBLE | Enriched | Amount converted to MYR (Rate: 4.70). |
| `processing_latency`| BIGINT | Enriched | Delay in seconds between initiation and update. |

---

## 🚀 CI/CD & Documentation Policy
This `SYSTEM_DESIGN.md` file serves as the **Single Source of Truth** for the Merchant Risk Department. 
- **Requirement**: This documentation MUST be updated automatically (as part of the Git Sync workflow) whenever a new e-wallet partner, table schema, or business transformation logic is introduced.
- **Consistency**: The `partners` list in Kestra variables and the `PARTNERS_CONFIG` in Python Lambda must always remain synchronized with the architectural overview defined here.
