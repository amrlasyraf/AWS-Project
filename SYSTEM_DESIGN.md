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

> [!NOTE]
> The original monolithic `wallet_data_pipeline.yaml` has been officially deprecated and removed from the repository. All orchestration is now handled via the Enterprise Master-Worker (Subflow) pattern.

### 🔌 Enterprise Orchestration (**Status**: `Verified`)
To ensure robustness and scalability, the pipeline follows the **Master-Worker (Subflow)** orchestration pattern using Kestra:
- **Lead Orchestrator**: Acts as the central controller, managing active partner lists and coordinating execution across workers.
- **Worker Bronze**: Parametrized flow for siloed partner data extraction into S3.
- **Worker Silver (Cleanse & Merge)**: Dedicated flows for multi-stage data transformation.

This separation of concerns allows for isolated failure modes—if Partner A's extraction fails, it does not impede the extraction for Partner B or subsequent transformation steps.

### 🛠️ Defensive "All-String" Bronze Extraction
A critical defensive measure at the **Bronze Layer** is the manual casting of **EVERY** column to **STRING (TEXT/VARCHAR)** during the initial extraction from RDS.
- **Goal**: Prevent pipeline failures caused by schema drift or datatype inconsistencies between Partner A and Partner B (e.g. `amount` as `INT` vs `NUMERIC`).
- **Resolution**: Casting back to correct types (Decimal, Date, etc.) is handled downstream in the **Silver 1 (Cleanse)** layer using `try_cast` logic.

### 💾 Stateful Incremental CDC (KV Store)
Project Shield-Stream uses **Kestra's internal Key-Value (KV) Store** to manage stateful, idempotent incremental syncs (CDC).
- **Strategy**: Instead of relying on brittle "schedule-based time windows" (start/end dates), each worker subflow maintains its own `watermark_{partner}_{table}` pointer.
- **Resilience**: This ensures that even if a scheduled execution is skipped or fails, the next successful run will pick up from the exact last timestamp successfully persisted to S3.
- **Idempotency**: By updating the watermark **only after** a successful S3 upload, we guarantee zero data loss without requiring complex time-range overhead in the master orchestrator.

---

## 🔄 The Medallion Flow

### 🧱 Bronze (The Vault)
**Status**: `In-Progress`  
**Storage**: `s3://{{vars.s3_bucket}}/bronze/`  
**Logic**: 
- **Python-Driven Extraction**: Parallel extraction from PostgreSQL RDS siloed tables (`transactions`, `users`, `cards`) using the standardized local pathing: `python kestra/bronze/extractor.py`. This ensures architectural consistency with the modular repository structure.
- **Data Persistence**: Extracted data is converted to **Snappy-compressed Parquet** format.
- **Storage Strategy**: Folders are structured using **Hive Partitioning**: `bronze/partner={id}/{table}/hour={H}/data.parquet`.

### 🥈 Silver (The Big Wallet)
**Storage**: `s3://{{vars.s3_bucket}}/silver/unified/`  
**Logic**:
- **Cross-Partner Unification**: Performs a `UNION ALL` across all discovered partner partitions via DuckDB to create three distinct unified entities:
    - `unified_transactions`
    - `unified_users`
    - `unified_cards`
- **Metadata Injection**: Injects an explicit `partner_id` column during the unification process to ensure global traceability across the medallion flow.
- **Business Enrichment**:
    - **Currency Normalization**: All transaction amounts are converted to MYR (Exchange Rate: 4.70).
    - **Latency Profiling**: Calculates `processing_latency` (in seconds) between `transaction_time` and `updated_at` to detect gateway delays.

### 🥇 Gold (The Showcase)
**Access**: AWS Athena / BI (Tableau/Power BI)  
**Logic**:
- Exposes a unified External Table (`risk_db.unified_wallet_data`) for the Risk Department.
- Optimized for analytical queries, allowing for rapid slicing by `partner`, `mcc`, or `risk_score`.

---

## 🚨 Monitoring & Alerting (**Status**: `Live`)
To ensure high availability and rapid incident response, Project Shield-Stream implements **Mandatory SNS Alerting** across all orchestration layers:
- **Scope**: Every flow (Master and Worker) contains a root-level `errors` block.
- **Mechanism**: Utilizes `io.kestra.plugin.aws.sns.Publish` to push failure notifications to a centralized SNS Topic (`arn:aws:sns:ap-southeast-1:400953388228:Kestra_Alerts`).
- **Plugin Syntax**: The `io.kestra.plugin.aws.sns.Publish` plugin specifically requires the `from` field to populate the SNS Subject line and the `body` field for the message content.
- **Payload**: Alerts include the Flow ID, Execution ID, Namespace, and a direct deep-link to the Kestra execution logs for immediate debugging.

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
- **Sync Mechanism**: The `github_sync.yaml` mechanism uses the `SyncNamespaceFiles` plugin with `gitDirectory: kestra/` to pull the modular repository structure directly into the `my.project` namespace for automated execution.
