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

## 🔐 Secure Credential Management (**Status**: `Production-Ready`)
To meet enterprise security standards for Project Shield-Stream, database credentials are managed through a **Zero-Governance** model:
- **No Internal Storage**: RDS credentials (host, user, password, dbname) are **NEVER** stored in Kestra’s internal Key-Value (KV) store or as flow variables.
- **Dynamic Secret Injection**:
    - **Cloud Native**: Kestra passes temporary AWS IAM credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) from the centralized KV store to the isolated Docker container.
    - **IAM Policy**: The Kestra AWS IAM user must have the `secretsmanager:GetSecretValue` policy attached to fetch database credentials at runtime.
    - **Runtime Retrieval**: The Python extraction script (`extractor.py`) utilizes the `boto3` client to dynamically fetch the production RDS credentials directly from **AWS Secrets Manager** (`shield-stream/bronze/db-credentials`) during execution.
- **Isolation**: This ensures that sensitive database access remains within the AWS security perimeter, while Kestra acts only as a secure orchestrator.

---

## 🔄 The Medallion Flow

### 🧱 Bronze (The Vault)
**Status**: `In-Progress`  
**Storage**: `s3://{{vars.s3_bucket}}/bronze/`  
**Logic**: 
- **Automated Data Pulls**: Efficiently collects data from multiple partner databases at once.
- **Improved Reliability**: Uses a secure method to transfer data between tools, ensuring critical information is never missed.
- **Smart "No Data" Handling**: If there’s no new information from a partner, the system intelligently skips the step instead of failing.
- **Cost-Effective Storage**: Saves data in a compressed format that speeds up analysis while lowering storage costs.
- **Smart Folder Organization**: Automatically sorts data into folders by **Year**, **Month**, **Day**, and **Hour** for easy retrieval.

### 🥈 Silver (The Big Wallet)
**Storage**: `s3://{{vars.s3_bucket}}/silver/unified/`  
**Logic**:
- **Unified Global View**: Joins data from all different partners into a single "Big Wallet" for easier analysis.
- **Automated Labeling**: Automatically adds tracking labels to every record so we always know exactly which partner provided the data.
- **Business Ready Features**:
    - **Global Currency**: Automatically converts all amounts into a single currency (MYR) for consistent reporting.
    - **Speed Checks**: Monitors how long transactions take so we can detect and fix any delays for our customers.

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
- **Reliability**:
    - **Looping**: Partner loops use a simplified **array of strings** (e.g., `["PARTNER_A", "PARTNER_B"]`) to eliminate object parsing overhead. Conditional logic (Ternary operators) is used to map partner-specific metadata like table suffixes.
    - **Localization**: The SNS Topic ARN variable is localized within each flow's `variables` block.
- **Plugin Syntax**: The production `io.kestra.plugin.aws.sns.Publish` plugin requires plain text messages to be formatted as an **array/list** within the `from` property to avoid "URI Scheme" and "Illegal character" validation errors.
- **Alert Format**: Failure alerts follow the array syntax: `from: ["FAILED: {{ flow.id }}. Execution: {{ execution.id }}"]`. All other message-carrying fields (subject, message, body, data) are omitted for compatibility.

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
