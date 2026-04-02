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

### ⚙️ Pebble Templating: `parents` Array Index inside Nested Flowables

> [!IMPORTANT]
> **Kestra-Specific Gotcha**: When a task is nested inside an `If` flowable that is itself nested inside a `ForEach` flowable, the Pebble expression `taskrun.value` is **not directly accessible** from within the `If`'s `then` block. The correct syntax to retrieve the inherited loop variable is **`parents[0].taskrun.value`** — NOT `parents[1].taskrun.value`.

**Explanation of the `parents` array:**
- `parents[0]` → The **immediate parent** task/flowable. When inside an `If` block nested in `ForEach`, this resolves to the `ForEach` iteration context, giving you the current loop value (e.g., `"transactions"`, `"users"`, `"cards"`).
- `parents[1]` → The next ancestor up the chain. In a single-level `ForEach → If` nesting, index `1` is **out of bounds** and will cause a runtime error.

**Rule**: Count the levels of nesting from the perspective of the task itself. A task directly inside an `If` that is directly inside a `ForEach` is **one level deep** — use `parents[0]`.

**Applied in `bronze-ingestion.yaml`:**
```yaml
# Correct — resolves the ForEach loop value from within the If.then block
from:  "{{ outputs.python_extract[parents[0].taskrun.value].outputFiles['extract.parquet'] }}"
key:   "bronze/partner={{ inputs.partner_id }}/table={{ parents[0].taskrun.value }}/..."
value: "{{ outputs.python_extract[parents[0].taskrun.value].vars.new_watermark }}"
```

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
- **Smart Folder Organization**: Automatically sorts data into folders by **Year**, **Month**, and **Day** for easy retrieval. Partitioning is intentionally set to a **daily grain** — hourly partitioning was evaluated and removed to prevent excessive small-file generation, which degrades Athena query performance and increases S3 API costs.
- **Row-Level Metadata Injection**: Before writing to Parquet, `extractor.py` injects three metadata columns into every extracted DataFrame: `ingest_ts` (UTC timestamp of the extraction run), `source_partner` (the originating partner ID), and `batch_table` (the source table name). This makes every Parquet file **fully self-describing** — downstream consumers require zero S3 path parsing to determine data lineage. It also ensures **file independence** (each file can be processed or re-ingested in isolation) and **simplified deduplication** in Silver layer joins.

> [!NOTE]
> **Bronze Metadata Columns injected by `extractor.py`**
>
> | Column | Type | Description |
> |---|---|---|
> | `ingest_ts` | STRING (UTC timestamp) | Exact UTC datetime the extraction ran (`%Y-%m-%d %H:%M:%S`). |
> | `source_partner` | STRING | The `--partner` argument passed by Kestra (e.g. `PARTNER_A`). |
> | `batch_table` | STRING | The `--table` argument passed by Kestra (e.g. `transactions`). |

> [!NOTE]
> **S3 Partitioning Strategy — Daily Grain, Plain Directory Paths**  
> Bronze files are stored at the following path structure:  
> `s3://ewallet-storage/bronze/partner={PARTNER}/table={TABLE}/{YYYY}/{MM}/{DD}/data.parquet`  
> Date segments use **standard directory paths** (e.g., `/2026/04/02/`) rather than Hive-style key=value prefixes (e.g., `year=2026/month=04/day=02/`). This ensures compatibility with generic S3 tooling and avoids requiring Hive partition-awareness in downstream consumers. The `partner=` prefix is retained as a logical partition anchor for Athena `MSCK REPAIR TABLE` operations.  
> Hourly sub-partitions were deliberately removed. Multiple daily runs overwrite a single `data.parquet` per day rather than scattering data across hourly objects, keeping downstream Athena scans efficient.

> [!IMPORTANT]
> **Pebble Templating — Inline Date Functions, Not `vars.*` Wrappers**  
> The `year`, `month`, and `day` variables have been **removed** from the `variables:` block. Storing `{{ now() | date('...') }}` inside a `vars.*` entry causes a **recursive rendering issue**: Kestra evaluates the variable reference first and may resolve it to an empty or stale string before the inner `now()` call is executed. The correct pattern is to call the Pebble date function **directly** inside the property string that needs it:  
> ```yaml
> key: "bronze/partner={{ inputs.partner_id }}/table={{ parents[0].taskrun.value }}/{{ now() | date('yyyy') }}/{{ now() | date('MM') }}/{{ now() | date('dd') }}/data.parquet"
> ```  
> This guarantees the timestamp is resolved fresh at task runtime with no intermediate variable indirection.

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
| `ingest_ts` | STRING | Bronze Meta | UTC timestamp of the extraction run (`%Y-%m-%d %H:%M:%S`). |
| `source_partner` | STRING | Bronze Meta | Originating partner ID injected by `extractor.py` (e.g. `PARTNER_A`). |
| `batch_table` | STRING | Bronze Meta | Source table name injected by `extractor.py` (e.g. `transactions`). |

---

## 🚀 CI/CD & Documentation Policy
This `SYSTEM_DESIGN.md` file serves as the **Single Source of Truth** for the Merchant Risk Department. 
- **Requirement**: This documentation MUST be updated automatically (as part of the Git Sync workflow) whenever a new e-wallet partner, table schema, or business transformation logic is introduced.
- **Consistency**: The `partners` list in Kestra variables and the `PARTNERS_CONFIG` in Python Lambda must always remain synchronized with the architectural overview defined here.
- **Sync Mechanism**: The `github_sync.yaml` mechanism uses the `SyncNamespaceFiles` plugin with `gitDirectory: kestra/` to pull the modular repository structure directly into the `my.project` namespace for automated execution.
