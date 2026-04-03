# Pipeline Improvements and Optimizations

> [!NOTE]
> Whenever an improvement is implemented, the **Current Stack / Implementation** section for that flow must be overwritten to reflect the newly updated architecture.

---

## 1. Bronze Ingestion Flow

### Current Stack / Implementation
The flow is now fully parallelized using Kestra, pulling data from multiple partners simultaneously and utilizing date-partitioned S3 paths (/year/month/day/) for optimized retrieval.

### Identified Improvements
- **Instant Setup**: Create a "ready-to-go" toolbox so the system doesn't waste time setting up its tools every time it starts.
- **Automatic Discovery**: Teach the system to automatically find new data tables without us having to manually update the code.

### Completed Improvements
- **Multi-Tasking**: Update the system to process data from different sources at the same time, significantly speeding up the whole operation.
- **Smart File Sorting**: We have upgraded the system to automatically sort files by **Year**, **Month**, **Day**, and **Hour** for much faster retrieval and lower search costs.

### Status
In Progress

---

## 2. Data Observability & Anomaly Detection — Bronze Layer

### Identified Improvements
- **Row Count Deviation Check**: Implement a row count deviation check during the extraction phase. The pipeline should calculate the monthly average of extracted rows per session. If a new extraction session deviates from this average by more than 20%, it should trigger an AWS SNS alert to warn the team of potential source system anomalies, such as missed records or a spike in source data.

### Status
In Progress (SNS alerting blocks are now integrated into the Kestra error handlers.)

---

## 3. Silver Layer Compute Engine

### Current Stack / Implementation
- **Silver Parity & Orchestration**: The pipeline is now fully parallelized across all domain ingestion and transformation tasks. Successfully deployed three separate domain tables with 100% stable DuckDB-to-Iceberg upserts.
- **Silver Compute**: We have stabilized a stack using Python 3.11-slim, DuckDB, and PyIceberg. The system now handles ACID upserts into AWS Glue with rigorous schema fixes: implemented rounding for Decimals, naive timestamp casting, and forced non-nullability for PyArrow identifier fields to satisfy strict Iceberg constraints.
- **Automation**: Table initialization is now 'self-healing,' automatically creating Iceberg tables with correct identifier field IDs if they are missing from the catalog.

### Identified Improvements
- **Custom Containerization**: Create a pre-built Docker image with DuckDB and PyIceberg pre-installed to eliminate the apt-get and pip install overhead in the beforeCommands.
- **Data Quality Quarantine**: Implement a "Data Quality Quarantine" process for rows that fail `TRY_CAST` validation during the 'Batch of Truth' querying phase. Malformed source entries that evaluate to NULL should be redirected to a dedicated Quarantine area instead of being silently skipped or uploaded as NULLs, allowing data engineers to review broken records.
- **Migrate to PySpark**: While DuckDB is effective for current data volumes, the planned upgrade path is to migrate the Silver layer compute engine to **PySpark**. This migration will unlock:
    - **Distributed Processing**: PySpark scales horizontally across a cluster (e.g. AWS EMR or Glue), enabling the pipeline to handle significantly larger data volumes without bottlenecks.
    - **Native Apache Iceberg Integration**: PySpark provides first-class support for the **Apache Iceberg** open table format, enabling ACID transactions, time-travel queries, schema evolution, and efficient partition management directly on S3 — capabilities that are critical for a production-grade Silver layer at scale.

### Completed Improvements
- **Type Parity Middleware & Schema Fixes**: Implemented explicit casting, rounding for Decimals (`DECIMAL(18,2)`), naive timestamp casting, and forced non-nullability for PyArrow identifier fields to successfully prevent conversion failures.

### Status
Complete (Silver Parity achieved)

---

## 4. Project Roadmap: The Next Phase (Gold & Scale)

### 4.1 Gold Layer Implementation
- **Unified Customer Table**: Draft a logic flow for a "Unified Customer" table. This will join the distinct domain tables using DuckDB to calculate aggregate metrics such as total spend, average transaction value, and consolidated risk profiles.

### 4.2 Metadata-Driven Refactor
- **Generic Templating**: Transition from multiple individual flows to a single generic template that utilizes a loop and a table registry to handle 100+ tables dynamically, drastically reducing engineering overhead.

### 4.3 dbt Integration
- **dbt Core for Gold Layer**: Plan the introduction of dbt Core specifically dedicated to the Gold layer. This will manage SQL lineage, documentation, and automated data quality testing.

### 4.4 Data Governance
- **DataHub Deployment**: Outline the steps to deploy DataHub to visualize metadata, trace data lineage, and ensure data governance for cross-functional stakeholders.

---

## Phase 3: Analytics Engineering

### Identified Improvements
- **Migrate Gold Layer logic to dbt**: We currently manage the Gold Layer transformation using Python scripts (`gold_user_risk_profile.py`). The next planned enhancement is migrating this layer to **dbt Core** utilizing the `dbt-duckdb` adapter. 
  - This refactor will unlock **Automated Data Lineage** (via dbt bounds/refs), **Schema Testing** (e.g., actively asserting `user_id` is unique and non-null), and **Auto-generated Documentation** ensuring the business dictionary is accessible to all stakeholders.
