# Pipeline Improvements and Optimizations

> [!NOTE]
> Whenever an improvement is implemented, the **Current Stack / Implementation** section for that flow must be overwritten to reflect the newly updated architecture.

---

## 1. Bronze Ingestion Flow

### Current Stack / Implementation
The system currently pulls data from three main sources (transactions, users, and cards) one by one. Every time it runs, it has to set up its own tools from scratch, which adds extra waiting time to the process.

### Identified Improvements
- **Instant Setup**: Create a "ready-to-go" toolbox so the system doesn't waste time setting up its tools every time it starts.
- **Multi-Tasking**: Update the system to process data from different sources at the same time, significantly speeding up the whole operation.
- **Automatic Discovery**: Teach the system to automatically find new data tables without us having to manually update the code.
- **Smart File Sorting (Completed)**: We have upgraded the system to automatically sort files by **Year**, **Month**, **Day**, and **Hour** for much faster retrieval and lower search costs.

### Status
Pending

---

## 2. Data Observability & Anomaly Detection — Bronze Layer

### Identified Improvements
- **Row Count Deviation Check**: Implement a row count deviation check during the extraction phase. The pipeline should calculate the monthly average of extracted rows per session. If a new extraction session deviates from this average by more than 20%, it should trigger an AWS SNS alert to warn the team of potential source system anomalies, such as missed records or a spike in source data.

### Status
Pending

---

## 3. Silver Layer Compute Engine

### Current Stack / Implementation
The Silver layer currently operates using a unified **Python, DuckDB, and Apache Iceberg** stack with AWS Glue as the catalog. DuckDB handles robust batch transformation execution directly from Bronze Parquet, and the results are upserted into an Iceberg table to enable ACID transactions, time-travel queries, and automated partition management.

### Identified Improvements
- **Data Quality Quarantine**: Implement a "Data Quality Quarantine" process for rows that fail `TRY_CAST` validation during the 'Batch of Truth' querying phase. Malformed source entries that evaluate to NULL should be redirected to a dedicated Quarantine area instead of being silently skipped or uploaded as NULLs, allowing data engineers to review broken records.
- **Migrate to PySpark**: While DuckDB is effective for current data volumes, the planned upgrade path is to migrate the Silver layer compute engine to **PySpark**. This migration will unlock:
    - **Distributed Processing**: PySpark scales horizontally across a cluster (e.g. AWS EMR or Glue), enabling the pipeline to handle significantly larger data volumes without bottlenecks.
    - **Native Apache Iceberg Integration**: PySpark provides first-class support for the **Apache Iceberg** open table format, enabling ACID transactions, time-travel queries, schema evolution, and efficient partition management directly on S3 — capabilities that are critical for a production-grade Silver layer at scale.

### Status
Pending
