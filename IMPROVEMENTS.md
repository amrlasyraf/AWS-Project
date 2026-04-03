# Project Shield-Stream: Architecture & Roadmap

> [!NOTE]
> This document tracks the evolution of the Medallion Architecture, summarizing completed milestones and outlining future technical debt and scalability initiatives.

---

## Phase 1: Completed Milestones (Stable v1.0)

### 1. Bronze Layer (Ingestion)
- **Parallel Orchestration**: Migrated to Kestra master-worker (Subflow) patterns for concurrent multi-partner data pulls.
- **Optimized Storage**: Enforced daily S3 path partitioning without over-fragmentation.

### 2. Silver Layer (Cleanse & Merge)
- **Metadata-Driven Framework**: Replaced hardcoded scripts with a dynamic `registry.json`. A single `silver_universal_engine.py` script now ingests configurations (`TASK_PAYLOAD`) to scale infinitely with zero new code.
- **Universal Deduplication**: Utilizes DuckDB `QUALIFY ROW_NUMBER() OVER(...) = 1` for consistent Upsert handling.
- **Auto-Schema Evolution**: Dynamically updates Iceberg tables via `update_schema()` during writes.

### 3. Gold Layer (Analytics)
- **Risk Profiling Engine**: Pre-aggregates 7-day spend velocities, failure rates, and multi-factor high-risk modeling for end-users.
- **Warning-Free Execution**: Integrated Python `warnings.catch_warnings()` for pristine log outputs.
- **Self-Healing Namespaces**: Uses `NoSuchNamespaceError` interception to seamlessly provision missing AWS Glue databases.

---

## Phase 2: Future Enhancements & Technical Debt

### 1. Incremental Processing (CDC) in the Silver Layer
**Goal:** Reduce DuckDB compute and S3 scan costs by deprecating "Full Refresh" operations.
- **High-Watermark**: Query target Iceberg tables via AWS Athena to fetch the maximum processed timestamp (`MAX(updated_at)` or `MAX(ingest_ts)`).
- **Filtered Reads**: Inject the watermark directly into DuckDB's Bronze scan query (`WHERE updated_at > <watermark>`).
- **Efficient Writes**: Transition PyIceberg from `overwrite()` to incremental merging.

### 2. Advanced Data Observability & Decoupling
**Goal:** Decouple data validation from data movement to ensure ingestion speed is never compromised by audit latency.
- **Standalone Observability Flow**: Migrate monitoring from a blocking "hook" to a standalone namespace that runs on its own independent schedule. This ensures ingestion pipelines finish at maximum speed without waiting for the `pipeline_audit` Iceberg commits to clear.
- **Global Reconciliation**: Implement a "Full Circuit" audit that reconciles row counts across all four states (Source, Bronze, Silver, and Gold) in a single unified view.
- **Advanced Data Quality (DQ) Probes**:
    - **Schema Integrity**: Move beyond basic row counts to check for schema drift or unexpected NULL values in non-primary key columns.
    - **SLA Monitoring**: Calculate and log "Data Freshness" (the time gap between `transaction_time` and `ingest_ts`) to alert when the pipeline exceeds a 2-hour latency threshold.
    - **Volume Anomaly Detection**: Implement statistical checks to trigger an SNS alert if a partner suddenly sends 0 records or a 500% spike in volume.
- **Dedicated Monitoring Service**: Transition the `pipeline_audit` table into a backend for a lightweight internal dashboard or API, using historical Iceberg snapshots to track how partner data quality evolves over months.

### 3. Analytics Engineering & Governance
**Goal:** Empower business stakeholders with trusted models and clear data lineage.
- **dbt Core**: Migrate the `gold_user_risk_profile.py` logic to `dbt-duckdb`, unlocking out-of-the-box Schema Testing and Auto-generated Documentation.
- **DataHub**: Deploy a metadata catalog UI to visualize cross-functional lineage from source-to-dashboard.
