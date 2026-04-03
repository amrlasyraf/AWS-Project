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

### 2. Data Observability & Monitoring
**Goal:** Move beyond basic operational alerts to deep data-quality validation.
- **Audit Table**: Log rows read/written, duplicate counts, and duration into a new `silver.pipeline_audit` Iceberg table for every Kestra run.
- **Circuit Breakers**: Implement inline DuckDB validation to intentionally fail the pipeline and trigger SNS alerts if critical anomalies (e.g., 0 rows fetched) occur.
- **Dashboarding**: Connect AWS QuickSight directly to the Glue catalog for serverless monitoring of pipeline lag and deduplication rates.

### 3. Analytics Engineering & Governance
**Goal:** Empower business stakeholders with trusted models and clear data lineage.
- **dbt Core**: Migrate the `gold_user_risk_profile.py` logic to `dbt-duckdb`, unlocking out-of-the-box Schema Testing and Auto-generated Documentation.
- **DataHub**: Deploy a metadata catalog UI to visualize cross-functional lineage from source-to-dashboard.
