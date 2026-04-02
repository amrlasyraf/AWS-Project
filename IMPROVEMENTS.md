# Pipeline Improvements and Optimizations

> [!NOTE]
> Whenever an improvement is implemented, the **Current Stack / Implementation** section for that flow must be overwritten to reflect the newly updated architecture.

---

## 1. Bronze Ingestion Flow

### Current Stack / Implementation
The pipeline utilizes a sequential `ForEach` loop to iterate through a hardcoded list of tables (`transactions`, `users`, `cards`). Each task executes a Python script using Kestra's `io.kestra.plugin.scripts.python.Commands` task, which dynamically installs dependencies (`pandas`, `boto3`, `psycopg2-binary`, etc.) at runtime using `uv pip install`.

### Identified Improvements
- **Custom Docker Images**: Pre-build a specialized Docker image containing all necessary Python libraries to eliminate the time-consuming `pip install` step in every task run.
- **Concurrent Execution**: Transition from sequential processing to parallel execution in the `ForEach` loop to reduce the overall pipeline duration.
- **Dynamic Table Discovery**: Replace the hardcoded table list with a dynamic discovery mechanism (e.g., querying the information schema) to automatically handle new tables.
- **Enhanced Partitioning**: Implement more granular partitioning in S3 (e.g., by data date instead of just ingestion hour) to optimize downstream Silver/Gold layer processing.

### Status
Pending
