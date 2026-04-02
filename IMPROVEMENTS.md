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
