# E-Wallet Data Pipeline (AWS-Project)

A production-grade data engineering pipeline for an E-Wallet application, simulating realistic transaction lifecycles using a modern AWS + Kestra stack.

---

## 🏗️ Stack

| Layer | Technology |
|---|---|
| **Database** | AWS RDS (PostgreSQL) |
| **Compute** | AWS Lambda (Python) |
| **Orchestration** | Kestra (Open-source) |
| **Storage** | AWS S3 |

---

## ⚙️ How It Works

### Pipeline Flow

```text
Kestra Master Orchestrator (Subflow Pattern)
      │
      ├─ 🧱 Bronze Layer (Parallel Extraction)
      │     └─ Parquet Files injected with metadata (ingest_ts, partner)
      │     └─ S3: bronze/partner={id}/table={name}/YYYY/MM/DD
      │
      ├─ 🥈 Silver Layer (Metadata-Driven Framework)
      │     └─ Reads config directly from `metadata/registry.json`
      │     └─ DuckDB Universal Engine performs Deduplication
      │     └─ PyIceberg Auto-Evolves Schema & Upserts to AWS Glue
      │
      ├─ 🥇 Gold Layer (Risk Analytics)
      │     └─ DuckDB aggregates 7-day velocities & risk scoring
      │     └─ Output: `gold.user_risk_profile` (Apache Iceberg)
      │
      └─ 📊 Data Observability (Inline Circuit Breakers)
            └─ Audit loggers integrated into Master Flow to halt on schema or PK failure
            └─ PyIceberg tracks row counts in Shared Ledger (`silver.pipeline_audit`)
```

### Transaction Lifecycle

Each transaction simulates a realistic processing delay by inserting **3 records** per transaction into the `transactions` table:

| State | Offset | Description |
|---|---|---|
| `PENDING` | t+0s | Transaction initiated |
| `APPROVED` | t+2s | Gateway approval received |
| `SUCCESS` / `FAILED` | t+7s | Final settlement outcome |

> This models real-world data latency where the same `transaction_id` appears multiple times with evolving states, enabling downstream consumers to track state transitions over time.

---

## 📂 Project Structure

```text
AWS-Project/
├── kestra/
│   ├── metadata/
│   │   └── registry.json               # Single Source of Truth for schemas
│   ├── pipeline/
│   │   ├── lead_orchestrator.yaml      # Master Flow (Bronze -> Silver -> Gold -> Audit)
│   │   ├── silver_layer_orchestrator.yaml
│   │   ├── bronze-monitoring.yaml      # Inline circuit wrappers
│   │   └── silver-monitoring.yaml
│   ├── scripts/
│   │   ├── extractor.py                # Bronze CDC metadata injection
│   │   ├── silver_universal_engine.py  # Dynamic DuckDB/PyIceberg Upsert logic
│   │   ├── gold_user_risk_profile.py   # High-Risk modeling engine
│   │   ├── bronze_audit_logger.py      # Source vs Bronze row integrity
│   │   └── silver_audit_logger.py      # Bronze vs Silver Iceberg integrity
│   └── gold/
│       └── gold-user-risk-profile.yaml # Gold deployment orchestrator
├── lambdas/
│   └── transaction_producer.py         # Simulates multi-partner transactions
├── SYSTEM_DESIGN.md                    # Comprehensive internal wiki
├── IMPROVEMENTS.md                     # v1.0 Milestones & Future Roadmap
└── .gitignore                          # Security exclusions
```

---

## 🗃️ Data Model

### `users`
| Column | Type | Notes |
|---|---|---|
| `user_id` | INT | PK, range 1000–1099 |
| `current_age` | INT | 18–70 |
| `yearly_income` | NUMERIC | 30,000–180,000 |
| `credit_score` | INT | 300–850 |

### `cards`
| Column | Type | Notes |
|---|---|---|
| `card_id` | INT | PK |
| `user_id` | INT | FK → users |
| `card_brand` | TEXT | Visa / Mastercard / Amex |
| `credit_limit` | NUMERIC | 5,000–25,000 |
| `card_on_dark_web` | BOOL | ~10% chance true |

### `transactions`
| Column | Type | Notes |
|---|---|---|
| `transaction_id` | UUID | Same ID across all 3 lifecycle states |
| `user_id` | INT | FK → users |
| `card_id` | INT | FK → cards |
| `amount` | NUMERIC | 10.00–500.00 |
| `status` | TEXT | `PENDING` → `APPROVED` → `SUCCESS`/`FAILED` |
| `transaction_time` | TIMESTAMP | Time of transaction initiation |
| `updated_at` | TIMESTAMP | Time of this state change |
| `use_chip` | TEXT | Swipe / Chip / Online |
| `merchant_id` | INT | 50,000–60,000 |
| `merchant_name` | TEXT | e.g. `Merchant_52341` |
| `mcc` | INT | Merchant Category Code (1000–9999) |
| `risk_score` | FLOAT | 0.01–0.99 |

---

## 🔁 Kestra Orchestration

`kestra/git_sync.yaml` — Keeps Kestra in sync with this GitHub repo automatically:

```yaml
id: git_sync
namespace: my.project
tasks:
  - id: sync
    type: io.kestra.plugin.git.Sync
    url: https://github.com/amrlasyraf/AWS-Project.git
    branch: main
    password: "{{ vars.github_token }}"
```

---

## 🛡️ Security

- All sensitive credentials (AWS Keys, DB Passwords, GitHub tokens) are excluded via `.gitignore`.
- All secrets are passed to Lambda via **environment variables** (`DB_HOST`, `DB_PASSWORD`).
- No credentials are hardcoded anywhere in the codebase.

---

## ⏱️ Transaction State Simulation

Simulates real-time e-wallet transaction states: PENDING (t+0), APPROVED (t+2s), and final SUCCESS/FAILED (t+7s) to model realistic data latency.