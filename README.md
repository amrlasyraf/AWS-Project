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

```
Kestra Scheduler
      │
      ▼
AWS Lambda: transaction_producer
      │  Generates 20 transactions per run
      │  Each transaction produces 3 records (lifecycle states)
      ▼
AWS RDS (PostgreSQL)
      │  transactions table
      ▼
AWS S3 (future: downstream analytics)
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

```
AWS-Project/
├── kestra/
│   └── git_sync.yaml          # Kestra Git Sync workflow (auto-deploys from GitHub)
├── lambdas/
│   └── transaction_producer.py # Lambda: generates synthetic e-wallet transactions
├── scripts/
│   └── sql/
│       └── seed_dimensions.sql # Seeds users (×100) and cards (×200) into RDS
└── .gitignore                  # Excludes all secrets and credentials
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