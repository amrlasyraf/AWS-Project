# E-Wallet Data Pipeline (AWS-Project)

A production-grade data engineering repository for an E-Wallet application using a modern AWS stack.

## 🏗 Stack
- **Database:** AWS RDS (PostgreSQL)
- **Execution:** AWS Lambda (Python)
- **Orchestration:** Kestra (Open-source Orchestrator)
- **Storage:** AWS S3

## 📂 Project Structure
- `kestra/`: YAML orchestration files for workflows.
- `lambdas/`: Python scripts for data transformations and event handling.
- `scripts/sql/`: SQL seeding, migrations, and DDL scripts.
- `infra/`: Infrastructure-as-Code (Terraform/CDK).

## 🛡️ Security
- All sensitive credentials (AWSKeys, DB Passwords, etc.) are ignored via `.gitignore`.
- Always use environment variables for passwords and keys.