# 🧱 Learning-Databricks

A repository tracking my Azure Databricks learning journey — hands-on projects covering data engineering, pipelines, SQL, and dashboards.

---

## 📂 Projects

### [Task 1 — NYC Taxi Trip Lakehouse](./Task-1/README.md)

An end-to-end data lakehouse pipeline built on Azure Databricks, processing millions of NYC Taxi trips.

**What's covered:**
- 🥉 **Bronze** — Raw ingestion using Auto Loader (`cloudFiles`) from Databricks Volumes
- 🥈 **Silver** — Data cleaning, type casting, deduplication across Yellow, Green & FHV taxi types
- 🥇 **Gold** — Star schema with dimension tables (`dim_vendor`, `dim_datetime`, `dim_location`, `dim_ratecode`) and a fact table (`fact_trips`)
- 🔄 **SCD Type 1 & 2** — Slowly Changing Dimensions on the location table
- ⚙️ **Orchestration** — Databricks Workflow with file-arrival trigger (`job.yaml`)
- 📊 **Dashboard** — Databricks SQL dashboard with KPIs, revenue breakdowns & route analysis

**Tech:** Delta Live Tables · PySpark · Delta Lake · Databricks SQL · Auto Loader · Databricks Workflows

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Azure Databricks | Cloud compute & unified analytics platform |
| Delta Live Tables (DLT) | Declarative pipeline framework |
| Delta Lake | ACID-compliant storage format |
| Auto Loader | Incremental file ingestion |
| PySpark | Data transformation |
| Databricks SQL | Ad-hoc queries & dashboards |
| Databricks Workflows | Job orchestration |
