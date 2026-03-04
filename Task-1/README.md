# 🚕 NYC Taxi Trip Lakehouse — Task 1

> A complete data engineering pipeline built on **Azure Databricks**, processing millions of NYC Taxi trips through a **Medallion Architecture** (Bronze → Silver → Gold) using **Delta Live Tables (DLT)**.

---

## 📄 Project Documents

| Document | Description |
|---|---|
| [`NYC Taxi Trip Lakehouse Project.pdf`](./NYC%20Taxi%20Trip%20Lakehouse%20Project.pdf) | Original project brief & requirements |
| [`Deeps Task-1.drawio.pdf`](./Deeps%20Task-1.drawio.pdf) | Architecture diagram |

---

## 🗺️ What Is This Project?

New York City releases taxi trip data every month — millions of rides, with details like pickup/dropoff locations, fares, timestamps, and more. This project builds a **data lakehouse** on Databricks that:

1. **Ingests** raw parquet files automatically when they're uploaded
2. **Cleans** and validates the data
3. **Transforms** it into an analytics-ready star schema
4. **Visualises** insights via a Databricks SQL Dashboard

---

## 🏗️ Architecture — Medallion Pattern

Think of it like refining raw ore into gold:

```
📦 Raw Parquet Files (Volumes)
         │
         ▼
🥉 BRONZE  ── Raw ingestion, no changes, just land the data
         │
         ▼
🥈 SILVER  ── Clean, rename columns, fix types, remove bad records
         │
         ▼
🥇 GOLD    ── Star schema: dimension tables + fact table for analytics
         │
         ▼
📊 DASHBOARD ── SQL queries on Gold tables → charts & KPIs
```

All of this runs as a **Delta Live Tables (DLT) pipeline** on Databricks. DLT figures out the order of execution automatically — you don't need to schedule anything manually.

---

## 📁 Folder Structure

```
Task-1/
├── job.yaml                     ← Databricks Workflow config (auto-trigger)
├── NYC Taxi Trip Lakehouse Project.pdf
├── Deeps Task-1.drawio.pdf
├── data/jan/                    ← Local parquet files for testing
│   ├── yellow_tripdata_2025-01.parquet
│   ├── green_tripdata_2025-01.parquet
│   └── fhv_tripdata_2025-01.parquet
└── Deeps Task-1/           ← DLT pipeline source code
        ├── extraction/          ← Bronze layer
        ├── transformations/     ← Silver layer
        └── loader/              ← Gold layer
```

---

## 🥉 Bronze Layer — `extraction/`

> **Plain English:** Just pick up the raw files and put them into Delta tables. No cleaning yet.

| File | What it creates | Source |
|---|---|---|
| `bronze_yellow.py` | `bronze.yellow_trips` | Yellow taxi parquets |
| `bronze_green.py` | `bronze.green_trips` | Green taxi parquets |
| `bronze_fhv.py` | `bronze.fhv_trips` | FHV (For-Hire Vehicle) parquets |
| `bronze_lookup.py` | `bronze.lookup` | NYC taxi zone reference CSV |

**Key concept — Auto Loader:**
```python
spark.readStream
    .format("cloudFiles")   # ← This is Auto Loader
    .option("cloudFiles.format", "parquet")
    .load("/Volumes/.../yellow/")
```
Auto Loader watches the folder and **only processes new files** — it won't re-read files you've already ingested, even if you run the pipeline again. It also stamps an `ingestion_timestamp` on every row.

---

## 🥈 Silver Layer — `transformations/`

> **Plain English:** Take the raw data, fix column names, convert data types, remove garbage records, and separate each taxi type into its own clean table. Then merge them all together.

### Step 1 — Clean each taxi type individually

| File | What it creates | What it fixes |
|---|---|---|
| `silver_yellow.py` | `silver.yellow_trips_clean` | Renames columns, casts types, filters invalid records |
| `silver_green.py` | `silver.green_trips_clean` | Same as yellow (different column names) |
| `silver_fhv.py` | `silver.fhv_trips_clean` | FHV has no fare/vendor data — fills NULLs for consistency |
| `silver_lookup.py` | `silver.lookup_zones_scd1` / `scd2` | Cleans zone reference data + applies SCD |

**What gets filtered out in yellow/green:**
- Fares less than 0
- Trips with 0 distance
- Records where dropoff time is before pickup time

**Why FHV is different:**
FHV (Uber, Lyft-style vehicles) don't share fare or vendor info publicly. So those columns are filled with `NULL` — the trip still counts, we just don't have that detail.

### Step 2 — Merge into one unified table

**`silver_merge.py`** → `silver.silver_trips`

```python
yellow.unionByName(green).unionByName(fhv)
      .dropDuplicates([...])
```

All three clean tables are stacked into one `silver_trips` table. Duplicates are removed using trip key columns (vendor, pickup time, dropoff time, location).

---

## 🥇 Gold Layer — `loader/`

> **Plain English:** Structure the data for easy querying. Create small "lookup" tables (dimensions) and one big "results" table (fact). This is called a **Star Schema**.

### What is a Star Schema?

```
          dim_vendor
          dim_datetime
fact_trips ──── dim_location  (pickup zone)
          dim_location  (dropoff zone)
          dim_ratecode
```

The **fact table** holds the numbers (fares, distances, counts). The **dimension tables** hold the descriptions (vendor names, zone names, date info). You join them together when querying.

### Dimension Tables

#### `gold_dim_vendor.py` → `gold.dim_vendor`
Maps vendor IDs to real company names:

| vendor_id | vendor_name |
|---|---|
| 1 | Creative Mobile Technologies, LLC |
| 2 | Curb Mobility, LLC |
| 6 | Myle Technologies Inc |
| 7 | Helix |

---

#### `gold_dim_datetime.py` → `gold.dim_datetime`
Breaks pickup timestamps into queryable date parts:

| date_key | day | month | year | weekday |
|---|---|---|---|---|
| 20250115 | 15 | 1 | 2025 | Wed |

Useful for queries like *"how many trips on Fridays?"* or *"revenue in January?"*

---

#### `gold_dim_ratecode.py` → `gold.dim_ratecode`
Maps rate codes to descriptions:

| ratecode_id | rate_description |
|---|---|
| 1 | Standard rate |
| 2 | JFK Airport |
| 3 | Newark Airport |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |

---

#### `gold_dim_location.py` → `gold.dim_location_scd1` + `gold.dim_location_scd2`

This is where **SCD (Slowly Changing Dimensions)** comes in.

> **What is SCD?** Sometimes reference data changes over time — e.g., a taxi zone gets renamed. SCD controls how we handle that change.

Two tables are created from the same source:

| Table | SCD Type | Behaviour |
|---|---|---|
| `dim_location_scd1` | Type 1 — Overwrite | New zone name replaces old one. No history kept. |
| `dim_location_scd2` | Type 2 — History | Old record is closed, new record added with timestamps. |

**SCD Type 2 example:** If zone "Midtown" gets renamed to "Midtown Central":

| location_id | zone | __START_AT | __END_AT |
|---|---|---|---|
| 5 | Midtown | 2025-01-01 | 2025-03-01 |
| 5 | Midtown Central | 2025-03-01 | NULL ← current |

> Query current records: `WHERE __END_AT IS NULL`

---

#### `gold_fact_trips.py` → `gold.fact_trips`

The central table. Every row = one taxi trip. Stores foreign keys to all dims + the measures.

```
vendor_id | date_key | pickup_location_id | dropoff_location_id |
ratecode_id | fare_amount | trip_distance | total_amount | passenger_count
```

Built by joining silver trips with all dimension tables (left joins to keep FHV trips with NULLs).

---

## ⚙️ Automation — `job.yaml`

The pipeline doesn't need to be triggered manually. The `job.yaml` sets up a **Databricks Workflow** that:

- 📂 **Watches** the Volumes folder `/Volumes/deeps-task/.../raw/nyc_taxi/` for new files
- ⚡ **Triggers** the DLT pipeline automatically when a file arrives
- 📧 **Sends email notifications** on start, success, and failure
- 🔁 **Retries once** if it fails, after a 15-minute wait

So the full workflow is: **Drop a parquet file → pipeline runs → Gold tables updated → Dashboard refreshes.**

---

## 📊 Dashboard (Databricks SQL)

Built in Databricks SQL using the Gold tables. Key widgets:

| Widget | Query |
|---|---|
| Total Trips & Revenue | `COUNT(*), SUM(fare_amount) FROM fact_trips` |
| Revenue by Vendor | `JOIN dim_vendor` |
| Trips by Month | `JOIN dim_datetime` |
| Top Pickup Boroughs | `JOIN dim_location_scd1` |
| Busiest Day of Week | `JOIN dim_datetime` |
| Rate Code Distribution | `JOIN dim_ratecode` |
| Top Routes | Self-join on `dim_location_scd1` for pickup + dropoff zones |

---

## 🔄 Full Pipeline Flow

```
bronze.yellow_trips ─────────────────────────────────────────────┐
bronze.green_trips  ──→ silver.yellow/green/fhv_trips_clean ──→  silver.silver_trips
bronze.fhv_trips  ───────────────────────────────────────────────┘
                                                                         │
bronze.lookup ──→ lookup_zones_raw ──→ dim_location_scd1 / scd2          │
                                                                         │
                                                                         ▼
                            dim_vendor, dim_datetime, dim_ratecode ──→ gold.fact_trips
                                                                         │
                                                                         ▼
                                                                    📊 Dashboard
```

---

## 📦 Data

| File | Rows (approx) | Size |
|---|---|---|
| `yellow_tripdata_2025-01.parquet` | ~3.5M | 56 MB |
| `green_tripdata_2025-01.parquet` | ~48K | 1.1 MB |
| `fhv_tripdata_2025-01.parquet` | ~3.1M | 20 MB |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Azure Databricks | Cloud compute platform |
| Delta Live Tables (DLT) | Pipeline framework |
| Delta Lake | Storage format (ACID transactions) |
| Auto Loader (`cloudFiles`) | Incremental file ingestion |
| PySpark | Data transformation language |
| Databricks SQL | Dashboard & ad-hoc queries |
| Databricks Volumes | File storage on the lakehouse |
| Databricks Workflows | Job orchestration via `job.yaml` |
