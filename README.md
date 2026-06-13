# DataPulse Lakehouse Migration

> **Migrating a local data engineering pipeline into the Databricks Lakehouse Platform using the Medallion Architecture (Bronze → Silver → Gold) with Delta Lake, PySpark, and Unity Catalog.**

---

## What This Project Does

DataPulse started as a local SQL Server, Medallion Architecture, Star Schema, BULK INSERT, Naming_Conventions, Architecture Desig data pipeline processing raw CSV datasets on a developer machine. This repository documents and implements its **full migration to Databricks**, transforming it into a scalable, cloud-native lakehouse solution.

The migration journey moves through three key transitions:

| Stage | Before (Local) | After (Databricks) |
|---|---|---|
| Storage | Local filesystem / CSV files | Delta Lake on DBFS / ADLS Gen2 |
| Processing | SQL Server scripts | PySpark on Databricks clusters |
| Orchestration | Manual script execution | Databricks Workflows (Jobs) |
| Data Quality | Ad-hoc checks | Delta constraints + expectations |
| Governance | None | Unity Catalog (schemas, lineage) |

---

## Repository Structure

```
DataPulse-Lakehouse-Migration/
├── Datasets/                   # Raw source data (CSV files for ingestion)
│   └── *.csv
├── script/                     # Databricks notebooks & migration scripts
│   ├── 01_bronze_ingestion.*   # Raw layer: ingest CSV → Delta Bronze
│   ├── 02_silver_transform.*   # Cleansed layer: apply quality rules
│   └── 03_gold_aggregation.*   # Business layer: analytics-ready tables
└── README.md
```

---

## Architecture Overview

The project follows the **Medallion Architecture**, a multi-hop data pattern recommended by Databricks:

```
Local CSV Files
      │
      ▼
┌─────────────────────────────────────────────────────┐
│                  DATABRICKS LAKEHOUSE               │
│                                                     │
│  ┌───────────┐   ┌───────────┐   ┌───────────┐     │
│  │  BRONZE   │──▶│  SILVER   │──▶│   GOLD    │     │
│  │  Raw      │   │ Cleansed  │   │ Aggregated│     │
│  │  Delta    │   │  Delta    │   │  Delta    │     │
│  └───────────┘   └───────────┘   └───────────┘     │
│                                                     │
│  Unity Catalog  ·  Databricks Workflows  ·  Spark   │
└─────────────────────────────────────────────────────┘
```

### Layer Definitions

**Bronze — Raw Ingestion Layer**
- Reads source CSV files from the `Datasets/` directory
- Writes data as-is into Delta Lake tables with minimal transformation
- Appends metadata columns: `_ingestion_timestamp`, `_source_file`
- Retains full history for auditability and reprocessing

**Silver — Cleansed & Conformed Layer**
- Applies schema enforcement and data type casting
- Deduplicates records using Delta MERGE (upserts)
- Handles null values, string normalization, and referential integrity
- Produces trusted, analysis-ready datasets

**Gold — Business Aggregation Layer**
- Builds aggregated, denormalized tables optimized for BI tools
- Computes KPIs and business metrics
- Structured for direct consumption by Power BI, Tableau, or SQL Analytics

---

## Migration: Local Project → Databricks

### Step 1 — Upload Source Data

Upload the `Datasets/` CSV files to Databricks File System (DBFS) or a connected cloud storage account:

```python
# Using Databricks CLI
databricks fs cp ./Datasets/ dbfs:/datapulse/raw/ --recursive

# Or via Unity Catalog Volume
dbutils.fs.cp("file:/Datasets/", "abfss://container@storage.dfs.core.windows.net/raw/")
```

### Step 2 — Create the Lakehouse Schema

In a Databricks notebook or SQL editor, provision the Unity Catalog structure:

```sql
CREATE CATALOG IF NOT EXISTS datapulse;
CREATE SCHEMA IF NOT EXISTS datapulse.bronze;
CREATE SCHEMA IF NOT EXISTS datapulse.silver;
CREATE SCHEMA IF NOT EXISTS datapulse.gold;
```

### Step 3 — Run Bronze Ingestion

Replace local `pandas.read_csv()` calls with Spark reads that write to Delta:

```python
# Before (local)
import pandas as pd
df = pd.read_csv("Datasets/data.csv")

# After (Databricks — Bronze layer)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("dbfs:/datapulse/raw/data.csv")

df.write.format("delta").mode("overwrite") \
    .saveAsTable("datapulse.bronze.raw_data")
```

### Step 4 — Transform to Silver

Apply business rules and write a clean Delta table:

```python
from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp

silver_df = spark.table("datapulse.bronze.raw_data") \
    .dropDuplicates() \
    .filter(col("id").isNotNull()) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("event_ts", to_timestamp(col("date_str"), "yyyy-MM-dd")) \
    .withColumn("_updated_at", current_timestamp())

silver_df.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("datapulse.silver.clean_data")
```

### Step 5 — Aggregate to Gold

Produce analytics-ready aggregates:

```python
gold_df = spark.table("datapulse.silver.clean_data") \
    .groupBy("category", "region") \
    .agg(
        {"amount": "sum", "id": "count"}
    ).withColumnRenamed("sum(amount)", "total_amount") \
     .withColumnRenamed("count(id)", "record_count")

gold_df.write.format("delta").mode("overwrite") \
    .saveAsTable("datapulse.gold.summary_by_category")
```

### Step 6 — Schedule with Databricks Workflows

Convert the manual script execution into an automated job:

1. In the Databricks UI, go to **Workflows → Jobs → Create Job**
2. Add three tasks in sequence: `bronze_ingestion` → `silver_transform` → `gold_aggregation`
3. Attach a cluster policy and set a cron schedule (e.g., `0 6 * * *` for 6 AM daily)
4. Enable email alerts on failure

---

## Tech Stack

| Technology | Purpose |
|---|---|
| **Databricks** | Unified lakehouse platform, cluster management, Workflows |
| **Apache Spark / PySpark** | Distributed data processing |
| **Delta Lake** | ACID-compliant storage format, time travel, schema evolution |
| **Unity Catalog** | Data governance, access control, lineage tracking |
| **Python / Jupyter Notebooks** | Script development and migration notebooks |
| **DBFS / ADLS Gen2** | Cloud file storage for raw and processed data |

---

## Key Migration Benefits

- **Scalability**: Spark distributes processing across a cluster — no more memory limits from local pandas
- **Data Reliability**: Delta Lake provides ACID transactions and automatic rollback via time travel
- **Governance**: Unity Catalog enforces column-level access controls and tracks data lineage end-to-end
- **Automation**: Databricks Workflows replaces manual cron jobs with observable, retryable pipelines
- **Cost Efficiency**: Clusters auto-scale and auto-terminate, so you pay only for active compute

---

## Getting Started

### Prerequisites
im' work in free trail , it's enough 
- Databricks workspace (AWS, Azure, or GCP)
- Python 3.8+ with `databricks-sdk` or `databricks-cli` installed
- Access to a Unity Catalog metastore

### Quick Setup

```bash
# 1. Clone the repository
git clone https://github.com/AhemdMahmoud/DataPulse-Lakehouse-Migration.git
cd DataPulse-Lakehouse-Migration

# 2. Install Databricks CLI
pip install databricks-cli

# 3. Configure your workspace
databricks configure --token

# 4. Upload datasets
databricks fs mkdirs dbfs:/datapulse/raw/
databricks fs cp Datasets/ dbfs:/datapulse/raw/ --recursive

# 5. Import notebooks into Databricks workspace
databricks workspace import_dir script/ /Users/<your-email>/datapulse/
```

Then open the imported notebooks in your Databricks workspace and run them in order: `01_bronze` → `02_silver` → `03_gold`.

---

## Project Status

- [x] Bronze ingestion layer (CSV → Delta)
- [x] Silver transformation layer (cleansing, dedup)
- [x] Gold aggregation layer (KPIs, summaries)
- [x] Databricks Workflows job configuration
- [x] Unity Catalog lineage setup
- [x] Data quality expectations (Great Expectations / Delta constraints)

---

## Author

**Ahmed Mahmoud**
Data Engineer · [GitHub](https://github.com/AhemdMahmoud)

---
