# 🏗️ Urban Traffic Crash Risk & Public Sentiment Intelligence Data Platform ( BATCH PROCESSING ) — Data Engineering Pipeline

> A production-style batch data pipeline built with **Apache Airflow**, **PySpark**, and **Streamlit** that ingests Chicago traffic crash data and YouTube road safety videos, processes them through a **Medallion Architecture** (Bronze → Silver → Gold), and visualizes insights on an interactive dashboard.

---

## 📌 Problem Statement

Chicago records tens of thousands of traffic crashes every year. While raw crash data is publicly available, it is unprocessed, large-scale, and difficult to analyze directly. There is also no existing tool that correlates **when crashes happen** with **public awareness content** (like YouTube road safety videos) uploaded around the same time.

This project answers:
- **Where** are the most dangerous crash hotspots in Chicago?
- **When** do crashes peak — by year, month, weather, lighting, road condition?
- **Is there a correlation** between crash frequency and YouTube road safety video uploads?
- **What is the road risk index** for specific Chicago locations over time?

---

## 🎯 Project Objectives

- Build a **scalable batch pipeline** that ingests data from two external APIs (Chicago Open Data + YouTube Data API)
- Apply **Medallion Architecture** to progressively clean and enrich raw data
- Use **Apache Airflow** for orchestration and scheduling (`@daily`)
- Use **PySpark** for distributed processing at the Silver and Gold layers
- Visualize all pipeline layers and insights in a **Streamlit dashboard**

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     APACHE AIRFLOW (Orchestrator)               │
│                   DAG: crash_pipeline (@daily)                  │
└────────────┬───────────────────────────┬────────────────────────┘
             │                           │
     ┌───────▼──────┐           ┌────────▼──────┐
     │  chicago.py  │           │  youtube.py   │
     │ Chicago Open │           │ YouTube Data  │
     │   Data API   │           │    API v3     │
     └───────┬──────┘           └────────┬──────┘
             │                           │
             ▼                           ▼
  ┌──────────────────────────────────────────────────┐
  │               🟤 BRONZE LAYER                    │
  │         data_lake/bronze/  (Raw JSON)            │
  │  crashes_partitioned/YYYY-MM-DD/crash_*.json     │
  │  youtube_sentiment/YYYY-MM-DD/youtube_*.json     │
  └──────────────────────────────────────────────────┘
             │                           │
     ┌───────▼──────┐           ┌────────▼──────┐
     │silver_chicago│           │silver_youtube │
     │    .py       │           │    .py        │
     │  (PySpark)   │           │  (PySpark)    │
     └───────┬──────┘           └────────┬──────┘
             │                           │
             ▼                           ▼
  ┌──────────────────────────────────────────────────┐
  │               ⚪ SILVER LAYER                    │
  │       data_lake/silver/  (Cleaned Parquet)       │
  │  traffic_crashes/year=YYYY/month=M/*.parquet     │
  │  youtube/year=YYYY/month=M/*.parquet             │
  └──────────────────────────────────────────────────┘
             │                           │
     ┌───────▼──────┐   ┌────────────┐  └──────┐
     │gold_crash_   │   │road_risk_  │  │gold_  │
     │hotspots.py   │   │index.py    │  │combined│
     └───────┬──────┘   └─────┬──────┘  └───┬───┘
             │                │             │
             ▼                ▼             ▼
  ┌──────────────────────────────────────────────────┐
  │               🥇 GOLD LAYER                      │
  │      data_lake/gold/  (Aggregated Parquet)       │
  │  crash_hotspots/   road_risk_index/              │
  │  youtube_chicago_monthly/                        │
  └──────────────────────┬───────────────────────────┘
                         │  data_warehouse.py
                         ▼
  ┌──────────────────────────────────────────────────┐
  │           🏛️ DATA WAREHOUSE                      │
  │        data_warehouse/  (Star Schema)            │
  │  dim_location/   dim_risk/   fact_accidents/     │
  └──────────────────────┬───────────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────────┐
  │         📊 STREAMLIT DASHBOARD                   │
  │         pipeline_dashboard.py                    │
  │   (Reads directly from all 3 lake layers)        │
  └──────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
pipline/
│
├── airflow/
│   └── dags/
│       └── crash_pipeline.py           # Airflow DAG — full pipeline orchestration
│
├── dashboard/
│   ├── .streamlit/                     # Streamlit config
│   ├── app.py                          # Streamlit app entry point
│   └── pipeline_dashboard.py          # Main dashboard (all charts + maps)
│
├── data_ingestion/
│   ├── chicago.py                      # Bronze: Chicago Open Data API ingestion
│   ├── youtube.py                      # Bronze: YouTube Data API v3 ingestion
│   ├── chicago_ingestion.log           # Ingestion run logs
│   ├── ingestion_v2.log                # Version 2 ingestion logs
│   └── last_timestamp.txt              # State file for incremental load
│
├── data_lake/
│   ├── bronze/                         # Raw JSON files (partitioned by date)
│   ├── silver/                         # Cleaned Parquet (year=/month= partitions)
│   └── gold/                           # Aggregated Parquet outputs
│
├── data_warehouse/
│   ├── dim_location/                   # Dimension table — location data
│   ├── dim_risk/                       # Dimension table — risk categories
│   └── fact_accidents/                 # Fact table — crash records
│
├── jobs/
│   ├── silver_chicago.py               # Silver: PySpark cleaning for crash data
│   ├── silver_youtube.py               # Silver: PySpark cleaning for YouTube data
│   ├── gold_crash_hotspots.py          # Gold: Top crash hotspot aggregation
│   ├── gold_combined.py                # Gold: Monthly crashes vs YouTube join
│   ├── road_risk_index.py              # Gold: Road risk scoring per location
│   └── data_warehouse.py              # Final warehouse load (dim + fact tables)
│
├── Dockerfile                          # Docker image for Spark/Airflow containers
└── config.json                         # API keys (YouTube Data API)
```

---

## 🔄 Medallion Architecture — Layer by Layer

### 🟤 Bronze Layer — Raw Ingestion

**Files:** `chicago.py`, `youtube.py`

The Bronze layer stores raw data exactly as received from external APIs — no transformation, no cleaning.

#### Chicago Open Data (`chicago.py`)
- Calls the [Chicago Traffic Crashes API](https://data.cityofchicago.org/resource/85ca-t3if.json)
- Fetches in **batches of 1000 records** (`BATCH_SIZE`) to avoid timeouts
- **Incremental loading** — reads `last_timestamp.txt` to fetch only new records since the last run
- Adds metadata: `ingestion_time`, `source`, `batch_id`
- Saves as partitioned JSON: `bronze/crashes_partitioned/YYYY-MM-DD/crash_<timestamp>.json`
- Built-in **retry logic** (3 retries with 2s delay) for API failures
- Updates `last_timestamp.txt` after each successful batch

#### YouTube Data API (`youtube.py`)
- Searches for 3 road safety queries: `"road accidents"`, `"traffic crash news"`, `"road safety awareness"`
- Fetches up to **250 videos per query** (5 pages × 50 results)
- **Incremental** — uses `publishedAfter` filter from `youtube_last_timestamp.txt`
- Extracts: `video_id`, `title`, `channel`, `published_at`, `query`, metadata
- Saves as partitioned JSON: `bronze/youtube_sentiment/YYYY-MM-DD/youtube_<timestamp>.json`

---

### ⚪ Silver Layer — Cleaning & Standardization

**Files:** `silver_chicago.py`, `silver_youtube.py` (PySpark jobs)

Silver layer cleans, deduplicates, and type-casts Bronze data into Parquet format with Hive partitioning.

**What happens in Silver:**
- Parse and validate `crash_date` / `published_at` timestamps
- Cast numeric fields (`latitude`, `longitude`, `injuries_total`, `injuries_fatal`, etc.) to correct types
- Deduplicate records
- Partition output by `year` and `month` → enables fast time-based filtering
- Output format: **Snappy-compressed Parquet** (10x smaller than JSON, columnar reads)

**Key fix — Hive partition columns:**
Silver files are stored as `silver/traffic_crashes/year=2022/month=4/part-*.parquet`. The dashboard reads the entire folder using `pd.read_parquet(path)` (not individual files) so that `year` and `month` are automatically restored as DataFrame columns from the folder names.

---

### 🥇 Gold Layer — Aggregation & Enrichment

**Files:** `gold_crash_hotspots.py`, `road_risk_index.py`, `gold_combined.py`

Gold layer produces business-ready aggregated tables — no raw records, only insights.
"Silver layer uses spark-submit because it processes raw Bronze JSON at scale — millions of records need distributed computing. Gold layer scripts use plain python because they operate on already-filtered Silver Parquet data which is small enough for Pandas aggregations. The Spark container is still used as the execution environment for both, just different runtimes inside it."

#### `gold_crash_hotspots.py`
- Groups Silver Chicago data by `(latitude, longitude, year, month)`
- Counts accidents per location per period → `accident_count`
- Output: top crash hotspots with coordinates, filterable by time

#### `road_risk_index.py`
- Aggregates all-time accident count per location
- Computes a normalized `risk_score` (0–100) based on accident frequency
- Assigns `risk_level`: 🔴 High (≥70) / 🟡 Medium (≥40) / 🟢 Low (<40)
- Not filtered by time — represents structural long-term road danger

#### `gold_combined.py`
- Joins Silver Chicago (monthly crash count) with Silver YouTube (monthly video count)
- Computes `videos_per_100_accidents` — the key cross-dataset metric
- Output: one row per year-month with both crash and YouTube counts

---

## 🏛️ Data Warehouse — Star Schema

**File:** `jobs/data_warehouse.py`

After Gold aggregation, `data_warehouse.py` loads the final data into a **Star Schema** structure inside the `data_warehouse/` folder.

```
                    ┌─────────────────┐
                    │  fact_accidents │  ◄── Central fact table
                    │─────────────────│
                    │ accident_id  PK │
                    │ location_id  FK │──────► dim_location
                    │ risk_id      FK │──────► dim_risk
                    │ crash_date      │
                    │ injuries_total  │
                    │ injuries_fatal  │
                    │ weather_cond    │
                    │ crash_type      │
                    └─────────────────┘
```

| Table | Type | Contents |
|---|---|---|
| `fact_accidents` | Fact | One row per crash — counts, dates, foreign keys |
| `dim_location` | Dimension | Unique lat/lon locations with area metadata |
| `dim_risk` | Dimension | Risk level categories (High / Medium / Low) |

This enables SQL-style analytical queries across the warehouse and is the final output of the pipeline before the dashboard reads it.

---

**File:** `dags/crash_pipeline.py`

The entire pipeline is orchestrated by a single Airflow DAG (`crash_pipeline`) that runs **daily**.

### DAG Structure

```
chicago_bronze ──► chicago_silver ──► crash_hotspots ──┐
                                                        ├──► data_warehouse
youtube_bronze ──► youtube_silver ──► road_risk_index ──┘
```

### Task Breakdown

| Task | Type | What it runs |
|---|---|---|
| `chicago_bronze` | BashOperator | `python /workspace/data_ingestion/chicago.py` |
| `youtube_bronze` | BashOperator | `python /workspace/data_ingestion/youtube.py` |
| `chicago_silver` | BashOperator | `spark-submit silver_chicago.py` inside Spark container |
| `youtube_silver` | BashOperator | `spark-submit silver_youtube.py` inside Spark container |
| `crash_hotspots` | BashOperator | `python gold_crash_hotspots.py` inside Spark container |
| `road_risk_index` | BashOperator | `python road_risk_index.py` inside Spark container |
| `data_warehouse` | BashOperator | `python data_warehouse.py` — final warehouse write |

### DAG Config
```python
schedule_interval = "@daily"
catchup          = False
max_active_runs  = 1
retries          = 1
retry_delay      = timedelta(minutes=2)
```

### Docker Setup
Airflow and Spark run in separate Docker containers:
- `crash_airflow` — runs Airflow scheduler + webserver
- `crash_spark_engine` — runs PySpark jobs via `docker exec`

To reset Airflow admin password:
```bash
docker exec crash_airflow airflow users reset-password -u admin -p admin123
```

---

## 📊 Streamlit Dashboard

**File:** `dashboard/pipeline_dashboard.py`

```bash
streamlit run pipeline_dashboard.py
```

### Dashboard Sections

| Section | Layer | What it shows |
|---|---|---|
| Pipeline Status | All | Bronze / Silver / Gold health + record counts |
| Year / Month Filter | Silver | Sidebar dropdowns — controls all charts below |
| 4 KPI Cards | Silver | Total crashes, injuries, fatalities, mappable points |
| Crashes by Weather | Silver | Bar chart — top 8 weather conditions at crash time |
| Crashes by Type | Silver | Bar chart — top 8 collision types |
| Road Surface Conditions | Silver | Bar chart — dry / wet / snow / ice |
| Lighting Conditions | Silver | Bar chart — daylight / dark / dawn / dusk |
| Crash Locations Map | Silver | Interactive map — all crashes in selected period |
| Top 10 Hotspots | Gold | Table + map — most dangerous intersections for that month |
| Road Risk Index | Gold | All-time riskiest roads + risk score distribution chart |
| Monthly Trend | Gold | Line chart — crashes vs YouTube uploads over time |
| Videos per 100 Accidents | Gold | Bar chart — derived ratio metric |
| Raw Data Preview | Silver | Expandable tables — first 100 rows of Silver data |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core language |
| Apache Airflow | Pipeline orchestration & scheduling |
| Apache Spark / PySpark | Distributed data processing |
| Pandas | Dashboard-layer data manipulation |
| Streamlit | Interactive web dashboard |
| Docker | Container isolation for Airflow + Spark |
| Parquet (Snappy) | Columnar storage format for Silver + Gold |
| Chicago Open Data API | Traffic crash source data |
| YouTube Data API v3 | Road safety video metadata |
| JSON | Bronze layer storage format |

---

## 🚀 How to Run

### 1. Configure API Keys

Create `config.json` in the project root:
```json
{
  "YOUTUBE_API_KEY": "your_youtube_api_key_here"
}
```

### 2. Run Bronze Ingestion (standalone, no Airflow needed)

```bash
# Fetch Chicago crash data
set BATCH_SIZE=5000
python data_ingestion/chicago.py

# Fetch YouTube video metadata
python data_ingestion/youtube.py
```

> To re-fetch from the beginning, delete `last_timestamp.txt` and `youtube_last_timestamp.txt` before running.

### 3. Run Silver Processing (PySpark)

```bash
spark-submit jobs/silver_chicago.py
spark-submit jobs/silver_youtube.py
```

### 4. Run Gold Aggregation

```bash
python jobs/gold_crash_hotspots.py
python jobs/road_risk_index.py
python jobs/gold_combined.py
```

### 5. Launch Dashboard

```bash
streamlit run dashboard/pipeline_dashboard.py
```

### 6. Run Full Pipeline via Airflow (Docker)

```bash
# Start containers
docker-compose up -d

# Trigger DAG manually
docker exec crash_airflow airflow dags trigger crash_pipeline

# Monitor at
http://localhost:8080
```

---

## 📈 Key Insights from the Data

- **Most crashes occur in clear weather** — high traffic volume, not just adverse conditions
- **Rear-end collisions** are the most common crash type across all years
- **Daylight hours** have the highest crash count; nighttime has higher fatality rates
- **Specific intersections** repeat as hotspots year after year → structural road design issues
- **YouTube road safety uploads spike** in months following high-crash periods — reactive public awareness pattern

---

## 🔮 Future Improvements

- Add **real-time streaming** layer using Kafka for live crash alerts
- Integrate **weather API** data directly instead of relying on crash record weather fields
- Add **geospatial heatmap** using Folium or Deck.gl
- Deploy dashboard to **Streamlit Cloud** or **AWS EC2**
- Add **dbt** for Gold layer transformations with lineage tracking
- Expand YouTube analysis to include **sentiment scoring** on video titles

---

## 👩‍💻 Author

**Thenmozhi** — Data Engineering Student  
Project: Urban Traffic Crash Risk & Public Sentiment Intelligence Data Platform – BATCH PROCESSING
Stack: Airflow · PySpark · Streamlit · Medallion Architecture
