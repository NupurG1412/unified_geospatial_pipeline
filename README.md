# Unified Geospatial Signal Intelligence Pipeline

A production-style data engineering pipeline that ingests heterogeneous real-world datasets and normalizes them into a **Unified Geospatial Signal Database** using PostgreSQL + PostGIS.

---

## What This Project Does

Instead of storing each dataset in its own isolated table, this pipeline transforms all observations into a **single unified signal schema** — enabling cross-dataset spatial queries, proximity searches, and temporal intelligence analysis.

```
Raw CSV Datasets
      │
      ▼
Normalization Scripts  →  dataset_registry (provenance + trust)
      │
      ▼
marine_signals  (unified PostGIS table — 95,438 signals)
      │
      ▼
Spatial Intelligence Queries
```

---

## Datasets Used

| Dataset | Source | Rows | Feature Type | Trust Level |
|---|---|---|---|---|
| Drifting Buoy (Korean Waters) | NOAA | 1,091 | `drifting_buoy_reading` | High |
| Meteorological Buoy 51001 (NW Hawaii) | NOAA NDBC | 6,507 | `meteorological_buoy_reading` | High |
| ATAL JAL Groundwater 2015–2022 | India ATAL JAL / data.gov.in | 87,840 signals (5,490 wells × 16 seasons) | `groundwater_level` | Medium |

**Total: 95,438 unified signals in one PostGIS table.**

---

## Unified Signal Schema

All datasets are normalized into this single schema:

```sql
marine_signals (
    signal_id         SERIAL PRIMARY KEY,
    source_id         VARCHAR(200),            -- original row identifier
    timestamp         TIMESTAMP,               -- observation time
    latitude          FLOAT,
    longitude         FLOAT,
    geom              GEOGRAPHY(Point, 4326),  -- PostGIS spatial field
    feature_type      VARCHAR(100),            -- type of observation
    normalized_value  FLOAT,                   -- primary measurement (NULL if missing)
    source_reference  VARCHAR(200),            -- human-readable source name
    dataset_id        INT REFERENCES dataset_registry(dataset_id)
)
```

---

## Dataset Registry Schema

Every dataset is tracked with provenance and trust classification:

```sql
dataset_registry (
    dataset_id       SERIAL PRIMARY KEY,
    source           VARCHAR(200),
    schema_version   VARCHAR(20),
    update_frequency VARCHAR(50),
    trust_level      VARCHAR(10),   -- 'high', 'medium', or 'low'
    ingested_at      TIMESTAMP,
    notes            TEXT
)
```

---

## Project Structure

```
unified_geospatial_pipeline/
├── config/
│   └── schema.sql                        ← PostGIS schema + spatial indexes
├── data_raw/                             ← place CSV files here (gitignored)
├── reports/                              ← auto-generated pipeline reports
├── logs/                                 ← auto-generated execution logs
├── src/
│   ├── pipeline.py                       ← main pipeline runner
│   ├── db_connect.py                     ← SQLAlchemy engine from .env
│   ├── logger.py                         ← shared logging (file + console)
│   ├── ingestion/
│   │   └── register_dataset.py           ← dataset registry insertion
│   ├── normalization/
│   │   ├── normalize_drifting_buoy.py    ← handles MM nulls, HHMM time parsing
│   │   ├── normalize_met_buoy.py         ← fixed coords from NOAA station metadata
│   │   └── normalize_groundwater.py      ← wide→long melt, Dry→NULL
│   ├── reporting/
│   │   └── generate_report.py            ← pipeline execution report
│   └── queries/
│       └── spatial_queries.sql           ← 3 spatial intelligence queries
├── .env.example                          ← credential template (copy to .env)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ with PostGIS extension

### 1. Install PostgreSQL + PostGIS

**Windows:** Download from https://www.postgresql.org/download/windows/ and use Stack Builder to install PostGIS.

**Mac:**
```bash
brew install postgresql@15 postgis
```

**Ubuntu:**
```bash
sudo apt update && sudo apt install postgresql postgresql-contrib postgis
```

### 2. Create the database

```bash
psql -U postgres -c "CREATE DATABASE marine_intelligence;"
psql -U postgres -d marine_intelligence -c "CREATE EXTENSION postgis;"
psql -U postgres -d marine_intelligence -c "SELECT PostGIS_Version();"
```

### 3. Run the schema

```bash
psql -U postgres -d marine_intelligence -f config/schema.sql
```

### 4. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 5. Configure credentials

```bash
cp .env.example .env
```

Edit `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=marine_intelligence
DB_USER=postgres
DB_PASSWORD=your_password_here
```

> Credentials are loaded from environment variables. They are never hardcoded in any source file.

### 6. Place data files

Copy your CSV files into `data_raw/`:
```
data_raw/
├── drifting_buoy_data.csv
├── meteorological_buoy_data.csv
└── Atal_Jal_Disclosed_Ground_Water_Level-2015-2022.csv
```

### 7. Test the database connection

```bash
python src/db_connect.py
```

Expected output:
```
Connected to database successfully.
PostGIS version: 3.x.x ...
```

### 8. Run the pipeline

```bash
python src/pipeline.py
```

Expected output:
```
========== UNIFIED GEOSPATIAL SIGNAL PIPELINE STARTED ==========
STEP 1: Registering datasets in dataset_registry
STEP 2: Normalizing datasets into unified signal schema

========== PIPELINE EXECUTION SUMMARY ==========
drifting_buoy    →   1,091 signals inserted
met_buoy_51001   →   6,507 signals inserted
groundwater      →  87,840 signals inserted
TOTAL            →  95,438 signals in marine_signals
========== PIPELINE COMPLETED ==========
```

### 9. Run spatial queries

In pgAdmin Query Tool or psql:
```bash
psql -U postgres -d marine_intelligence -f src/queries/spatial_queries.sql
```

---

## Spatial Queries

### Query 1 — Proximity Search
Finds all signals within 1,000 km of Mumbai using `ST_DWithin`. Demonstrates distance-based spatial filtering.

### Query 2 — Signal Density Grid
Aggregates signals into a 1-degree geographic grid using `FLOOR(latitude/longitude)`. Used to build observation density maps.

### Query 3 — Temporal Trend
Groups average normalized values by month and feature type using `DATE_TRUNC`. Reveals seasonal patterns in groundwater and ocean temperature.

---

## Transformation Notes

### Drifting Buoy
- Time column is integer HHMM format (e.g. `1400` = 14:00) — parsed to ISO timestamp
- `MM` values (NOAA standard missing marker) → stored as `NULL`, never filled

### Meteorological Buoy 51001
- File has no lat/lon columns — standard for fixed NOAA stations
- Coordinates hardcoded from NOAA station metadata: **23.445°N, 162.279°W**
- `NaN` values → stored as `NULL`, never filled

### ATAL JAL Groundwater
- Wide format (16 seasonal columns per well) → melted to long format
- Each well × season = one signal row (5,490 × 16 = 87,840)
- Pre-monsoon → June 1 of that year; Post-monsoon → November 1
- `"Dry"` entries → `NULL` (well had no water that season — not a missing value)
- `NaN` entries → `NULL` (measurement not recorded)

---

## Data Integrity Rules

- Missing values are **never filled** with median, mean, zero, or placeholder strings
- `"Dry"` is treated as a scientific observation, stored as NULL without fabricating a numeric value
- All spatial data uses `GEOGRAPHY(Point, 4326)` — not plain FLOAT lat/lon columns
- A spatial index (`GIST`) is created on the `geom` column for fast spatial queries
- Credentials are never stored in source code — loaded from `.env` only

---

## Example Input

**drifting_buoy_data.csv** (first 2 rows):
```
year,month,day,time,latitude,longitude,water_temp,...
2026,3,15,1400,37.24,126.02,5.4,...
2026,3,15,1300,37.24,126.02,5.5,...
```

**Atal_Jal_Groundwater.csv** (wide format, first row):
```
Well_ID,Latitude,Longitude,Pre-monsoon_2015,...,Post-monsoon_2022
G_1_BK_021,24.4425,72.42,16.3,...,12.3
```

---

## Example Output (marine_signals table)

| signal_id | source_id | timestamp | latitude | longitude | feature_type | normalized_value | source_reference |
|---|---|---|---|---|---|---|---|
| 1 | DRIFTING_2026315_1400 | 2026-03-15 14:00 | 37.24 | 126.02 | drifting_buoy_reading | 5.4 | NOAA Drifting Buoy |
| 1092 | NOAA_51001_2026-03-15... | 2026-03-15 15:00 | 23.445 | -162.279 | meteorological_buoy_reading | 23.6 | NOAA NDBC Station 51001 |
| 1599 | G_1_BK_021_pre_2015 | 2015-06-01 00:00 | 24.44 | 72.42 | groundwater_level | 16.3 | ATAL JAL Groundwater |

---

## Dependencies

```
pandas==2.2.1
sqlalchemy==2.0.29
psycopg2-binary==2.9.9
python-dotenv==1.0.1
```
