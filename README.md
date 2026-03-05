# Sleep-EDF → SQL: Sleep Staging Mini Warehouse (SQLite)

This project builds a small relational database from Sleep-EDF (PhysioNet) hypnogram annotations and demonstrates practical SQL analytics:
joins, aggregation, cohort selection/QC rules, views, and window functions.

## What’s in the database
- Participants → Sessions (nights) → 30s Epochs → Sleep stages (W/N1/N2/N3/REM/UNKNOWN)
- Raw EEG samples are not stored in SQL; only metadata + epoch-level labels are stored.

## Tech stack
- SQLite (sqlite3 CLI) for schema + analytics queries
- Python (MNE + sqlite3) for ETL (extract Sleep-EDF annotations → load tables)

## Schema (core tables)
- `participants`
- `sessions`
- `epochs`
- `sleep_stages`
- (optional) `qc_epoch_flags`

## Key outputs
- `v_night_sleep_summary` view: sleep window, TST, sleep efficiency, stage minutes, REM latency, UNKNOWN%
- Cohort selection example: filter nights by TST / efficiency / UNKNOWN%
- Fragmentation metrics: stage transitions, awakenings, longest N3/REM bouts (window functions)

## How to run
### 1) Create database + tables
```bash
sqlite3 data_out/sleepedf.db ".read sql/schema.sql"



### 2) Load data (ETL)

conda activate sleep_sql
python etl/01_extract_sleepedf.py
python etl/02_build_epochs_and_stages.py


### 3) Create views + run queries

sqlite3 data_out/sleepedf.db ".read sql/views.sql"
sqlite3 -header -column data_out/sleepedf.db ".read sql/queries.sql"


### 4) Populate QC table (DML) + run QA tests
sqlite3 -header -column data_out/sleepedf.db ".read sql/dml.sql"
sqlite3 -header -column data_out/sleepedf.db ".read sql/qa_tests.sql"


### 4) Export analysis-ready CSVs

sqlite3 -header -csv data_out/sleepedf.db "SELECT * FROM v_night_sleep_summary;" > data_out/night_sleep_summary.csv



## Key outputs

# View: v_night_sleep_summary

sleep window, TST, sleep efficiency, stage minutes, REM latency, UNKNOWN%

# Window-function metrics (fragmentation proxies)

stage transitions, awakenings (wake bouts), longest N3/REM bouts

# QA tests (sql/qa_tests.sql)

validates 1:1 epochs↔stages, no invalid labels, no duplicates

# Exported CSV artefacts:

data_out/night_sleep_summary.csv

data_out/cohort_selected.csv

data_out/fragmentation_summary.csv



================================================================================================

### What this demonstrates (skills)

Relational modelling (PK/FK, normalisation, indexing)

SQL analytics (joins, aggregation, cohort filters, views)

Advanced SQL (CTEs, window functions for fragmentation & bouts)

Basic optimisation (EXPLAIN QUERY PLAN, composite indexing)

DML & analytics engineering patterns (INSERT…SELECT QC table, QA tests)

Python ETL (EDF annotations → epoch-level labels → SQL tables)