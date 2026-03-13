# SleepSQL (Sleep-EDF → SQLite) — Sleep Staging Mini Warehouse

This repo is a **learning / practice project** designed to build *transferable* SQL + data workflow skills (schema design, joins/aggregation, views, window functions, QA checks, exports to BI/Excel).

It uses **Sleep-EDF (PhysioNet)** sleep-stage annotations (hypnogram EDF files) to build an **analysis-ready** SQLite database.  
⚠️ **Some fields may be synthetic** (e.g., demographics and questionnaire-style flags) to practise joins, reporting, and cohort/QC logic.

---

## What’s in the database (big picture)

We do **not** store raw EEG samples in the database. Instead we store:

- **patients** → who
- **recordings** → which night/recording
- **epochs** → 30-second slices of time per recording
- **notes** → 1:1 “questionnaire” flags per recording (coffee/alcohol/stress etc.)

From these tables we build **views** that compute sleep metrics such as:
- sleep window / in-bed window
- TST (Total Sleep Time)
- sleep efficiency within the window
- stage minutes (N1/N2/N3/REM/W)
- REM latency
- fragmentation proxies (stage transitions, awakenings)

---

## Tech stack

- **SQLite**: lightweight relational database stored as a single `.db` file
- **sqlite3 CLI** (*command-line interface*): run schema/views/queries reproducibly
- **Python** ETL (*extract–transform–load*): reads Sleep-EDF annotations and loads tidy tables
- Optional: **DBeaver** (interactive DB client + ER diagrams), **Power BI**, **Excel**

---

## Project structure

- `data_raw/` — Sleep-EDF EDF files (kept out of git)
- `data_out/` — generated database and exported CSV artefacts for BI/Excel
- `etl/` — Python scripts (ETL + seeding)
- `sql/` — schema, indexes, views, queries, QA tests
- `docs/` — screenshots (ER diagram, Power BI, Excel dashboard)

---

## Quickstart (end-to-end)

### 0) Requirements
- Python 3.11+
- SQLite (sqlite3)
- Recommended: conda environment (example):
  ```bash
  conda create -n sleep_sql python=3.11
  conda activate sleep_sql
  pip install mne pandas tqdm
  ```

### 1) Create tables + indexes
```bash
sqlite3 data_out/sleepedf_test.db ".read sql/schemas.sql"
sqlite3 data_out/sleepedf_test.db ".read sql/indexes.sql"
```

### 2) Load Sleep-EDF into the DB (ETL)
```bash
conda activate sleep_sql
python etl/01_extract_sleepedf.py
python etl/02_build_epochs.py
```

### 3) (Optional) Seed synthetic fields (for practice)
This fills missing demographics (age/sex/BMI) and questionnaire-style notes (0/1 flags) using a reproducible random seed.
```bash
python etl/03_seed_synthetic.py
```

### 4) Create views and run QA tests
```bash
sqlite3 data_out/sleepedf_test.db ".read sql/views.sql"
sqlite3 -header -column data_out/sleepedf_test.db ".read sql/qa_tests.sql"
```

### 5) Run analysis queries
```bash
sqlite3 -header -column data_out/sleepedf_test.db ".read sql/queries.sql"
```

---

## Power BI / Excel exports

This project uses three “BI-friendly” exports:

- `bi_nights.csv` — one row per recording/night (metrics + demographics + notes + derived QC)
- `bi_stage_minutes.csv` — one row per (night × stage), great for stacked bar charts
- `bi_fragmentation.csv` — fragmentation metrics (transitions, awakenings), normalised rates

**Export tip:** on Windows (German locale), CSVs often open best with `;` as delimiter.  
If you open a CSV and everything appears in one column, import via **Excel → Data → From Text/CSV** and choose the delimiter.

---

## Suggested Power BI dashboard (one page)

**Tables**
- Import: `bi_nights.csv` (main), `bi_stage_minutes.csv`, `bi_fragmentation.csv`

**Relationships**
- Join on `rec_id` (preferred), or `patients_code + rec_code` if you export those.

**Visuals**
- KPI cards: Avg TST (h), Avg sleep efficiency, Avg REM latency, Pass rate
- Stacked bar: stage minutes per night (from `bi_stage_minutes.csv`)
- Scatter: TST vs efficiency, size = transitions, colour = pass_qc (from `bi_fragmentation.csv` + `bi_nights.csv`)
- Table: night metrics + qc_reason + notes flags

Save screenshots to `docs/powerbi_dashboard.png` for your portfolio.

---

## Excel mini-assignment (skills practice)

Recommended workbook: `docs/SleepSQL_Excel_Analysis.xlsx`

Tasks:
- Use **XLOOKUP** to bring `pass_qc`, `qc_reason`, and fragmentation metrics into the main table
- Create **PivotTables** comparing pass vs fail nights (Avg TST, efficiency, REM latency)
- Stacked stage composition pivot + chart
- Conditional formatting for low efficiency / high REM latency

Save a screenshot to `docs/excel_dashboard.png`.

---

## SQL skills demonstrated

**Core**
- SELECT / WHERE / ORDER BY
- JOINs (inner + left joins, anti-join checks)
- GROUP BY aggregates + cohort filtering logic
- CASE / NULL handling
- Normalised schema design (PK/FK, 1:many and 1:1 patterns)
- Indexes for performance

**Advanced / optional**
- CTEs (`WITH ...`)
- Window functions (`LAG`, `ROW_NUMBER`) for transitions and bout-style metrics
- Basic optimisation via `EXPLAIN QUERY PLAN`
- QA checks as repeatable tests (`sql/qa_tests.sql`)

---

## Notes on data provenance

- Sleep staging labels are derived from **Sleep-EDF hypnogram annotations**.
- Some demographic and questionnaire-style fields may be **synthetic** (seeded) for learning and reporting practice.
- This repo is intended for educational/portfolio use.

---

## Licence

Add a licence if you plan to publish publicly (e.g., MIT).  
If you include any Sleep-EDF files locally, keep them in `data_raw/` and out of git.
