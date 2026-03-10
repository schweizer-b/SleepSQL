-- ==========================================================
-- Sleep-EDF SQL Analysis Queries
-- ==========================================================
-- Ad-hoc queries used for exploratory analysis and QA. - temporary analysis queries used to explore data or answer a one-off question.
-- Ad-hoc means: created for a specific analysis purpose,
-- not part of the permanent/ETL pipeline.

-- TABLE OF CONTENTS
    -- 1. Session-level sleep metrics
    -- 2. Cohort statistics & QC flags
    -- 3. Advanced temporal analysis (window functions)
    -- 4. Performance & query optimisation

-- Run with:
-- sqlite3 -header -column data_out/sleepedf_T.db ".read sql/queries.sql"



-- ==========================================================
-- 1. SESSION-LEVEL SLEEP ANALYSIS
-- ==========================================================



-- Q7) Total sleep minutes per session (within sleep window)
-- Interpretation:
-- Computes total sleep time per recording by counting epochs
-- labelled as sleep stages and converting them to minutes
-- (each epoch = 30 seconds). This is a basic estimate of
-- Total Sleep Time (TST) within the detected sleep window.

SELECT
  v.patients_code,
  r.psg_filename,
  ROUND(
  SUM(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN 1 ELSE 0 END) * 30.0 / 60.0,1) AS total_sleep_min
  FROM v_in_bed_window v
JOIN recordings_T r ON r.rec_id = v.rec_id 
GROUP BY v.patients_code, r.psg_filename
ORDER BY v.patients_code, r.psg_filename;



-- Q8) Stage minutes per session
-- Interpretation:
-- Computes the duration of each stage per session within the sleep window.
-- The ORDER BY CASE ensures stages are presented in physiological order:
-- W → N1 → N2 → N3 → REM → unknown.

SELECT
  v.patients_code,
  r.psg_filename,
  v.stage_label,
  ROUND(COUNT(*) * 30.0 / 60.0, 1) AS minutes
  FROM v_in_bed_window v
JOIN recordings_T r ON r.rec_id = v.rec_id 
GROUP BY v.patients_code, r.psg_filename, v.stage_label
ORDER BY v.patients_code, r.psg_filename,
         CASE v.stage_label
             WHEN 'W' THEN 0
             WHEN 'N1' THEN 1
             WHEN 'N2' THEN 2
             WHEN 'N3' THEN 3
             WHEN 'REM' THEN 4
             ELSE 5
         END;



-- Q9) Sleep summary view output
-- Interpretation:
-- Displays the core sleep metrics calculated in the summary view.
-- Each row corresponds to a recording and provides clinically
-- relevant indicators such as:
--   • Total Sleep Time (TST)
--   • Wake after sleep onset (WASO proxy)
--   • Sleep efficiency
--   • REM latency

SELECT
  patients_code, rec_code,
  in_bed_window_h, tst_h, wake_in_window_min,
  sleep_eff_window, rem_latency_min,
  rem_pct_tst, N2_N3_pct_tst, unknown_pct_window
FROM v_sleep_summary
ORDER BY patients_code;



-- Q10) Cohort filtering for analysis-ready nights
-- Interpretation:
-- Filters sessions that meet typical quality thresholds used
-- in sleep studies:
--   • at least 6 hours of sleep
--   • sleep efficiency ≥ 80%
--   • ≤ 5% unknown epochs
-- These criteria define "analysis-ready" nights.

SELECT *
FROM v_sleep_summary
WHERE tst_h >= 6.0
  AND sleep_eff_window >= 0.80
  AND unknown_pct_window <= 5.0
ORDER BY tst_h DESC;



-- ==========================================================
-- 2. COHORT STATISTICS & QUALITY CONTROL
-- ==========================================================



-- Q11) Compare each patient’s deep sleep percentage with cohort average
-- Interpretation:
-- Compares each recording's deep sleep percentage
-- (N2 + N3) against the overall cohort average.
-- The window function AVG() OVER () calculates the
-- cohort mean without collapsing rows.

SELECT
   patients_code,
   tst_min,
   N2_N3_pct_tst,
   ROUND(AVG (N2_N3_pct_tst) OVER (), 1) AS avg_deep_sleep
FROM v_sleep_summary;



-- Q12) Detect statistical outliers (2 SD from mean)
-- Interpretation:
-- Identifies statistical outliers based on the
-- "two standard deviations from the mean" rule.
-- Sessions outside ±2 SD would normally be flagged
-- for further inspection in data quality control.

SELECT *
FROM (
    SELECT
        patients_code,
        tst_min,
        N2_N3_pct_tst,
        ROUND(AVG(N2_N3_pct_tst) OVER (), 2) AS mean_val,
        SQRT(
            AVG(N2_N3_pct_tst * N2_N3_pct_tst) OVER ()
            - AVG(N2_N3_pct_tst) OVER () * AVG(N2_N3_pct_tst) OVER ()
        ) AS sd_val
    FROM v_sleep_summary
) t
WHERE ABS(N2_N3_pct_tst - mean_val) <= 2 * sd_val;



-- Q13) QC ranking: sessions with highest UNKNOWN percentage
-- Interpretation:
-- Categorises sessions based on unknown epoch proportion.
-- Helps quickly identify nights with potential data quality issues.

SELECT
  patients_code,
  unknown_pct_window,
  unknown_pct_window || '%' AS unknown_pct_label            -- concatenates the number with a percent sign

  CASE
        WHEN unknown_pct_window <= 5 THEN 'OK'
        WHEN unknown_pct_window <= 20 THEN 'REVIEW'
        ELSE 'BAD'
  END AS qc_flag
FROM v_sleep_summary
ORDER BY unknown_pct_window DESC;



-- Q14) Stage distribution across dataset
-- Interpretation:
-- Shows the distribution of sleep stages across the
-- entire dataset. The hours_total column converts
-- epoch counts into hours to give a more intuitive
-- sense of dataset composition.

SELECT
  stage_label,
  COUNT(*) AS n_epochs,
  ROUND(COUNT(*) * 30.0 / 3600.0, 2) AS hours_total
FROM epochs_T
GROUP BY stage_label
ORDER BY n_epochs DESC;



-- ==========================================================
-- 3. ADVANCED TEMPORAL ANALYSIS (WINDOW FUNCTIONS)
-- ==========================================================



-- Q15) Stage transitions per night (sleep fragmentation proxy)
-- Interpretation:
-- Counts transitions between sleep stages across
-- consecutive epochs. Frequent transitions indicate
-- fragmented sleep architecture.

WITH base AS (
  SELECT
    v.rec_id,
    v.patients_code,
    v.epoch_idx,
    v.stage_label,
    LAG(v.stage_label) OVER (PARTITION BY v.rec_id ORDER BY v.epoch_idx) AS prev_stage
  FROM v_in_bed_window v
)

SELECT
  b.patients_code,
  b.rec_id,
  SUM(
    CASE
      WHEN b.prev_stage IS NOT NULL AND b.stage_label <> b.prev_stage THEN 1 ELSE 0
    END
  ) AS n_stage_transitions,
  ROUND(
    1.0 * SUM(                                                  -- With 1.0 * SQL sees a decimal number
      CASE                                  
        WHEN b.prev_stage IS NOT NULL AND b.stage_label <> b.prev_stage THEN 1 ELSE 0
      END) / NULLIF(s.in_bed_window_h, 0), 2                    -- if = 0 --> return NULL
  ) AS transitions_per_hour_window                              -- normalise by sleep window - makes sessions comparable across nights of different length
FROM base b
JOIN v_sleep_summary s ON s.rec_id = b.rec_id
GROUP BY b.patients_code, b.rec_id, s.in_bed_window_h
ORDER BY transitions_per_hour_window DESC;



-- Q16) Number of awakenings within sleep window
-- Interpretation:
-- Detects awakenings by counting transitions
-- from any non-wake stage into W. This approximates
-- Wake After Sleep Onset (WASO) events.

WITH base AS (
  SELECT
    rec_id,
    patients_code,
    epoch_idx,
    stage_label,
    LAG(in_bed.stage_label) OVER (PARTITION BY in_bed.rec_id ORDER BY in_bed.epoch_idx) AS prev_stage
  FROM v_in_bed_window AS in_bed
)

SELECT
  b.patients_code,
  b.rec_id,
  v.wake_in_window_min,
  SUM(
    CASE WHEN b.stage_label='W' AND (b.prev_stage IS NULL OR b.prev_stage <> 'W') THEN 1 ELSE 0 END
  ) AS n_awakenings,
  ROUND(
    1.0 * SUM(
      CASE WHEN b.stage_label='W' AND (b.prev_stage IS NULL OR b.prev_stage <> 'W') THEN 1 ELSE 0 END
    ) / NULLIF(v.in_bed_window_h, 0), 2
  ) AS awakenings_per_hour_sleep
FROM base b
JOIN v_sleep_summary v ON v.rec_id = b.rec_id
GROUP BY b.patients_code, b.rec_id, v.wake_in_window_min
ORDER BY awakenings_per_hour_sleep DESC;



-- Q17) Longest continuous sleep bouts (N3, REM, and any sleep)
-- Interpretation:
-- Calculates the longest uninterrupted sleep bouts.
-- Long runs of N3 represent consolidated deep sleep,
-- while REM bouts indicate stable REM cycles.

WITH rn AS (
  SELECT
    rec_id,
    patients_code,
    epoch_idx,
    stage_label,
    ROW_NUMBER() OVER (PARTITION BY rec_id ORDER BY epoch_idx) AS rn_all,                   -- counts every epoch in order for the session (rec_id)
    ROW_NUMBER() OVER (PARTITION BY rec_id, stage_label ORDER BY epoch_idx) AS rn_stage     -- counts epochs of the same stage separately
  FROM v_in_bed_window
), 

runs AS (
  SELECT
    rec_id,
    patients_code,
    epoch_idx,
    stage_label,
    (rn_all - rn_stage) AS run_id,      -- identifies consecutive runs
    COUNT(*) AS run_epochs              -- number of epochs in this run
  FROM rn
  GROUP BY rec_id, patients_code, stage_label, run_id
)

SELECT
  rec_id,
  patients_code,
  ROUND(COALESCE(MAX(CASE WHEN stage_label='N3'  THEN run_epochs END), 0) * 30.0 / 60.0, 1) AS longest_n3_min,
  ROUND(COALESCE(MAX(CASE WHEN stage_label='REM' THEN run_epochs END), 0) * 30.0 / 60.0, 1) AS longest_rem_min,
  ROUND(COALESCE(MAX(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN run_epochs END), 0) * 30.0 / 60.0, 1) AS longest_sleep_bout_min
FROM runs
GROUP BY rec_id, patients_code
ORDER BY longest_sleep_bout_min DESC;



-- Alternative implementation using pure window functions

SELECT
    rec_id,
    patients_code,
    ROUND(COALESCE(MAX(CASE WHEN stage_label='N3'  THEN run_length END), 0) * 30.0 / 60.0, 1) AS longest_n3_min,
    ROUND(COALESCE(MAX(CASE WHEN stage_label='REM' THEN run_length END), 0) * 30.0 / 60.0, 1) AS longest_rem_min,
    ROUND(COALESCE(MAX(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN run_length END), 0) * 30.0 / 60.0, 1) AS longest_sleep_bout_min
FROM (
    SELECT
        rec_id,
        patients_code,
        stage_label,
        COUNT(*) OVER (PARTITION BY rec_id, stage_label, run_id) AS run_length
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY rec_id ORDER BY epoch_idx)
            - ROW_NUMBER() OVER (PARTITION BY rec_id, stage_label ORDER BY epoch_idx) AS run_id
        FROM v_in_bed_window
    ) t
) x
GROUP BY rec_id, patients_code
ORDER BY longest_sleep_bout_min DESC;



-- ==========================================================
-- 4. PERFORMANCE / QUERY OPTIMISATION
-- ==========================================================
-- These queries demonstrate how SQLite executes analytical
-- queries and how indexes improve performance.



-- ----------------------------------------------------------
-- Q6.1 Inspect the query plan of a typical analytical query
-- ----------------------------------------------------------
-- EXPLAIN QUERY PLAN shows how SQLite will execute a query.
-- Look for:
--   SCAN  → full table scan (slow)
--   SEARCH → index lookup (faster)

EXPLAIN QUERY PLAN
SELECT
  stage_label,
  COUNT(*) AS n_epochs
FROM epochs_T
GROUP BY stage_label;



-- ----------------------------------------------------------
-- Q6.2 Example: query plan for window-function query
-- ----------------------------------------------------------

EXPLAIN QUERY PLAN
SELECT
  rec_id,
  epoch_idx,
  stage_label,
  LAG(stage_label) OVER (PARTITION BY rec_id ORDER BY epoch_idx)
FROM epochs_T;



-- ----------------------------------------------------------
-- Q6.3 Create a composite index for window queries
-- ----------------------------------------------------------
-- This index helps queries that:
--   PARTITION BY rec_id
--   ORDER BY epoch_idx

CREATE INDEX IF NOT EXISTS idx_epochs_rec_epoch_perf
ON epochs_T(rec_id, epoch_idx);



-- ----------------------------------------------------------
-- Q6.4 Re-run query plan after index creation
-- ----------------------------------------------------------
-- Compare the output with Q6.2.

EXPLAIN QUERY PLAN
SELECT
  rec_id,
  epoch_idx,
  stage_label,
  LAG(stage_label) OVER (PARTITION BY rec_id ORDER BY epoch_idx)
FROM epochs_T;