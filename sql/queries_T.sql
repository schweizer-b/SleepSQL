-- Ad-hoc means: Created for a specific, immediate purpose — not part of the permanent pipeline.
    -- temporary analysis queries used to explore data or answer a one-off question.

-- Run ad-hoc with:
-- sqlite3 -header -column data_out/sleepedf_T.db ".read sql/queries.sql"



-- ==========================================================
-- Step 7.5: Data QA (LEFT JOIN / anti-join) + Performance
-- ==========================================================

-- Q10 – Q14 – Data QA / Anti-joins
    -- Checks for missing links in data (e.g., epochs without a stage).
    -- Anti-joins are very common in research and clinical data pipelines for consistency checks.

-- Q11) Anti-join: sessions with no detected sleep window (no N1/N2/N3/REM)
SELECT
  p.patients_code,
  r.psg_filename,
  r.hyp_filename
FROM recordings_T r
JOIN patients_T p ON p.patients_id = r.patients_id
LEFT JOIN v_in_bed_window w ON w.rec_id = r.rec_id
WHERE w.rec_id IS NULL
ORDER BY p.patients_code, r.psg_filename;

-- Q12) Anti-join: participants with no sessions (should be 0)
SELECT
  p.patients_code
FROM patients_T p
LEFT JOIN recordings_T r ON r.patients_id = p.patients_id
WHERE r.rec_id IS NULL
ORDER BY p.patients_code;


-- Q13) Quick “dashboard” via UNION ALL (classic reporting pattern)
SELECT 'patients' AS table_name, COUNT(*) AS n FROM patients_T
UNION ALL
SELECT 'recordings',     COUNT(*) FROM recordings_T
UNION ALL
SELECT 'epochs',       COUNT(*) FROM epochs_T;

-- Q14) mini-report table -- whole rec
SELECT 'epochs_total' AS metric, COUNT(*) AS value FROM epochs_T
UNION ALL
SELECT 'epochs_sleep', COUNT(*) FROM epochs_T WHERE stage_label IN ('N1','N2','N3','REM')
UNION ALL
SELECT 'epochs_wake', COUNT(*) FROM epochs_T WHERE stage_label='W'
UNION ALL
SELECT 'unknown_epochs', COUNT(*) FROM epochs_T WHERE stage_label='UNKNOWN';


-- Q14) mini-report table -- in_bed windown only 
SELECT 'in_bed_epochs_total' AS metric, COUNT(*) AS value FROM v_in_bed_window
UNION ALL
SELECT 'in_bed_epochs_sleep', COUNT(*) FROM v_in_bed_window WHERE stage_label IN ('N1','N2','N3','REM')
UNION ALL
SELECT 'in_bed_epochs_wake', COUNT(*) FROM v_in_bed_window WHERE stage_label='W'
UNION ALL
SELECT 'in_bed_unknown_epochs', COUNT(*) FROM v_in_bed_window WHERE stage_label='UNKNOWN';


-- Q14.5) full-rec vs in_bed windown only 
SELECT
    'full_recording' AS scope,
    COUNT(*) AS epochs_total,
    SUM(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN 1 ELSE 0 END) AS epochs_sleep,
    SUM(CASE WHEN stage_label='W' THEN 1 ELSE 0 END) AS epochs_wake,
    SUM(CASE WHEN stage_label='UNKNOWN' THEN 1 ELSE 0 END) AS epochs_unknown
FROM epochs_T

UNION ALL

SELECT
    'in_bed_window' AS scope,
    COUNT(*) AS epochs_total,
    SUM(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN 1 ELSE 0 END) AS epochs_sleep,
    SUM(CASE WHEN stage_label='W' THEN 1 ELSE 0 END) AS epochs_wake,
    SUM(CASE WHEN stage_label='UNKNOWN' THEN 1 ELSE 0 END) AS epochs_unknown
FROM v_in_bed_window;



-- ================================================================================

-- Q1) Basic join + aggregation: total sleep minutes per session (within sleep window)
SELECT
  v.patients_code,
  r.psg_filename,
  ROUND(
  SUM(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN 1 ELSE 0 END) * 30.0 / 60.0,1) AS total_sleep_min
  FROM v_in_bed_window v
JOIN recordings_T r ON r.rec_id = v.rec_id 
GROUP BY v.patients_code, r.psg_filename
ORDER BY v.patients_code, r.psg_filename;

-- NOTE: v_sleep_summary was not used for practising purposes 

-- Q2) Basic join + aggregation: stage minutes per session (within sleep window)
SELECT
  v.patients_code,
  r.psg_filename,
  v.stage_label,
  ROUND(COUNT(*) * 30.0 / 60.0, 1) AS minutes
  FROM v_in_bed_window v
JOIN recordings_T r ON r.rec_id = v.rec_id 
GROUP BY v.patients_code, r.psg_filename, v.stage_label
ORDER BY v.patients_code, r.psg_filename,
        -- physiological sleep order instead of alphabetical. The CASE is used only inside ORDER BY for sorting:
         CASE v.stage_label WHEN 'W' THEN 0 WHEN 'N1' THEN 1 WHEN 'N2' THEN 2 WHEN 'N3' THEN 3 WHEN 'REM' THEN 4 ELSE 5 END;

-- Purpose:
    -- Counts how many minutes each sleep stage occurred within the sleep window of each session.
    -- Uses v_in_bed_window view to simplify joins.
    -- Aggregates per subject, per session, per stage.
    -- Orders stages in a logical sleep order (W → N1 → N2 → N3 → REM → unknown).


-- Q3) “Night/Sleep summary” view output (one row per night)
SELECT
  patients_code, rec_code,
  in_bed_window_h, tst_h, wake_in_window_min,
  sleep_eff_window, rem_latency_min, 
  rem_pct_tst, N2_N3_pct_tst, unknown_pct_window
FROM v_sleep_summary
ORDER BY patients_code;

-- Purpusoe:
    -- Retrieves precomputed summary metrics from the v_sleep_summary view.
    -- This is basically a report-like extraction; simpler than Q1 because all aggregations are already done.


-- Q4) Cohort selection (HAVING + thresholds) — “analysis-ready nights”
-- Example criteria: TST >= 6h, sleep efficiency >= 0.80, unknown <= 5%
SELECT *
FROM v_sleep_summary
WHERE tst_h >= 6.0
  AND sleep_eff_window >= 0.80
  AND unknown_pct_window <= 5.0
ORDER BY tst_h DESC;

-- Purpose:
    -- Filters for “analysis-ready nights” using thresholds.
    -- Example of applying criteria to a precomputed view.



-- Q5) Compare each patient deep sleep pct with the cohort average 

SELECT
   patients_code,
   tst_min,
   N2_N3_pct_tst,
   ROUND(AVG (N2_N3_pct_tst) OVER (), 1) AS avg_deep_sleep
FROM v_sleep_summary;

-- Q) which rows are outliers 
-- SELECT
--     patients_code,
--     N2_N3_pct_tst,
--     AVG(N2_N3_pct_tst) OVER () AS mean_val,
--     STDDEV(N2_N3_pct_tst) OVER () AS sd_val,
--     CASE
--         WHEN ABS(N2_N3_pct_tst - AVG(N2_N3_pct_tst) OVER ())
--              > 2 * STDDEV(N2_N3_pct_tst) OVER ()
--         THEN 'outlier'
--         ELSE 'normal'
--     END AS status
-- FROM v_sleep_summary;


-- Q6) Subquery and window: select only 

-- SELECT
--     patients_code,
--     tst_min,
--     N2_N3_pct_tst
-- FROM (
--     SELECT
--         patients_code,
--         tst_min,
--         N2_N3_pct_tst,
--         AVG(N2_N3_pct_tst) OVER () AS mean_val,
--         STDDEV(N2_N3_pct_tst) OVER () AS sd_val
--     FROM v_sleep_summary
-- ) t
-- WHERE ABS(N2_N3_pct_tst - mean_val) <= 2 * sd_val;

---- NOTE: SQLite does not have a built-in STDDEV() function

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


-- Q7) QC-style ranking: nights with most UNKNOWN in sleep window   
-- identify nights/sessions with potential data quality issues

SELECT
  patients_code, 
  unknown_pct_window, unknown_pct_window || '%' AS unknown_pct_label   -- concatenates the number with a percent sign
  CASE
        WHEN unknown_pct_window <= 5 THEN 'OK'
        WHEN unknown_pct_window <= 20 THEN 'REVIEW'
        ELSE 'BAD'
    END AS qc_flag
FROM v_sleep_summary
ORDER BY unknown_pct_window DESC;


-- Q8) Distribution check: epochs by stage across all included sessions (aggregation)   
SELECT
  stage_label,
  COUNT(*) AS n_epochs,
  ROUND(COUNT(*) * 30.0 / 3600.0, 2) AS hours_total
FROM epochs_T
GROUP BY stage_label
ORDER BY n_epochs DESC;



-- =========================================
-- Advanced (window functions): Step 7
-- =========================================
-- Note: 1 epoch = 30s = 0.5 minutes

-- Q9) Stage transitions per night (fragmentation proxy)
-- Counts how often the stage changes from one epoch to the next within the sleep window.

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
    1.0 * SUM(                                  -- With 1.0 * SQL sees a decimal number
      CASE
        WHEN b.prev_stage IS NOT NULL AND b.stage_label <> b.prev_stage THEN 1 ELSE 0 
      END) / NULLIF(s.in_bed_window_h, 0), 2)   -- if = 0 --> return NULL
      AS transitions_per_hour_window            -- normalise by sleep window - makes sessions comparable across nights of different length
FROM base b
JOIN v_sleep_summary s ON s.rec_id = b.rec_id
GROUP BY b.patients_code, b.rec_id, s.in_bed_window_h
ORDER BY transitions_per_hour_window DESC;


    -- LAG() is a window function → looks at the previous row’s value.
    -- Helps calculate stage transitions per night as a proxy for fragmentation.
    -- Window functions work on ordered subsets (here, PARTITION BY session_id ORDER BY epoch_index).




-- Q10) Number of awakenings (W bouts) within the sleep window + wake minutes (WASO proxy)
-- A "wake bout" starts when stage becomes W and the previous epoch was not W.

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
    CASE WHEN b.stage_label='W' AND (b.prev_stage IS NULL OR b.prev_stage <> 'W') THEN 1 ELSE 0 END) AS n_awakenings,
  ROUND(
    1.0 * SUM( CASE WHEN b.stage_label='W' AND (b.prev_stage IS NULL OR b.prev_stage <> 'W') THEN 1 ELSE 0 END)
    / NULLIF(v.in_bed_window_h, 0), 2) AS awakenings_per_hour_sleep 
FROM base b
JOIN v_sleep_summary v ON v.rec_id = b.rec_id
GROUP BY b.patients_code, b.rec_id, v.wake_in_window_min
ORDER BY awakenings_per_hour_sleep DESC;


-- Q9) Longest continuous bouts (N3, REM, and any sleep) within the sleep window
-- Uses a classic window-function trick to label consecutive runs.

WITH rn AS (
  SELECT
    rec_id,
    patients_code,
    epoch_idx,
    stage_label,
    ROW_NUMBER() OVER (PARTITION BY rec_id ORDER BY epoch_idx) AS rn_all,                 -- counts every epoch in order for the session (rec_id)
    ROW_NUMBER() OVER (PARTITION BY rec_id, stage_label ORDER BY epoch_idx) AS rn_stage   -- counts epochs of the same stage separately
  FROM v_in_bed_window 
),
runs AS (
  SELECT
    rec_id,
    patients_code,
    epoch_idx,
    stage_label,
    (rn_all - rn_stage) AS run_id,        -- rn_all - rn_stage is constant for consecutive same-stage epochs -→ identifies runs | NOTE: run_id is just a temporary identifier to separate consecutive blocks; it doesn’t matter if it’s 0, 2, 5, etc. What counts is that all epochs in the same run have the same run_id.
    COUNT(*) AS run_epochs                -- number of epochs in this run
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


-- Option 2: ????????????????????????

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





-- -------------------------
-- Performance / optimisation
-- -------------------------

-- Q17) See how SQLite plans a typical analytic query (EXPLAIN QUERY PLAN)
EXPLAIN QUERY PLAN
SELECT
  es.patients_code,
  es.psg_filename,
  es.stage_label,
  COUNT(*) AS n_epochs
FROM v_epoch_stage es
JOIN v_sleep_window w ON w.session_id = es.session_id
WHERE es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
GROUP BY es.subject_code, es.psg_filename, es.stage;


-- Q18) Add a composite index that helps window-range queries (run once)
-- This speeds up filters like: WHERE session_id = ? AND epoch_index BETWEEN a AND b
CREATE INDEX IF NOT EXISTS idx_epochs_session_epochindex
ON epochs(session_id, epoch_index);

    -- Improves query performance, especially for range queries on epochs.
    -- Indexes are part of schemas, but creating them in queries.sql is fine if it’s optional/performance-based.
    

-- Q19) Re-run EXPLAIN after the index (compare output with Q17)
EXPLAIN QUERY PLAN
SELECT
  es.subject_code,
  es.psg_filename,
  es.stage,
  COUNT(*) AS n_epochs
FROM v_epoch_stage es
JOIN v_sleep_window w ON w.session_id = es.session_id
WHERE es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
GROUP BY es.subject_code, es.psg_filename, es.stage;



-- Tiny polish for reproducibility 
-- so that Q18 created index exists when someone else runs your project
CREATE INDEX IF NOT EXISTS idx_epochs_session_epochindex
ON epochs(session_id, epoch_index);

