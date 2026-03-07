-- Ad-hoc means: Created for a specific, immediate purpose — not part of the permanent pipeline.
    -- temporary analysis queries used to explore data or answer a one-off question.

-- Run ad-hoc with:
-- sqlite3 -header -column data_out/sleepedf_T.db ".read sql/queries.sql"

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



-- Q5) CTE example: compare REM percentage across nights    ???????
WITH base AS (
  SELECT
    patients_code,
    tst_min,
    ROUND(100.0 * rem_min / NULLIF(tst_min, 0), 1) AS rem_pct_of_sleep
  FROM v_sleep_summary
)
SELECT *
FROM base
ORDER BY rem_pct_of_sleep DESC;

    -- WITH base AS (...) defines a temporary “named result set”.
    -- You can then query it as if it were a table.
    -- Useful for multi-step calculations (like REM percentage of total sleep).
    -- NULLIF(tst_min, 0) prevents division by zero.



-- Q6) QC-style ranking: nights with most UNKNOWN in sleep window   ???????
SELECT
  patients_code, 
  unknown_pct_window, unknown_pct_window || '%' AS unknown_pct_label
FROM v_sleep_summary
ORDER BY unknown_pct_window DESC;

-- Q7) Distribution check: epochs by stage across all included sessions (aggregation)   ???????
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

-- Q??) Comparing totals w averages !!!!!


-- Q7) Stage transitions per night (fragmentation proxy)
-- Counts how often the stage changes from one epoch to the next within the sleep window.
WITH in_window AS (
  SELECT es.*
  FROM v_epoch_stage es
  JOIN v_sleep_window w ON w.session_id = es.session_id
  WHERE es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
),
base AS (
  SELECT
    subject_code,
    session_id,
    psg_filename,
    epoch_index,
    stage,
    LAG(stage) OVER (PARTITION BY session_id ORDER BY epoch_index) AS prev_stage
  FROM in_window
)
SELECT
  subject_code,
  psg_filename,
  SUM(CASE WHEN prev_stage IS NOT NULL AND stage <> prev_stage THEN 1 ELSE 0 END) AS n_stage_transitions,
  ROUND(
    1.0 * SUM(CASE WHEN prev_stage IS NOT NULL AND stage <> prev_stage THEN 1 ELSE 0 END)
    / NULLIF((SELECT sleep_window_h FROM v_night_sleep_summary ns WHERE ns.psg_filename = base.psg_filename), 0),
    2
  ) AS transitions_per_hour_window
FROM base
GROUP BY subject_code, psg_filename
ORDER BY transitions_per_hour_window DESC;

    -- LAG() is a window function → looks at the previous row’s value.
    -- Helps calculate stage transitions per night as a proxy for fragmentation.
    -- Window functions work on ordered subsets (here, PARTITION BY session_id ORDER BY epoch_index).



-- Q8) Number of awakenings (W bouts) within the sleep window + wake minutes (WASO proxy)
-- A "wake bout" starts when stage becomes W and the previous epoch was not W.
WITH in_window AS (
  SELECT es.*
  FROM v_epoch_stage es
  JOIN v_sleep_window w ON w.session_id = es.session_id
  WHERE es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
),
base AS (
  SELECT
    subject_code,
    session_id,
    psg_filename,
    epoch_index,
    stage,
    LAG(stage) OVER (PARTITION BY session_id ORDER BY epoch_index) AS prev_stage
  FROM in_window
)
SELECT
  subject_code,
  psg_filename,
  ROUND(SUM(CASE WHEN stage='W' THEN 1 ELSE 0 END) * 30.0 / 60.0, 1) AS wake_in_window_min,
  SUM(CASE WHEN stage='W' AND (prev_stage IS NULL OR prev_stage <> 'W') THEN 1 ELSE 0 END) AS n_awakenings,
  ROUND(
    1.0 * SUM(CASE WHEN stage='W' AND (prev_stage IS NULL OR prev_stage <> 'W') THEN 1 ELSE 0 END)
    / NULLIF((SELECT tst_h FROM v_night_sleep_summary ns WHERE ns.psg_filename = base.psg_filename), 0),
    2
  ) AS awakenings_per_hour_sleep
FROM base
GROUP BY subject_code, psg_filename
ORDER BY awakenings_per_hour_sleep DESC;

-- Q9) Longest continuous bouts (N3, REM, and any sleep) within the sleep window
-- Uses a classic window-function trick to label consecutive runs.
WITH in_window AS (
  SELECT es.*
  FROM v_epoch_stage es
  JOIN v_sleep_window w ON w.session_id = es.session_id
  WHERE es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
),
rn AS (
  SELECT
    subject_code,
    session_id,
    psg_filename,
    epoch_index,
    stage,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY epoch_index) AS rn_all,
    ROW_NUMBER() OVER (PARTITION BY session_id, stage ORDER BY epoch_index) AS rn_stage
  FROM in_window
),
runs AS (
  SELECT
    subject_code,
    session_id,
    psg_filename,
    stage,
    (rn_all - rn_stage) AS run_id,
    COUNT(*) AS run_epochs
  FROM rn
  GROUP BY subject_code, session_id, psg_filename, stage, run_id
)
SELECT
  subject_code,
  psg_filename,
  ROUND(COALESCE(MAX(CASE WHEN stage='N3'  THEN run_epochs END), 0) * 30.0 / 60.0, 1) AS longest_n3_min,
  ROUND(COALESCE(MAX(CASE WHEN stage='REM' THEN run_epochs END), 0) * 30.0 / 60.0, 1) AS longest_rem_min,
  ROUND(COALESCE(MAX(CASE WHEN stage IN ('N1','N2','N3','REM') THEN run_epochs END), 0) * 30.0 / 60.0, 1) AS longest_sleep_bout_min
FROM runs
GROUP BY subject_code, psg_filename
ORDER BY longest_sleep_bout_min DESC;

-- Q10) (Optional) Show the first 40 epochs of the sleep window with prev stage (nice for debugging/demonstration)
-- Change 'SC4001' to any subject you like.
WITH in_window AS (
  SELECT es.*
  FROM v_epoch_stage es
  JOIN v_sleep_window w ON w.session_id = es.session_id
  WHERE es.subject_code = 'SC4001'
    AND es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
),
base AS (
  SELECT
    subject_code,
    psg_filename,
    epoch_index,
    stage,
    LAG(stage) OVER (PARTITION BY psg_filename ORDER BY epoch_index) AS prev_stage
  FROM in_window
)
SELECT *
FROM base
ORDER BY epoch_index
LIMIT 40;


-- =========================================
-- Step 7.5: Data QA (LEFT JOIN / anti-join) + Performance
-- =========================================

-- Q11 – Q14 – Data QA / Anti-joins
    -- Checks for missing links in data (e.g., epochs without a stage).
    -- Anti-joins are very common in research and clinical data pipelines for consistency checks.

-- Q11) Anti-join: epochs without a sleep stage (should be 0)
SELECT
  COUNT(*) AS epochs_missing_stage
FROM epochs e
LEFT JOIN sleep_stages st ON st.epoch_id = e.epoch_id
WHERE st.epoch_id IS NULL;

-- Q12) Anti-join: sleep stages without an epoch (should be 0)
SELECT
  COUNT(*) AS stages_missing_epoch
FROM sleep_stages st
LEFT JOIN epochs e ON e.epoch_id = st.epoch_id
WHERE e.epoch_id IS NULL;

-- Q13) Anti-join: sessions with no detected sleep window (no N1/N2/N3/REM)
SELECT
  p.subject_code,
  s.psg_filename,
  s.hyp_filename
FROM sessions s
JOIN participants p ON p.participant_id = s.participant_id
LEFT JOIN v_sleep_window w ON w.session_id = s.session_id
WHERE w.session_id IS NULL
ORDER BY p.subject_code, s.psg_filename;

-- Q14) Anti-join: participants with no sessions (should be 0)
SELECT
  p.subject_code
FROM participants p
LEFT JOIN sessions s ON s.participant_id = p.participant_id
WHERE s.session_id IS NULL
ORDER BY p.subject_code;

-- Q15) Consistency check: per-session stage rows should equal epoch rows (should return 0 rows)
SELECT
  p.subject_code,
  s.psg_filename,
  COUNT(DISTINCT e.epoch_id) AS n_epochs,
  COUNT(DISTINCT st.epoch_id) AS n_stage_rows
FROM participants p
JOIN sessions s ON s.participant_id = p.participant_id
LEFT JOIN epochs e ON e.session_id = s.session_id
LEFT JOIN sleep_stages st ON st.epoch_id = e.epoch_id
GROUP BY p.subject_code, s.psg_filename
HAVING n_epochs <> n_stage_rows
ORDER BY p.subject_code;

-- Q16) Quick “dashboard” via UNION ALL (classic reporting pattern)
SELECT 'participants' AS table_name, COUNT(*) AS n FROM participants
UNION ALL
SELECT 'sessions',     COUNT(*) FROM sessions
UNION ALL
SELECT 'epochs',       COUNT(*) FROM epochs
UNION ALL
SELECT 'sleep_stages', COUNT(*) FROM sleep_stages
UNION ALL
SELECT 'qc_flags',     COUNT(*) FROM qc_epoch_flags;

-- -------------------------
-- Performance / optimisation
-- -------------------------

-- Q17) See how SQLite plans a typical analytic query (EXPLAIN QUERY PLAN)
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

