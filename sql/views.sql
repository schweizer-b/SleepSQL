-- ==========================================================
-- Sleep-EDF Derived Views
-- ==========================================================
-- This file defines analytical views used to progressively
-- transform raw sleep staging data into analysis-ready metrics.
--
-- View pipeline:
--
-- notes
--   ↓
-- v_notes
--   ↓
-- v_sleep_boundaries
--   ↓
-- v_in_bed_window
--   ↓
-- v_sleep_summary  (main analytical view)
--
-- Each layer adds structure and derived metrics that simplify
-- downstream SQL analysis queries.
--
-- Run with:
-- sqlite3 data_out/sleepedf_test.db ".read sql/views.sql"
-- ==========================================================


PRAGMA foreign_keys = ON;                                        -- SQLite disables FK enforcement by default



-- ==========================================================
-- CLEANUP PREVIOUS VIEWS
-- ==========================================================
-- Dropping views ensures the script can be re-run safely.

DROP VIEW IF EXISTS v_epoch_stage;
DROP VIEW IF EXISTS v_sleep_window;
DROP VIEW IF EXISTS v_night_sleep_summary;

DROP VIEW IF EXISTS v_notes;
DROP VIEW IF EXISTS v_sleep_boundaries;
DROP VIEW IF EXISTS v_in_bed_window;
DROP VIEW IF EXISTS v_sleep_summary;



-- ==========================================================
-- 1) SESSION NOTES / OBSERVATIONS
-- ==========================================================
-- Includes obs from the questionnaire recorded for each sleep recording (e.g. caffeine intake, alcohol, stress)
-- Two derived fields are created:
--   has_obs: boolean indicator (0/1) showing whether any positve response
--   obs_count: number of positive responses
--
-- These variables may later be used for cohort comparisons or exploratory analyses

DROP VIEW IF EXISTS v_notes;

CREATE VIEW v_notes AS 
SELECT
    r.rec_id,
    r.rec_code,
    n.had_coffee,
    n.had_alcohol,
    n.has_pain,
    n.sleep_deprived,
    n.stress,   
    -- Boolean version (0 or 1)
    CASE            
        WHEN (n.had_coffee + n.had_alcohol + n.has_pain + n.sleep_deprived + n.stress) > 0 
        THEN 1
        ELSE 0
    END AS has_obs,
     -- Count of positive answers
    (n.had_coffee + n.had_alcohol + n.has_pain + n.sleep_deprived + n.stress) 
    AS obs_count
FROM notes n
JOIN recordings r ON r.rec_id = n.rec_id;

-- NB: ❌ SUM() cannot be used directly inside the same row like that
-- ✅ only use SUM() when aggregating across rows



-- ==========================================================
-- 2) SLEEP BOUNDARIES PER SESSION
-- ==========================================================
-- Determines the first and last sleep epochs within each recording to define the sleep window used in most downstream analyses
--
-- first_sleep_epoch
--     first epoch that is not Wake/Unknown (i.e. N1/N2/N3/REM)
-- last_sleep_epoch
--     last epoch that is not Wake/Unknown


DROP VIEW IF EXISTS v_sleep_boundaries;

CREATE VIEW v_sleep_boundaries AS
WITH sleep_epochs AS (
    SELECT
        rec_id,
        MIN(epoch_idx) AS first_sleep_epoch,
        MAX(epoch_idx) AS last_sleep_epoch
    FROM epochs
    WHERE stage_label IN ('N1','N2','N3','REM')
    GROUP BY rec_id
)
SELECT
    r.rec_id,
    p.patients_code,
    r.rec_code,
    COALESCE(v.has_obs, 0) AS has_obs,             -- if v.has_obs is not NULL → return it. if v.has_obs is NULL → return 0
    COALESCE(v.obs_count, 0) AS obs_count,         
    ROUND((se.last_sleep_epoch - se.first_sleep_epoch + 1) * 30.0 / 60.0, 1) AS sleep_window_min,
    se.first_sleep_epoch,
    se.last_sleep_epoch
FROM recordings r
JOIN patients p ON p.patients_id = r.patients_id 
LEFT JOIN v_notes v ON v.rec_id = r.rec_id
JOIN sleep_epochs se ON se.rec_id = r.rec_id;

-- NOTE: maybe COALESCE should not be used in a real case as no answer/NULL is different than a negative/0 for analyses(?)


-- ==========================================================
-- 3) IN-BED WINDOW (WINDOWED EPOCHS VIEW)
-- ==========================================================
-- Extracts only epochs that fall within the sleep window boudaries defined above
-- Each row corresponds to one 30s epoch within the detected sleep window of a recording
--
-- This dataset is used to compute most sleep metrics.

DROP VIEW IF EXISTS v_in_bed_window;

CREATE VIEW v_in_bed_window AS
SELECT
    sb.rec_id,
    sb.patients_code,
    sb.rec_code,
    sb.has_obs,
    sb.first_sleep_epoch,
    sb.sleep_window_min, 
    e.epoch_idx,
    e.stage_label
FROM v_sleep_boundaries sb
JOIN epochs e ON e.rec_id = sb.rec_id
WHERE e.epoch_idx BETWEEN sb.first_sleep_epoch AND sb.last_sleep_epoch;

-- Alternative minimal implementation (kept for ref.):
--
-- DROP VIEW IF EXISTS v_in_bed_window;
-- CREATE VIEW v_in_bed_window AS
-- SELECT
--     e.*
-- FROM epochs e
-- JOIN v_sleep_boundaries sb ON sb.rec_id = e.rec_id
-- WHERE e.epoch_idx BETWEEN sb.first_sleep_epoch AND sb.last_sleep_epoch;


-- ==========================================================
-- 4) NIGHTLY SLEEP SUMMARY (MAIN ANALYTICAL VIEW)
-- ==========================================================
-- Produces one row per recording/session with key sleep metrics
--
-- Metrics include:
--   • sleep window duration
--   • total sleep time (TST)
--   • wake after sleep onset (WASO proxy)
--   • stage durations
--   • stage percentages
--   • sleep efficiency
--   • sleep onset latency
--   • REM latency

DROP VIEW IF EXISTS v_sleep_summary;

CREATE VIEW v_sleep_summary AS
WITH w AS (
    SELECT * FROM v_in_bed_window
),
counts AS (
    SELECT
        rec_id,
        patients_code,
        rec_code,
        has_obs,
        first_sleep_epoch,
        COUNT(*) AS window_epochs, 
        SUM(CASE WHEN stage_label IN ('N1','N2','N3','REM') THEN 1 ELSE 0 END) AS sleep_epochs,
        SUM(CASE WHEN stage_label ='W' THEN 1 ELSE 0 END) AS wake_epochs,
        SUM(CASE WHEN stage_label = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown_epochs,
        SUM(CASE WHEN stage_label IN ('REM') THEN 1 ELSE 0 END) AS rem_epochs,
        SUM(CASE WHEN stage_label IN ('N2','N3') THEN 1 ELSE 0 END) AS N2_N3_epochs,
        MIN(CASE WHEN stage_label='REM' THEN epoch_idx END) AS first_rem_epoch
    FROM w
    GROUP BY rec_id, patients_code, rec_code, has_obs, first_sleep_epoch
)

SELECT 
    rec_id,
    patients_code,
    rec_code,
    has_obs,

  -- Output final KPIs - KPI = Key Performance Indicator

  -- In bed window (minutes/hours)
    ROUND(window_epochs * 30.0 / 60.0, 1) AS in_bed_window_min,
    ROUND(window_epochs * 30.0 / 3600.0, 2) AS in_bed_window_h,
  
  -- Total Sleep Time (TST)
    ROUND(sleep_epochs * 30.0 / 60.0, 1) AS tst_min,
    ROUND(sleep_epochs * 30.0 / 3600.0, 2) AS tst_h,

   -- Wake after sleep onset within the window (WASO proxy)
    ROUND(wake_epochs * 30.0 / 60.0, 1) AS wake_in_window_min,

  -- Stages duration
    ROUND(rem_epochs * 30.0 / 60.0, 1) AS rem_min,
    ROUND(N2_N3_epochs * 30.0 / 60.0, 1) AS N2_N3_min,

-- Percentages inside sleep window
    ROUND(100.0 * sleep_epochs / window_epochs, 1) AS sleep_pct_window,
    ROUND(100.0 * unknown_epochs / window_epochs, 1) AS unknown_pct_window,

   -- Stages percentage of TST
    ROUND(100.0 * rem_epochs / NULLIF(sleep_epochs,0), 1) AS rem_pct_tst,
    ROUND(100.0 * N2_N3_epochs / NULLIF(sleep_epochs,0), 1) AS N2_N3_pct_tst,
    ROUND(100.0 * wake_epochs / NULLIF(sleep_epochs,0), 1) AS wake_pct_tst,

-- Sleep efficiency within window
    ROUND(1.0 * sleep_epochs / window_epochs, 3) AS sleep_eff_window,         -- Why multiply by 1.0? Forces floating-point division

-- Latencies (minutes)
    ROUND(first_sleep_epoch * 30.0 / 60.0, 1) AS sleep_onset_min_from_recording_start,
  CASE
    WHEN first_rem_epoch IS NULL THEN NULL                                  -- Prevents calculating REM latency on missing data 
    ELSE ROUND((first_rem_epoch - first_sleep_epoch) * 30.0 / 60.0, 1)      -- NB: if first_rem_epoch is NULL, the whole expression would return NULL anyway in SQLite, this is more 
  END AS rem_latency_min
FROM counts;