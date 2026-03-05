-- Run with:
-- sqlite3 data_out/sleepedf_T.db ".read sql/views_T.sql"

PRAGMA foreign_keys = ON;                                        -- SQLite disables FK enforcement by default.

DROP VIEW IF EXISTS v_epoch_stage;
DROP VIEW IF EXISTS v_sleep_window;
DROP VIEW IF EXISTS v_night_sleep_summary;


-- 1) View combining notes/obs

DROP VIEW IF EXISTS v_notes;
CREATE VIEW v_notes AS 
SELECT
    rec_id,
    had_coffee,
    had_alcohol,
    has_pain,
    sleep_deprived,
    stress,   
    CASE            -- Boolean version (0 or 1)
        WHEN (had_coffee + had_alcohol + has_pain + sleep_deprived + stress) > 0 
        THEN 1
        ELSE 0
    END AS has_obs,

    (had_coffee + had_alcohol + has_pain + sleep_deprived + stress)  -- -- Count of positive answers
    AS obs_count
FROM notes_T;

-- NB: ❌ You cannot use SUM() directly inside the same row like that. ✅ You can use SUM() when aggregating across rows.



-- 2) Sleep boundaries to calculate window per recording/session:
-- first_sleep_epoch = first epoch that is not Wake/Unknown (i.e., N1/N2/N3/REM)
-- last_sleep_epoch  = last epoch that is not Wake/Unknown
DROP VIEW IF EXISTS v_sleep_boundaries;
CREATE VIEW v_sleep_boundaries AS
WITH sleep_epochs AS (
    SELECT
        rec_id,
        MIN(epoch_idx) AS first_sleep_epoch,
        MAX(epoch_idx) AS last_sleep_epoch
    FROM epochs_T
    WHERE stage_label IN ('N1','N2','N3','REM')
    GROUP BY rec_id )
SELECT
    r.rec_id,
    p.patients_code,
    r.rec_code,
    v.has_obs,
    v.obs_count,
    ROUND((se.last_sleep_epoch - se.first_sleep_epoch + 1) * 30.0 / 60.0, 1) AS sleep_window_min,
    se.first_sleep_epoch,
    se.last_sleep_epoch
FROM recordings_T r
JOIN patients_T p ON p.patients_id = r.patients_id 
JOIN v_notes v ON v.rec_id = r.rec_id
JOIN sleep_epochs se ON se.rec_id = r.rec_id;

-- 3) In bed window per recording/session / Windowed Epochs View:
-- DROP VIEW IF EXISTS v_in_bed_window;
-- CREATE VIEW v_in_bed_window AS
-- SELECT
--     e.*
-- FROM epochs_T e
-- JOIN v_sleep_boundaries sb ON sb.rec_id = e.rec_id
-- WHERE e.epoch_idx BETWEEN sb.first_sleep_epoch AND sb.last_sleep_epoch;

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
    JOIN epochs_T e ON e.rec_id = sb.rec_id
    WHERE e.epoch_idx BETWEEN sb.first_sleep_epoch AND sb.last_sleep_epoch;

-- 4) Nightly summary (main “portfolio” view) -- One row per night (per session) with all sleep metrics calculated:
    -- Total sleep time and sleep letency (onset)
    -- Total stage time and stage letency (REM and Deep sleep N2/N3)
    -- Number of W in the sleep window (fragmented sleep)

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

    -- REM percentage of TST
    ROUND(100.0 * rem_epochs / NULLIF(sleep_epochs,0), 1) AS rem_pct_tst,

  -- Wake after sleep onset within the window (WASO proxy)
    ROUND(wake_epochs * 30.0 / 60.0, 1) AS wake_in_window_min,

  -- Stages duration
    ROUND(rem_epochs * 30.0 / 60.0, 1) AS rem_min,
    ROUND(N2_N3_epochs * 30.0 / 60.0, 1) AS N2_N3_min,

-- Percentages inside sleep window
    ROUND(100.0 * sleep_epochs / window_epochs, 1) AS sleep_pct_window,
    ROUND(100.0 * unknown_epochs / window_epochs, 1) AS unknown_pct_window,


-- Sleep efficiency within window
    ROUND(1.0 * sleep_epochs / window_epochs, 3) AS sleep_eff_window,         -- Why multiply by 1.0? Forces floating-point division

-- Latencies (minutes)
    ROUND(first_sleep_epoch * 30.0 / 60.0, 1) AS sleep_onset_min_from_recording_start,
  CASE
    WHEN first_rem_epoch IS NULL THEN NULL                                  -- Prevents calculating REM latency on missing data. NB: if first_rem_epoch is NULL, the whole expression would return NULL anyway in SQLite, this is more 
    ELSE ROUND((first_rem_epoch - first_sleep_epoch) * 30.0 / 60.0, 1)          
  END AS rem_latency_min
FROM counts;













