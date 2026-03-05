-- Run with:
-- sqlite3 data_out/sleepedf.db ".read sql/views.sql"

PRAGMA foreign_keys = ON;                                        -- SQLite disables FK enforcement by default.

-- 1) Convenience view: one row per epoch with participant + session info
DROP VIEW IF EXISTS v_epoch_stage;                               -- If it exists → delete it. Prevents errors when re-running.
CREATE VIEW v_epoch_stage AS                                     --- A VIEW is: A saved query. It does NOT store data. It stores SQL logic.
SELECT                                                           -- This creates: One row per epoch, with participant + session + stage info attached.     
  p.subject_code,
  s.session_id,
  s.session_code,
  s.psg_filename,
  e.epoch_id,
  e.epoch_index,
  e.start_sec,
  e.duration_sec,
  st.stage
FROM participants p       --?????                               -- This connects: participants → sessions → epochs → sleep_stages 
JOIN sessions s     ON s.participant_id = p.participant_id      -- Result: One big flat table. Very analytics-friendly.
JOIN epochs e       ON e.session_id = s.session_id
JOIN sleep_stages st ON st.epoch_id = e.epoch_id;

-- 2) Sleep window per session:
-- first_sleep_epoch = first epoch that is not Wake/Unknown (i.e., N1/N2/N3/REM)
-- last_sleep_epoch  = last epoch that is not Wake/Unknown
DROP VIEW IF EXISTS v_sleep_window;
CREATE VIEW v_sleep_window AS
WITH sleep_epochs AS (                             -- CTE (WITH clause) This creates a temporary mini-table  NB: this ❌ is invalid: WITH w AS v_sleep_window   since CTE is not aliasing an existing table
  SELECT                                                                                                      -- “w” is the result of this SELECT statement.
    session_id,
    MIN(epoch_index) AS first_sleep_epoch,
    MAX(epoch_index) AS last_sleep_epoch
  FROM v_epoch_stage
  WHERE stage IN ('N1','N2','N3','REM')
  GROUP BY session_id
)
SELECT
  s.session_id,
  s.psg_filename,
  s.session_code,
  p.subject_code,
  se.first_sleep_epoch,
  se.last_sleep_epoch,
  -- window length in minutes
  ROUND((se.last_sleep_epoch - se.first_sleep_epoch + 1) * 30.0 / 60.0, 1) AS sleep_window_min    -- Remember +1 !!
FROM sessions s
JOIN participants p ON p.participant_id = s.participant_id
JOIN sleep_epochs se ON se.session_id = s.session_id;



-- 3) Nightly summary (main “portfolio” view) ---- total time of each stage??????????
-- One row per night (per session) with all sleep metrics calculated
DROP VIEW IF EXISTS v_night_sleep_summary;
CREATE VIEW v_night_sleep_summary AS
WITH w AS (
  SELECT * FROM v_sleep_window                             -- STEP 1 — w (sleep window): Shorter name - It contains: session_id first_sleep_epoch last_sleep_epoch sleep_window_min
),
in_window AS (                                             -- STEP 2 — in_window: Take all epochs. Keep only epochs between: first sleep epoch and last sleep epoch
  SELECT
    es.*,                                                     -- es.*: All columns from v_epoch_stage
    w.first_sleep_epoch,                                      -- then we add: first_sleep_epoch  last_sleep_epoch
    w.last_sleep_epoch
  FROM v_epoch_stage es
  JOIN w ON w.session_id = es.session_id
  WHERE es.epoch_index BETWEEN w.first_sleep_epoch AND w.last_sleep_epoch
),
counts AS (                                               -- STEP 3 — counts: 
  SELECT                                                     -- these columns will define one row per session because they go into GROUP BY.
    subject_code,
    session_id,
    session_code,
    psg_filename,
    first_sleep_epoch,
    last_sleep_epoch,
    COUNT(*) AS window_epochs,                               -- counts how many epochs exist inside the sleep window.
    SUM(CASE WHEN stage IN ('N1','N2','N3','REM') THEN 1 ELSE 0 END) AS sleep_epochs,      -- conditional SUM (Sleep epochs): Each row: If stage is sleep → count 1 | Else → count 0  Total = number of sleep epochs
    SUM(CASE WHEN stage = 'W' THEN 1 ELSE 0 END) AS wake_epochs,                           -- counts wake inside sleep window - approximates WASO (Wake After Sleep Onset)
    SUM(CASE WHEN stage = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown_epochs,                  -- counts bad/missing labeling.
    SUM(CASE WHEN stage = 'REM' THEN 1 ELSE 0 END) AS rem_epochs,                          -- counts each sleep stage individually -- 
    SUM(CASE WHEN stage = 'N3'  THEN 1 ELSE 0 END) AS n3_epochs,                              -- eg.., How this works: If row is REM: Return epoch_index
    SUM(CASE WHEN stage = 'N2'  THEN 1 ELSE 0 END) AS n2_epochs,                                                    -- If not: Return NULL
    SUM(CASE WHEN stage = 'N1'  THEN 1 ELSE 0 END) AS n1_epochs,                                                    
    MIN(CASE WHEN stage='REM' THEN epoch_index END) AS first_rem_epoch                        -- Then: If stage = REM → return epoch_index and MIN() picks the smallest REM epoch. That equals: First REM occurrence This is how you compute REM latency.
  FROM in_window                                                                              -- NB: Otherwise → NULL is implied if you don’t specify an ELSE
  GROUP BY subject_code, session_id, session_code, psg_filename, first_sleep_epoch, last_sleep_epoch
)
SELECT                                                   -- FINAL SELECT: Now we convert counts into real metrics.
  subject_code,
  session_code,
  psg_filename,

-- Output final KPIs - KPI = Key Performance Indicator

  -- Sleep window (minutes/hours)
  ROUND(window_epochs * 30.0 / 60.0, 1) AS sleep_window_min,
  ROUND(window_epochs * 30.0 / 3600.0, 2) AS sleep_window_h,

  -- Total Sleep Time (TST)
  ROUND(sleep_epochs * 30.0 / 60.0, 1) AS tst_min,
  ROUND(sleep_epochs * 30.0 / 3600.0, 2) AS tst_h,

  -- Wake after sleep onset within the window (WASO proxy)
  ROUND(wake_epochs * 30.0 / 60.0, 1) AS wake_in_window_min,

  -- Stage minutes
  ROUND(n1_epochs * 30.0 / 60.0, 1) AS n1_min,
  ROUND(n2_epochs * 30.0 / 60.0, 1) AS n2_min,
  ROUND(n3_epochs * 30.0 / 60.0, 1) AS n3_min,
  ROUND(rem_epochs * 30.0 / 60.0, 1) AS rem_min,

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