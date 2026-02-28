-- Run with:
-- sqlite3 -header -column data_out/sleepedf.db ".read sql/qa_tests.sql"

PRAGMA foreign_keys = ON;

-- Test 1: epochs must have 1 stage row
SELECT
  'epochs_missing_stage' AS test_name,
  COUNT(*) AS n_failed
FROM epochs e
LEFT JOIN sleep_stages st ON st.epoch_id = e.epoch_id
WHERE st.epoch_id IS NULL;

-- Test 2: stage rows must reference an epoch
SELECT
  'stages_missing_epoch' AS test_name,
  COUNT(*) AS n_failed
FROM sleep_stages st
LEFT JOIN epochs e ON e.epoch_id = st.epoch_id
WHERE e.epoch_id IS NULL;

-- Test 3: stage label must be in allowed set
SELECT
  'invalid_stage_label' AS test_name,
  COUNT(*) AS n_failed
FROM sleep_stages
WHERE stage NOT IN ('W','N1','N2','N3','REM','UNKNOWN');

-- Test 4: no duplicate epoch indices per session (should be 0 rows)
SELECT
  'duplicate_epoch_index' AS test_name,
  COUNT(*) AS n_failed
FROM (
  SELECT session_id, epoch_index, COUNT(*) AS c
  FROM epochs
  GROUP BY session_id, epoch_index
  HAVING c > 1
);

-- Test 5: sleep window exists for each session (informational)
SELECT
  'sessions_without_sleep_window' AS test_name,
  COUNT(*) AS n_sessions
FROM sessions s
LEFT JOIN v_sleep_window w ON w.session_id = s.session_id
WHERE w.session_id IS NULL;