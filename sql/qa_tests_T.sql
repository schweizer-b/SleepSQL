-- ==========================================================
-- Sleep-EDF SQL QA Tests
-- ==========================================================
-- These queries validate that the dataset was loaded correctly
-- and that the relationships between tables are consistent.
--
-- They are designed to catch ETL errors, missing data, or
-- unexpected anomalies before performing analytical queries.
--
-- Expected behaviour:
--   • Anti-joins should usually return zero rows
--   • Counts should match expectations for the dataset
--   • Unknown stages should remain very small

-- Run with:
-- sqlite3 -header -column data_out/sleepedf_T.db ".read sql/qa_tests.sql"


-- ==========================================================
-- 1. DATA INTEGRITY / PIPELINE QA
-- ==========================================================
-- These queries verify the dataset was loaded correctly and
-- check for missing relationships between tables.



-- Q1) Anti-join: sessions with no detected sleep window
-- (no N1/N2/N3/REM epochs in the recording)
-- Interpretation:
-- Each recording should normally contain at least one sleep stage.
-- If rows appear here, the pipeline may have failed to detect
-- sleep onset or stage labels.

SELECT
  p.patients_code,
  r.psg_filename,
  r.hyp_filename
FROM recordings_T r
JOIN patients_T p ON p.patients_id = r.patients_id
LEFT JOIN v_in_bed_window w ON w.rec_id = r.rec_id
WHERE w.rec_id IS NULL
ORDER BY p.patients_code, r.psg_filename;



-- Q2) Anti-join: participants with no sessions (should be zero)
-- Interpretation:
-- Every participant in the patients table should have at least
-- one recording session. Rows here indicate orphaned records.

SELECT
  p.patients_code
FROM patients_T p
LEFT JOIN recordings_T r ON r.patients_id = p.patients_id
WHERE r.rec_id IS NULL
ORDER BY p.patients_code;



-- ==========================================================
-- 2. DATASET SIZE CHECKS
-- ==========================================================
-- These checks provide a quick overview of the dataset scale.
-- They are useful to confirm that ETL loaded all expected rows.



-- Q3) Dataset size dashboard
-- Interpretation:
-- Displays the number of rows in each core table. These numbers
-- should remain stable across pipeline runs.

SELECT 'patients' AS table_name, COUNT(*) AS n FROM patients_T
UNION ALL
SELECT 'recordings', COUNT(*) FROM recordings_T
UNION ALL
SELECT 'epochs', COUNT(*) FROM epochs_T;



-- Q4) Epoch summary for full recordings
-- Interpretation:
-- Shows the distribution of epoch labels across the entire dataset.
-- Useful to verify that stage labels were mapped correctly during ETL.

SELECT 'epochs_total' AS metric, COUNT(*) AS value FROM epochs_T
UNION ALL
SELECT 'epochs_sleep', COUNT(*) FROM epochs_T WHERE stage_label IN ('N1','N2','N3','REM')
UNION ALL
SELECT 'epochs_wake', COUNT(*) FROM epochs_T WHERE stage_label='W'
UNION ALL
SELECT 'unknown_epochs', COUNT(*) FROM epochs_T WHERE stage_label='UNKNOWN';



-- Q5) Epoch summary within the sleep window only
-- Interpretation:
-- Similar to Q4 but restricted to epochs inside the detected
-- sleep window (after sleep onset). Helps verify that the
-- window detection logic behaved correctly.

SELECT 'in_bed_epochs_total' AS metric, COUNT(*) AS value FROM v_in_bed_window
UNION ALL
SELECT 'in_bed_epochs_sleep', COUNT(*) FROM v_in_bed_window WHERE stage_label IN ('N1','N2','N3','REM')
UNION ALL
SELECT 'in_bed_epochs_wake', COUNT(*) FROM v_in_bed_window WHERE stage_label='W'
UNION ALL
SELECT 'in_bed_unknown_epochs', COUNT(*) FROM v_in_bed_window WHERE stage_label='UNKNOWN';



-- Q6) Full recording vs in-bed window comparison
-- Interpretation:
-- Compares the entire recording to the detected sleep window.
-- The window should normally contain fewer epochs than the full
-- recording because pre-sleep wake is excluded.

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



-- ==========================================================
-- 3. ADDITIONAL QA CHECKS
-- ==========================================================
-- These checks catch subtle ETL or dataset issues not covered above


-- Q7) Missing stage labels
-- Interpretation:
-- Ensures all epochs have a stage_label assigned.
-- Any rows returned indicate missing or incomplete data.

SELECT COUNT(*)
FROM epochs_T
WHERE stage_label IS NULL;



-- Q8) Invalid stage labels
-- Interpretation:
-- Ensures all stage labels belong to the expected set.
-- Any row returned indicates unexpected labels that may break analyses.

SELECT DISTINCT stage_label
FROM epochs_T
WHERE stage_label NOT IN ('W','N1','N2','N3','REM','UNKNOWN');



-- Q9) Epoch continuity check
-- Interpretation:
-- Detects missing epochs within each recording.
-- Expected: MAX(epoch_idx) - MIN(epoch_idx) + 1 = COUNT(*)
-- Any difference indicates gaps in the recording.

SELECT rec_id,
       COUNT(*) AS epochs,
       MAX(epoch_idx) - MIN(epoch_idx) + 1 AS expected_epochs
FROM epochs_T
GROUP BY rec_id
HAVING epochs <> expected_epochs;