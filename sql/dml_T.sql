-- ==========================================================
-- Sleep-EDF Sample Data / DML with Demonstrations
-- ==========================================================
-- Purpose:
-- Provides sample INSERTs plus demo UPDATEs, transactions, 
-- and inspection queries for testing and QA.
-- ==========================================================

-- ==========================================================
-- 0) Configuration
-- ==========================================================
PRAGMA foreign_keys = OFF;  -- temporarily disable FKs for inserts

-- ==========================================================
-- 1) Sample Participants
-- ==========================================================
INSERT INTO patients (patients_id, patients_code, age, sex, bmi)
VALUES
  (1, 'P001', 25, 'M', 22),
  (2, 'P002', 30, 'F', 27),
  (3, 'P003', 45, 'O', 24);

-- ==========================================================
-- 2) Sample Recordings / Sessions
-- ==========================================================
INSERT INTO recordings (rec_id, patients_id, rec_code, psg_filename, hyp_filename, rec_date, rec_log)
VALUES
  (1, 1, 'A', 'psg_001.edf', 'hyp_001.edf', '2026-03-01', 'No issues'),
  (2, 2, 'A', 'psg_002.edf', 'hyp_002.edf', '2026-03-02', 'Had caffeine before sleep'),
  (3, 3, 'A', 'psg_003.edf', 'hyp_003.edf', '2026-03-03', 'Stressful day');

-- ==========================================================
-- 3) Sample Epochs
-- ==========================================================
INSERT INTO epochs (epochs_id, rec_id, epoch_idx, start_sec, stage_label)
VALUES
  (1, 1, 0, 0, 'W'),
  (2, 1, 1, 30, 'N1'),
  (3, 1, 2, 60, 'N2'),
  (4, 1, 3, 90, 'N3'),
  (5, 1, 4, 120, 'REM'),
  (6, 2, 0, 0, 'W'),
  (7, 2, 1, 30, 'N2'),
  (8, 2, 2, 60, 'N2'),
  (9, 2, 3, 90, 'REM'),
  (10, 3, 0, 0, 'UNKNOWN'),
  (11, 3, 1, 30, 'W'),
  (12, 3, 2, 60, 'N1');

-- ==========================================================
-- 4) Sample Notes / Questionnaire
-- ==========================================================
INSERT INTO notes (rec_id, had_coffee, had_alcohol, has_pain, sleep_deprived, stress)
VALUES
  (1, 0, 0, 0, 0, 0),
  (2, 1, 0, 0, 0, 1),
  (3, 0, 1, 1, 1, 1);

-- ==========================================================
-- 5) Demo Updates / Transaction Examples
-- ==========================================================
-- These are demonstration-only updates.
-- They show how to correct or re-tag without touching raw staging data.

-- ----------------------------------------------------------
-- 5.1 Update single record safely
-- Example: participant 2 was mis-tagged for stress
UPDATE notes
SET stress = 0
WHERE rec_id = 2;

-- Inspect results
SELECT * FROM notes WHERE rec_id = 2;

-- ----------------------------------------------------------
-- 5.2 Tighten criteria and re-tag multiple rows
-- Example: mark all sessions with coffee + stress as 'observed'
UPDATE notes
SET has_coffee = 1, stress = 1
WHERE had_coffee = 1 OR stress = 1;

-- Inspect results
SELECT * FROM notes;

-- ----------------------------------------------------------
-- 5.3 Transaction demo (safe + reversible)
-- BEGIN a transaction
BEGIN TRANSACTION;

-- Temporary adjustment: mark UNKNOWN epochs as W for demo
UPDATE epochs
SET stage_label = 'W'
WHERE stage_label = 'UNKNOWN';

-- Inspect intermediate results
SELECT * FROM epochs WHERE rec_id = 3;

-- Rollback to original state
ROLLBACK;  -- All changes undone

-- Inspect to confirm rollback
SELECT * FROM epochs WHERE rec_id = 3;

-- ==========================================================
-- 6) Restore FK enforcement
-- ==========================================================
PRAGMA foreign_keys = ON;