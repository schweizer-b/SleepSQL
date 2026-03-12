-- ==========================================================
-- Indexes for Sleep-EDF SQL Project
-- ==========================================================
-- These indexes speed up joins, aggregations, and window
-- functions used in queries.sql.
--
-- Run once after the database is built:
-- sqlite3 data_out/sleepedf.db ".read sql/indexes.sql"



-- ==========================================================
-- Foreign-key join indexes
-- ==========================================================

-- recordings → patients join
CREATE INDEX IF NOT EXISTS idx_recordings_patient
ON recordings(patients_id);


-- epochs → recordings join
CREATE INDEX IF NOT EXISTS idx_epochs_rec
ON epochs(rec_id);



-- ==========================================================
-- Window function optimisation
-- ==========================================================
-- Many queries use:
-- PARTITION BY rec_id ORDER BY epoch_idx
--
-- This composite index allows SQLite to retrieve rows
-- already grouped and ordered.

CREATE INDEX IF NOT EXISTS idx_epochs_rec_epoch
ON epochs(rec_id, epoch_idx);



-- ==========================================================
-- Stage filtering
-- ==========================================================
-- Helps stage distribution queries and QC checks.

CREATE INDEX IF NOT EXISTS idx_epochs_stage
ON epochs(stage_label);



