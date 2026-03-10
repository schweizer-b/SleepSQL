-- Run with:
-- sqlite3 data_out/sleepedf_T.db ".read sql/indexes.sql"

CREATE INDEX idx_recordings_patient
ON recordings_T(patients_id);

CREATE INDEX idx_epochs_rec
ON epochs_T(rec_id);

-- Index to speed up window functions and joins on epochs
CREATE INDEX idx_epochs_rec_epoch
ON epochs_T(rec_id, epoch_idx);