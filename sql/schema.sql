---> This SQL script is designed to turn Sleep-EDF files into a relational SQLite database for easy querying.

-- Sleep-EDF → SQLite schema (MVP + room to grow)
-- Run with: sqlite3 data_out/sleepedf.db ".read sql/schema.sql"
-- Cmd: sqlite3 .\data_out\sleepedf.db ".read sql/schema.sql"

PRAGMA foreign_keys = ON; -- This turns them on so that references between tables (like participant → session → epoch) are enforced.

-- 1) Participants (one person) - Each participant (person in the study) gets one row.
CREATE TABLE IF NOT EXISTS participants (
  participant_id INTEGER PRIMARY KEY,    -- auto-incremented unique integer --> This column uniquely identifies each row
  subject_code   TEXT NOT NULL UNIQUE,   -- e.g., "SC4001"
  sex       TEXT CHECK (sex IN ('M','F','O')),          -- optional (if you have it)
  age_years INTEGER CHECK (age_years BETWEEN 0 AND 120), -- optional (if you have it)
  notes          TEXT
);

-- 2) Sessions (one night / one study session) -- Each row = one sleep session / night / recording.
CREATE TABLE IF NOT EXISTS sessions (
  session_id     INTEGER PRIMARY KEY,
  participant_id INTEGER NOT NULL,
  session_code   TEXT NOT NULL,           -- e.g., "E0" (from filename)
  psg_filename   TEXT NOT NULL,           -- e.g., "SC4001E0-PSG.edf"
  hyp_filename   TEXT NOT NULL,           -- e.g., "SC4001EC-Hypnogram.edf" (or EH)
  start_datetime TEXT,                    -- ISO string if available
  duration_sec   INTEGER,                 -- recording duration if available
  FOREIGN KEY (participant_id) REFERENCES participants(participant_id) ON DELETE CASCADE,
  UNIQUE (participant_id, session_code)   -- prevents duplicates for the same participant/session. 
);                                        -- A participant cannot have two sessions with the same session_code


-- 3) Epochs (one row per 30s epoch)
CREATE TABLE IF NOT EXISTS epochs (
  epoch_id     INTEGER PRIMARY KEY,
  session_id   INTEGER NOT NULL,
  epoch_index  INTEGER NOT NULL,          -- 0..N-1 -- its order (0,1,2...)
  start_sec    INTEGER NOT NULL,          -- from recording start
  duration_sec INTEGER NOT NULL DEFAULT 30,
  FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
  UNIQUE (session_id, epoch_index),       -- a session
  CHECK (epoch_index >= 0),               -- errors happen - these checks defensive programming at the database level.
  CHECK (start_sec >= 0),
  CHECK (duration_sec > 0)
);

-- 4) Sleep stages (label per epoch) -- One row per epoch, storing the sleep stage label.
CREATE TABLE IF NOT EXISTS sleep_stages (
  epoch_id      INTEGER PRIMARY KEY,      -- 1:1 with epochs
  stage         TEXT NOT NULL,            -- W, N1, N2, N3, REM, UNKNOWN
  source_label  TEXT,                     -- original annotation label (optional)
  FOREIGN KEY (epoch_id) REFERENCES epochs(epoch_id) ON DELETE CASCADE,
  CHECK (stage IN ('W','N1','N2','N3','REM','UNKNOWN'))
);

-- 5) Optional: per-epoch QC (quality control) flags (artefact/bad signal/etc.)
-- Allows for bad data to be marked without deleting rows
CREATE TABLE IF NOT EXISTS qc_epoch_flags (
  qc_id        INTEGER PRIMARY KEY,
  epoch_id     INTEGER NOT NULL,
  flag_type    TEXT NOT NULL,             -- Example flags: "ARTEFACT", "DROP", "UNKNOWN_LABEL"
  flag_value   INTEGER NOT NULL DEFAULT 1,
  notes        TEXT,
  FOREIGN KEY (epoch_id) REFERENCES epochs(epoch_id)
);

-- Helpful indexes (speed up joins and aggregations)
-- Example: filtering all N2 stages → idx_stages_stage helps SQLite find rows faster
-- [table name](colunm)
CREATE INDEX IF NOT EXISTS idx_sessions_participant ON sessions(participant_id);
CREATE INDEX IF NOT EXISTS idx_epochs_session      ON epochs(session_id);
CREATE INDEX IF NOT EXISTS idx_stages_stage        ON sleep_stages(stage);
CREATE INDEX IF NOT EXISTS idx_qc_epoch            ON qc_epoch_flags(epoch_id);




-- ================================================================================

-- QC summary table (demonstrates DML: INSERT/UPDATE)
CREATE TABLE IF NOT EXISTS session_qc (
  session_id     INTEGER PRIMARY KEY,
  pass_qc        INTEGER NOT NULL DEFAULT 0,     -- 0/1
  qc_reason      TEXT,
  evaluated_at   TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_session_qc_pass ON session_qc(pass_qc);


-- ===============================================================================
-- ===============================================================================

-- Big Picture / Relationships
    -- Your schema assumes:
    -- 1 participant → many sessions (w no duplicate session_code)
    -- 1 session → many epochs (w no duplicate epoch_idx)
    -- 1 epoch → exactly 1 sleep stage
    -- One epoch → optionally many QC flags
    -- This is a classic hierarchical structure.


-- “smoke test” insert (optional but useful):

-- sqlite3 .\data_out\sleepedf.db "
-- INSERT OR IGNORE INTO participants(subject_code) VALUES ('SC_TEST');
-- INSERT OR IGNORE INTO sessions(participant_id, session_code, psg_filename, hyp_filename)
-- VALUES ((SELECT participant_id FROM participants WHERE subject_code='SC_TEST'),
--         'E0','TEST-PSG.edf','TEST-Hypnogram.edf');
-- "
    -- If SC_TEST already exists → it does nothing (OR IGNORE prevents error).
    -- OR IGNORE -- Because subject_code is UNIQUE, without it, running this twice would cause an error.
    -- What happens:
        -- It finds the participant_id of SC_TEST using a subquery.
        -- Uses that ID to insert a session.
        -- Again uses OR IGNORE to avoid duplicate insert error.
    -- This tests:
        -- Foreign key constraint (participant must exist)
        -- Composite uniqueness (participant_id + session_code)
        -- Subquery inside VALUES

-- Then check:

-- sqlite3 .\data_out\sleepedf.db "
-- SELECT p.subject_code, s.session_code, s.psg_filename
-- FROM participants p
-- JOIN sessions s ON s.participant_id = p.participant_id;
-- "

    -- What this does:
        -- FROM participants p
        -- Alias participants as p
        -- JOIN sessions s
        -- Alias sessions as s
        -- ON s.participant_id = p.participant_id
    -- This connects both tables via foreign key.
   -- It tests:
        -- Foreign key relationship exists
        -- Join works
        -- Data is correctly linked
