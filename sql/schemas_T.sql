-- Purpose: 
    --define the database structure. The schema is the blueprint: what tables exist, which columns they have, 
    --and how they link together.
-- Run with: sqlite3 data_out/sleepedf_T.db ".read sql/schemas_T.sql"

PRAGMA foreign_keys = ON; -- This turns them on so that references between tables (like participant → session → epoch) are enforced.

-- -- Disable FK checks to safely drop tables
-- PRAGMA foreign_keys = OFF;

-- -- Drop child → parent
-- DROP TABLE IF EXISTS session_qc;
-- DROP TABLE IF EXISTS qc_epoch_flags;
-- DROP TABLE IF EXISTS sleep_stages;
-- DROP TABLE IF EXISTS epochs;
-- DROP TABLE IF EXISTS sessions;
-- DROP TABLE IF EXISTS participants;

-- -- Turn FK checks back on
-- PRAGMA foreign_keys = ON;


PRAGMA foreign_keys = OFF;   -- turn off FKs to allow dropping tables

DROP TABLE IF EXISTS notes_T;
DROP TABLE IF EXISTS epochs_T;
DROP TABLE IF EXISTS recordings_T;
DROP TABLE IF EXISTS patients_T;

PRAGMA foreign_keys = ON;    -- re-enable FKs

-- 1) Participants (one person) - Each participant (person in the study) gets one row.

CREATE TABLE patients_T (                                 
    patients_id INTEGER PRIMARY KEY,                    -- w PK one should always use INTEGER ant not INT
    patients_code VARCHAR(8) NOT NULL UNIQUE,
    age INT CHECK (age between 17 and 100),             -- or: age INT CHECK (age >= 17 AND age <= 100)
    sex TEXT CHECK (sex LIKE 'M%' OR sex LIKE 'F%' OR sex LIKE 'O%')    -- or to be more controlled: sex TEXT CHECK (sex IN ('M','F','O'))   or   sex = 'M' OR sex = 'F' OR sex = 'W' OR sex = 'O'
); 


-- 2) Sessions (one night / one study session) -- Each row = one sleep session / night / recording.

CREATE TABLE recordings_T (
    rec_id INTEGER PRIMARY KEY,
    patients_id INT NOT NULL,
    rec_code VARCHAR(3) NOT NULL,
    psg_filename VARCHAR(30) NOT NULL,
    hyp_filename VARCHAR(30) NOT NULL,
    rec_date TEXT NOT NULL,                                                       -- PostgreSQL: DATE
    rec_log TEXT,                                                                 -- PostgreSQL: TIMESTAMPTZ
    FOREIGN KEY (patients_id) REFERENCES patients_T(patients_id) ON DELETE CASCADE,
    UNIQUE (patients_id, rec_code)
);

-- 3) Epochs (one row per 30s epoch)

CREATE TABLE epochs_T (
    epochs_id INTEGER PRIMARY KEY,
    rec_id INT NOT NULL,
    epoch_idx INT CHECK (epoch_idx >= 0) NOT NULL,
    start_sec INT CHECK (start_sec >= 0) NOT NULL,
    duration_sec INT NOT NULL DEFAULT 30,
    stage_label VARCHAR(10) NOT NULL,
    FOREIGN KEY (rec_id) REFERENCES recordings_T(rec_id) ON DELETE CASCADE,
    UNIQUE (rec_id, epoch_idx)
);

-- 4) NOTE Sleep stages (label per epoch) was incl in Epochs table

-- 5) Questionnaire answers

CREATE TABLE notes_T (
    rec_id  INTEGER PRIMARY KEY,
    had_coffee INT NOT NULL CHECK (had_coffee IN (0,1)),             -- PostgreSQL: BOOLEAN
    had_alcohol INT NOT NULL CHECK (had_alcohol IN (0,1)),           -- PostgreSQL: BOOLEAN
    has_pain INT NOT NULL CHECK (has_pain IN (0,1)),                 -- PostgreSQL: BOOLEAN
    sleep_deprived INT NOT NULL CHECK (sleep_deprived IN (0,1)),     -- PostgreSQL: BOOLEAN
    stress INT NOT NULL CHECK (stress IN (0,1)),                     -- PostgreSQL: BOOLEAN
    FOREIGN KEY (rec_id) REFERENCES recordings_T(rec_id) ON DELETE CASCADE
);


--- INSERT VALUES + LINK TO EEG/fMRI + QUESTIONARRIES  !!!
--- Commit???????????