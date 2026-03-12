-- ==========================================================
-- Sleep-EDF Database Schema
-- ==========================================================
-- Purpose:
    -- Defines the relational database structure used in the Sleep-EDF project

-- The schema acts as a structure/blueprint:
--   • Tables
--   • Columns
--   • Relationships

-- Hierarchy:
    -- patients → recordings → epochs
    -- recordings → notes

-- Run with:
-- sqlite3 data_out/sleepedf_test.db ".read sql/schemas.sql"
-- ==========================================================


-- ==========================================================
-- 0) SQLite config step
-- ==========================================================
PRAGMA foreign_keys = ON;       -- ensure FKs are enforced


-- ==========================================================
-- 1) Reset tables (when re-running)
-- ==========================================================

PRAGMA foreign_keys = OFF;      -- temporarily disable FK checks to drop tables in any order

DROP TABLE IF EXISTS notes_T;
DROP TABLE IF EXISTS epochs_T;
DROP TABLE IF EXISTS recordings_T;
DROP TABLE IF EXISTS patients_T;

PRAGMA foreign_keys = ON;       -- re-enable FK checks



-- ==========================================================
-- 2) Patients Table
-- ==========================================================
-- Each row = one participant/patient
-- patients_code: assigned ID eg., SC4001
-- age, sex, bmi: constrained for realistic values

CREATE TABLE patients (                                 
    patients_id INTEGER PRIMARY KEY,                    
    patients_code VARCHAR(8) NOT NULL UNIQUE,
    age INT CHECK (age BETWEEN 17 AND 100),             
    sex TEXT CHECK (sex LIKE 'M%' OR sex LIKE 'F%' OR sex LIKE 'O%'),    
    bmi INT CHECK (bmi BETWEEN 1 AND 70)
); 



-- ==========================================================
-- 3) Recordings Table
-- ==========================================================
-- Each row = one sleep session / night / recording
-- patients_id -→ recordings (1:N) 
-- UNIQUE(patients_id, rec_code) ensures no duplicate recording codes eg., E0
-- ON DELETE CASCADE: deleting a patient removes their recordings

CREATE TABLE recordings (
    rec_id INTEGER PRIMARY KEY,
    patients_id INT NOT NULL,
    rec_code VARCHAR(3) NOT NULL,
    psg_filename VARCHAR(30) NOT NULL,
    hyp_filename VARCHAR(30) NOT NULL,
    rec_date TEXT NOT NULL,        -- could be DATE in other RDBMS
    rec_log TEXT,                  -- optional notes
    FOREIGN KEY (patients_id) REFERENCES patients(patients_id) ON DELETE CASCADE,
    UNIQUE (patients_id, rec_code)
);



-- ==========================================================
-- 4) Epochs Table
-- ==========================================================
-- Each row = one 30s epoch within a recording
-- stage_label = sleep stage classification
-- epoch_idx = sequential number of epochs within recording

CREATE TABLE epochs (
    epochs_id INTEGER PRIMARY KEY,
    rec_id INT NOT NULL,
    epoch_idx INT CHECK (epoch_idx >= 0) NOT NULL,
    start_sec INT CHECK (start_sec >= 0) NOT NULL,
    duration_sec INT NOT NULL DEFAULT 30,
    stage_label TEXT NOT NULL CHECK (stage_label IN ('W','N1','N2','N3','REM','UNKNOWN')),
    FOREIGN KEY (rec_id) REFERENCES recordings(rec_id) ON DELETE CASCADE,
    UNIQUE (rec_id, epoch_idx)
);


-- ==========================================================
-- 5) Notes / Questionnaire Table
-- ==========================================================
-- Each row = context information / questionnaire answers for one recording
-- Boolean variables stored as 0/1 for no/yes
-- Can be used to flag sessions w observations

CREATE TABLE notes (
    rec_id  INTEGER PRIMARY KEY,
    had_coffee INT NOT NULL CHECK (had_coffee IN (0,1)),             
    had_alcohol INT NOT NULL CHECK (had_alcohol IN (0,1)),           
    has_pain INT NOT NULL CHECK (has_pain IN (0,1)),                 
    sleep_deprived INT NOT NULL CHECK (sleep_deprived IN (0,1)),     
    stress INT NOT NULL CHECK (stress IN (0,1)),                     
    FOREIGN KEY (rec_id) REFERENCES recordings(rec_id) ON DELETE CASCADE
);



