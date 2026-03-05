# What This Script Achieves
# participants table:
#     1 | SC4001
#     2 | SC4002
# sessions table:
#     10 | 1 | E0 | SC4001E0-PSG.edf | SC4001EC-Hypnogram.edf | ...


from __future__ import annotations  # ????

import re                           # Used for regex (pattern matching filenames)
import sqlite3                      # Lets Python talk to your SQLite database
from dataclasses import dataclass   # Used to create a clean container class (FilePair)
from pathlib import Path            # Used to handle file paths safely
from typing import Optional         # For type hints (means "this value might be None")

import mne                          # Used to read EDF files -- Magnetoencephalography (MEG) + Neurophysiology + Electrophysiology (EEG
from tqdm import tqdm               # Creates a progress bar in terminal


# -----------------------------
# Helpers: filename parsing
# -----------------------------

# Regex = smart pattern matcher -- They extract: subject = SC4001  sess = E0  hyp = EC
PSG_RE = re.compile(r"^(?P<subject>[A-Z]{2}\d{4})(?P<sess>E[01])\-PSG\.edf$", re.IGNORECASE)
HYP_RE = re.compile(r"^(?P<subject>[A-Z]{2}\d{4})(?P<hyp>E[A-Z])\-Hypnogram\.edf$", re.IGNORECASE)

# Sleep-EDF (Sleep Cassette) convention is commonly:
# E0 PSG pairs with EC Hypnogram
# E1 PSG pairs with EH Hypnogram
SESS_TO_HYP = {"E0": "EC", "E1": "EH"}

# It just keeps paired files organised is a simple container
@dataclass
class FilePair:
    subject_code: str      # e.g., "SC4001"
    session_code: str      # e.g., "E0"
    psg_path: Path
    hyp_path: Path


# Finds the root folder of the project automatically. So no need for hard-code paths
def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


# Looks inside EDF header and Tries to get recording start time. Converts it to ISO format (YYYY-MM-DDTHH:MM:SS). If fails → returns None
def safe_iso_datetime(raw: mne.io.BaseRaw) -> Optional[str]:
    """Return measurement date/time as ISO string if available."""
    meas_date = raw.info.get("meas_date", None)
    if meas_date is None:
        return None
    try:
        # meas_date may be datetime or tuple; mne handles conversions internally
        if hasattr(meas_date, "isoformat"):
            return meas_date.isoformat()
        # fallback string
        return str(meas_date)
    except Exception:
        return None


# Extract duration

def get_duration_sec(raw: mne.io.BaseRaw) -> Optional[int]:
    try:
        sfreq = float(raw.info["sfreq"])        # e.g., 100 Hz sampling
        n_times = int(raw.n_times)              # e.g, 3,000,000 samples
        return int(round(n_times / sfreq))
    except Exception:
        return None


# [IMPROVED] Finds all *-PSG.edf. Finds all *-Hypnogram.edf. Tries to match them correctly

def find_pairs(data_raw: Path) -> tuple[list[FilePair], list[str]]:
    psg_files = sorted(data_raw.glob("*-PSG.edf"))
    hyp_files = sorted(data_raw.glob("*-Hypnogram.edf"))

    # Index hypnogram files by subject
    hyp_by_subject: dict[str, list[Path]] = {}

    for hp in hyp_files:
        m = HYP_RE.match(hp.name)
        if not m:
            continue
        subject = m.group("subject").upper()
        hyp_by_subject.setdefault(subject, []).append(hp)

    pairs: list[FilePair] = []
    warnings: list[str] = []

    for pp in psg_files:
        m = PSG_RE.match(pp.name)
        if not m:
            warnings.append(f"Skipped PSG with unexpected name: {pp.name}")
            continue

        subject = m.group("subject").upper()
        sess = m.group("sess").upper()  # E0 or E1

        candidates = hyp_by_subject.get(subject, [])
        hyp_path = None

        # -----------------------------
        # 1️⃣ Preferred mapping (if valid)
        # -----------------------------
        preferred_map = {"E0": "EC", "E1": "EH"}
        preferred_hyp = preferred_map.get(sess)

        if preferred_hyp:
            for hp in candidates:
                m_h = HYP_RE.match(hp.name)
                if not m_h:
                    continue
                if m_h.group("hyp").upper() == preferred_hyp:
                    hyp_path = hp
                    break

        # -----------------------------
        # 2️⃣ If preferred not found,
        # try looser matching by subject
        # -----------------------------
        if hyp_path is None and len(candidates) > 0:
            if len(candidates) == 1:
                hyp_path = candidates[0]
                warnings.append(
                    f"Fallback pairing used for {pp.name} -> {hyp_path.name}"
                )
            else:
                warnings.append(
                    f"Ambiguous hypnogram pairing for {pp.name}. "
                    f"Candidates: {[c.name for c in candidates]}"
                )
                continue

        # -----------------------------
        # 3️⃣ No hypnogram found
        # -----------------------------
        if hyp_path is None:
            warnings.append(f"No hypnogram found for PSG: {pp.name}")
            continue

        pairs.append(
            FilePair(
                subject_code=subject,
                session_code=sess,
                psg_path=pp,
                hyp_path=hyp_path,
            )
        )

    return pairs, warnings


# Sessions need participant_id (foreign key).If participant already exists → do nothing   If not → insert. Then it fetches participant_id.

def upsert_participant(cur: sqlite3.Cursor, subject_code: str) -> int:
    cur.execute(
        "INSERT OR IGNORE INTO participants(subject_code) VALUES (?)",
        (subject_code,),
    )
    cur.execute(
        "SELECT participant_id FROM participants WHERE subject_code = ?",
        (subject_code,),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Failed to fetch participant_id for subject_code={subject_code}")
    return int(row[0])

# [CORRECTED]
# If session exists → update
# If not → insert
# Uses proper SQLite UPSERT (does NOT delete row like REPLACE)

def upsert_session(
    cur: sqlite3.Cursor,
    participant_id: int,
    session_code: str,
    psg_filename: str,
    hyp_filename: str,
    start_datetime: Optional[str],
    duration_sec: Optional[int],
) -> None:
    cur.execute(
        """
        INSERT INTO sessions(
          participant_id,
          session_code,
          psg_filename,
          hyp_filename,
          start_datetime,
          duration_sec
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(participant_id, session_code)
        DO UPDATE SET
            psg_filename = excluded.psg_filename,
            hyp_filename = excluded.hyp_filename,
            start_datetime = excluded.start_datetime,
            duration_sec = excluded.duration_sec
        """,
        (
            participant_id,
            session_code,
            psg_filename,
            hyp_filename,
            start_datetime,
            duration_sec,
        ),
    )


def main() -> None:
    root = get_project_root()
    data_raw = root / "data_raw"
    db_path = root / "data_out" / "sleepedf.db"

    if not data_raw.exists():
        raise FileNotFoundError(f"Missing folder: {data_raw}")
    if not db_path.exists():
        raise FileNotFoundError(f"Missing database: {db_path}")

    pairs, warnings = find_pairs(data_raw)

    print(f"\nFound {len(pairs)} PSG↔Hypnogram pairs.")
    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print("  -", w)

    # Connect DB
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    inserted_subjects: set[str] = set()
    inserted_sessions = 0

    # Read only PSG headers for timing/duration
    # (we do NOT load raw EEG samples into SQL)
    for pair in tqdm(pairs, desc="Inserting participants/sessions"):
        participant_id = upsert_participant(cur, pair.subject_code)
        inserted_subjects.add(pair.subject_code)

        start_dt = None
        dur_sec = None
        try:
            raw = mne.io.read_raw_edf(pair.psg_path, preload=False, verbose="ERROR")
            start_dt = safe_iso_datetime(raw)
            dur_sec = get_duration_sec(raw)
        except Exception as e:
            print(f"\nHeader read failed for {pair.psg_path.name}: {e}")

        upsert_session(
            cur=cur,
            participant_id=participant_id,
            session_code=pair.session_code,
            psg_filename=pair.psg_path.name,
            hyp_filename=pair.hyp_path.name,
            start_datetime=start_dt,
            duration_sec=dur_sec,
        )
        inserted_sessions += 1

    conn.commit()
    conn.close()

    print("\nDone ✅")
    print(f"Participants present/updated: {len(inserted_subjects)}")
    print(f"Sessions inserted/updated:    {inserted_sessions}")
    print(f"Database: {db_path}")


if __name__ == "__main__":
    main()