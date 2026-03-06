from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import mne
from tqdm import tqdm

PSG_RE = re.compile(r"^(?P<subject>[A-Z]{2}\d{4})(?P<sess>E[01])\-PSG\.edf$", re.IGNORECASE)
HYP_RE = re.compile(r"^(?P<subject>[A-Z]{2}\d{4})(?P<hyp>E[A-Z])\-Hypnogram\.edf$", re.IGNORECASE)
SESS_TO_HYP = {"E0": "EC", "E1": "EH"}


@dataclass
class FilePair:
    subject_code: str
    rec_code: str
    psg_path: Path
    hyp_path: Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def meas_datetime_from_psg(psg_path: Path) -> datetime:
    """
    Prefer meas_date from EDF header; fallback to file modified time.
    Returns timezone-aware UTC datetime.
    """
    try:
        raw = mne.io.read_raw_edf(psg_path, preload=False, verbose="ERROR")
        meas_date = raw.info.get("meas_date", None)
        if meas_date is not None and hasattr(meas_date, "timestamp"):
            # mne usually gives a datetime
            dt = meas_date
            # ensure tz-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    mtime = datetime.fromtimestamp(psg_path.stat().st_mtime, tz=timezone.utc)
    return mtime


def find_pairs(data_raw: Path) -> tuple[list[FilePair], list[str]]:
    psg_files = sorted(data_raw.glob("*-PSG.edf"))
    hyp_files = sorted(data_raw.glob("*-Hypnogram.edf"))

    hyp_index: dict[tuple[str, str], Path] = {}
    hyp_by_subject: dict[str, list[Path]] = {}

    for hp in hyp_files:
        m = HYP_RE.match(hp.name)
        if not m:
            continue
        subject = m.group("subject").upper()
        hyp_code = m.group("hyp").upper()
        hyp_index[(subject, hyp_code)] = hp
        hyp_by_subject.setdefault(subject, []).append(hp)

    pairs: list[FilePair] = []
    warnings: list[str] = []

    for pp in psg_files:
        m = PSG_RE.match(pp.name)
        if not m:
            warnings.append(f"Skipped PSG with unexpected name: {pp.name}")
            continue

        subject = m.group("subject").upper()
        rec_code = m.group("sess").upper()  # E0/E1

        preferred_hyp = SESS_TO_HYP.get(rec_code)
        hyp_path = hyp_index.get((subject, preferred_hyp), None) if preferred_hyp else None

        if hyp_path is None:
            candidates = hyp_by_subject.get(subject, [])
            if len(candidates) == 1:
                hyp_path = candidates[0]
                warnings.append(f"Fallback pairing: {pp.name} -> {hyp_path.name}")
            elif len(candidates) > 1:
                warnings.append(f"Ambiguous hypnogram for {pp.name}: {[c.name for c in candidates]}")
                continue
            else:
                warnings.append(f"No hypnogram found for PSG: {pp.name}")
                continue

        pairs.append(FilePair(subject_code=subject, rec_code=rec_code, psg_path=pp, hyp_path=hyp_path))

    return pairs, warnings


def upsert_patient(cur: sqlite3.Cursor, patients_code: str) -> int:
    cur.execute(
        "INSERT OR IGNORE INTO patients_T(patients_code) VALUES (?)",
        (patients_code,),
    )
    cur.execute("SELECT patients_id FROM patients_T WHERE patients_code=?", (patients_code,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Could not get patients_id for {patients_code}")
    return int(row[0])


def upsert_recording(
    cur: sqlite3.Cursor,
    patients_id: int,
    rec_code: str,
    psg_filename: str,
    hyp_filename: str,
    rec_date: str,
    rec_log: str,
) -> int:
    # stable upsert (avoid OR REPLACE changing rec_id)
    cur.execute(
        "SELECT rec_id FROM recordings_T WHERE patients_id=? AND rec_code=?",
        (patients_id, rec_code),
    )
    row = cur.fetchone()
    if row:
        rec_id = int(row[0])
        cur.execute(
            """
            UPDATE recordings_T
            SET psg_filename=?, hyp_filename=?, rec_date=?, rec_log=?
            WHERE rec_id=?
            """,
            (psg_filename, hyp_filename, rec_date, rec_log, rec_id),
        )
        return rec_id

    cur.execute(
        """
        INSERT INTO recordings_T(patients_id, rec_code, psg_filename, hyp_filename, rec_date, rec_log)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (patients_id, rec_code, psg_filename, hyp_filename, rec_date, rec_log),
    )
    return int(cur.lastrowid)


def ensure_notes_row(cur: sqlite3.Cursor, rec_id: int) -> None:
    # placeholder zeros; later the seed script can overwrite
    cur.execute(
        """
        INSERT OR IGNORE INTO notes_T(rec_id, had_coffee, had_alcohol, has_pain, sleep_deprived, stress)
        VALUES (?, 0, 0, 0, 0, 0)
        """,
        (rec_id,),
    )


def main() -> None:
    root = project_root()
    data_raw = root / "data_raw"
    db_path = root / "data_out" / "sleepedf_T.db"

    pairs, warnings = find_pairs(data_raw)
    print(f"Found {len(pairs)} PSG↔Hypnogram pairs.")
    for w in warnings:
        print("  -", w)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    for pair in tqdm(pairs, desc="Inserting patients + recordings"):
        patients_id = upsert_patient(cur, pair.subject_code)

        dt = meas_datetime_from_psg(pair.psg_path)
        rec_date = dt.date().isoformat()
        rec_log = dt.isoformat()

        rec_id = upsert_recording(
            cur=cur,
            patients_id=patients_id,
            rec_code=pair.rec_code,
            psg_filename=pair.psg_path.name,
            hyp_filename=pair.hyp_path.name,
            rec_date=rec_date,
            rec_log=rec_log,
        )
        ensure_notes_row(cur, rec_id)

    conn.commit()
    conn.close()
    print("Done ✅")


if __name__ == "__main__":
    main()