from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable

import mne
from tqdm import tqdm


EPOCH_LEN_SEC = 30


@dataclass
class SessionRow:
    session_id: int
    participant_id: int
    session_code: str
    psg_filename: str
    hyp_filename: str
    start_datetime: Optional[str]
    duration_sec: Optional[int]


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def map_stage(desc: str) -> str:
    d = desc.strip().lower()

    if "sleep stage w" in d:
        return "W"
    if "sleep stage r" in d:
        return "REM"
    if "sleep stage 1" in d:
        return "N1"
    if "sleep stage 2" in d:
        return "N2"
    if "sleep stage 3" in d or "sleep stage 4" in d:
        return "N3"

    if "sleep stage ?" in d:
        return "UNKNOWN"
    if "movement time" in d:
        return "UNKNOWN"

    return "UNKNOWN"


def read_stage_annotations(hyp_path: Path) -> list[tuple[float, float, str]]:
    """
    Read only sleep-stage-related annotations from the hypnogram EDF.

    Important: many datasets store stage changes with duration==0.
    In that case, we infer duration until the next onset.
    """
    ann = mne.read_annotations(hyp_path)

    rows: list[tuple[float, float, str]] = []
    for onset, dur, desc in zip(ann.onset, ann.duration, ann.description):
        desc = str(desc)
        dl = desc.lower()
        if ("sleep stage" in dl) or ("movement time" in dl):
            rows.append((float(onset), float(dur), desc))

    rows.sort(key=lambda x: x[0])
    if not rows:
        return []

    # Infer durations where dur <= 0 (common for "change point" annotations)
    inferred: list[tuple[float, float, str]] = []
    for i, (on, du, desc) in enumerate(rows):
        if du <= 0:
            if i + 1 < len(rows):
                du = rows[i + 1][0] - on
            else:
                du = EPOCH_LEN_SEC
        inferred.append((on, float(du), desc))

    # Normalise onsets so the first stage starts at t=0
    min_on = inferred[0][0]
    inferred = [(on - min_on, du, desc) for (on, du, desc) in inferred]

    return inferred


def annotation_span_seconds(ann: Iterable[tuple[float, float, str]]) -> Optional[int]:
    latest = None
    for onset, duration, _ in ann:
        end = onset + duration
        latest = end if latest is None else max(latest, end)
    if latest is None:
        return None
    return int(math.ceil(latest))


def build_epoch_stage_vectors(
    duration_sec: int,
    annotations: list[tuple[float, float, str]],
) -> tuple[list[tuple[int, int, int]], list[tuple[str, str]]]:
    """
    Returns:
      epochs_rows: list of (epoch_index, start_sec, duration_sec)
      stage_rows:  list of (stage, source_label) aligned to epoch_index
    """
    n_epochs = int(math.ceil(duration_sec / EPOCH_LEN_SEC))
    if n_epochs <= 0:
        raise ValueError(f"Non-positive n_epochs computed from duration_sec={duration_sec}")

    stages = ["UNKNOWN"] * n_epochs
    sources = [""] * n_epochs

    for onset, dur, desc in annotations:
        stage = map_stage(desc)

        start_i = int(math.floor(onset / EPOCH_LEN_SEC))
        end_excl = int(math.ceil((onset + dur) / EPOCH_LEN_SEC))

        start_i = max(0, start_i)
        end_excl = min(n_epochs, end_excl)
        if end_excl <= start_i:
            continue

        for i in range(start_i, end_excl):
            stages[i] = stage
            sources[i] = desc

    epochs_rows = [(i, i * EPOCH_LEN_SEC, EPOCH_LEN_SEC) for i in range(n_epochs)]
    stage_rows = list(zip(stages, sources))
    return epochs_rows, stage_rows


def fetch_sessions(cur: sqlite3.Cursor) -> list[SessionRow]:
    cur.execute(
        """
        SELECT session_id, participant_id, session_code, psg_filename, hyp_filename, start_datetime, duration_sec
        FROM sessions
        ORDER BY session_id
        """
    )
    rows = cur.fetchall()
    return [
        SessionRow(
            session_id=int(r[0]),
            participant_id=int(r[1]),
            session_code=str(r[2]),
            psg_filename=str(r[3]),
            hyp_filename=str(r[4]),
            start_datetime=r[5],
            duration_sec=(int(r[6]) if r[6] is not None else None),
        )
        for r in rows
    ]


def main() -> None:
    root = get_project_root()
    db_path = root / "data_out" / "sleepedf.db"
    data_raw = root / "data_raw"

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    sessions = fetch_sessions(cur)
    if not sessions:
        raise RuntimeError("No sessions found. Run etl/01_extract_sleepedf.py first.")

    total_epochs_inserted = 0

    for s in tqdm(sessions, desc="Building epochs + stages (fixed)"):
        hyp_path = data_raw / s.hyp_filename
        if not hyp_path.exists():
            print(f"\n⚠️ Missing hypnogram file for session_id={s.session_id}: {hyp_path}")
            continue

        annotations = read_stage_annotations(hyp_path)
        if not annotations:
            print(f"\n⚠️ No stage annotations found in: {hyp_path.name}")
            continue

        duration = annotation_span_seconds(annotations)
        if duration is None or duration <= 0:
            print(f"\n⚠️ Could not infer annotation span for session_id={s.session_id}")
            continue

        # Clean reruns
        cur.execute(
            "DELETE FROM sleep_stages WHERE epoch_id IN (SELECT epoch_id FROM epochs WHERE session_id = ?)",
            (s.session_id,),
        )
        cur.execute("DELETE FROM epochs WHERE session_id = ?", (s.session_id,))

        epochs_rows, stage_rows = build_epoch_stage_vectors(duration, annotations)

        # Insert epochs
        cur.executemany(
            "INSERT INTO epochs(session_id, epoch_index, start_sec, duration_sec) VALUES (?, ?, ?, ?)",
            [(s.session_id, ei, st, du) for (ei, st, du) in epochs_rows],
        )

        # Get epoch_ids
        cur.execute(
            "SELECT epoch_id, epoch_index FROM epochs WHERE session_id = ? ORDER BY epoch_index",
            (s.session_id,),
        )
        epoch_id_by_index = {int(ei): int(eid) for (eid, ei) in cur.fetchall()}

        to_insert = []
        for epoch_index, (stage, source) in enumerate(stage_rows):
            epoch_id = epoch_id_by_index.get(epoch_index)
            if epoch_id is None:
                continue
            to_insert.append((epoch_id, stage, source if source else None))

        cur.executemany(
            "INSERT INTO sleep_stages(epoch_id, stage, source_label) VALUES (?, ?, ?)",
            to_insert,
        )

        # Update session duration to staging span
        cur.execute("UPDATE sessions SET duration_sec = ? WHERE session_id = ?", (int(duration), s.session_id))

        total_epochs_inserted += len(epochs_rows)

    conn.commit()
    conn.close()

    print("\nDone ✅")
    print(f"Total epochs inserted: {total_epochs_inserted}")


if __name__ == "__main__":
    main()