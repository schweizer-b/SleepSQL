from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Optional

import mne
from tqdm import tqdm

EPOCH_LEN_SEC = 30


def project_root() -> Path:
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
    if "sleep stage ?" in d or "movement time" in d:
        return "UNKNOWN"
    return "UNKNOWN"


def read_stage_annotations(hyp_path: Path) -> list[tuple[float, float, str]]:
    """
    Sleep-EDF often stores stage changes with duration==0; infer duration to next onset.
    Returns (onset_sec, duration_sec, desc), sorted by onset, with onset shifted so first=0.
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

    inferred: list[tuple[float, float, str]] = []
    for i, (on, du, desc) in enumerate(rows):
        if du <= 0:
            du = (rows[i + 1][0] - on) if i + 1 < len(rows) else EPOCH_LEN_SEC
        inferred.append((on, float(du), desc))

    min_on = inferred[0][0]
    return [(on - min_on, du, desc) for (on, du, desc) in inferred]


def annotation_span_seconds(ann: list[tuple[float, float, str]]) -> Optional[int]:
    if not ann:
        return None
    latest = max(on + du for on, du, _ in ann)
    return int(math.ceil(latest))


def build_epoch_labels(duration_sec: int, ann: list[tuple[float, float, str]]) -> list[str]:
    n_epochs = int(math.ceil(duration_sec / EPOCH_LEN_SEC))
    labels = ["UNKNOWN"] * n_epochs

    for onset, dur, desc in ann:
        label = map_stage(desc)
        start_i = int(math.floor(onset / EPOCH_LEN_SEC))
        end_excl = int(math.ceil((onset + dur) / EPOCH_LEN_SEC))
        start_i = max(0, start_i)
        end_excl = min(n_epochs, end_excl)
        for i in range(start_i, end_excl):
            labels[i] = label

    return labels


def main() -> None:
    root = project_root()
    db_path = root / "data_out" / "sleepedf_T.db"
    data_raw = root / "data_raw"

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    cur.execute("SELECT rec_id, hyp_filename FROM recordings_T ORDER BY rec_id;")
    recs = cur.fetchall()
    if not recs:
        raise RuntimeError("No recordings found. Run etl/01_extract_sleepedf_T.py first.")

    for rec_id, hyp_fn in tqdm(recs, desc="Building epochs_T"):
        hyp_path = data_raw / hyp_fn
        if not hyp_path.exists():
            print(f"⚠️ Missing hyp file: {hyp_path}")
            continue

        ann = read_stage_annotations(hyp_path)
        span = annotation_span_seconds(ann)
        if span is None or span <= 0:
            print(f"⚠️ No stage span inferred for rec_id={rec_id}")
            continue

        labels = build_epoch_labels(span, ann)

        # rerun-safe
        cur.execute("DELETE FROM epochs_T WHERE rec_id=?", (rec_id,))

        rows = [
            (rec_id, i, i * EPOCH_LEN_SEC, EPOCH_LEN_SEC, labels[i])
            for i in range(len(labels))
        ]
        cur.executemany(
            """
            INSERT INTO epochs_T(rec_id, epoch_idx, start_sec, duration_sec, stage_label)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )

    conn.commit()
    conn.close()
    print("Done ✅")


if __name__ == "__main__":
    main()