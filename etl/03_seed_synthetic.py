from __future__ import annotations

import random
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main(seed: int = 42) -> None:
    random.seed(seed)

    root = project_root()
    db_path = root / "data_out" / "sleepedf_test.db"

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    # 1) Fill patient demographics if missing
    cur.execute("SELECT patients_id, patients_code, age, sex, bmi FROM patients;")
    patients = cur.fetchall()

    for pid, code, age, sex, bmi in patients:
        if age is None:
            # adults, a bit wide on purpose
            age = random.randint(18, 80)
        if sex is None:
            sex = random.choice(["M", "F"])
        if bmi is None:
            # BMI as integer in your schema; generate plausible values
            bmi = random.randint(18, 34)

        cur.execute(
            "UPDATE patients SET age=?, sex=?, bmi=? WHERE patients_id=?",
            (age, sex, bmi, pid),
        )

    # 2) Ensure every recording has a notes row + seed note values
    cur.execute("SELECT rec_id FROM recordings;")
    rec_ids = [r[0] for r in cur.fetchall()]

    for rec_id in rec_ids:
        # create row if missing
        cur.execute(
            """
            INSERT OR IGNORE INTO notes(rec_id, had_coffee, had_alcohol, has_pain, sleep_deprived, stress)
            VALUES (?, 0, 0, 0, 0, 0)
            """,
            (rec_id,),
        )

        # seeded probabilities (tweakable)
        had_coffee = 1 if random.random() < 0.35 else 0
        had_alcohol = 1 if random.random() < 0.20 else 0
        has_pain = 1 if random.random() < 0.15 else 0
        sleep_deprived = 1 if random.random() < 0.25 else 0

        # make stress slightly more likely if sleep deprived or pain
        stress_p = 0.20 + (0.20 if sleep_deprived else 0.0) + (0.10 if has_pain else 0.0)
        stress = 1 if random.random() < min(stress_p, 0.80) else 0

        cur.execute(
            """
            UPDATE notes
            SET had_coffee=?, had_alcohol=?, has_pain=?, sleep_deprived=?, stress=?
            WHERE rec_id=?
            """,
            (had_coffee, had_alcohol, has_pain, sleep_deprived, stress, rec_id),
        )

    conn.commit()
    conn.close()
    print(f"Seeded synthetic fields ✅ (seed={seed})")


if __name__ == "__main__":
    main()