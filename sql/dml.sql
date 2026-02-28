-- Run with:
-- sqlite3 -header -column data_out/sleepedf.db ".read sql/dml.sql"

PRAGMA foreign_keys = ON;

-- 1) INSERT…SELECT: populate session_qc from the nightly summary view
INSERT OR REPLACE INTO session_qc (session_id, pass_qc, qc_reason)
SELECT
  s.session_id,
  CASE
    WHEN ns.tst_h >= 6.0
     AND ns.sleep_eff_window >= 0.80
     AND ns.unknown_pct_window <= 5.0
    THEN 1 ELSE 0
  END AS pass_qc,
  CASE
    WHEN ns.tst_h < 6.0 THEN 'fail: TST < 6h'
    WHEN ns.sleep_eff_window < 0.80 THEN 'fail: sleep efficiency < 0.80'
    WHEN ns.unknown_pct_window > 5.0 THEN 'fail: UNKNOWN% > 5%'
    ELSE 'pass'
  END AS qc_reason
FROM v_night_sleep_summary ns
JOIN sessions s ON s.psg_filename = ns.psg_filename;

-- 2) UPDATE example: tighten criteria and re-tag (demo-only)
-- (Shows UPDATE without changing raw staging tables.)
UPDATE session_qc
SET pass_qc = 0,
    qc_reason = 'fail: stricter criteria (demo)'
WHERE session_id IN (
  SELECT s.session_id
  FROM v_night_sleep_summary ns
  JOIN sessions s ON s.psg_filename = ns.psg_filename
  WHERE ns.rem_latency_min IS NOT NULL AND ns.rem_latency_min > 120
);

-- 3) Transaction demo (safe + reversible)
BEGIN;
  UPDATE session_qc
  SET qc_reason = qc_reason || ' | reviewed'
  WHERE pass_qc = 1;
COMMIT;

-- Inspect results
SELECT
  p.subject_code,
  s.psg_filename,
  q.pass_qc,
  q.qc_reason,
  q.evaluated_at
FROM session_qc q
JOIN sessions s ON s.session_id = q.session_id
JOIN participants p ON p.participant_id = s.participant_id
ORDER BY p.subject_code;