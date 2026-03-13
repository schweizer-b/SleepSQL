# Exports for Power BI / Excel (SleepSQL)
# Run from project root (where data_out/ exists)
# powershell -ExecutionPolicy Bypass -File .\export_powerbi.ps1

$DB = "data_out/sleepedf_test.db"

# 1) Main fact table: one row per recording/night (includes demographics + filenames + notes + QC flags)
sqlite3 -header -csv $DB "
WITH base AS (
  SELECT
    s.*,
    r.patients_id,
    r.rec_date,
    p.age, p.sex, p.bmi,
    n.had_coffee, n.had_alcohol, n.has_pain, n.sleep_deprived, n.stress
  FROM v_sleep_summary s
  JOIN recordings r ON r.rec_id = s.rec_id
  JOIN patients p ON p.patients_id = r.patients_id
  LEFT JOIN notes n ON n.rec_id = s.rec_id
)
SELECT
  *,
  CASE
    WHEN tst_h >= 6.0 AND sleep_eff_window >= 0.80 AND unknown_pct_window <= 5.0 THEN 1
    ELSE 0
  END AS pass_qc,
  CASE
    WHEN tst_h < 6.0 THEN 'fail: TST < 6h'
    WHEN sleep_eff_window < 0.80 THEN 'fail: efficiency < 0.80'
    WHEN unknown_pct_window > 5.0 THEN 'fail: UNKNOWN% > 5%'
    ELSE 'pass'
  END AS qc_reason
FROM base
ORDER BY patients_code, rec_code; " > data_out/bi_nights.csv

# 2) Stage minutes per night (for stacked bar charts)
sqlite3 -header -csv $DB "
SELECT
  v.rec_id,
  v.patients_code,
  v.rec_code,
  v.stage_label,
  ROUND(COUNT(*) * 30.0 / 60.0, 1) AS minutes
FROM v_in_bed_window v
JOIN recordings r ON r.rec_id = v.rec_id
GROUP BY v.rec_id, v.patients_code, v.rec_code, v.stage_label
ORDER BY v.patients_code, 
         CASE v.stage_label
             WHEN 'W' THEN 0
             WHEN 'N1' THEN 1
             WHEN 'N2' THEN 2
             WHEN 'N3' THEN 3
             WHEN 'REM' THEN 4
             ELSE 5
         END;
" > data_out/bi_stage_minutes.csv

# 3) Fragmentation metrics (transitions + awakenings per night)
sqlite3 -header -csv $DB "
WITH base AS (
  SELECT
    v.rec_id,
    v.patients_code,
    v.epoch_idx,
    v.stage_label,
    LAG(v.stage_label) OVER (PARTITION BY v.rec_id ORDER BY v.epoch_idx) AS prev_stage
  FROM v_in_bed_window v
  JOIN recordings r ON r.rec_id = v.rec_id
),
agg AS (
  SELECT
    b.rec_id,
    b.patients_code,
    SUM(CASE WHEN b.prev_stage IS NOT NULL AND b.stage_label <> b.prev_stage THEN 1 ELSE 0 END) AS n_stage_transitions,
    SUM(CASE WHEN b.stage_label='W' AND (b.prev_stage IS NULL OR b.prev_stage <> 'W') THEN 1 ELSE 0 END) AS n_awakenings
  FROM base b
  GROUP BY b.rec_id, b.patients_code
)
SELECT
  a.*,
  s.in_bed_window_h,
  s.tst_h,
  ROUND(1.0 * a.n_stage_transitions / NULLIF(s.in_bed_window_h, 0), 2) AS transitions_per_hour_window,
  ROUND(1.0 * a.n_awakenings / NULLIF(s.tst_h, 0), 2) AS awakenings_per_hour_sleep
FROM agg a
JOIN v_sleep_summary s ON s.rec_id = a.rec_id
ORDER BY transitions_per_hour_window DESC;
" > data_out/bi_fragmentation.csv

Write-Host "Exported:"
Write-Host " - data_out/bi_nights.csv"
Write-Host " - data_out/bi_stage_minutes.csv"
Write-Host " - data_out/bi_fragmentation.csv"
