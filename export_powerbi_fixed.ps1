# Exports for Power BI / Excel (SleepSQL)
# Fixes two common Windows issues:
#  1) PowerShell ">" redirection writes UTF-16 by default (can look like one column in Excel/BI)
#  2) German/European Excel often expects ";" as delimiter (comma may open as one column)
#
# Run from project root:
#   powershell -ExecutionPolicy Bypass -File .\export_powerbi.ps1

$DB  = "data_out/sleepedf_test.db"
$OUT = "data_out"

# Choose delimiter:
#   ";" works best if your Excel/Power BI locale is German/European
#   "," is also fine if you import via Data -> From Text/CSV and select comma
$SEP = ";"

function Export-CsvUtf8([string]$Sql, [string]$OutFile) {
    # Use sqlite3 meta-commands to control delimiter, and pipe to Out-File to force UTF-8 output
    sqlite3 $DB `
      -cmd ".headers on" `
      -cmd ".mode csv" `
      -cmd ".separator $SEP" `
      "$Sql" | Out-File -Encoding utf8 "$OutFile"
}

# 1) Main fact table: one row per recording/night (sleep metrics + demographics + notes + derived QC)
$SQL_NIGHTS = @"
WITH base AS (
  SELECT
    s.*,
    r.patients_id,
    r.rec_date,
    p.age, p.sex, p.bmi,
    n.had_coffee, n.had_alcohol, n.has_pain, n.sleep_deprived, n.stress
  FROM v_sleep_summary s
  JOIN recordings r ON r.rec_id = s.rec_id
  JOIN patients p   ON p.patients_id = r.patients_id
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
ORDER BY patients_code, rec_code;
"@

Export-CsvUtf8 $SQL_NIGHTS (Join-Path $OUT "bi_nights.csv")

# 2) Stage minutes per night (for stacked bar charts)
$SQL_STAGE_MIN = @"
SELECT
  v.rec_id,
  v.patients_code,
  v.rec_code,
  v.stage_label,
  ROUND(COUNT(*) * 30.0 / 60.0, 1) AS minutes
FROM v_in_bed_window v
GROUP BY v.rec_id, v.patients_code, v.rec_code, v.stage_label
ORDER BY v.patients_code, v.rec_code,
         CASE v.stage_label
             WHEN 'W' THEN 0
             WHEN 'N1' THEN 1
             WHEN 'N2' THEN 2
             WHEN 'N3' THEN 3
             WHEN 'REM' THEN 4
             ELSE 5
         END;
"@

Export-CsvUtf8 $SQL_STAGE_MIN (Join-Path $OUT "bi_stage_minutes.csv")

# 3) Fragmentation metrics (transitions + awakenings per night)
$SQL_FRAG = @"
WITH base AS (
  SELECT
    v.rec_id,
    v.patients_code,
    v.epoch_idx,
    v.stage_label,
    LAG(v.stage_label) OVER (PARTITION BY v.rec_id ORDER BY v.epoch_idx) AS prev_stage
  FROM v_in_bed_window v
),
agg AS (
  SELECT
    rec_id,
    patients_code,
    SUM(CASE WHEN prev_stage IS NOT NULL AND stage_label <> prev_stage THEN 1 ELSE 0 END) AS n_stage_transitions,
    SUM(CASE WHEN stage_label='W' AND (prev_stage IS NULL OR prev_stage <> 'W') THEN 1 ELSE 0 END) AS n_awakenings
  FROM base
  GROUP BY rec_id, patients_code
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
"@

Export-CsvUtf8 $SQL_FRAG (Join-Path $OUT "bi_fragmentation.csv")

Write-Host "Exported UTF-8 CSVs to $OUT using delimiter '$SEP':"
Write-Host " - bi_nights.csv"
Write-Host " - bi_stage_minutes.csv"
Write-Host " - bi_fragmentation.csv"
