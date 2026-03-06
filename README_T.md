

# 0) build schema
sqlite3 data_out/sleepedf_T.db ".read sql/schemas_T.sql"

# 1) load patients/recordings + placeholder notes
python etl/01_extract_sleepedf_T.py

# 2) build epochs with stage labels
python etl/02_build_epochs_T.py

# 3) fill missing demographics + seed note 0/1 values
python etl/03_seed_synthetic_T.py

# 4) load views
sqlite3 data_out/sleepedf_T.db ".read sql/views_T.sql"