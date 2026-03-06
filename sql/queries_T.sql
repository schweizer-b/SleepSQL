-- Ad-hoc means: Created for a specific, immediate purpose — not part of the permanent pipeline.
    -- temporary analysis queries used to explore data or answer a one-off question.

-- Run ad-hoc with:
-- sqlite3 -header -column data_out/sleepedf_T.db ".read sql/queries.sql"

-- Q1) Basic join + aggregation: stage minutes per session (within sleep window)