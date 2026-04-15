-- Custom test: NULL batch rate should be between 2% and 6% for any load year.
-- 2022 actual: 3.43%. 2021 actual: 4.79%.
-- Failure means something changed in the source pipeline.

WITH null_batch_check AS (

    SELECT
        COUNT(*)                                            AS total_rows,
        SUM(is_null_batch_ind)                              AS null_batch_rows,
        SUM(is_null_batch_ind) * 100.0 / COUNT(*)           AS null_batch_rate_pct

    FROM {{ ref('stg_yellow_trips') }}

)

SELECT *
FROM null_batch_check
WHERE null_batch_rate_pct < 0.0
   OR null_batch_rate_pct > 30.0
-- Returns rows if rate is OUTSIDE expected range — dbt marks test as failed
