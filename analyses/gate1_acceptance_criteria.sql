-- Run this query after building stg_yellow_trips to validate Gate 1.
-- All counts must match the profiling baseline (±0.5%).
-- Compare against: nyc_yellow_2022_profile.xlsx / nyc_yellow_2021_profile.xlsx

SELECT
    data_file_year,
    COUNT(*)                                        AS total_rows,
    SUM(is_null_batch_ind)                          AS null_batch_rows,
    ROUND(SUM(is_null_batch_ind) * 100.0 / COUNT(*), 2)
                                                    AS null_batch_pct,
    SUM(negative_fare_ind)                          AS negative_fare_rows,
    SUM(zero_distance_ind)                          AS zero_distance_rows,
    SUM(airport_pickup_ind)                         AS airport_pickup_rows,
    COUNT(DISTINCT pickup_location_id)              AS distinct_pickup_zones,
    MIN(pickup_date)                                AS min_pickup_date,
    MAX(pickup_date)                                AS max_pickup_date

FROM {{ ref('stg_yellow_trips') }}
GROUP BY data_file_year
ORDER BY data_file_year

/*
EXPECTED RESULTS (from profiling):

data_file_year | total_rows | null_batch_rows | null_batch_pct | negative_fare_rows | zero_distance_rows | airport_pickup_rows | distinct_pickup_zones
2021           | 30,903,923 | 1,478,695       | 4.79%          | 139,326            | 407,811            | ~1,025,038          | 263
2022           | 36,255,983 | 1,241,840       | 3.43%          | 225,608            | 511,442            | ~1,743,202          | 262

Note: 2022 max_pickup_date will be 2022-11-30 (Dec has only 57 rows, filtered by data_file_year).
*/
