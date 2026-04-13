{{ config(materialized='view', schema='intermediate') }}

-- Aggregates trip-level data to zone+day grain.
-- Used as input for: fct_daily_demand, Prophet forecasting model.
-- Excludes null_batch and negative_fare rows from revenue aggregation.
-- Note: does NOT include weather columns — those join in fct_daily_demand from stg_weather.

WITH enriched AS (
    SELECT * FROM {{ ref('int_trips_enriched') }}
),

aggregated AS (

    SELECT
        pickup_date,
        pickup_location_id,
        pickup_borough,
        pickup_zone,

        -- Volume
        COUNT(*)                                AS trip_count,
        SUM(airport_pickup_ind)                 AS airport_pickup_count,
        SUM(is_null_batch_ind)                  AS null_batch_trip_count,
        SUM(negative_fare_ind)                  AS negative_fare_count,
        SUM(zero_distance_ind)                  AS zero_distance_count,
        SUM(cross_borough_ind)                  AS cross_borough_count,

        -- Revenue (exclude negative fares from revenue metrics)
        SUM(CASE WHEN negative_fare_ind = 0 THEN fare_amount  ELSE 0 END) AS total_fare_revenue,
        SUM(CASE WHEN negative_fare_ind = 0 THEN total_amount ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN negative_fare_ind = 0 THEN tip_amount   ELSE 0 END) AS total_tips,

        -- Averages (exclude null_batch and negative fares for cleaner metrics)
        AVG(CASE WHEN is_null_batch_ind = 0 AND negative_fare_ind = 0
                 THEN fare_amount END)           AS avg_fare,
        AVG(CASE WHEN is_null_batch_ind = 0
                 THEN trip_distance_miles END)   AS avg_distance_miles,
        AVG(CASE WHEN is_null_batch_ind = 0
                 THEN trip_duration_minutes END) AS avg_duration_minutes,
        AVG(CASE WHEN is_null_batch_ind = 0 AND negative_fare_ind = 0
                 THEN tip_amount END)            AS avg_tip,

        -- Tip rate (credit card trips only — cash tips not captured)
        SUM(CASE WHEN payment_type = 1 AND tip_amount > 0 THEN 1 ELSE 0 END)
            * 100.0
            / NULLIF(SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END), 0)
                                                 AS credit_card_tip_rate_pct,

        -- Payment mix
        SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
        SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,

        -- Time of day buckets
        SUM(CASE WHEN pickup_hour BETWEEN  7 AND  9 THEN 1 ELSE 0 END) AS morning_rush_trips,
        SUM(CASE WHEN pickup_hour BETWEEN 17 AND 19 THEN 1 ELSE 0 END) AS evening_rush_trips,
        SUM(CASE WHEN pickup_hour BETWEEN 22 AND 23
                  OR pickup_hour BETWEEN  0 AND  4 THEN 1 ELSE 0 END)  AS overnight_trips

    FROM enriched
    GROUP BY
        pickup_date,
        pickup_location_id,
        pickup_borough,
        pickup_zone

)

SELECT * FROM aggregated
