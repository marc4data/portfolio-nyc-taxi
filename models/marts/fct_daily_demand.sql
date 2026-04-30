{{ config(
    materialized         = 'table',
    cluster_by           = ['pickup_date', 'pickup_location_id']
) }}

-- Zone+day fact. One row per pickup_location_id per pickup_date.
-- Primary input for Prophet demand forecasting model.
-- Joins weather here at the day grain (not at trip grain).
-- Lookback window = 7 days on incremental to handle late-arriving source data.

WITH daily AS (
    SELECT * FROM {{ ref('int_daily_demand') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (

    SELECT
        -- Surrogate key
        MD5(CONCAT(
            CAST(d.pickup_location_id AS VARCHAR), '|',
            CAST(d.pickup_date AS VARCHAR)
        ))                                          AS demand_id,

        -- Date + derived date attributes (features for forecasting models)
        d.pickup_date,
        YEAR(d.pickup_date)                         AS pickup_year,
        MONTH(d.pickup_date)                        AS pickup_month,
        DAYOFWEEK(d.pickup_date)                    AS day_of_week,
        CASE WHEN DAYOFWEEK(d.pickup_date) IN (0, 6) THEN 1 ELSE 0 END      AS is_weekend,
        dd.is_holiday                               AS is_holiday,

        -- Geography
        d.pickup_location_id,
        d.pickup_borough,
        d.pickup_zone,
        d.pickup_service_zone,                      -- added: needed for zone-type filtering (airport vs street hail)

        -- Volume (raw)
        d.trip_count,
        d.airport_pickup_count,
        d.null_batch_trip_count,
        d.fare_exception_count,
        d.zero_distance_count,
        d.cross_borough_count,

        -- Volume (model-ready target)
        -- Excludes null-batch trips (data artifacts, not real demand).
        -- zero_distance_count is retained as a separate field; exclude downstream if desired.
        d.trip_count - d.null_batch_trip_count      AS adjusted_trip_count,

        -- Null batch quality flags (for filtering and QA)
        CASE
            WHEN d.null_batch_trip_count > 0 THEN 1
            ELSE 0
        END                                         AS has_null_batch_ind,
        ROUND(
            d.null_batch_trip_count / NULLIF(d.trip_count, 0) * 100,
            2
        )                                           AS null_batch_pct,

        -- Revenue
        d.total_fare_revenue,
        d.total_revenue,
        d.total_tips,
        d.avg_fare,
        d.avg_distance_miles,
        d.avg_duration_minutes,
        d.avg_tip,
        d.credit_card_tip_rate_pct,
        d.credit_card_trips,
        d.cash_trips,

        -- Demand buckets
        d.morning_rush_trips,
        d.evening_rush_trips,
        d.overnight_trips,

        -- Weather (day grain — same for all zones on a given date)
        w.temp_max_f,
        w.temp_min_f,
        w.temp_avg_f,
        w.precipitation_in,
        w.snowfall_in,
        w.snow_depth_in,
        w.avg_wind_speed_mph,                       -- added: present in fct_trips, was missing here
        w.rain_day_ind,
        w.snow_day_ind,
        w.freezing_day_ind

    FROM daily d
    LEFT JOIN weather w ON d.pickup_date = w.date
    LEFT JOIN dim_date dd ON d.pickup_date = dd.date

)

SELECT * FROM final
