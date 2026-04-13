{{ config(
    materialized         = 'incremental',
    unique_key           = 'demand_id',
    incremental_strategy = 'merge',
    cluster_by           = ['pickup_date']
) }}

-- Zone+day fact. One row per pickup_location_id per pickup_date.
-- Primary input for Prophet demand forecasting model.
-- Joins weather here at the day grain (not at trip grain).
-- Lookback window = 7 days on incremental to handle late-arriving source data.

WITH daily AS (

    SELECT * FROM {{ ref('int_daily_demand') }}
    {% if is_incremental() %}
    WHERE pickup_date >= (SELECT MAX(pickup_date) - 7 FROM {{ this }})
    {% endif %}

),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

final AS (

    SELECT
        -- Surrogate key
        MD5(CONCAT(
            CAST(d.pickup_location_id AS VARCHAR), '|',
            CAST(d.pickup_date AS VARCHAR)
        ))                                          AS demand_id,

        d.pickup_date,
        d.pickup_location_id,
        d.pickup_borough,
        d.pickup_zone,

        -- Volume
        d.trip_count,
        d.airport_pickup_count,
        d.null_batch_trip_count,
        d.negative_fare_count,
        d.zero_distance_count,
        d.cross_borough_count,

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
        w.temp_max_c,
        w.temp_min_c,
        w.temp_avg_c,
        w.precipitation_mm,
        w.snowfall_mm,
        w.snow_depth_mm,
        w.rain_day_ind,
        w.snow_day_ind,
        w.freezing_day_ind

    FROM daily d
    LEFT JOIN weather w ON d.pickup_date = w.date

)

SELECT * FROM final
