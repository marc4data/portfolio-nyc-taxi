{{ config(
    materialized = 'incremental',
    unique_key   = 'demand_id',
    cluster_by   = ['pickup_hour_ts', 'pickup_location_id'],
    on_schema_change = 'sync_all_columns'
) }}

-- Hourly demand fact. One row per pickup_location_id per pickup_hour_ts.
-- Primary input for the ML demand prediction model (LightGBM, t+24 horizon).
-- Joins daily weather and dim_date — those values are repeated across all 24
-- hourly slices of each date (NOAA weather is daily-grain; hourly weather
-- isn't available from the source). Lookback window = 7 days on incremental
-- runs to handle late-arriving source data.

WITH hourly AS (
    SELECT * FROM {{ ref('int_hourly_demand') }}

    {% if is_incremental() %}
        WHERE pickup_hour_ts > (
            SELECT DATEADD(day, -7, MAX(pickup_hour_ts)) FROM {{ this }}
        )
    {% endif %}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

calendar AS (
    SELECT * FROM {{ ref('dim_date') }}
),

final AS (

    SELECT
        -- Surrogate key
        MD5(CONCAT(
            CAST(h.pickup_location_id AS VARCHAR), '|',
            CAST(h.pickup_hour_ts     AS VARCHAR)
        ))                                          AS demand_id,

        h.pickup_hour_ts,
        h.pickup_date,
        h.pickup_hour,
        h.pickup_location_id,
        h.pickup_borough,
        h.pickup_zone,
        h.pickup_service_zone,

        -- Airport flag — LGA, JFK, EWR all carry service_zone='Airports'.
        -- Lets the ML model learn airport-specific demand patterns in one feature.
        CASE WHEN h.pickup_service_zone = 'Airports' THEN 1 ELSE 0 END
                                                    AS is_airport,

        -- Target + supporting volume
        h.trip_count,
        h.airport_pickup_count,

        -- Calendar (daily — repeated across the 24 hourly slices of this date)
        c.day_of_week,
        c.is_weekend,
        c.is_holiday,
        c.month,
        c.year,

        -- Weather (daily — repeated across the 24 hourly slices of this date)
        w.temp_avg_f,
        w.temp_max_f,
        w.temp_min_f,
        w.precipitation_in,
        w.snowfall_in,
        w.snow_depth_in,
        w.avg_wind_speed_mph,
        w.rain_day_ind,
        w.snow_day_ind,
        w.freezing_day_ind

    FROM hourly h
    LEFT JOIN weather  w ON h.pickup_date = w.date
    LEFT JOIN calendar c ON h.pickup_date = c.date

)

SELECT * FROM final
