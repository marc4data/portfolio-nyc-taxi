{{ config(
    materialized         = 'incremental',
    unique_key           = 'trip_id',
    incremental_strategy = 'merge',
    cluster_by           = ['pickup_date', 'pickup_location_id']
) }}

-- Core fact table. One row per trip.
-- Joins weather here (time-series data — separate failure domain from zone lookup).
-- Incremental: merge on trip_id, partitioned by data_file_year for backfill.

WITH enriched AS (

    SELECT * FROM {{ ref('int_trips_enriched') }}
    {% if is_incremental() %}
    -- For incremental runs: load one year at a time via var
    WHERE data_file_year = {{ var('taxi_load_year', 2022) }}
      AND pickup_datetime >= (SELECT MAX(pickup_datetime) FROM {{ this }})
    {% else %}
    -- Full refresh: load the configured year
    WHERE data_file_year = {{ var('taxi_load_year', 2022) }}
    {% endif %}

),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

final AS (

    SELECT
        -- Keys
        e.trip_id,

        -- Time
        e.pickup_datetime,
        e.dropoff_datetime,
        e.pickup_date,
        e.pickup_year,
        e.pickup_month,
        e.pickup_hour,
        e.day_of_week,
        e.trip_duration_minutes,

        -- Trip dimensions
        e.vendor_id,
        e.payment_type,
        e.payment_type_label,
        e.rate_code,
        e.rate_code_label,
        e.store_and_fwd_flag,

        -- Geography
        e.pickup_location_id,
        e.pickup_borough,
        e.pickup_zone,
        e.pickup_service_zone,
        e.dropoff_location_id,
        e.dropoff_borough,
        e.dropoff_zone,
        e.dropoff_service_zone,
        e.cross_borough_ind,

        -- Passengers & distance
        e.passenger_count,
        e.trip_distance_miles,

        -- Fare components
        e.fare_amount,
        e.extra_surcharge,
        e.mta_tax,
        e.tip_amount,
        e.tolls_amount,
        e.improvement_surcharge,
        e.airport_fee,
        e.total_amount,

        -- Weather (LEFT JOIN — NULL if weather data missing for that date)
        w.temp_max_c            AS weather_temp_max_c,
        w.temp_min_c            AS weather_temp_min_c,
        w.temp_avg_c            AS weather_temp_avg_c,
        w.precipitation_mm      AS weather_precip_mm,
        w.snowfall_mm           AS weather_snow_mm,
        w.snow_depth_mm         AS weather_snow_depth_mm,
        w.avg_wind_speed        AS weather_avg_wind_speed,
        w.rain_day_ind          AS weather_rain_day_ind,
        w.snow_day_ind          AS weather_snow_day_ind,
        w.freezing_day_ind      AS weather_freezing_day_ind,

        -- Quality indicators (all 1/0)
        e.is_null_batch_ind,
        e.negative_fare_ind,
        e.zero_distance_ind,
        e.zero_passenger_ind,
        e.negative_duration_ind,
        e.long_duration_ind,
        e.airport_pickup_ind,
        e.airport_dropoff_ind,
        e.jfk_flat_rate_ind

    FROM enriched e
    LEFT JOIN weather w ON e.pickup_date = w.date

)

SELECT * FROM final
