{{ config(
    materialized         = 'table',
    cluster_by           = ['pickup_date', 'pickup_location_id']
) }}

-- Core fact table. One row per trip.
-- Joins weather here (time-series data — separate failure domain from zone lookup).
-- Incremental: merge on trip_id, partitioned by data_file_year for backfill.

WITH enriched AS (

    SELECT * FROM {{ ref('int_trips_enriched') }}
    WHERE 1=1

    {% if var('taxi_load_year', none) is not none %}
        AND data_file_year = {{ var('taxi_load_year') }}
    {% endif %}

    {% if is_incremental() %}
        AND pickup_datetime >= (SELECT MAX(pickup_datetime) FROM {{ this }})
    {% endif %}

),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

prep AS (

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
        e.rate_code_id,
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

        -- Quality indicators (all 1/0)
        e.airport_pickup_ind,
        e.airport_dropoff_ind,
        e.extra_surcharge_exception_ind,
        e.fare_exception_ind,
        e.improvement_surcharge_exception_ind,  
        e.is_null_batch_ind,
        e.jfk_flat_rate_ind,
        e.mta_tax_exception_ind,   
        e.passenger_count_missing_ind,
        e.tip_amount_exception_ind,      
        e.tolls_amount_exception_ind,
        e.trip_distance_miles_exception_ind,
        e.trip_duration_exception_ind,
        case
            when e.fare_exception_ind                       = 0
                and e.trip_duration_exception_ind           = 0 
                and e.trip_distance_miles_exception_ind     = 0
                and e.is_null_batch_ind                     = 0
            then 1
            else 0
        end                                                 as is_valid_trip,


        -- Weather (LEFT JOIN — NULL if weather data missing for that date)
        w.temp_max_f            AS weather_temp_max_f,
        w.temp_min_f            AS weather_temp_min_f,
        w.temp_avg_f            AS weather_temp_avg_f,
        w.precipitation_in      AS weather_precip_in,
        w.snowfall_in           AS weather_snow_in,
        w.snow_depth_in         AS weather_snow_depth_in,
        w.avg_wind_speed_mph    AS weather_avg_wind_speed_mph,
        w.rain_day_ind          AS weather_rain_day_ind,
        w.snow_day_ind          AS weather_snow_day_ind,
        w.freezing_day_ind      AS weather_freezing_day_ind,
        
    FROM enriched e
    LEFT JOIN weather w ON e.pickup_date = w.date

),

final AS (

    SELECT * 
    FROM prep p
    WHERE p.is_valid_trip = 1
)


SELECT * FROM final
