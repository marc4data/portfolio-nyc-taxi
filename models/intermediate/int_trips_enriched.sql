{{ config(materialized='view', schema='intermediate') }}

-- Zone join only. Static seed — never changes.
-- Used downstream by: fct_trips, int_daily_demand
-- Weather does NOT join here — it joins in fct_trips (time-series data, different failure domain).

WITH trips AS (
    SELECT * FROM {{ ref('stg_yellow_trips') }}
),

zones AS (
    SELECT * FROM {{ ref('stg_taxi_zones') }}
),

enriched AS (

    SELECT
        t.*,

        -- Pickup zone attributes
        pu.borough          AS pickup_borough,
        pu.zone             AS pickup_zone,
        pu.service_zone     AS pickup_service_zone,

        -- Dropoff zone attributes
        do.borough          AS dropoff_borough,
        do.zone             AS dropoff_zone,
        do.service_zone     AS dropoff_service_zone,

        -- Cross-borough trip flag
        CASE WHEN pu.borough IS NOT NULL
              AND do.borough IS NOT NULL
              AND pu.borough != do.borough
             THEN 1 ELSE 0 END                AS cross_borough_ind

    FROM trips t
    LEFT JOIN zones pu ON t.pickup_location_id  = pu.location_id
    LEFT JOIN zones do ON t.dropoff_location_id = do.location_id
    -- LEFT JOIN: keeps all trips even if zone lookup has gaps (zone 264/265 = unknown)
    -- Monitor: NULL pickup_borough rate should be < 0.5% at Gate 2

)

SELECT * FROM enriched
