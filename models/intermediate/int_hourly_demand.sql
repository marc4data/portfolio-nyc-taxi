{{ config(materialized='view', schema='intermediate') }}

-- Aggregates trip-level data to zone+hour grain.
-- Used as input for: fct_hourly_demand (ML demand prediction).
-- Lean by design — only the columns the ML pipeline needs. Full revenue and
-- payment-mix aggregates live in int_daily_demand for daily-grain analytics.

WITH enriched AS (
    SELECT * FROM {{ ref('int_trips_enriched') }}
),

aggregated AS (

    SELECT
        DATE_TRUNC('hour', pickup_datetime)     AS pickup_hour_ts,
        pickup_date,
        pickup_hour,
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        pickup_service_zone,

        -- Volume
        COUNT(*)                                AS trip_count,
        SUM(airport_pickup_ind)                 AS airport_pickup_count

    FROM enriched
    GROUP BY
        DATE_TRUNC('hour', pickup_datetime),
        pickup_date,
        pickup_hour,
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        pickup_service_zone

)

SELECT * FROM aggregated
