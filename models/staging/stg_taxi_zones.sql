{{ config(materialized='view', schema='staging') }}

SELECT
    LocationID                        AS location_id,
    LOWER(TRIM(Borough))              AS borough,
    LOWER(TRIM(Zone))                 AS zone,
    LOWER(TRIM(service_zone))         AS service_zone
FROM {{ ref('taxi_zone_lookup') }}
