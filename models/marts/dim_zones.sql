{{ config(materialized='table', schema='marts') }}

-- 265 NYC taxi zones. Sourced from dbt seed (seeds/taxi_zone_lookup.csv).
-- Download from: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

SELECT
    location_id,
    borough,
    zone,
    service_zone,

    -- Borough groupings for dashboard filters
    CASE borough
        WHEN 'manhattan'     THEN 'Manhattan'
        WHEN 'brooklyn'      THEN 'Outer Boroughs'
        WHEN 'queens'        THEN 'Outer Boroughs'
        WHEN 'bronx'         THEN 'Outer Boroughs'
        WHEN 'staten island' THEN 'Outer Boroughs'
        WHEN 'ewr'           THEN 'Airport / EWR'
        ELSE                      'Unknown'
    END                                         AS borough_group,

    -- Airport flags
    CASE WHEN location_id IN (132, 138) THEN 1 ELSE 0 END AS is_airport_zone,
    CASE WHEN location_id = 132         THEN 1 ELSE 0 END AS is_jfk_zone,
    CASE WHEN location_id = 138         THEN 1 ELSE 0 END AS is_lga_zone,
    CASE WHEN service_zone = 'airports' THEN 1 ELSE 0 END AS is_airport_service_zone

FROM {{ ref('stg_taxi_zones') }}
