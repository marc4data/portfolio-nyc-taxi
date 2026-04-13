{{ config(materialized='view', schema='staging') }}

-- NOAA GHCN-Daily, station USW00094728 (Central Park, NYC)
-- Raw values are in tenths of units — convert here.
-- TMAX/TMIN: tenths of degrees Celsius → degrees Celsius
-- PRCP:      tenths of mm → mm

SELECT
    date                                             AS date,
    CAST(tmax AS FLOAT) / 10.0                       AS temp_max_c,
    CAST(tmin AS FLOAT) / 10.0                       AS temp_min_c,
    ROUND((CAST(tmax AS FLOAT) + CAST(tmin AS FLOAT)) / 20.0, 2)
                                                     AS temp_avg_c,
    CAST(prcp AS FLOAT) / 10.0                       AS precipitation_mm,
    COALESCE(CAST(snow AS FLOAT), 0)                 AS snowfall_mm,
    COALESCE(CAST(snwd AS FLOAT), 0)                 AS snow_depth_mm,
    CAST(awnd AS FLOAT)                              AS avg_wind_speed,

    -- Derived weather indicators (useful for dashboard filters)
    CASE WHEN CAST(prcp AS FLOAT) / 10.0 > 5        THEN 1 ELSE 0 END AS rain_day_ind,
    CASE WHEN COALESCE(CAST(snow AS FLOAT), 0) > 0  THEN 1 ELSE 0 END AS snow_day_ind,
    CASE WHEN CAST(tmax AS FLOAT) / 10.0 < 0        THEN 1 ELSE 0 END AS freezing_day_ind

FROM {{ source('raw', 'weather_daily') }}
WHERE date BETWEEN '{{ var("weather_start_date", "2021-01-01") }}'
                AND '{{ var("weather_end_date",   "2022-12-31") }}'
