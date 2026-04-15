{{ config(materialized='view', schema='staging') }}

-- NOAA GHCN-Daily, station USW00094728 (Central Park, NYC)
-- Raw values are in tenths of units — convert here.
-- TMAX/TMIN: tenths of degrees Celsius → degrees Fahrenheit
-- PRCP/SNOW: tenths of mm → inches

SELECT
    date                                                                    AS date,
    ROUND((CAST(tmax AS FLOAT) / 10.0) * 9/5 + 32, 2)                       AS temp_max_f,
    ROUND((CAST(tmin AS FLOAT) / 10.0) * 9/5 + 32, 2)                       AS temp_min_f,
    ROUND(((CAST(tmax AS FLOAT) + CAST(tmin AS FLOAT)) / 20.0) * 9/5 + 32, 2)
                                                                            AS temp_avg_f,
    ROUND(CAST(prcp AS FLOAT) / 10.0 / 25.4, 4)                             AS precipitation_in,
    ROUND(COALESCE(CAST(snow AS FLOAT), 0) / 25.4, 4)                       AS snowfall_in,
    ROUND(COALESCE(CAST(snwd AS FLOAT), 0) / 25.4, 4)                       AS snow_depth_in,
    ROUND(CAST(awnd AS FLOAT) * 0.2237, 2)                                  AS avg_wind_speed_mph,

    -- Derived weather indicators (useful for dashboard filters)
    CASE WHEN CAST(prcp AS FLOAT) / 10.0 / 25.4 > 0.2  THEN 1 ELSE 0 END  AS rain_day_ind,
    CASE WHEN COALESCE(CAST(snow AS FLOAT), 0) > 0      THEN 1 ELSE 0 END  AS snow_day_ind,
    CASE WHEN CAST(tmax AS FLOAT) / 10.0 < 0            THEN 1 ELSE 0 END  AS freezing_day_ind

FROM {{ source('raw', 'weather_daily') }}
WHERE date BETWEEN '{{ var("weather_start_date", "2021-01-01") }}'
                AND '{{ var("weather_end_date",   "2022-12-31") }}'