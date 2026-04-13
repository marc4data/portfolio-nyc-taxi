{{ config(materialized='table', schema='marts') }}

-- Date spine covering Phase 1 + Phase 2 range.
-- Uses dbt_utils package (dbt-labs/dbt_utils).

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2019-01-01' as date)",
        end_date   = "cast('2025-12-31' as date)"
    ) }}

),

final AS (

    SELECT
        date_day                                              AS date,
        EXTRACT(YEAR     FROM date_day)                       AS year,
        EXTRACT(MONTH    FROM date_day)                       AS month,
        EXTRACT(DAY      FROM date_day)                       AS day_of_month,
        EXTRACT(QUARTER  FROM date_day)                       AS quarter,
        EXTRACT(DAYOFWEEK FROM date_day)                      AS day_of_week,  -- 1=Sun, 7=Sat
        DAYNAME(date_day)                                     AS day_name,
        MONTHNAME(date_day)                                   AS month_name,
        DATE_TRUNC('week',  date_day)                         AS week_start_date,
        DATE_TRUNC('month', date_day)                         AS month_start_date,
        DATE_TRUNC('year',  date_day)                         AS year_start_date,

        -- COVID period flags (for Phase 2 YoY analysis)
        CASE WHEN date_day BETWEEN '2019-01-01' AND '2019-12-31' THEN 1 ELSE 0 END AS is_pre_covid,
        CASE WHEN date_day BETWEEN '2020-03-22' AND '2021-06-15' THEN 1 ELSE 0 END AS is_lockdown_period,
        CASE WHEN date_day BETWEEN '2021-06-16' AND '2022-12-31' THEN 1 ELSE 0 END AS is_recovery_period,

        -- Weekend flag
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)  THEN 1 ELSE 0 END    AS is_weekend,

        -- NYC major holidays (demand drops or spikes)
        CASE WHEN (EXTRACT(MONTH FROM date_day) = 1  AND EXTRACT(DAY FROM date_day) = 1)    -- New Year's
              OR  (EXTRACT(MONTH FROM date_day) = 7  AND EXTRACT(DAY FROM date_day) = 4)    -- July 4
              OR  (EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 25)   -- Christmas
              OR  (EXTRACT(MONTH FROM date_day) = 11 AND EXTRACT(DAY FROM date_day) BETWEEN 22 AND 28
                   AND EXTRACT(DAYOFWEEK FROM date_day) = 5)                                 -- Thanksgiving
             THEN 1 ELSE 0 END                                                               AS is_holiday

    FROM date_spine

)

SELECT * FROM final
