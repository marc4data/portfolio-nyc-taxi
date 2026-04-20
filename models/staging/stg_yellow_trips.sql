{{ config(
    materialized = 'view',
    schema       = 'staging'
) }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'yellow_taxi_trips') }}
    WHERE data_file_year = {{ var('taxi_load_year', 2022) }}
    {% if var('taxi_load_month', none) is not none %}
      AND data_file_month = {{ var('taxi_load_month') }}
    {% endif %}

),

renamed AS (

    SELECT

        -- ── Surrogate primary key ─────────────────────────────────────────────
        -- No natural PK in source. vendor + pickup time + dropoff time +
        -- pickup zone + dropoff zone + fare provides enough entropy to be unique.
        -- vendor|pickup_datetime alone is not granular enough — multiple trips
        -- can start at the same second from the same zone (rush hour, busy zones).
        -- Snowflake MD5() returns a hex string directly (no TO_HEX wrapper needed).
        MD5(CONCAT(
            COALESCE(CAST(vendor_id AS VARCHAR),           'null'), '|',
            COALESCE(CAST(pickup_datetime AS VARCHAR),     'null'), '|',
            COALESCE(CAST(dropoff_datetime AS VARCHAR),    'null'), '|',
            COALESCE(CAST(pickup_location_id AS VARCHAR),  'null'), '|',
            COALESCE(CAST(dropoff_location_id AS VARCHAR), 'null'), '|',
            COALESCE(CAST(fare_amount AS VARCHAR),         'null')
        ))                                                            AS trip_id,

        -- ── Timestamps ────────────────────────────────────────────────────────
        pickup_datetime,
        dropoff_datetime,
        DATE(pickup_datetime)                                        AS pickup_date,
        EXTRACT(YEAR  FROM pickup_datetime)                          AS pickup_year,
        EXTRACT(MONTH FROM pickup_datetime)                          AS pickup_month,
        EXTRACT(HOUR  FROM pickup_datetime)                          AS pickup_hour,
        EXTRACT(DAYOFWEEK FROM pickup_datetime)                      AS day_of_week,  -- 1=Sun, 7=Sat
        DATEDIFF('minute', pickup_datetime, dropoff_datetime)        AS trip_duration_minutes,

        -- ── Dimension fields ──────────────────────────────────────────────────
        vendor_id,

        -- rate_code stored as STRING float ('1.0', '2.0') — cast to INT
        TRY_CAST(TRY_CAST(rate_code AS FLOAT) AS INT)                AS rate_code,

        LOWER(store_and_fwd_flag)                                    AS store_and_fwd_flag,

        -- payment_type stored as STRING ('1', '2') — cast to INT
        TRY_CAST(payment_type AS INT)                                AS payment_type,

        -- ── Location IDs (STRING in source → INT for zone lookup join) ────────
        TRY_CAST(pickup_location_id  AS INT)                         AS pickup_location_id,
        TRY_CAST(dropoff_location_id AS INT)                         AS dropoff_location_id,

        -- ── Metric fields (NUMERIC in source → FLOAT) ─────────────────────────
        TRY_CAST(fare_amount    AS FLOAT)                            AS fare_amount,
        TRY_CAST(extra          AS FLOAT)                            AS extra_surcharge,
        TRY_CAST(mta_tax        AS FLOAT)                            AS mta_tax,
        TRY_CAST(tip_amount     AS FLOAT)                            AS tip_amount,
        TRY_CAST(tolls_amount   AS FLOAT)                            AS tolls_amount,
        TRY_CAST(imp_surcharge  AS FLOAT)                            AS improvement_surcharge,  -- renamed
        TRY_CAST(airport_fee    AS FLOAT)                            AS airport_fee,
        TRY_CAST(trip_distance  AS FLOAT)                            AS trip_distance_miles,
        TRY_CAST(total_amount   AS FLOAT)                            AS total_amount,
        TRY_CAST(passenger_count AS INT)                             AS passenger_count,

        -- ── Indicator fields (1/0; sum()=count, avg()=rate) ───────────────────
        CASE WHEN passenger_count IS NULL                                    THEN 1 ELSE 0 END AS is_null_batch_ind,
        CASE WHEN TRY_CAST(fare_amount   AS FLOAT) <= 0                      THEN 1 ELSE 0 END AS negative_fare_ind,
        CASE WHEN TRY_CAST(trip_distance AS FLOAT) = 0                       THEN 1 ELSE 0 END AS zero_distance_ind,
        CASE WHEN TRY_CAST(passenger_count AS INT) = 0                       THEN 1 ELSE 0 END AS zero_passenger_ind,
        CASE WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) < 0  THEN 1 ELSE 0 END AS negative_duration_ind,
        CASE WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 180 THEN 1 ELSE 0 END AS long_duration_ind,
        CASE WHEN TRY_CAST(pickup_location_id  AS INT) IN (132, 138)         THEN 1 ELSE 0 END AS airport_pickup_ind,
        CASE WHEN TRY_CAST(dropoff_location_id AS INT) IN (132, 138)         THEN 1 ELSE 0 END AS airport_dropoff_ind,
        CASE WHEN TRY_CAST(TRY_CAST(rate_code AS FLOAT) AS INT) = 2          THEN 1 ELSE 0 END AS jfk_flat_rate_ind,

        -- ── Decoded labels ────────────────────────────────────────────────────
        CASE TRY_CAST(payment_type AS INT)
            WHEN 1 THEN 'credit_card'
            WHEN 2 THEN 'cash'
            WHEN 3 THEN 'no_charge'
            WHEN 4 THEN 'dispute'
            ELSE        'unknown'
        END                                                                   AS payment_type_label,

        CASE TRY_CAST(TRY_CAST(rate_code AS FLOAT) AS INT)
            WHEN 1  THEN 'standard'
            WHEN 2  THEN 'jfk'
            WHEN 3  THEN 'newark'
            WHEN 4  THEN 'nassau_westchester'
            WHEN 5  THEN 'negotiated'
            WHEN 99 THEN 'unknown'
            ELSE        'other'
        END                                                                   AS rate_code_label,

        -- ── Partition helpers (carry through for incremental filter) ──────────
        data_file_year,
        data_file_month

    FROM source

)

SELECT * FROM renamed
