{{ config(
    materialized = 'view',
    schema       = 'staging'
) }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'yellow_taxi_trips') }}
    -- Original scope, but changing b/c of different source and what's available (GCP source was stale)
    -- WHERE data_file_year = {{ var('taxi_load_year', 2022) }}
    WHERE data_file_year IN (2024, 2025)
    {% if var('taxi_load_month', none) is not none %}
      AND data_file_month = {{ var('taxi_load_month') }}
    {% endif %}
      AND pickup_datetime BETWEEN '2024-01-01' AND '2026-01-01'
      AND dropoff_datetime BETWEEN '2024-01-01' AND '2026-01-01'

),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY vendor_id, pickup_datetime, dropoff_datetime,
                     pickup_location_id, dropoff_location_id, trip_distance,
                     fare_amount, tip_amount, total_amount, passenger_count
        ORDER BY 1  
    ) = 1
),

renamed AS (

    SELECT

        -- ── Surrogate primary key ─────────────────────────────────────────────
        -- No natural PK in source. MD5 of vendor + pickup time + pickup zone (and someother stuff)
        -- Snowflake MD5() returns a hex string directly (no TO_HEX wrapper needed).
        MD5(
            COALESCE(CAST(vendor_id AS VARCHAR), '') || '|' ||
            COALESCE(CAST(pickup_datetime AS VARCHAR), '') || '|' ||
            COALESCE(CAST(dropoff_datetime AS VARCHAR), '') || '|' ||
            COALESCE(CAST(pickup_location_id AS VARCHAR), '') || '|' ||
            COALESCE(CAST(dropoff_location_id AS VARCHAR), '') || '|' ||
            COALESCE(CAST(trip_distance AS VARCHAR), '') || '|' ||
            COALESCE(CAST(fare_amount AS VARCHAR), '') || '|' ||
            COALESCE(CAST(tip_amount AS VARCHAR), '') || '|' ||
            COALESCE(CAST(total_amount AS VARCHAR), '') || '|' ||
            COALESCE(CAST(passenger_count AS VARCHAR), '')
        )                                                            AS trip_id,

        -- ── Timestamps ────────────────────────────────────────────────────────
        pickup_datetime,
        dropoff_datetime,
        DATE(pickup_datetime)                                        AS pickup_date,
        EXTRACT(YEAR  FROM pickup_datetime)                          AS pickup_year,
        EXTRACT(MONTH FROM pickup_datetime)                          AS pickup_month,
        EXTRACT(HOUR  FROM pickup_datetime)                          AS pickup_hour,
        EXTRACT(DAYOFWEEKISO FROM pickup_datetime)                   AS day_of_week,  -- 1=Mon, 7=Sun
        DATEDIFF('minute', pickup_datetime, dropoff_datetime)        AS trip_duration_minutes,

        -- ── Dimension fields ──────────────────────────────────────────────────
        vendor_id,

        -- rate_code stored as STRING float ('1.0', '2.0') — cast to INT
        NULLIF(CAST(ratecode_id AS INT), 99)                         AS rate_code_id,

        LOWER(store_and_fwd_flag)                                    AS store_and_fwd_flag,

        -- payment_type stored as STRING ('1', '2') — cast to INT
        NULLIF(CAST(payment_type AS INT), 0)                         AS payment_type,

        -- ── Location IDs (STRING in source → INT for zone lookup join) ────────
        CAST(pickup_location_id  AS INT)                         AS pickup_location_id,
        CAST(dropoff_location_id AS INT)                         AS dropoff_location_id,

        -- ── Metric fields (NUMERIC in source → FLOAT) ─────────────────────────
        CAST(fare_amount    AS FLOAT)                            AS fare_amount,
        CAST(extra          AS FLOAT)                            AS extra_surcharge,
        CAST(mta_tax        AS FLOAT)                            AS mta_tax,
        CAST(tip_amount     AS FLOAT)                            AS tip_amount,
        CAST(tolls_amount   AS FLOAT)                            AS tolls_amount,
        CAST(improvement_surcharge  AS FLOAT)                    AS improvement_surcharge,
        CAST(coalesce(airport_fee, 0) AS FLOAT)                  AS airport_fee,
        CAST(trip_distance  AS FLOAT)                            AS trip_distance_miles,
        CAST(total_amount   AS FLOAT)                            AS total_amount,
        CASE
            WHEN CAST(passenger_count AS INT) BETWEEN 1 AND 6 THEN CAST(passenger_count AS INT)
            ELSE NULL
        END                                                      AS passenger_count,        

        -- ── Indicator fields ──────────────────────────────────────────────────────────
        CASE WHEN passenger_count IS NULL                                  THEN 1 ELSE 0 END AS passenger_count_missing_ind,
        CASE WHEN fare_amount < 0
            OR fare_amount > 100                                           THEN 1 ELSE 0 END AS fare_exception_ind,
        CASE WHEN tip_amount < 0
            OR tip_amount > 40                                             THEN 1 ELSE 0 END AS tip_amount_exception_ind,
        CASE WHEN tolls_amount < 0
            OR tolls_amount > 10                                           THEN 1 ELSE 0 END AS tolls_amount_exception_ind,
        CASE WHEN extra < 0
            OR extra > 10                                                  THEN 1 ELSE 0 END AS extra_surcharge_exception_ind,
        CASE WHEN mta_tax < 0
            OR mta_tax > 4                                                 THEN 1 ELSE 0 END    AS mta_tax_exception_ind,
        CASE WHEN improvement_surcharge < 0                                  THEN 1 ELSE 0 END  AS improvement_surcharge_exception_ind,
        CASE WHEN COALESCE(trip_distance, 0) <= 0
            OR trip_distance > 30                                          THEN 1 ELSE 0 END    AS trip_distance_miles_exception_ind,
        CASE WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) <= 0
            OR DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 180 THEN 1 ELSE 0 END    AS trip_duration_exception_ind,
        CASE WHEN pickup_location_id IN (132, 138)                           THEN 1 ELSE 0 END  AS airport_pickup_ind,
        CASE WHEN dropoff_location_id IN (132, 138)                          THEN 1 ELSE 0 END  AS airport_dropoff_ind,
        CASE WHEN ratecode_id = 2                                            THEN 1 ELSE 0 END  AS jfk_flat_rate_ind,
        CASE WHEN pickup_datetime IS NULL
            OR dropoff_datetime IS NULL
            OR fare_amount IS NULL
            OR pickup_location_id IS NULL                                  THEN 1 ELSE 0 END    AS is_null_batch_ind,    

        -- ── Decoded labels ────────────────────────────────────────────────────
        CASE CAST(payment_type AS INT)
            WHEN 1 THEN 'credit_card'
            WHEN 2 THEN 'cash'
            WHEN 3 THEN 'no_charge'
            WHEN 4 THEN 'dispute'
            ELSE        'unknown'
        END                                                                                     AS payment_type_label,

        CASE CAST(ratecode_id AS INT)
            WHEN 1  THEN 'standard'
            WHEN 2  THEN 'jfk'
            WHEN 3  THEN 'newark'
            WHEN 4  THEN 'nassau_westchester'
            WHEN 5  THEN 'negotiated'
            WHEN 99 THEN 'unknown'
            ELSE        'other'
        END                                                                                     AS rate_code_label,

        -- ── Partition helpers (carry through for incremental filter) ──────────
        data_file_year,
        data_file_month

    FROM deduplicated

)

SELECT * FROM renamed
