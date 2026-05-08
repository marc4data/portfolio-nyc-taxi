USE WAREHOUSE LOADING;

-- 2. Truncate the raw table
-- TRUNCATE TABLE taxi_portfolio.raw.yellow_taxi_trips;


COPY INTO TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS (
  vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
  trip_distance, ratecode_id, store_and_fwd_flag,
  pickup_location_id, dropoff_location_id, payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount,
  improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
  data_file_year, data_file_month
)
FROM (
  SELECT
    $1:VendorID::INTEGER,
    $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
    $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
    $1:passenger_count::NUMBER,
    $1:trip_distance::FLOAT,
    $1:RatecodeID::NUMBER,
    $1:store_and_fwd_flag::STRING,
    $1:PULocationID::STRING,
    $1:DOLocationID::STRING,
    $1:payment_type::NUMBER,
    $1:fare_amount::NUMERIC(12,2),
    $1:extra::NUMERIC(12,2),
    $1:mta_tax::NUMERIC(12,2),
    $1:tip_amount::NUMERIC(12,2),
    $1:tolls_amount::NUMERIC(12,2),
    $1:improvement_surcharge::NUMERIC(12,2),
    $1:total_amount::NUMERIC(12,2),
    $1:congestion_surcharge::NUMERIC(12,2),
    $1:airport_fee::NUMERIC(12,2),
    -- Pull year and month out of the filename, e.g. "yellow_tripdata_2022-01.parquet"
    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, '(\\d{4})-\\d{2}\\.parquet', 1, 1, 'e', 1)),
    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, '\\d{4}-(\\d{2})\\.parquet', 1, 1, 'e', 1))
  FROM @TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE/yellow/
)
PATTERN = '.*yellow_tripdata_\\d{4}-\\d{2}\\.parquet'
FILE_FORMAT = (FORMAT_NAME = 'TAXI_PORTFOLIO.RAW.PARQUET_FORMAT')
ON_ERROR = 'CONTINUE';


//###################################################################
//Verify the load
SELECT *
FROM TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS
limit 1000;


SELECT
  data_file_year,
  data_file_month,
  COUNT(*) AS row_count
FROM TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS
GROUP BY 1, 2
ORDER BY 1, 2;



LIST @TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE/yellow/;

SELECT
  data_file_year,
  COUNT(*) AS row_count,
  COUNT(DISTINCT data_file_month) AS months_present
FROM TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS
GROUP BY 1
ORDER BY 1;


SELECT
  FILE_NAME,
  STATUS,
  ROW_COUNT,
  ROW_PARSED,
  ERROR_COUNT,
  FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS',
  START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY FILE_NAME;
