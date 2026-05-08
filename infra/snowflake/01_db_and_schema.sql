-- =========================================
-- WAREHOUSE
-- =========================================
create warehouse IF NOT EXISTS TRANSFORMING; 

-- =========================================
-- DATABASE & SCHEMA
-- =========================================
CREATE DATABASE IF NOT EXISTS TAXI_PORTFOLIO;
CREATE SCHEMA IF NOT EXISTS TAXI_PORTFOLIO.RAW;

-- =========================================
-- WAREHOUSE GUARDRAILS (credit protection)
-- =========================================
ALTER WAREHOUSE TRANSFORMING SET
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1;

-- Create a separate loading warehouse (optional but clean)
CREATE WAREHOUSE IF NOT EXISTS LOADING
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

SHOW ROLES;

-- =========================================
-- GRANTS for the TRANSFORMER role (dbt's role)
-- =========================================
GRANT USAGE ON DATABASE TAXI_PORTFOLIO TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA TAXI_PORTFOLIO.RAW TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA TAXI_PORTFOLIO.RAW TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TAXI_PORTFOLIO.RAW TO ROLE TRANSFORMER;

-- =========================================
-- FILE FORMAT for parquet
-- =========================================
CREATE OR REPLACE FILE FORMAT TAXI_PORTFOLIO.RAW.PARQUET_FORMAT
    TYPE = PARQUET
    USE_LOGICAL_TYPE = TRUE
    BINARY_AS_TEXT = FALSE;

-- =========================================
-- INTERNAL STAGE for uploads
-- =========================================
CREATE OR REPLACE STAGE TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE
  FILE_FORMAT = TAXI_PORTFOLIO.RAW.PARQUET_FORMAT;

-- =========================================
-- YELLOW TAXI TABLE
-- Columns match your sources.yml exactly
-- =========================================
CREATE OR REPLACE TABLE TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS (
  vendor_id               INTEGER,
  pickup_datetime         TIMESTAMP_NTZ,
  dropoff_datetime        TIMESTAMP_NTZ,
  passenger_count         NUMBER,
  trip_distance           FLOAT,
  ratecode_id             NUMBER,
  store_and_fwd_flag      STRING,
  pickup_location_id      STRING,   -- sources.yml says STRING not INTEGER
  dropoff_location_id     STRING,
  payment_type            NUMBER,
  fare_amount             NUMERIC(12,2),
  extra                   NUMERIC(12,2),
  mta_tax                 NUMERIC(12,2),
  tip_amount              NUMERIC(12,2),
  tolls_amount            NUMERIC(12,2),
  improvement_surcharge   NUMERIC(12,2),
  total_amount            NUMERIC(12,2),
  congestion_surcharge    NUMERIC(12,2),
  airport_fee             NUMERIC(12,2),
  data_file_year          INTEGER,
  data_file_month         INTEGER,
  loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =========================================
-- WEATHER TABLE (NOAA GHCN daily, Central Park)
-- =========================================
CREATE OR REPLACE TABLE TAXI_PORTFOLIO.RAW.WEATHER_DAILY (
  date    DATE,
  tmax    NUMBER,    -- tenths of °C
  tmin    NUMBER,    -- tenths of °C
  prcp    NUMBER,    -- tenths of mm
  snow    NUMBER,    -- mm
  snwd    NUMBER,    -- mm (snow depth)
  awnd    NUMBER,    -- avg wind speed
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =========================================
-- GRANT on the new tables specifically
-- =========================================
GRANT SELECT ON TABLE TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS TO ROLE TRANSFORMER;
GRANT SELECT ON TABLE TAXI_PORTFOLIO.RAW.WEATHER_DAILY TO ROLE TRANSFORMER;