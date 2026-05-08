USE ROLE ACCOUNTADMIN;

-- =========================================
-- 1. CREATE ROLES
-- =========================================
-- TRANSFORMER is what dbt uses to build models
CREATE ROLE IF NOT EXISTS TRANSFORMER
  COMMENT = 'Role for dbt transformations — reads raw, writes to analytics';

-- LOADER is what ingests raw data (optional, but clean separation)
CREATE ROLE IF NOT EXISTS LOADER
  COMMENT = 'Role for loading raw data into Snowflake';

-- =========================================
-- 2. ROLE HIERARCHY
-- =========================================
-- TRANSFORMER and LOADER roll up to SYSADMIN
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE LOADER TO ROLE SYSADMIN;

-- =========================================
-- 3. GRANT TO YOUR USER
-- =========================================
GRANT ROLE TRANSFORMER TO USER ALEX8675;
GRANT ROLE LOADER TO USER ALEX8675;

-- Set TRANSFORMER as your default role (so dbt uses it without prompting)
ALTER USER ALEX8675 SET DEFAULT_ROLE = TRANSFORMER;

-- =========================================
-- 4. WAREHOUSE USAGE
-- =========================================
GRANT USAGE ON WAREHOUSE TRANSFORMING TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE LOADING TO ROLE LOADER;
-- TRANSFORMER also needs to query raw during dbt runs
GRANT USAGE ON WAREHOUSE LOADING TO ROLE TRANSFORMER;

-- =========================================
-- 5. DATABASE & SCHEMA ACCESS
-- =========================================
-- LOADER owns and writes raw
GRANT OWNERSHIP ON DATABASE TAXI_PORTFOLIO TO ROLE LOADER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA TAXI_PORTFOLIO.RAW TO ROLE LOADER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA TAXI_PORTFOLIO.RAW TO ROLE LOADER COPY CURRENT GRANTS;

-- TRANSFORMER reads from raw
GRANT USAGE ON DATABASE TAXI_PORTFOLIO TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA TAXI_PORTFOLIO.RAW TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA TAXI_PORTFOLIO.RAW TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TAXI_PORTFOLIO.RAW TO ROLE TRANSFORMER;

-- =========================================
-- 6. ANALYTICS DATABASE (dbt's output)
-- =========================================
-- If this doesn't exist yet from your earlier work, create it:
CREATE DATABASE IF NOT EXISTS ANALYTICS;

GRANT OWNERSHIP ON DATABASE ANALYTICS TO ROLE TRANSFORMER COPY CURRENT GRANTS;
GRANT ALL ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE TRANSFORMER;