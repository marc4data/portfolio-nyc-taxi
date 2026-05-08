# Snowflake Infrastructure Setup

SQL scripts that provision the Snowflake environment this project runs on: warehouses, databases, roles, raw tables, the parquet file format, and the bulk-load commands. These run *once* per account; everything in `models/` and `ml/` builds on top of what these scripts create.

## What gets provisioned

| Object type | Names | Created by |
|---|---|---|
| Warehouses | `TRANSFORMING` (XS), `LOADING` (XS) | `01_db_and_schema.sql` |
| Databases | `TAXI_PORTFOLIO`, `ANALYTICS` | `01_db_and_schema.sql`, `02_roles.sql` |
| Schemas | `TAXI_PORTFOLIO.RAW`, plus dbt-managed schemas under `ANALYTICS` | `01_db_and_schema.sql` + dbt |
| Roles | `TRANSFORMER`, `LOADER` (both granted to `SYSADMIN`) | `02_roles.sql` |
| Raw tables | `TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS`, `TAXI_PORTFOLIO.RAW.WEATHER_DAILY` | `01_db_and_schema.sql` |
| File format | `TAXI_PORTFOLIO.RAW.PARQUET_FORMAT` | `01_db_and_schema.sql` |
| Internal stage | `TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE` | `01_db_and_schema.sql` |

Both warehouses are XS-sized with `AUTO_SUSPEND = 60` (seconds) — credit-conscious defaults that won't bleed budget if you forget to suspend.

## Run order

**The filename order is not the run order.** `02` creates the roles that `01` references in its grant statements. Run `02` first.

| Step | File | Run as | What it does |
|---|---|---|---|
| 1 | `02_roles.sql` | `ACCOUNTADMIN` | Creates `TRANSFORMER` + `LOADER` roles, grants them to `SYSADMIN`, grants both to your user, creates `ANALYTICS` database, hands ownership of `ANALYTICS` to `TRANSFORMER` |
| 2 | `01_db_and_schema.sql` | `ACCOUNTADMIN` or `SYSADMIN` | Creates warehouses, `TAXI_PORTFOLIO` database + `RAW` schema, raw tables, parquet file format, internal stage, grants `SELECT` on raw tables to `TRANSFORMER` |
| 3 | (manual) | `LOADER` via SnowSQL CLI | Upload your local parquet files to the internal stage with `PUT` — see "Loading the data" below |
| 4 | `03_load_data.sql` | `LOADER` | `COPY INTO` from stage → raw table; includes verification queries |

## Prerequisites

| Item | How to get it |
|---|---|
| Snowflake account | Free trial at https://signup.snowflake.com/ — Standard edition is sufficient |
| `ACCOUNTADMIN` role on your user | Default for new trial accounts |
| SnowSQL CLI | `brew install --cask snowflake-snowsql` (macOS) or download from Snowflake docs |
| Yellow Taxi parquet files | Download from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page — one parquet per month, e.g. `yellow_tripdata_2024-01.parquet` |
| NOAA weather CSV | `WEATHER_DAILY` table is created here but loading is out of scope for these scripts |

## Before running — values to update

These scripts are written for the author's account. Three values you'll likely change:

| File | Line | Current | Change to |
|---|---|---|---|
| `02_roles.sql` | 24, 25, 28 | `ALEX8675` | Your Snowflake username |
| `01_db_and_schema.sql` | (database/schema names) | `TAXI_PORTFOLIO`, `RAW` | Whatever names you prefer (then update `dbt-profiles.example.yml` to match) |
| `03_load_data.sql` | 39 | `@TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE/yellow/` | Your stage path if you renamed the stage |

`grep -n "ALEX8675\|TAXI_PORTFOLIO" infra/snowflake/*.sql` to find every place that needs updating.

## Loading the data — the manual step in between

Steps 2 and 4 leave you with empty raw tables and an empty internal stage. To load actual data, between scripts `01` and `03`:

```bash
# Authenticate to your Snowflake account
snowsql -a <your-account-id> -u <your-user>

# Inside the SnowSQL prompt:
USE WAREHOUSE LOADING;
USE DATABASE TAXI_PORTFOLIO;
USE SCHEMA RAW;

# Upload all yellow taxi parquet files (adjust glob to your local path)
PUT 'file:///Users/marcalexander/Downloads/yellow_tripdata_*.parquet'
    @NYC_TLC_STAGE/yellow/
    AUTO_COMPRESS=TRUE
    OVERWRITE=TRUE;

# Verify they're staged
LIST @NYC_TLC_STAGE/yellow/;
```

After that, `03_load_data.sql` can run the `COPY INTO`.

## Verification queries (built into `03_load_data.sql`)

After the `COPY INTO` finishes, the same script runs three checks:

```sql
-- Total rows
SELECT data_file_year, data_file_month, COUNT(*) AS row_count
FROM TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS
GROUP BY 1, 2 ORDER BY 1, 2;

-- Files in the stage
LIST @TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE/yellow/;

-- Recent COPY history (errors, row counts per file)
SELECT FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'TAXI_PORTFOLIO.RAW.YELLOW_TAXI_TRIPS',
  START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY FILE_NAME;
```

Expected for the project's 2024–2025 data:
- ~80M rows total across 24 monthly parquet files
- All files `STATUS = 'LOADED'`, `ERROR_COUNT = 0`

## What these scripts do *not* cover

- **Public-key registration.** `ALTER USER <you> SET RSA_PUBLIC_KEY = '...'` is run separately when setting up dbt + ML key-pair auth. See the project root `README.md` "Credentials" section.
- **Weather data load.** `WEATHER_DAILY` is created here but the NOAA CSV → table COPY is project-specific. (The user's `load_weather_to_snowflake.py` handles it in this repo.)
- **Schema isolation per developer.** dbt's `generate_schema_name` macro handles per-developer schema naming under `ANALYTICS`. No additional Snowflake setup needed.
- **dbt cloud / CI roles.** These scripts are sized for a single developer's local workflow. A team setup would add CI-specific roles + service users.

## Idempotency

All three scripts use `CREATE [OR REPLACE | IF NOT EXISTS]` patterns, so re-running them is safe. The exception:

- `CREATE OR REPLACE TABLE` in `01_db_and_schema.sql` will **drop and recreate** the raw tables. If you've already loaded data, you'll lose it. To preserve existing data, change those to `CREATE TABLE IF NOT EXISTS`.
- `GRANT OWNERSHIP ... COPY CURRENT GRANTS` in `02_roles.sql` will fail benignly if the ownership is already in place.
