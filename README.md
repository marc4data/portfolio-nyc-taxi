# NYC TLC Yellow Taxi — Analytics Engineering Portfolio Project

A full analytics engineering stack built on NYC Taxi & Limousine Commission (TLC) Yellow Taxi trip data.
Demonstrates dbt best practices, incremental modeling, data quality testing, and weather enrichment across
a 36M+ row dataset.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Warehouse | Snowflake |
| Transformation | dbt (dbt-core) |
| Source Data | NYC TLC Yellow Taxi Parquet files |
| Weather Data | NOAA GHCN-Daily (Central Park, USW00094728) |
| Packages | dbt_utils, dbt_date |

---

## Project Scope

**Phase 1:** Yellow Taxi 2022 (36.3M trips, Jan–Nov)
**Phase 2:** Yellow Taxi 2019–2022 (multi-year COVID recovery story — incremental extension)

---

## Architecture

```
raw.yellow_taxi_trips  (Snowflake source)
seeds.taxi_zone_lookup (265-row static CSV)
raw.weather_daily      (NOAA Central Park)
        │
        ▼
   stg_yellow_trips    (view) — renames, casts, indicator fields, trip_id
   stg_taxi_zones      (view) — normalise seed columns
   stg_weather         (view) — convert NOAA tenths-units to real units
        │
        ▼
   int_trips_enriched  (view) — zone join: pickup + dropoff borough/zone
   int_daily_demand    (view) — zone+day aggregation (input to forecast)
        │
        ▼
   fct_trips           (incremental table) — weather join happens here
   fct_daily_demand    (incremental table) — zone+day fact for forecasting
   dim_zones           (table) — from seed
   dim_date            (table) — date spine 2019-01-01 to 2025-12-31
```

**Key design decisions:**
- Zone join (static seed) happens in the intermediate layer — reused by both fact tables
- Weather join (time-series) happens at the mart layer — separate failure domain
- No `int_trips_with_weather` model — avoids a redundant intermediate materialization

---

## File Structure

```
portfolio_nyc_tlc/
├── dbt_project.yml
├── packages.yml
├── .gitignore
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   ├── stg_yellow_trips.sql
│   │   ├── stg_taxi_zones.sql
│   │   └── stg_weather.sql
│   ├── intermediate/
│   │   ├── schema.yml
│   │   ├── int_trips_enriched.sql
│   │   └── int_daily_demand.sql
│   └── marts/
│       ├── schema.yml
│       ├── fct_trips.sql
│       ├── fct_daily_demand.sql
│       ├── dim_zones.sql
│       └── dim_date.sql
├── macros/
│   └── generate_schema_name.sql
├── seeds/
│   └── taxi_zone_lookup.csv        ← download separately (see Setup)
├── tests/
│   └── assert_null_batch_rate_within_bounds.sql
└── analyses/
    └── gate1_acceptance_criteria.sql
```

---

## Setup

### Prerequisites

- Snowflake account with `taxi_portfolio` database and `raw` schema
- `raw.yellow_taxi_trips` loaded from TLC Parquet files
- `raw.weather_daily` loaded from NOAA GHCN-Daily
- dbt installed (`pip install dbt-snowflake`)
- `profiles.yml` configured for the `nyc_taxi` profile

### 1. Download the taxi zone seed file

```bash
curl -o seeds/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### 2. Install dbt packages

```bash
dbt deps
```

### 3. Build in gates

```bash
# Gate 1 — Staging
dbt seed
dbt run  --select staging
dbt test --select staging

# Gate 2 — Intermediate
dbt run  --select intermediate
dbt test --select intermediate

# Gate 3 — Marts
dbt run  --select marts
dbt test --select marts

# Or run everything at once
dbt build
```

---

## Key Variables

| Variable | Default | Purpose |
|---|---|---|
| `taxi_load_year` | `2022` | Controls which year is loaded |
| `taxi_load_month` | `null` | Set to load a single month |
| `weather_start_date` | `2021-01-01` | Weather filter window start |
| `weather_end_date` | `2022-12-31` | Weather filter window end |

To load a different year:

```bash
dbt run --vars '{"taxi_load_year": 2021}'
```

---

## Data Quality Notes

### Known source quirks (handled in staging)

| Column | Issue | Fix |
|---|---|---|
| `pickup_location_id` / `dropoff_location_id` | Stored as STRING | `TRY_CAST(... AS INT)` |
| `rate_code` | STRING float: `'1.0'`, `'2.0'` | `TRY_CAST(TRY_CAST(... AS FLOAT) AS INT)` |
| `payment_type` | STRING: `'1'`, `'2'` | `TRY_CAST(... AS INT)` |
| `imp_surcharge` | Misnamed column | Renamed to `improvement_surcharge` |
| `fare_amount`, `total_amount`, etc. | NUMERIC type | `TRY_CAST(... AS FLOAT)` |
| December 2022 | Only 57 rows — effectively Jan–Nov | Filtered via `data_file_year` var |

### NULL batch pattern

3.43% of 2022 rows (1,241,840) have simultaneous NULLs on `passenger_count`,
`rate_code`, `store_and_fwd_flag`, and `payment_type = '0'`. These are a known
upstream batch artifact — **not errors**. They are retained and flagged with
`is_null_batch_ind = 1`. Revenue averages exclude them; counts retain them.

### Indicator columns

All `_ind` columns use a `1/0` integer convention:
- `SUM(col_ind)` = count of flagged rows
- `AVG(col_ind)` = rate of flagged rows

---

## Gate 1 Acceptance Criteria

After building staging, run the analysis query to validate against profiling baselines:

```bash
dbt run-operation run_query --args '{"query": "analyses/gate1_acceptance_criteria.sql"}'
```

Expected results:

| Year | Total Rows | Null Batch % | Negative Fares | Zero Distance | Airport Pickups | Distinct Zones |
|---|---|---|---|---|---|---|
| 2022 | 36,255,983 | 3.43% | 225,608 | 511,442 | ~1,743,202 | 262 |
| 2021 | 30,903,923 | 4.79% | 139,326 | 407,811 | ~1,025,038 | 263 |

---

## Snowflake-Specific Notes

- `MD5()` returns a hex string directly — no `TO_HEX()` wrapper needed
- `EXTRACT(DAYOFWEEK FROM ...)` returns `1=Sunday, 7=Saturday`
- `DATEDIFF('minute', start, end)` is valid Snowflake syntax
- Incremental models use `merge` strategy with `cluster_by` for partition pruning

---

## Data Sources

- **NYC TLC Trip Records:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Taxi Zone Lookup:** https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- **NOAA GHCN-Daily:** Central Park station `USW00094728`
