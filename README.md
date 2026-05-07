# NYC TLC Yellow Taxi — Analytics Engineering + ML Portfolio

A full-stack analytics project on NYC Taxi & Limousine Commission Yellow Taxi data: warehouse modeling, exploratory analysis, hourly-demand forecasting, and LLM-generated stakeholder briefings. Two years (2024–2025), ~80M trips, ~265 zones.

## At a glance

| Layer | Tooling | Headline |
|---|---|---|
| **Data warehouse** | Snowflake + dbt (staging → intermediate → marts) | 5 marts incl. `fct_hourly_demand` for ML, with schema tests + relationship checks |
| **EDA toolkit** | Reusable Python helpers (`notebooks/eda_helpers.py`) + Jupyter notebooks | 40+ chart functions covering distributions, time series, small-multiples, scatter w/ SHAP-style annotations |
| **Demand prediction** | LightGBM + Optuna + SHAP | **6.4% MAE lift over a strong 4-week-DoW baseline** for 24-hour-ahead zone-level forecasts |
| **LLM integration** | Anthropic Claude API (Sonnet 4.6) + prompt caching | Auto-generates a 700-word executive briefing from model artifacts; ~90% input-token cost reduction on re-runs via prompt cache |

## Tech Stack

| Area | Tool |
|---|---|
| Data warehouse | Snowflake |
| Transformation | dbt-core (Snowflake adapter) |
| Source data | NYC TLC Yellow Taxi parquet |
| Weather data | NOAA GHCN-Daily, Central Park (USW00094728) |
| EDA / ML | Python 3.12, pandas, numpy, matplotlib, snowflake-connector-python (Arrow) |
| Modeling | LightGBM, Optuna, SHAP, joblib |
| LLM | Anthropic Claude API (`claude-sonnet-4-6`), `python-dotenv` |
| dbt packages | dbt_utils, dbt_date |

## Architecture

```
NOAA daily weather                     NYC TLC parquet (2024-25)
       │                                      │
       ▼                                      ▼
Snowflake raw schema  ────────────────────────┘
       │
       ▼  dbt
┌──────────────────────────────────────────────────┐
│ staging   → intermediate   → marts               │
│   stg_*       int_*           fct_trips          │
│                               fct_daily_demand   │
│                               fct_hourly_demand  │  ← ML primary input
│                               dim_zones, dim_date│
└──────────────────────────────────────────────────┘
       │
       ▼  Python (notebooks/, ml/demand_prediction/)
┌──────────────────────────────────────────────────┐
│ EDA notebooks  ← chart helpers (eda_helpers.py)  │
│                                                  │
│ ML pipeline:                                     │
│   extract → features → split → baseline →        │
│   train → tune (Optuna) → explain (SHAP)         │
│                                                  │
│ AI layer:                                        │
│   briefing.py → Claude → executive_briefing.md   │
└──────────────────────────────────────────────────┘
```

## What's built

### 1. Warehouse layer (dbt)

- **5 marts**: `fct_trips` (trip-level), `fct_daily_demand` (zone × day for analytics), `fct_hourly_demand` (zone × hour for ML), `dim_zones`, `dim_date`
- **Incremental materialization** with `cluster_by(pickup_date, pickup_location_id)` for partition pruning
- **Schema tests**: `unique`, `not_null`, `relationships`, `accepted_values` on indicator columns
- **NULL-batch DQ flag**: 3.43% of 2024 rows have a known upstream batch artifact — flagged with `is_null_batch_ind=1`, retained in counts, excluded from revenue averages
- **Time-grain layering**: zone joins (static seed) live in intermediate; weather joins (time-series) live in marts — separate failure domains so a weather-feed glitch doesn't break the trip pipeline

### 2. Exploratory data analysis

- **`notebooks/eda_helpers.py`**: 40+ reusable chart helpers (distributions, histograms, scatter w/ trendlines + Pearson r badges, small-multiples by category, field-aggregation bar charts) with consistent typography across the report
- **`notebooks/eda_helpers_call_templates.py`**: copy-paste templates documenting every helper's params — kept in lockstep with `eda_helpers.py`
- **EDA notebooks** for the daily-demand and trip-level marts, exporting to HTML via the user's separate `nb2report` tool

### 3. ML pipeline — hourly demand forecasting

Lives in [`ml/demand_prediction/`](ml/demand_prediction/). Predicts `trip_count` per zone, **24 hours ahead**.

| Stage | Module | What it does |
|---|---|---|
| Extract | `data/extract.py` | Pulls `fct_hourly_demand` from Snowflake via Arrow, caches to parquet for fast iteration |
| Features | `data/features.py` | Densifies sparse zone × hour grid; adds cyclical hour/DoW/month encodings; lag features (1h/2h/24h/48h/168h **plus target-aligned** `lag_t24_1w/2w/3w/4w`); rolling 7d mean/std; `zone_trip_volume_rank` (training-period only — no leakage) |
| Split | `data/split.py` | Time-series-aware: train Jan-Dec 2024 / val Jan-Mar 2025 / test Apr-Dec 2025. Never shuffled. |
| Baseline | `predictors/baseline.py` | `NaiveForwardWeeklyMean` — averages trip count over the prior 4 weeks at the same hour-of-day and same day-of-week (lookbacks 144/312/480/648h from row time T) |
| Train | `predictors/train.py` | LightGBM with early stopping; baseline computed on full feature matrix before split for apples-to-apples lift |
| Tune | `predictors/tune.py` | Optuna 30-trial search over 7 hyperparameters on a 3-month subset; final model retrained on full year |
| Explain | `predictors/explain.py` | TreeSHAP on 50K validation rows; saves global importance bar, beeswarm summary, and dependence plots |
| Evaluate | `predictors/evaluate.py` | MAE / RMSE / MAPE primitives + breakdowns by borough and time-of-day |

**Headline result:** val MAE **5.03 → 4.98** (model vs. tuned), baseline **5.31**, lift **6.4%**. Best feature by SHAP: `lag_t24_1w` (mean |SHAP| = 13.98) — the original `lag_168h` ranked 7th at 0.58, **24× less important**, after a mid-project realignment of the weekly lags to the target time. SHAP plots in `artifacts/plots/`.

Two notebooks frame the work:
- [`ml/demand_prediction/notebooks/01_eda.ipynb`](ml/demand_prediction/notebooks/01_eda.ipynb) — demand patterns by hour, day, borough, weather; sparsity discussion
- [`ml/demand_prediction/notebooks/03_model_results.ipynb`](ml/demand_prediction/notebooks/03_model_results.ipynb) — modeling story, the 1.1% → 5.3% → 6.4% journey, SHAP analysis, limitations

### 4. AI layer — LLM-authored executive briefings

[`ml/demand_prediction/ai/`](ml/demand_prediction/ai/) — reads model artifacts (`metrics.json`, `shap_importance.csv`, feature column list) and asks Claude to write a 700-word stakeholder briefing aimed at an operations director, not a data scientist.

| File | Purpose |
|---|---|
| `prompts.py` | System prompt + `build_user_message()` builder. Senior-analyst voice, 6 required sections, inline jargon translation. |
| `briefing.py` | Loads artifacts, calls Claude (default `claude-sonnet-4-6`), saves output to `artifacts/executive_briefing.md`. |

**Demonstrated patterns:**
- **Layered credential resolution** — checks `os.environ` → `~/.anthropic/.env` → project `.env`, in that order, so a single home-directory key file works across multiple portfolio projects
- **Prompt caching** — `cache_control: ephemeral` marker on the user message; ~90% input-token cost reduction on re-runs (verified via `cache_creation_input_tokens` / `cache_read_input_tokens` in usage stats)
- **Cost transparency** — every call prints token usage so the iteration cost is visible and the cache hits are auditable

## File structure

```
portfolio_nyc_tlc/
├── README.md                          ← this file
├── dbt_project.yml
├── packages.yml
├── .env.example                       ← Snowflake config; Anthropic key lives in ~/.anthropic/.env
├── .gitignore
├── models/                            ← dbt
│   ├── staging/
│   ├── intermediate/
│   └── marts/
│       ├── fct_trips.sql
│       ├── fct_daily_demand.sql
│       ├── fct_hourly_demand.sql      ← ML primary input
│       ├── dim_zones.sql
│       └── dim_date.sql
├── seeds/
│   └── taxi_zone_lookup.csv
├── tests/                             ← dbt singular tests
├── analyses/                          ← dbt acceptance queries
├── notebooks/                         ← EDA layer
│   ├── eda_helpers.py
│   ├── eda_helpers_call_templates.py
│   ├── eda_fct_daily_demand.ipynb
│   └── eda_fct_trips.ipynb
└── ml/demand_prediction/              ← ML + AI pipeline
    ├── requirements.txt
    ├── config.py
    ├── data/
    │   ├── extract.py
    │   ├── features.py
    │   └── split.py
    ├── predictors/
    │   ├── baseline.py
    │   ├── train.py                   ← LightGBM
    │   ├── tune.py                    ← Optuna
    │   ├── evaluate.py                ← MAE/RMSE/MAPE breakdowns
    │   └── explain.py                 ← SHAP
    ├── ai/
    │   ├── prompts.py
    │   └── briefing.py                ← Claude API
    ├── notebooks/
    │   ├── 01_eda.ipynb
    │   └── 03_model_results.ipynb
    └── artifacts/                     ← gitignored: parquet cache, models, plots, briefings
```

## Setup

### Prerequisites

- Snowflake account with the `analytics` database and a writable schema (default `dbt_<your_user>`)
- Source data loaded into `analytics.raw`:
  - `yellow_taxi_trips` — TLC Yellow Taxi parquet
  - `weather_daily` — NOAA GHCN-Daily station `USW00094728`
- Python 3.12 with a venv
- macOS users: `brew install libomp` (LightGBM dependency)

### 1. Clone and install

```bash
git clone https://github.com/marc4data/portfolio-nyc-taxi.git
cd portfolio-nyc-taxi

# Python deps for the ML pipeline (and EDA helpers)
pip install -r ml/demand_prediction/requirements.txt
```

### 2. Credentials — never inside the project tree

This project follows a strict "credentials live in `~/`, not the repo" convention:

| Credential | Location | Why |
|---|---|---|
| Snowflake key-pair | `~/.ssh/snowflake/rsa_key.p8` | Standard SSH-style location |
| dbt connection profile | `~/.dbt/profiles.yml` | dbt's canonical location |
| Anthropic API key | `~/.anthropic/.env` (one line: `ANTHROPIC_API_KEY=sk-ant-...`) | Single key shared across all your portfolio projects |

Set them up:

```bash
# Snowflake key-pair (generate once; register public key with ALTER USER ... SET RSA_PUBLIC_KEY=...)
mkdir -p ~/.ssh/snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/snowflake/rsa_key.p8 -nocrypt
openssl rsa -in ~/.ssh/snowflake/rsa_key.p8 -pubout -out ~/.ssh/snowflake/rsa_key.pub
chmod 600 ~/.ssh/snowflake/rsa_key.p8

# dbt profile (copy from this project's example, fill in your account/user)
cp dbt-profiles.example.yml ~/.dbt/profiles.yml

# Anthropic key (skip if you don't intend to run the AI briefing)
mkdir -p ~/.anthropic
echo "ANTHROPIC_API_KEY=sk-ant-api03-..." > ~/.anthropic/.env
chmod 600 ~/.anthropic/.env
```

### 3. Build the warehouse

```bash
dbt deps                            # install dbt_utils, dbt_date
dbt seed                            # taxi zone lookup
dbt run                             # all models
dbt test                            # schema tests
```

### 4. Run the ML pipeline

```bash
cd ml/demand_prediction
python predictors/train.py          # LightGBM with fixed hyperparams (~5 min)
python predictors/tune.py           # Optuna 30 trials (~1 hour)
python predictors/explain.py        # SHAP plots (~3 min)
python ai/briefing.py               # LLM executive briefing (~10 sec)
```

Outputs in `ml/demand_prediction/artifacts/`:
- `metrics.json` — final tuned-model metrics, broken out by overall / borough / time-of-day
- `models/lgbm_tuned.pkl`, `models/best_params.json`, `models/optuna_study.pkl`
- `plots/shap_global_importance.png`, `plots/shap_summary_beeswarm.png`, `plots/shap_dependence_*.png`
- `plots/shap_importance.csv` — full feature-importance ranking
- `executive_briefing.md` — 700-word stakeholder briefing

### 5. Open the narrative notebooks

Open the two model-results notebooks in your editor's Jupyter integration:

- `notebooks/eda_fct_daily_demand.ipynb` — exploratory analysis on the daily-demand mart
- `ml/demand_prediction/notebooks/01_eda.ipynb` — hourly-demand patterns
- `ml/demand_prediction/notebooks/03_model_results.ipynb` — full modeling narrative

## Data quality notes

### Source quirks (handled in staging)

| Column | Issue | Fix |
|---|---|---|
| `pickup_location_id` / `dropoff_location_id` | Stored as STRING | `TRY_CAST(... AS INT)` |
| `rate_code` | STRING float: `'1.0'`, `'2.0'` | Double-cast: `TRY_CAST(TRY_CAST(... AS FLOAT) AS INT)` |
| `payment_type` | STRING: `'1'`, `'2'` | `TRY_CAST(... AS INT)` |
| `imp_surcharge` | Misnamed | Renamed to `improvement_surcharge` |
| `fare_amount`, `total_amount` | NUMERIC type with mixed precision | `TRY_CAST(... AS FLOAT)` |

### NULL batch pattern

3.43% of 2024 rows have simultaneous NULLs on `passenger_count`, `rate_code`, `store_and_fwd_flag`, and `payment_type='0'`. Known upstream batch artifact — **not errors**. Retained and flagged with `is_null_batch_ind=1`. Revenue averages exclude them; row counts retain them.

### Indicator column convention

All `_ind` columns use a `1/0` integer convention so:
- `SUM(col_ind)` = count of flagged rows
- `AVG(col_ind)` = rate of flagged rows

## Snowflake-specific notes

- `MD5()` returns a hex string directly — no `TO_HEX()` wrapper needed
- `EXTRACT(DAYOFWEEK FROM ...)` returns `1=Sunday, 7=Saturday` — but `DAYNAME(...)` is more reliable across session settings
- `DATEDIFF('minute', start, end)` is valid Snowflake syntax
- `fetch_pandas_all()` returns Arrow-backed DataFrames; NUMBER columns come back as float64. Cast to int before `.map()` against int-keyed dicts to avoid silent NaN

## Data sources

- **NYC TLC Trip Records:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Taxi Zone Lookup:** https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- **NOAA GHCN-Daily:** Central Park station `USW00094728`

## License

MIT.
