"""
config.py — central configuration for the ML demand prediction pipeline.

Loads Snowflake credentials from ~/.dbt/profiles.yml (key-pair auth — same
config as the dbt project and the EDA notebooks). Run-time constants like
date splits and hyperparameters live here so every script (extract, train,
evaluate) reads from one source of truth.
"""

from pathlib import Path
import yaml


# ── Paths ────────────────────────────────────────────────────────────────────
ML_ROOT      = Path(__file__).parent
ARTIFACTS    = ML_ROOT / 'artifacts'
DATA_CACHE   = ARTIFACTS / 'data_cache'
MODELS_DIR   = ARTIFACTS / 'models'
PLOTS_DIR    = ARTIFACTS / 'plots'
METRICS_FILE = ARTIFACTS / 'metrics.json'

# Create artifact directories on import so downstream code can write freely.
for _d in (ARTIFACTS, DATA_CACHE, MODELS_DIR, PLOTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ── Snowflake (key-pair auth via dbt profiles) ───────────────────────────────
DBT_PROFILES_PATH = Path.home() / '.dbt' / 'profiles.yml'
DBT_PROFILE       = 'nyc_taxi'
TARGET_SCHEMA     = 'DBT_MALEX_MARTS'   # mart schema where fct_hourly_demand lives


def get_snowflake_connection():
    """Open a Snowflake connection using key-pair auth from ~/.dbt/profiles.yml.

    Mirrors the _connect() helper used in the EDA notebooks so the same auth
    pattern is in effect everywhere in the project.
    """
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        load_pem_private_key, Encoding, PrivateFormat, NoEncryption
    )

    with open(DBT_PROFILES_PATH) as f:
        profile = yaml.safe_load(f)
    target = profile[DBT_PROFILE]['target']
    creds  = profile[DBT_PROFILE]['outputs'][target]

    with open(Path(creds['private_key_path']).expanduser(), 'rb') as f:
        pk = load_pem_private_key(f.read(), password=None)

    kw = dict(
        account     = creds['account'],
        user        = creds['user'],
        warehouse   = creds.get('warehouse'),
        database    = creds.get('database'),
        schema      = TARGET_SCHEMA,
        private_key = pk.private_bytes(
            encoding=Encoding.DER, format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        ),
    )
    if creds.get('role'):
        kw['role'] = creds['role']
    return snowflake.connector.connect(**kw)


# ── ML pipeline constants ────────────────────────────────────────────────────
RANDOM_SEED    = 42
TARGET_COL     = 'trip_count'
HORIZON_HOURS  = 24                       # predict t+24 (single-step horizon)
MIN_ZONE_TRIPS = 1000                     # min total trips in train window per zone

# Source mart for hourly demand (in TARGET_SCHEMA above)
DEMAND_TABLE   = 'fct_hourly_demand'

# Train / val / test date splits — expanded from the requirements doc's
# 9-3-3 to use the full data range. Full year of training captures all
# annual seasonality (holidays, summer/winter weather).
TRAIN_START = '2024-01-01'
TRAIN_END   = '2024-12-31'
VAL_START   = '2025-01-01'
VAL_END     = '2025-03-31'
TEST_START  = '2025-04-01'
TEST_END    = '2025-12-31'
