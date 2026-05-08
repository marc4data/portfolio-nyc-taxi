"""
load_weather_to_snowflake.py
----------------------------
Downloads NOAA GHCN-Daily data for Central Park station USW00094728 and loads
it into Snowflake table: TAXI_PORTFOLIO.RAW.WEATHER_DAILY

Source URL (permanent NOAA direct-download):
    https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00094728.csv

The raw CSV has many columns; we keep only what stg_weather.sql needs:
    DATE, TMAX, TMIN, PRCP, SNOW, SNWD, AWND

Units (preserved as-is — stg_weather.sql handles all conversions):
    TMAX / TMIN : tenths of °C
    PRCP        : tenths of mm
    SNOW        : mm
    SNWD        : mm   (snow depth)
    AWND        : tenths of m/s

Usage:
    python load_weather_to_snowflake.py

    # Use a previously-downloaded local file instead of fetching from NOAA:
    WEATHER_CSV=/path/to/USW00094728.csv python load_weather_to_snowflake.py

Connection:
    Reads config_snowflake.yaml — credentials via .env (see .env.example).
    Uses dbprofile's SnowflakeConnector (key-pair preferred, password fallback).
"""

import io
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from snowflake.connector.pandas_tools import write_pandas

from snowflake_conn import get_connector

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

NOAA_URL = (
    "https://www.ncei.noaa.gov/data/"
    "global-historical-climatology-network-daily/access/USW00094728.csv"
)

# Set WEATHER_CSV env var to skip the download and read a local file instead
LOCAL_OVERRIDE = os.environ.get("WEATHER_CSV")

TARGET_DATABASE = "TAXI_PORTFOLIO"
TARGET_SCHEMA   = "RAW"
TARGET_TABLE    = "WEATHER_DAILY"

# Columns we need from the NOAA CSV (lowercase; AWND may be absent on some pulls)
KEEP_COLS = {"date", "tmax", "tmin", "prcp", "snow", "snwd", "awnd"}

# ─────────────────────────────────────────────────────────────────────────────


def fetch_csv() -> pd.DataFrame:
    """Download from NOAA (or read local override) and return a raw DataFrame."""
    if LOCAL_OVERRIDE:
        src = Path(LOCAL_OVERRIDE)
        print(f"Reading local file: {src} …")
        import gzip
        opener = gzip.open if str(src).endswith(".gz") else open
        with opener(src, "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, low_memory=False)
    else:
        print(f"Downloading from NOAA …")
        print(f"  {NOAA_URL}")
        resp = requests.get(NOAA_URL, timeout=120)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), low_memory=False)

    print(f"  Raw shape: {df.shape[0]:,} rows × {df.shape[1]} cols")
    print(f"  Columns: {list(df.columns)}")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and clean the columns stg_weather.sql needs.

    NOAA GHCN-Daily CSV column notes:
      - Column names are uppercase (DATE, TMAX, TMIN, ...)
      - Attribute columns like TMAX_ATTRIBUTES are present but not needed
      - Missing observations are empty strings or NaN
      - DATE is YYYY-MM-DD
    """
    # Normalise to lowercase for reliable matching
    df.columns = [c.strip().lower() for c in df.columns]

    # Warn on missing columns; fill with NULL (AWND occasionally absent)
    missing = KEEP_COLS - set(df.columns)
    if missing:
        print(f"  WARNING: columns not in CSV — will be NULL: {sorted(missing)}")
        for col in missing:
            df[col] = None

    # Keep only the columns we need
    df = df[[c for c in df.columns if c in KEEP_COLS]].copy()

    # DATE → Python date (drops any rows with unparseable dates)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    before = len(df)
    df = df.dropna(subset=["date"])
    if (dropped := before - len(df)):
        print(f"  Dropped {dropped} rows with unparseable dates.")

    # Numeric columns: coerce blanks / 'T' (trace) to NaN → NULL in Snowflake
    for col in ("tmax", "tmin", "prcp", "snow", "snwd", "awnd"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  Rows after cleaning: {len(df):,}")
    print(f"  Date range: {df['date'].min()} → {df['date'].max()}")

    # Uppercase column names to match Snowflake table schema
    df.columns = [c.upper() for c in df.columns]
    df = df.reset_index(drop=True)

    return df


def truncate_and_load(connector, df: pd.DataFrame):
    """Truncate existing rows and reload — full-refresh pattern."""
    print("\nTruncating existing rows …")
    connector.execute(
        f"TRUNCATE TABLE {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"
    )

    print(f"Loading {len(df):,} rows via write_pandas …")
    success, nchunks, nrows, _ = write_pandas(
        conn=connector._conn,
        df=df,
        table_name=TARGET_TABLE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        auto_create_table=False,
        overwrite=False,
    )

    if success:
        print(f"  ✓ Loaded {nrows:,} rows in {nchunks} chunk(s).")
    else:
        print("  ✗ write_pandas reported failure — check Snowflake logs.")
        sys.exit(1)

    # Sanity check
    rows = connector.execute(
        f"SELECT MIN(DATE) AS min_dt, MAX(DATE) AS max_dt, COUNT(*) AS n "
        f"FROM {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"
    )
    r = rows[0]
    print(f"\n  Table stats after load:")
    print(f"    Min date : {r['min_dt']}")
    print(f"    Max date : {r['max_dt']}")
    print(f"    Row count: {r['n']:,}")


def main():
    raw_df   = fetch_csv()
    clean_df = clean(raw_df)

    if len(clean_df) == 0:
        sys.exit("ERROR: No rows remain after cleaning — check column names above.")

    print("\nConnecting to Snowflake …")
    connector = get_connector()
    print("  Connected.")

    try:
        truncate_and_load(connector, clean_df)
    finally:
        connector.close()

    print("\nDone. Run `dbt run --select stg_weather` to build the staging view.")


if __name__ == "__main__":
    main()
