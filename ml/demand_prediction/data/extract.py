"""
extract.py — pull hourly demand data from Snowflake with parquet caching.

Cache pattern: an extract call for a given date range writes (and re-reads)
a parquet file in artifacts/data_cache/. Re-running with the same range hits
the cache, so iterating on features.py / train.py doesn't re-query Snowflake.
Pass force_reload=True to bypass.
"""

import sys
from pathlib import Path

# Add ml/demand_prediction/ to sys.path so this script can import config.py
# from its parent regardless of the current working directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

import config


def _cache_path(start_date: str, end_date: str) -> Path:
    """Stable filename keyed by the date range so re-runs share a cache."""
    return config.DATA_CACHE / f'hourly_demand_{start_date}_{end_date}.parquet'


def extract_hourly_demand(
    start_date: str = config.TRAIN_START,
    end_date:   str = config.TEST_END,
    force_reload:    bool = False,
) -> pd.DataFrame:
    """Pull hourly demand from Snowflake's fct_hourly_demand mart, with caching.

    Parameters
    ----------
    start_date   : inclusive start, 'YYYY-MM-DD'
    end_date     : inclusive end,   'YYYY-MM-DD'
    force_reload : if True, ignore the cache and re-query Snowflake

    Returns
    -------
    DataFrame with one row per (pickup_hour_ts, pickup_location_id), columns
    matching the mart (target=trip_count, plus calendar/weather/zone features).
    """
    cache = _cache_path(start_date, end_date)

    if cache.exists() and not force_reload:
        df = pd.read_parquet(cache)
        print(f'Cache hit: {cache.name} ({len(df):,} rows)')
        return df

    print(f'Querying Snowflake for {start_date} → {end_date}…')
    conn = config.get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT *
            FROM   {config.DEMAND_TABLE}
            WHERE  pickup_hour_ts >= '{start_date}'
              AND  pickup_hour_ts <  DATEADD(day, 1, '{end_date}')
            ORDER BY pickup_hour_ts, pickup_location_id
        """)
        df = cur.fetch_pandas_all()
        df.columns = [c.lower() for c in df.columns]
    finally:
        conn.close()

    df.to_parquet(cache, index=False)
    print(f'Saved cache: {cache.name} ({len(df):,} rows)')
    return df


if __name__ == '__main__':
    # Convenience: `python extract.py` runs a small test pull (Jan 2024 only)
    df = extract_hourly_demand('2024-01-01', '2024-01-07')
    print(df.head())
    print(f'\nShape: {df.shape}')
    print(f'Columns: {df.columns.tolist()}')
