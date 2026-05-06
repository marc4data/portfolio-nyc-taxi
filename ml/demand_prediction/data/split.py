"""
split.py — time-series-aware train/val/test split.

Splits by pickup_hour_ts using the date thresholds in config.py. NEVER
shuffles — temporal ordering is preserved so the lag features computed
in features.py don't leak future information into earlier splits.
"""

import sys
from pathlib import Path

# Add ml/demand_prediction/ to sys.path so config.py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

import config


def time_series_split(
    df: pd.DataFrame,
    time_col: str = 'pickup_hour_ts',
):
    """Split a feature matrix into (train, val, test) by `time_col`.

    Boundaries from config.py:
        TRAIN: TRAIN_START → TRAIN_END  (inclusive on both ends)
        VAL:   VAL_START   → VAL_END
        TEST:  TEST_START  → TEST_END

    Each split's "end" date is treated as inclusive — rows where time_col
    falls anywhere on that calendar day are included.

    Returns
    -------
    (train_df, val_df, test_df) — three DataFrames, same columns as input.
    Rows outside any window are not returned (warning printed).
    """
    train_lo = pd.Timestamp(config.TRAIN_START)
    train_hi = pd.Timestamp(config.TRAIN_END) + pd.Timedelta(days=1)
    val_lo   = pd.Timestamp(config.VAL_START)
    val_hi   = pd.Timestamp(config.VAL_END)   + pd.Timedelta(days=1)
    test_lo  = pd.Timestamp(config.TEST_START)
    test_hi  = pd.Timestamp(config.TEST_END)  + pd.Timedelta(days=1)

    t = df[time_col]
    train = df[(t >= train_lo) & (t < train_hi)].copy()
    val   = df[(t >= val_lo)   & (t < val_hi)].copy()
    test  = df[(t >= test_lo)  & (t < test_hi)].copy()

    dropped = len(df) - (len(train) + len(val) + len(test))
    if dropped > 0:
        print(f'[time_series_split] Note: {dropped:,} rows fell outside '
              f'TRAIN / VAL / TEST windows and were dropped.')

    return train, val, test


if __name__ == '__main__':
    # Smoke test using the features pipeline end-to-end
    from extract  import extract_hourly_demand
    from features import build_feature_matrix

    raw = extract_hourly_demand(config.TRAIN_START, '2025-01-31')
    fm  = build_feature_matrix(raw, train_end_date=config.TRAIN_END)

    train, val, test = time_series_split(fm)
    print(f'Train: {len(train):>10,} rows | '
          f'{train["pickup_hour_ts"].min()} → {train["pickup_hour_ts"].max()}')
    print(f'Val:   {len(val):>10,} rows | '
          f'{val["pickup_hour_ts"].min()} → {val["pickup_hour_ts"].max()}')
    print(f'Test:  {len(test):>10,} rows | '
          f'(would be {config.TEST_START} → {config.TEST_END}, '
          f'this smoke test only pulled through 2025-01-31)')
