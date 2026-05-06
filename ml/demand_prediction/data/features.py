"""
features.py — feature engineering for the hourly demand prediction model.

Pipeline:
    raw hourly demand DataFrame (from extract.py)
    → densify to complete (zone × hour) grid (fill missing cells with trip_count=0)
    → filter zones below MIN_ZONE_TRIPS (computed on TRAINING period only — no leakage)
    → cyclical encodings for hour, day_of_week, month
    → lag features (1h, 2h, 24h, 48h, 168h) per zone
    → rolling features (7-day mean and std) per zone
    → zone_trip_volume_rank (computed on TRAINING period only — no leakage)
    → target = trip_count shifted -HORIZON_HOURS so the row at time T predicts T+24
    → drop rows missing any lag, rolling stat, or target
    → final feature matrix ready for split.py

Two leakage traps to be aware of (and how this code avoids them):
    1. Lag features on a sparse grid: if hour T-1 has no row in the source,
       a naive shift(1) on the per-zone time series picks up trips from T-2
       and labels them as "1 hour ago." Densifying first eliminates this.
    2. Zone filter and zone-rank features computed on the full dataset would
       leak future information (e.g. a zone that explodes in volume during
       test would get a high rank in train). Both are computed using only
       data with pickup_hour_ts <= train_end_date.
"""

import sys
from pathlib import Path

# Add ml/demand_prediction/ to sys.path so this script can import config.py
# from its parent regardless of the current working directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

import config


# ── Densify ──────────────────────────────────────────────────────────────────
def densify_hourly_grid(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing (zone × hour) cells with trip_count=0.

    The mart only emits rows for hours where a zone had at least one trip.
    For lag computation we need a complete time grid — gaps would otherwise
    produce wrong lag values (e.g. lag_1h pulling from 2h ago when the
    immediately-prior hour had no trips).
    """
    if df.empty:
        return df

    hours = pd.date_range(df['pickup_hour_ts'].min(),
                          df['pickup_hour_ts'].max(),
                          freq='h')
    zones = df['pickup_location_id'].unique()
    grid  = pd.MultiIndex.from_product(
        [hours, zones], names=['pickup_hour_ts', 'pickup_location_id']
    ).to_frame(index=False)

    # Demand columns: missing cells get filled with 0
    demand_cols = ['trip_count', 'airport_pickup_count']
    grid = grid.merge(
        df[['pickup_hour_ts', 'pickup_location_id'] + demand_cols],
        on=['pickup_hour_ts', 'pickup_location_id'], how='left'
    )
    for c in demand_cols:
        grid[c] = grid[c].fillna(0).astype(int)

    # Static zone attributes — broadcast from the first non-null per zone
    zone_static_cols = ['pickup_borough', 'pickup_zone',
                        'pickup_service_zone', 'is_airport']
    zone_static = (df[['pickup_location_id'] + zone_static_cols]
                     .drop_duplicates('pickup_location_id'))
    grid = grid.merge(zone_static, on='pickup_location_id', how='left')

    # Calendar fields derived directly from the timestamp
    grid['pickup_date'] = pd.to_datetime(grid['pickup_hour_ts'].dt.date)
    grid['pickup_hour'] = grid['pickup_hour_ts'].dt.hour

    # Daily-grain calendar + weather — one set of values per date
    daily_cols = ['day_of_week', 'is_weekend', 'is_holiday', 'month', 'year',
                  'temp_avg_f', 'temp_max_f', 'temp_min_f',
                  'precipitation_in', 'snowfall_in', 'snow_depth_in',
                  'avg_wind_speed_mph', 'rain_day_ind', 'snow_day_ind',
                  'freezing_day_ind']
    daily_lookup = (df[['pickup_date'] + daily_cols]
                      .drop_duplicates('pickup_date')
                      .copy())
    daily_lookup['pickup_date'] = pd.to_datetime(daily_lookup['pickup_date'])
    grid = grid.merge(daily_lookup, on='pickup_date', how='left')

    return (grid
            .sort_values(['pickup_location_id', 'pickup_hour_ts'])
            .reset_index(drop=True))


# ── Cyclical encoding ────────────────────────────────────────────────────────
def add_cyclical_features(df: pd.DataFrame) -> pd.DataFrame:
    """sin/cos encodings for circular variables — prevents the model from
    treating hour 23 as far from hour 0, December as far from January, etc.
    Standard trick for tree models that benefit from continuous features."""
    out = df.copy()
    out['hour_sin']  = np.sin(2 * np.pi * out['pickup_hour']  / 24)
    out['hour_cos']  = np.cos(2 * np.pi * out['pickup_hour']  / 24)
    out['dow_sin']   = np.sin(2 * np.pi * out['day_of_week']  / 7)
    out['dow_cos']   = np.cos(2 * np.pi * out['day_of_week']  / 7)
    out['month_sin'] = np.sin(2 * np.pi * out['month']        / 12)
    out['month_cos'] = np.cos(2 * np.pi * out['month']        / 12)
    return out


# ── Lag and rolling features ─────────────────────────────────────────────────
def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Lag and rolling features computed PER ZONE on the densified grid.

    Rolling features are shifted by 1 before the rolling window so the
    current hour isn't included — that would leak the present into the
    feature, which is conceptually wrong for forecasting.
    """
    df = df.sort_values(['pickup_location_id', 'pickup_hour_ts']).copy()
    g  = df.groupby('pickup_location_id')['trip_count']

    # Same-hour lags relative to the row time T
    df['lag_1h']   = g.shift(1)
    df['lag_2h']   = g.shift(2)
    df['lag_24h']  = g.shift(24)
    df['lag_48h']  = g.shift(48)
    df['lag_168h'] = g.shift(168)

    # Target-aligned weekly lags. For predicting trip_count[T + HORIZON_HOURS],
    # the strongest seasonal signal is at the same hour-of-day and same DoW
    # 1/2/3/4 weeks before the TARGET time, not before T. Expressed as shifts
    # from row time T: shift(168·k − HORIZON_HOURS) for k=1..4. With H=24 these
    # are 144, 312, 480, 648.
    H = config.HORIZON_HOURS
    df['lag_t24_1w'] = g.shift(168 * 1 - H)  # 144
    df['lag_t24_2w'] = g.shift(168 * 2 - H)  # 312
    df['lag_t24_3w'] = g.shift(168 * 3 - H)  # 480
    df['lag_t24_4w'] = g.shift(168 * 4 - H)  # 648

    df['rolling_mean_7d'] = g.transform(
        lambda x: x.shift(1).rolling(168).mean()
    )
    df['rolling_std_7d']  = g.transform(
        lambda x: x.shift(1).rolling(168).std()
    )

    return df


# ── Zone rank (training period only — no leakage) ────────────────────────────
def compute_zone_rank(df: pd.DataFrame, train_end_date: str) -> pd.Series:
    """Rank zones by total demand in the TRAINING period only.

    Returns a Series indexed by pickup_location_id mapping zone → integer rank
    (1 = highest demand). Calling code merges this back onto the full df, so
    val/test rows get the same training-derived rank — never a future-aware
    rank that would constitute leakage.
    """
    train_mask   = df['pickup_hour_ts'] <= pd.Timestamp(train_end_date)
    train_totals = (df.loc[train_mask]
                      .groupby('pickup_location_id')['trip_count']
                      .sum()
                      .sort_values(ascending=False))
    return train_totals.rank(method='dense', ascending=False).astype(int)


# ── Zone filter (training period only — no leakage) ──────────────────────────
def filter_low_volume_zones(df: pd.DataFrame, train_end_date: str,
                             min_zone_trips: int = config.MIN_ZONE_TRIPS) -> pd.DataFrame:
    """Drop zones with fewer than min_zone_trips total trips in the TRAINING
    period. The same zone list is then applied to val/test — preventing zones
    that "appeared" mid-test from influencing train-time decisions.
    """
    train_mask   = df['pickup_hour_ts'] <= pd.Timestamp(train_end_date)
    train_totals = (df.loc[train_mask]
                      .groupby('pickup_location_id')['trip_count']
                      .sum())
    keep = train_totals[train_totals >= min_zone_trips].index
    return df[df['pickup_location_id'].isin(keep)].copy()


# ── Borough one-hot ──────────────────────────────────────────────────────────
def add_borough_dummies(df: pd.DataFrame) -> pd.DataFrame:
    """One-hot encode pickup_borough. Tree models can split on integer
    categoricals directly, but one-hots keep the feature semantics explicit
    in SHAP plots later."""
    dummies = pd.get_dummies(df['pickup_borough'], prefix='borough', dtype=int)
    return pd.concat([df, dummies], axis=1)


# ── End-to-end ───────────────────────────────────────────────────────────────
def build_feature_matrix(df: pd.DataFrame, train_end_date: str) -> pd.DataFrame:
    """End-to-end feature build.

    Parameters
    ----------
    df              : raw DataFrame from extract_hourly_demand()
    train_end_date  : 'YYYY-MM-DD' — used for the no-leakage zone filter and
                      zone_trip_volume_rank computation. Validation/test
                      rows must come from AFTER this date.

    Returns the feature matrix ready for split.py. Rows where any lag,
    rolling stat, or target is NaN are dropped (first 168h and last 24h
    per zone, by definition).
    """
    df = densify_hourly_grid(df)
    df = filter_low_volume_zones(df, train_end_date)
    df = add_cyclical_features(df)
    df = add_lag_features(df)

    # Target — trip_count HORIZON_HOURS in the future (default 24)
    df = df.sort_values(['pickup_location_id', 'pickup_hour_ts'])
    df['target'] = (df.groupby('pickup_location_id')['trip_count']
                      .shift(-config.HORIZON_HOURS))

    # Zone rank (training period only)
    rank_map = compute_zone_rank(df, train_end_date)
    df['zone_trip_volume_rank'] = df['pickup_location_id'].map(rank_map)

    # Borough one-hot
    df = add_borough_dummies(df)

    # Drop rows missing lags, rolling stats, or target — first 168h and last
    # 24h of each zone's series, plus any zones that never reached the
    # 168-row warm-up window
    required = ['lag_1h', 'lag_2h', 'lag_24h', 'lag_48h', 'lag_168h',
                'lag_t24_1w', 'lag_t24_2w', 'lag_t24_3w', 'lag_t24_4w',
                'rolling_mean_7d', 'rolling_std_7d', 'target',
                'zone_trip_volume_rank']
    df = df.dropna(subset=required).reset_index(drop=True)

    return df


if __name__ == '__main__':
    # Quick smoke test — pull 6 weeks so we have enough rows past the 168h
    # warm-up to produce a non-empty feature matrix.
    from extract import extract_hourly_demand

    raw = extract_hourly_demand('2024-01-01', '2024-02-15')
    fm  = build_feature_matrix(raw, train_end_date='2024-02-15')

    print(f'Raw rows:     {len(raw):,}')
    print(f'Feature rows: {len(fm):,}')
    print(f'Distinct zones kept: {fm["pickup_location_id"].nunique()}')
    print(f'Time range:   {fm["pickup_hour_ts"].min()} → {fm["pickup_hour_ts"].max()}')
    print(f'\nTarget stats:')
    print(fm['target'].describe().round(2))
    print(f'\nLag NaN check (should all be 0):')
    for col in ['lag_1h', 'lag_24h', 'lag_168h', 'rolling_mean_7d', 'target']:
        print(f'  {col:20s}: {fm[col].isna().sum()}')
