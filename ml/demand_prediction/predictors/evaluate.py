"""
evaluate.py — metrics + breakdowns for the demand prediction model.

Provides:
    mae(), rmse(), mape()       — scalar metrics over a (y_true, y_pred) pair
    compute_metrics()           — bundles the three plus row count
    compute_metrics_breakdown() — same metrics grouped by a categorical column
    evaluate_against_baseline() — model + baseline metrics + lift
    add_time_of_day()           — adds 5-bucket time_of_day column
                                  (overnight/morning/midday/evening/night)

Phase 3 stops here — SHAP plots come in Phase 6 once a tuned LightGBM model
exists to explain.
"""

import sys
from pathlib import Path

# Add ml/demand_prediction/ to sys.path so config.py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

import config


# ── Time-of-day bucketing ────────────────────────────────────────────────────
TIME_OF_DAY_BINS   = [-1, 5, 10, 15, 20, 23]
TIME_OF_DAY_LABELS = ['overnight', 'morning', 'midday', 'evening', 'night']


def add_time_of_day(df: pd.DataFrame, hour_col: str = 'pickup_hour') -> pd.DataFrame:
    """Add a 'time_of_day' column. Boundaries match the requirements doc:
    overnight 0-5, morning 6-10, midday 11-15, evening 16-20, night 21-23."""
    out = df.copy()
    out['time_of_day'] = pd.cut(
        out[hour_col], bins=TIME_OF_DAY_BINS, labels=TIME_OF_DAY_LABELS,
        include_lowest=True
    )
    return out


# ── Scalar metrics ───────────────────────────────────────────────────────────
def mae(y_true, y_pred) -> float:
    """Mean Absolute Error — average |true - pred|."""
    y_true = pd.Series(y_true).dropna()
    y_pred = pd.Series(y_pred).reindex(y_true.index)
    return float(np.abs(y_true - y_pred).mean())


def rmse(y_true, y_pred) -> float:
    """Root Mean Squared Error — penalizes large misses more than small ones."""
    y_true = pd.Series(y_true).dropna()
    y_pred = pd.Series(y_pred).reindex(y_true.index)
    return float(np.sqrt(((y_true - y_pred) ** 2).mean()))


def mape(y_true, y_pred) -> float:
    """Mean Absolute Percentage Error.

    Skips rows where y_true == 0 (otherwise division explodes). Returns NaN
    if every truth is zero. For demand data with many zero hours in low-
    volume zones, MAPE alone can be misleading — read it alongside MAE.
    """
    y_true = pd.Series(y_true)
    y_pred = pd.Series(y_pred).reindex(y_true.index)
    mask   = (y_true != 0) & y_true.notna() & y_pred.notna()
    if mask.sum() == 0:
        return float('nan')
    return float((np.abs(y_true[mask] - y_pred[mask])
                  / np.abs(y_true[mask])).mean() * 100)


# ── Bundled metrics ──────────────────────────────────────────────────────────
def compute_metrics(y_true, y_pred) -> dict:
    """All three scalar metrics + row count, returned as a dict."""
    y_true = pd.Series(y_true)
    y_pred = pd.Series(y_pred).reindex(y_true.index)
    valid  = y_true.notna() & y_pred.notna()
    return {
        'n':    int(valid.sum()),
        'mae':  mae(y_true[valid], y_pred[valid]),
        'rmse': rmse(y_true[valid], y_pred[valid]),
        'mape': mape(y_true[valid], y_pred[valid]),
    }


def compute_metrics_breakdown(
    df: pd.DataFrame,
    true_col: str,
    pred_col: str,
    group_col: str,
) -> pd.DataFrame:
    """Group by `group_col`, compute MAE/RMSE/MAPE/n in each group.

    Returns a DataFrame indexed by group value with metric columns.
    """
    rows = {}
    for grp, sub in df.groupby(group_col, observed=True):
        rows[grp] = compute_metrics(sub[true_col], sub[pred_col])
    return (pd.DataFrame.from_dict(rows, orient='index')
              .sort_values('n', ascending=False))


# ── Lift over baseline ───────────────────────────────────────────────────────
def evaluate_against_baseline(
    df: pd.DataFrame,
    target_col: str,
    model_pred_col: str,
    baseline_pred_col: str,
) -> dict:
    """Compute model + baseline metrics overall, plus % MAE lift of the
    model over the baseline. Positive lift = model is better."""
    model_m = compute_metrics(df[target_col], df[model_pred_col])
    base_m  = compute_metrics(df[target_col], df[baseline_pred_col])
    lift = (
        (base_m['mae'] - model_m['mae']) / base_m['mae'] * 100
        if base_m['mae'] and base_m['mae'] > 0 else float('nan')
    )
    return {
        'model':         model_m,
        'baseline':      base_m,
        'lift_mae_pct':  lift,
    }


if __name__ == '__main__':
    # End-to-end smoke test: feature build → baseline predict → metrics
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'data'))
    from extract  import extract_hourly_demand
    from features import build_feature_matrix
    from split    import time_series_split
    from baseline import NaiveForwardWeeklyMean

    raw = extract_hourly_demand(config.TRAIN_START, '2025-01-31')
    fm  = build_feature_matrix(raw, train_end_date=config.TRAIN_END)
    train, val, _ = time_series_split(fm)

    model = NaiveForwardWeeklyMean().fit(train)
    val   = add_time_of_day(val.copy())
    val['baseline_pred'] = model.predict(val)

    print(f'\n── Validation overall ──────────────────────')
    print(compute_metrics(val['target'], val['baseline_pred']))

    print(f'\n── By borough ──────────────────────────────')
    print(compute_metrics_breakdown(val, 'target', 'baseline_pred',
                                    'pickup_borough').round(2))

    print(f'\n── By time of day ──────────────────────────')
    print(compute_metrics_breakdown(val, 'target', 'baseline_pred',
                                    'time_of_day').round(2))
