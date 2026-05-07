"""
test.py — Phase 9: final held-out test evaluation.

Loads the tuned LightGBM model and evaluates it on the test set
(Apr–Dec 2025), the partition that has been held out across all
training and hyperparameter tuning. This is the "final cut" number —
the metric to claim in the portfolio narrative.

Pipeline is identical to tune.py's evaluation step, just on the test
split instead of validation. No hyperparameter search, no retraining —
pure inference + metric computation.

Outputs:
    artifacts/test_metrics.json       — overall + breakdowns by borough,
                                        time-of-day, and month
    artifacts/predictions_test.parquet — per-row predictions for any
                                        downstream dashboard / analysis

Usage
-----
    python test.py
"""

import sys
import json
from pathlib import Path

# Add ml/demand_prediction/ + subfolders to sys.path
_ML_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ML_ROOT))
sys.path.insert(0, str(_ML_ROOT / 'data'))
sys.path.insert(0, str(_ML_ROOT / 'predictors'))

import joblib
import pandas as pd

import config
from extract  import extract_hourly_demand
from features import build_feature_matrix
from split    import time_series_split
from baseline import NaiveForwardWeeklyMean
from evaluate import (
    compute_metrics_breakdown,
    evaluate_against_baseline, add_time_of_day,
)
from train import get_feature_columns


def main():
    # ── 1. Load the tuned model ────────────────────────────────────────────
    model_path = config.MODELS_DIR / 'lgbm_tuned.pkl'
    if not model_path.exists():
        raise FileNotFoundError(
            f'Tuned model not found at {model_path}. Run tune.py first.'
        )
    model = joblib.load(model_path)
    print(f'Loaded tuned model: {model_path}')

    # ── 2. Build feature matrix through test period ───────────────────────
    print('Building feature matrix through test period…')
    raw = extract_hourly_demand(config.TRAIN_START, config.TEST_END)
    fm  = build_feature_matrix(raw, train_end_date=config.TRAIN_END)

    # Baseline computed on the full feature matrix BEFORE splitting so its
    # lookbacks have access to all prior history (apples-to-apples with model)
    fm['baseline_pred'] = NaiveForwardWeeklyMean().predict(fm)

    train, val, test = time_series_split(fm)
    print(f'  Train rows: {len(train):>12,}')
    print(f'  Val rows:   {len(val):>12,}')
    print(f'  Test rows:  {len(test):>12,}')

    # ── 3. Predict on test ────────────────────────────────────────────────
    feature_cols = get_feature_columns(train)
    test = add_time_of_day(test.copy())
    test['model_pred'] = model.predict(test[feature_cols])

    # ── 4. Compute metrics ────────────────────────────────────────────────
    summary        = evaluate_against_baseline(test, 'target',
                                               'model_pred', 'baseline_pred')
    by_borough     = compute_metrics_breakdown(test, 'target', 'model_pred',
                                                'pickup_borough')
    by_time_of_day = compute_metrics_breakdown(test, 'target', 'model_pred',
                                                'time_of_day')
    by_month       = compute_metrics_breakdown(test, 'target', 'model_pred',
                                                'month')

    print('\n── TEST overall — TUNED model vs baseline ─────────────────────')
    print(json.dumps(summary, indent=2, default=str))
    print('\n── By borough ─────────────────────────────────────────────────')
    print(by_borough.round(2))
    print('\n── By time of day ─────────────────────────────────────────────')
    print(by_time_of_day.round(2))
    print('\n── By month ───────────────────────────────────────────────────')
    print(by_month.round(2))

    # ── 5. Save artifacts ──────────────────────────────────────────────────
    test_metrics_path = config.ARTIFACTS / 'test_metrics.json'
    predictions_path  = config.ARTIFACTS / 'predictions_test.parquet'

    metrics_payload = {
        'phase':          'phase_9_test_holdout',
        'overall':        summary,
        'by_borough':     by_borough.to_dict(orient='index'),
        'by_time_of_day': by_time_of_day.to_dict(orient='index'),
        'by_month':       by_month.to_dict(orient='index'),
        'feature_count':  len(feature_cols),
        'test_period':    {'start': config.TEST_START, 'end': config.TEST_END},
    }
    test_metrics_path.write_text(json.dumps(metrics_payload, indent=2, default=str))

    # Per-row predictions for downstream dashboarding / deeper analysis
    pred_cols = ['pickup_hour_ts', 'pickup_location_id', 'pickup_borough',
                 'pickup_zone', 'is_holiday', 'is_weekend', 'month',
                 'target', 'model_pred', 'baseline_pred']
    test[pred_cols].to_parquet(predictions_path, index=False)

    print(f'\nSaved test metrics: {test_metrics_path}')
    print(f'Saved predictions:  {predictions_path}')


if __name__ == '__main__':
    main()
