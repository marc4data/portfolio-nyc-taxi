"""
train.py — Phase 4: LightGBM with fixed hyperparameters.

Trains a single LGBMRegressor on the full training feature matrix, predicts
on validation, prints model-vs-baseline metrics, and saves the model +
metrics to artifacts/.

Phase 5 will add Optuna hyperparameter tuning. Phase 6 adds SHAP plots.

Usage
-----
    python train.py

Training on the full 12-month train set (~1.9M rows after feature engineering)
takes a few minutes on a laptop. Output is saved to:
    artifacts/models/lgbm_baseline_hyperparams.pkl
    artifacts/models/feature_columns.json
    artifacts/metrics.json
"""

import sys
import json
from pathlib import Path

# Add ml/demand_prediction/ and its data/ subfolder to sys.path so we can
# import config + the upstream pipeline modules regardless of cwd.
_ML_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ML_ROOT))
sys.path.insert(0, str(_ML_ROOT / 'data'))
sys.path.insert(0, str(_ML_ROOT / 'predictors'))

import joblib
import numpy as np
import pandas as pd
import lightgbm as lgb

import config
from extract  import extract_hourly_demand
from features import build_feature_matrix
from split    import time_series_split
from baseline import NaiveForwardWeeklyMean
from evaluate import (
    compute_metrics, compute_metrics_breakdown,
    evaluate_against_baseline, add_time_of_day,
)


# ── Feature column selection ─────────────────────────────────────────────────
# Columns that must NEVER be used as features:
#   - identifiers (would let the model memorize specific rows / zones / hours)
#   - raw time fields that cyclical encodings already capture
#   - the target itself
EXCLUDED_COLS = {
    'demand_id', 'pickup_hour_ts', 'pickup_date',
    'pickup_location_id', 'pickup_borough', 'pickup_zone',
    'pickup_service_zone',
    'pickup_hour', 'day_of_week', 'month', 'year',
    'target',
    # Side-channel signals attached after split — never use as features
    'baseline_pred',     # would let the model trivially copy the baseline's answer
    'model_pred',        # added after model fits
    'time_of_day',       # added for metric breakdowns only
}


def get_feature_columns(df: pd.DataFrame) -> list:
    """Every column in df EXCEPT the excluded set."""
    return [c for c in df.columns if c not in EXCLUDED_COLS]


# ── Default hyperparameters ──────────────────────────────────────────────────
# Reasonable starting values. Phase 5 will replace these via Optuna tuning.
# n_estimators bumped to 2000 + lr lowered to 0.02 so early stopping has room
# to find a true optimum instead of hitting the trees ceiling.
DEFAULT_PARAMS = dict(
    objective         = 'regression',     # MSE loss, predicts conditional mean
    metric            = 'mae',            # what early-stopping watches
    n_estimators      = 2000,             # max trees; early stopping cuts this
    learning_rate     = 0.02,
    num_leaves        = 63,
    min_child_samples = 100,              # min rows in a leaf — guards overfit
    subsample         = 0.8,              # row sampling per tree
    colsample_bytree  = 0.8,              # feature sampling per tree
    random_state      = config.RANDOM_SEED,
    n_jobs            = -1,
    verbose           = -1,
)


def train_lgbm(
    train: pd.DataFrame,
    val:   pd.DataFrame,
    params: dict | None = None,
    feature_cols: list | None = None,
    early_stopping_rounds: int = 50,
):
    """Fit a LGBMRegressor with early stopping on validation MAE.

    Parameters
    ----------
    train, val      : feature-matrix DataFrames produced by features.py
    params          : optional override for DEFAULT_PARAMS
    feature_cols    : optional explicit feature list (default: derived)
    early_stopping_rounds : stop if val MAE doesn't improve for this many rounds

    Returns
    -------
    (model, feature_cols) — fitted LGBMRegressor and the feature list it used
    """
    p  = {**DEFAULT_PARAMS, **(params or {})}
    fc = feature_cols or get_feature_columns(train)

    model = lgb.LGBMRegressor(**p)
    model.fit(
        train[fc], train['target'],
        eval_set=[(val[fc], val['target'])],
        eval_metric='mae',
        callbacks=[lgb.early_stopping(early_stopping_rounds, verbose=False)],
    )
    return model, fc


def main():
    # ── 1. Build the data ──────────────────────────────────────────────────
    print('Building feature matrix…')
    raw = extract_hourly_demand(config.TRAIN_START, config.VAL_END)
    fm  = build_feature_matrix(raw, train_end_date=config.TRAIN_END)

    # Compute baseline predictions on the FULL feature matrix BEFORE splitting,
    # so it has access to all the prior history it needs (a baseline call on
    # val alone would NaN out the first 6 days where lookbacks fall before
    # val's start).
    print('Computing naive baseline predictions on full feature matrix…')
    fm['baseline_pred'] = NaiveForwardWeeklyMean().predict(fm)

    train, val, _ = time_series_split(fm)
    print(f'  Train rows: {len(train):>12,}')
    print(f'  Val rows:   {len(val):>12,}')

    # ── 2. Train LightGBM ──────────────────────────────────────────────────
    print('\nTraining LightGBM (fixed hyperparameters)…')
    model, feature_cols = train_lgbm(train, val)
    print(f'  Best iteration: {model.best_iteration_}')
    print(f'  Features used:  {len(feature_cols)}')

    # ── 3. Predict on val with model (baseline already computed above) ─────
    val = add_time_of_day(val.copy())
    val['model_pred'] = model.predict(val[feature_cols])

    # ── 4. Compute and print metrics ───────────────────────────────────────
    summary = evaluate_against_baseline(val, 'target',
                                        'model_pred', 'baseline_pred')
    print('\n── Validation overall — model vs baseline ──────────────────────')
    print(json.dumps(summary, indent=2, default=str))

    by_borough     = compute_metrics_breakdown(val, 'target', 'model_pred',
                                                'pickup_borough')
    by_time_of_day = compute_metrics_breakdown(val, 'target', 'model_pred',
                                                'time_of_day')

    print('\n── Model metrics by borough ────────────────────────────────────')
    print(by_borough.round(2))
    print('\n── Model metrics by time of day ────────────────────────────────')
    print(by_time_of_day.round(2))

    # ── 5. Save artifacts ──────────────────────────────────────────────────
    model_path   = config.MODELS_DIR / 'lgbm_baseline_hyperparams.pkl'
    feature_path = config.MODELS_DIR / 'feature_columns.json'
    metrics_path = config.METRICS_FILE

    joblib.dump(model, model_path)
    feature_path.write_text(json.dumps(feature_cols, indent=2))

    metrics_payload = {
        'phase': 'phase_4_fixed_hyperparams',
        'overall':        summary,
        'by_borough':     by_borough.to_dict(orient='index'),
        'by_time_of_day': by_time_of_day.to_dict(orient='index'),
        'best_iteration': int(model.best_iteration_) if model.best_iteration_ else None,
        'feature_count':  len(feature_cols),
    }
    metrics_path.write_text(json.dumps(metrics_payload, indent=2, default=str))

    print(f'\nSaved model:    {model_path}')
    print(f'Saved features: {feature_path}')
    print(f'Saved metrics:  {metrics_path}')


if __name__ == '__main__':
    main()
