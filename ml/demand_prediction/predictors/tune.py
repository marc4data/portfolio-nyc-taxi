"""
tune.py — Phase 5: hyperparameter tuning with Optuna.

Runs N Optuna trials optimizing validation MAE. Each trial:
    1. Samples hyperparameters from defined search spaces.
    2. Trains LGBMRegressor on a 3-month subset of train (Oct-Dec 2024).
    3. Evaluates on validation set, returns val MAE.

After the study completes, retrains the FINAL model on the full training
set using the best hyperparameters and saves everything to artifacts/.

Why tune on a subset:
    Each trial takes ~3-4 min on the full year of train. Cutting to the
    last 3 months drops per-trial time to ~30-60 sec while preserving the
    relative ranking of hyperparameter combos (the patterns LightGBM finds
    on Q4 2024 are nearly the same as those it finds on the full year).
    The final model still uses all 12 months of train for its actual fit.

Usage
-----
    python tune.py                # 30 trials (default)
    python tune.py --n-trials 60  # more trials, longer run
"""

import sys
import json
import argparse
from pathlib import Path

# Add ml/demand_prediction/ and its data/ + predictors/ subfolders to sys.path
_ML_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ML_ROOT))
sys.path.insert(0, str(_ML_ROOT / 'data'))
sys.path.insert(0, str(_ML_ROOT / 'predictors'))

import joblib
import numpy as np
import pandas as pd
import optuna
import lightgbm as lgb

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


# ── Tuning subset window (last 3 months of train) ────────────────────────────
TUNE_TRAIN_START = '2024-10-01'
TUNE_TRAIN_END   = config.TRAIN_END


# ── Optuna objective ─────────────────────────────────────────────────────────
def make_objective(train_subset: pd.DataFrame, val: pd.DataFrame):
    """Build the Optuna objective that returns val MAE for a given trial.

    Closed over `train_subset` and `val` so each trial doesn't have to
    re-derive the feature columns or re-load anything.
    """
    feature_cols = get_feature_columns(train_subset)

    def objective(trial: optuna.Trial) -> float:
        params = dict(
            objective         = 'regression',
            metric            = 'mae',
            n_estimators      = 1000,                          # capped for trial speed
            learning_rate     = trial.suggest_float('learning_rate',     0.005, 0.05, log=True),
            num_leaves        = trial.suggest_int(  'num_leaves',        15,    255),
            min_child_samples = trial.suggest_int(  'min_child_samples', 10,    500),
            subsample         = trial.suggest_float('subsample',         0.5,   1.0),
            colsample_bytree  = trial.suggest_float('colsample_bytree',  0.5,   1.0),
            reg_alpha         = trial.suggest_float('reg_alpha',         1e-8,  1.0,  log=True),
            reg_lambda        = trial.suggest_float('reg_lambda',        1e-8,  1.0,  log=True),
            random_state      = config.RANDOM_SEED,
            n_jobs            = -1,
            verbose           = -1,
        )
        model = lgb.LGBMRegressor(**params)
        model.fit(
            train_subset[feature_cols], train_subset['target'],
            eval_set=[(val[feature_cols], val['target'])],
            eval_metric='mae',
            callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)],
        )
        preds = model.predict(val[feature_cols])
        return float(np.abs(val['target'].values - preds).mean())

    return objective


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n-trials', type=int, default=30,
                        help='Number of Optuna trials (default 30)')
    args = parser.parse_args()

    # ── 1. Build data ──────────────────────────────────────────────────────
    print('Building feature matrix…')
    raw = extract_hourly_demand(config.TRAIN_START, config.VAL_END)
    fm  = build_feature_matrix(raw, train_end_date=config.TRAIN_END)
    fm['baseline_pred'] = NaiveForwardWeeklyMean().predict(fm)

    train, val, _ = time_series_split(fm)
    train_subset  = train[train['pickup_hour_ts'] >= pd.Timestamp(TUNE_TRAIN_START)].copy()
    print(f'  Train rows:        {len(train):>12,}')
    print(f'  Train subset rows: {len(train_subset):>12,}  (Oct–Dec 2024, used for Optuna trials)')
    print(f'  Val rows:          {len(val):>12,}')

    # ── 2. Run Optuna study ────────────────────────────────────────────────
    print(f'\nRunning Optuna study — {args.n_trials} trials…')
    study = optuna.create_study(
        direction='minimize',
        sampler=optuna.samplers.TPESampler(seed=config.RANDOM_SEED),
    )
    objective = make_objective(train_subset, val)
    study.optimize(objective, n_trials=args.n_trials, show_progress_bar=True)

    print(f'\nBest val MAE (on subset training): {study.best_value:.4f}')
    print(f'Best params:')
    for k, v in study.best_params.items():
        print(f'  {k:20s} = {v}')

    # ── 3. Final model on FULL train data with best params ────────────────
    print('\nTraining FINAL model on full train data with best params…')
    feature_cols = get_feature_columns(train)
    final_params = {
        **study.best_params,
        'objective':    'regression',
        'metric':       'mae',
        'n_estimators': 2000,
        'random_state': config.RANDOM_SEED,
        'n_jobs':       -1,
        'verbose':      -1,
    }
    final_model = lgb.LGBMRegressor(**final_params)
    final_model.fit(
        train[feature_cols], train['target'],
        eval_set=[(val[feature_cols], val['target'])],
        eval_metric='mae',
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )
    print(f'  Best iteration on full train: {final_model.best_iteration_}')

    # ── 4. Evaluate ────────────────────────────────────────────────────────
    val = add_time_of_day(val.copy())
    val['model_pred'] = final_model.predict(val[feature_cols])

    summary        = evaluate_against_baseline(val, 'target', 'model_pred', 'baseline_pred')
    by_borough     = compute_metrics_breakdown(val, 'target', 'model_pred', 'pickup_borough')
    by_time_of_day = compute_metrics_breakdown(val, 'target', 'model_pred', 'time_of_day')

    print('\n── Validation overall — TUNED model vs baseline ──────────────')
    print(json.dumps(summary, indent=2, default=str))
    print('\n── By borough ─────────────────────────────────────────────────')
    print(by_borough.round(2))
    print('\n── By time of day ─────────────────────────────────────────────')
    print(by_time_of_day.round(2))

    # ── 5. Save artifacts ──────────────────────────────────────────────────
    model_path   = config.MODELS_DIR / 'lgbm_tuned.pkl'
    params_path  = config.MODELS_DIR / 'best_params.json'
    study_path   = config.MODELS_DIR / 'optuna_study.pkl'
    metrics_path = config.METRICS_FILE

    joblib.dump(final_model, model_path)
    joblib.dump(study,       study_path)
    params_path.write_text(json.dumps(study.best_params, indent=2))

    metrics_payload = {
        'phase':           'phase_5_optuna_tuned',
        'n_trials':        args.n_trials,
        'best_subset_mae': study.best_value,
        'overall':         summary,
        'by_borough':      by_borough.to_dict(orient='index'),
        'by_time_of_day':  by_time_of_day.to_dict(orient='index'),
        'best_iteration':  int(final_model.best_iteration_) if final_model.best_iteration_ else None,
        'feature_count':   len(feature_cols),
        'best_params':     study.best_params,
    }
    metrics_path.write_text(json.dumps(metrics_payload, indent=2, default=str))

    print(f'\nSaved tuned model:  {model_path}')
    print(f'Saved best params:  {params_path}')
    print(f'Saved Optuna study: {study_path}')
    print(f'Saved metrics:      {metrics_path}')


if __name__ == '__main__':
    main()
