"""
explain.py — Phase 6: SHAP-based interpretability for the tuned LightGBM model.

Loads the Phase 5 tuned model, computes SHAP values on a sampled subset of
validation rows, and saves four diagnostic plots:

    1. shap_global_importance.png   — top 15 features by mean |SHAP value|
    2. shap_summary_beeswarm.png    — per-feature direction of effect
    3. shap_dependence_lag_t24_1w.png — how last-week-same-hour-DoW drives prediction
    4. shap_dependence_hour_sin.png   — cyclical hour-of-day effect

Plus shap_importance.csv with the full feature ranking — useful for the
portfolio narrative when you want a tabular reference instead of just plots.

Why SHAP on a sample, not all 466K val rows:
    TreeSHAP is fast but still O(rows · trees · leaves²). Computing it on
    the full val set takes ~10x longer for results that are visually
    indistinguishable on the plots — feature rankings and direction-of-
    effect stabilize well before 50K samples.

Usage
-----
    python explain.py                    # 50,000 sample (~1-3 min)
    python explain.py --sample-size N    # custom sample size
"""

import sys
import argparse
from pathlib import Path

# Add ml/demand_prediction/ and subfolders to sys.path
_ML_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ML_ROOT))
sys.path.insert(0, str(_ML_ROOT / 'data'))
sys.path.insert(0, str(_ML_ROOT / 'predictors'))

import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import shap

import config
from extract  import extract_hourly_demand
from features import build_feature_matrix
from split    import time_series_split
from baseline import NaiveForwardWeeklyMean
from train    import get_feature_columns


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample-size', type=int, default=50_000,
                        help='Number of val rows to compute SHAP on (default 50,000)')
    args = parser.parse_args()

    # ── 1. Load the tuned model ────────────────────────────────────────────
    model_path = config.MODELS_DIR / 'lgbm_tuned.pkl'
    if not model_path.exists():
        raise FileNotFoundError(
            f'Tuned model not found at {model_path}. Run tune.py first.'
        )
    model = joblib.load(model_path)
    print(f'Loaded tuned model: {model_path}')

    # ── 2. Rebuild val (we don't need train for SHAP) ──────────────────────
    print('Building feature matrix…')
    raw = extract_hourly_demand(config.TRAIN_START, config.VAL_END)
    fm  = build_feature_matrix(raw, train_end_date=config.TRAIN_END)
    fm['baseline_pred'] = NaiveForwardWeeklyMean().predict(fm)
    train, val, _ = time_series_split(fm)

    feature_cols = get_feature_columns(train)
    print(f'  Feature columns: {len(feature_cols)}')

    # ── 3. Sample val and compute SHAP ────────────────────────────────────
    n = min(args.sample_size, len(val))
    print(f'\nSampling {n:,} rows from val ({len(val):,} total)…')
    sample_idx = val.sample(n=n, random_state=config.RANDOM_SEED).index
    X_sample   = val.loc[sample_idx, feature_cols].copy()

    print('Computing SHAP values (TreeSHAP, ~1-3 min)…')
    explainer   = shap.TreeExplainer(model)
    shap_values = explainer(X_sample)

    # ── 4. Generate plots ─────────────────────────────────────────────────
    plots_dir = config.PLOTS_DIR
    plots_dir.mkdir(parents=True, exist_ok=True)
    print(f'\nGenerating SHAP plots → {plots_dir}/')

    # Plot 1: global importance bar
    plt.figure()
    shap.plots.bar(shap_values, max_display=15, show=False)
    plt.title('Global feature importance — mean |SHAP value|', pad=12)
    plt.tight_layout()
    plt.savefig(plots_dir / 'shap_global_importance.png',
                dpi=120, bbox_inches='tight')
    plt.close()
    print('  ✓ shap_global_importance.png')

    # Plot 2: beeswarm summary
    plt.figure()
    shap.plots.beeswarm(shap_values, max_display=15, show=False)
    plt.title('SHAP summary — feature value vs effect on prediction', pad=12)
    plt.tight_layout()
    plt.savefig(plots_dir / 'shap_summary_beeswarm.png',
                dpi=120, bbox_inches='tight')
    plt.close()
    print('  ✓ shap_summary_beeswarm.png')

    # Plot 3: dependence on lag_t24_1w (the target-aligned weekly lag)
    if 'lag_t24_1w' in feature_cols:
        plt.figure()
        shap.plots.scatter(
            shap_values[:, 'lag_t24_1w'],
            color=shap_values, show=False,
        )
        plt.title("Dependence: lag_t24_1w (last week's same-hour-DoW demand)",
                  pad=12)
        plt.tight_layout()
        plt.savefig(plots_dir / 'shap_dependence_lag_t24_1w.png',
                    dpi=120, bbox_inches='tight')
        plt.close()
        print('  ✓ shap_dependence_lag_t24_1w.png')

    # Plot 4: dependence on hour_sin (cyclical hour-of-day)
    if 'hour_sin' in feature_cols:
        plt.figure()
        shap.plots.scatter(
            shap_values[:, 'hour_sin'],
            color=shap_values, show=False,
        )
        plt.title('Dependence: hour_sin (cyclical encoding of hour-of-day)',
                  pad=12)
        plt.tight_layout()
        plt.savefig(plots_dir / 'shap_dependence_hour_sin.png',
                    dpi=120, bbox_inches='tight')
        plt.close()
        print('  ✓ shap_dependence_hour_sin.png')

    # ── 5. Save full feature importance as CSV ────────────────────────────
    importance_df = pd.DataFrame({
        'feature':       feature_cols,
        'mean_abs_shap': np.abs(shap_values.values).mean(axis=0),
    }).sort_values('mean_abs_shap', ascending=False).reset_index(drop=True)

    csv_path = plots_dir / 'shap_importance.csv'
    importance_df.to_csv(csv_path, index=False)
    print(f'  ✓ shap_importance.csv')

    print('\n── Top 15 features by mean |SHAP| ─────────────────────────────')
    print(importance_df.head(15).to_string(index=False))


if __name__ == '__main__':
    main()
