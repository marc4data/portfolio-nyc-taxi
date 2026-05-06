"""
baseline.py — naive forecast benchmark.

Predicts trip_count[T+24] as the average of trip_count at the same hour-of-day
and same day-of-week over the prior 4 weeks. If the LightGBM model can't
beat this, something is wrong with the modeling approach (not the data).

Why this baseline (vs. just "same hour last week"):
    A 4-week average smooths out single-week noise (a one-off rainstorm, a
    stadium concert) — making it a slightly stronger and more stable baseline
    that's harder to beat. Beating it cleanly is a real signal that the
    features are doing useful work.
"""

import sys
from pathlib import Path

# Add ml/demand_prediction/ to sys.path so config.py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

import config


class NaiveForwardWeeklyMean:
    """Same-hour-of-day-and-DoW averaged over the prior 4 weeks.

    Mathematics — given a row at time T predicting target = trip_count at T+H
    (H = HORIZON_HOURS, default 24):

        target time:   T + H
        same hour, same DoW lookbacks: (T+H) - 168·k  for k = 1, 2, 3, 4

    Expressed as shifts from the row time T:

        shift = 168·k - H,  for k = 1, 2, 3, 4

    With H=24 this becomes shifts of 144, 312, 480, 648 hours. All positive,
    all backward-looking, all valid features at row time T.
    """

    HORIZON_HOURS  = config.HORIZON_HOURS
    LOOKBACK_WEEKS = (1, 2, 3, 4)

    def fit(self, df):  # noqa: ARG002 — kept for API consistency with sklearn-style models
        """No fitting needed — this is a fully data-driven baseline."""
        return self

    def predict(self, df: pd.DataFrame) -> pd.Series:
        """Compute baseline predictions on a feature DataFrame.

        Required columns: ``pickup_location_id``, ``pickup_hour_ts``,
        ``trip_count``. Returns a Series of predictions aligned to the
        original index of ``df``.

        Rows where fewer than all 4 weekly lookbacks are available will use
        the average of whatever IS available (skipna=True). For very young
        zones with less than 4 weeks of history, predictions may rely on
        only 1-3 prior weeks.
        """
        df = df.sort_values(['pickup_location_id', 'pickup_hour_ts'])
        g  = df.groupby('pickup_location_id')['trip_count']

        # Per-row prior-week lookups, computed on the densified grid.
        shifts = [168 * k - self.HORIZON_HOURS for k in self.LOOKBACK_WEEKS]
        weekly_lags = pd.DataFrame(
            {f'wk{i}': g.shift(s) for i, s in enumerate(shifts, 1)},
            index=df.index
        )

        return weekly_lags.mean(axis=1, skipna=True)


if __name__ == '__main__':
    # Quick sanity: build a tiny feature matrix and predict with the baseline
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'data'))
    from extract  import extract_hourly_demand
    from features import build_feature_matrix

    raw = extract_hourly_demand('2024-01-01', '2024-03-15')
    fm  = build_feature_matrix(raw, train_end_date='2024-03-15')

    model = NaiveForwardWeeklyMean().fit(fm)
    fm['baseline_pred'] = model.predict(fm)

    n_pred = fm['baseline_pred'].notna().sum()
    print(f'Feature rows:               {len(fm):,}')
    print(f'Rows with baseline pred:    {n_pred:,}')
    print(f'Rows missing baseline pred: {len(fm) - n_pred:,}')
    print(f'\nBaseline predictions vs target:')
    print(fm[['target', 'baseline_pred']].describe().round(2))
