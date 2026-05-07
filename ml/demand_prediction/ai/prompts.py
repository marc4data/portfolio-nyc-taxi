"""
prompts.py — prompt templates for the LLM-authored executive briefing.

The system prompt frames Claude as a senior data analyst writing for an
operations executive. The user message bundles the structured artifacts
(metrics, SHAP importance, model details) into a single cacheable payload.

Why these are split into separate files (prompts.py vs briefing.py):
    Prompt engineering is its own discipline — keeping the prompts in a
    standalone module makes them easy to iterate on, diff in version
    control, and reuse if other AI features are added later.
"""

import json
from typing import Any


# ── System prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a senior data analyst writing an executive briefing for the operations director of NYC's Taxi & Limousine Commission. Your reader is a busy decision-maker who needs to understand a new demand-forecasting model — not a data scientist.

Your output must be:
- A markdown document
- 600–900 words, structured with clear section headers
- Specific: every claim grounded in the metrics provided
- Plainspoken: translate technical concepts to business language
- Honest about what the model can and can't do

Required sections (use these exact H2 headers):
1. ## Executive Summary  (3–4 sentences capturing the headline)
2. ## Model Performance  (model vs. baseline, plain English)
3. ## Where It Works Best — and Where It Doesn't  (borough + time-of-day breakdown)
4. ## What Drives Predictions  (SHAP-based, kept accessible)
5. ## Caveats & Limitations
6. ## Recommended Next Steps  (3–5 concrete actions)

Translate any technical term that appears (e.g. "MAE", "lift", "SHAP") with a brief inline explanation the first time it's used, then use plain-language equivalents thereafter. Use specific numbers from the data — round to 1 or 2 decimals where they help readability.

Open with the headline, not with caveats. End with action, not summary."""


def build_user_message(artifacts: dict[str, Any]) -> str:
    """Compose the user message that delivers structured artifacts to Claude.

    Putting all the data in one large message (rather than spreading it
    across many small messages) lets Anthropic's prompt cache treat it
    as a single re-usable block — subsequent runs hit the cache and pay
    ~10% of the input-token cost.

    The user-message body is intentionally substantial (>2K tokens) so that
    Sonnet 4.6 actually engages prompt caching — the model has a minimum
    cached-prefix size and a leaner prompt won't trigger it.
    """
    metrics       = artifacts['metrics']
    shap_top      = artifacts['shap_top10']
    feature_cols  = artifacts.get('feature_columns', [])
    split_summary = artifacts.get('split_summary', {})

    # Note: `generated_at` is intentionally NOT included here. It changes on
    # every call and would invalidate the prompt cache (cache key = hash of
    # cached content). The timestamp lives in the markdown output header
    # written by briefing.py instead.
    return f"""Generate the executive briefing per the system prompt, using the artifacts below.

## Run metadata
- Pipeline phase: {metrics['phase']}
- Total Optuna trials: {metrics.get('n_trials', 'n/a')}
- Algorithm: LightGBM gradient-boosted regressor (LGBMRegressor)
- Trees used (best_iteration after early stopping): {metrics.get('best_iteration')}
- Total feature columns: {metrics.get('feature_count')}

## Train / Validation / Test split
{json.dumps(split_summary, indent=2, default=str)}

## Overall validation metrics (model vs. naive baseline)
{json.dumps(metrics['overall'], indent=2, default=str)}

## Performance by borough
{json.dumps(metrics['by_borough'], indent=2, default=str)}

## Performance by time-of-day
{json.dumps(metrics['by_time_of_day'], indent=2, default=str)}

## Top 10 features by mean |SHAP value|
{json.dumps(shap_top, indent=2, default=str)}

## Full feature inventory (40 columns) — for grounding the SHAP discussion
{json.dumps(feature_cols, indent=2, default=str)}

## Glossary the briefing should silently rely on
- **Hourly demand grain**: one observation = (NYC TLC zone, hour-truncated timestamp).
  The mart `fct_hourly_demand` materializes this in Snowflake.
- **Naive baseline**: for predicting demand at hour T+24, averages the trip
  count from 4 prior weeks at the same hour-of-day and same day-of-week
  (lookbacks: 168, 336, 504, 672 hours before T+24). NYC taxi demand is
  heavily weekly-cyclical, so this baseline already captures most of the
  signal — meaningful lift over it requires the model to find non-weekly
  patterns (recent shocks, weather, holidays, zone-specific drift).
- **MAE**: Mean Absolute Error — the typical |predicted − actual| miss size,
  in trips per zone-hour. Easy for non-technical readers: "off by N trips."
- **RMSE**: Root Mean Squared Error — squares the misses before averaging,
  so big misses dominate. RMSE > MAE means errors aren't uniform; tail
  misses are amplified.
- **MAPE**: Mean Absolute Percentage Error — relative error. Useful for
  comparing zones with very different volumes; misleading on hours with
  many near-zero counts (small denominators inflate the percentage).
- **lift_mae_pct**: % improvement of model MAE over baseline MAE on
  out-of-sample validation data. Positive = model is better.
- **lag_t24_Nw**: trip count from N weeks before the prediction time T+24.
  These features were added mid-project after the original lag set
  referenced the wrong time anchor (row time T instead of target time T+24)
  — the single most impactful feature-engineering change in the project.
- **SHAP value**: signed contribution of one feature to one prediction.
  Mean |SHAP| ranks features by overall influence; for predictions, SHAP
  decomposes the model's output into per-feature pushes up or down.
- **Optuna**: Bayesian hyperparameter tuner; 30 trials searched the LightGBM
  configuration space minimizing validation MAE.

## Dataset context
- 24 months of NYC TLC Yellow Taxi trip data (2024–2025).
- ~265 distinct TLC zones span all 5 boroughs plus Newark Airport (EWR).
- After densifying to a complete (zone × hour) grid and filtering zones
  with <1,000 trips in the training window, the model trains on ~150
  high-volume zones; lower-volume zones aren't dropped from analytics,
  just from this specific predictive model.
- Weather joined from NOAA daily weather (Central Park station). Daily-grain
  weather is repeated across the 24 hourly slices of each date — known
  coarseness, not a bug.
- Train: 2024 calendar year (full annual seasonality).
- Validation: 2025 Q1 (3 months) — used for hyperparameter tuning.
- Test: 2025 Q2–Q4 — held out for final evaluation (not yet executed).

Write the briefing now."""
