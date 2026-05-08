"""
briefing.py — generate an LLM-authored executive briefing from model artifacts.

Reads the structured outputs from the ML pipeline:
    - artifacts/metrics.json          (overall + breakdown metrics)
    - artifacts/plots/shap_importance.csv (top features)

Sends them to Claude with a senior-analyst system prompt and saves the
markdown response to artifacts/executive_briefing.md.

Demonstrates three Claude API patterns relevant to production LLM apps:
    1. Structured prompting (system + user roles, exact-format output)
    2. Prompt caching (the long structured payload is cached so that
       re-runs during iteration cost ~10% of the first run)
    3. Cost transparency — usage stats printed after each call

Usage
-----
    python briefing.py                             # default Sonnet 4.6
    python briefing.py --model claude-haiku-4-5-20251001   # cheaper, quicker
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add ml/demand_prediction/ to sys.path so config and ai/ are importable
_ML_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ML_ROOT))

import pandas as pd
from dotenv import load_dotenv
import anthropic

import config
from ai.prompts import SYSTEM_PROMPT, build_user_message


# Layered ANTHROPIC_API_KEY resolution — first match wins, never overrides:
#   1. Already set in os.environ (shell export, CI, etc.)
#   2. ~/.anthropic/.env  ← preferred: one credential file, all projects
#   3. <repo>/.env        ← fallback for local one-off overrides
#
# This matches how the rest of the project handles credentials — Snowflake
# auth comes from ~/.dbt/profiles.yml + ~/.ssh/snowflake/, not project files.
_REPO_ROOT = _ML_ROOT.parent.parent
_HOME_ENV  = Path.home() / '.anthropic' / '.env'

if not os.getenv('ANTHROPIC_API_KEY'):
    load_dotenv(_HOME_ENV, override=False)
if not os.getenv('ANTHROPIC_API_KEY'):
    load_dotenv(_REPO_ROOT / '.env', override=False)


# Sonnet 4.6 by default — better narrative prose than Haiku at modest cost
DEFAULT_MODEL = 'claude-sonnet-4-6'


def load_artifacts() -> dict:
    """Read every structured artifact the briefing summarizes."""
    metrics_path = config.METRICS_FILE
    if not metrics_path.exists():
        raise FileNotFoundError(
            f'No metrics at {metrics_path}.\n'
            f'Run train.py + tune.py first to populate it.'
        )
    metrics = json.loads(metrics_path.read_text())

    shap_path = config.PLOTS_DIR / 'shap_importance.csv'
    if not shap_path.exists():
        raise FileNotFoundError(
            f'No SHAP importance at {shap_path}.\n'
            f'Run explain.py first.'
        )
    shap_top = pd.read_csv(shap_path).head(10).to_dict(orient='records')

    # Feature column list saved by train.py — gives Claude grounding for the
    # SHAP discussion (knows what features exist, not just the top-10 names)
    features_path = config.MODELS_DIR / 'feature_columns.json'
    feature_cols  = (json.loads(features_path.read_text())
                     if features_path.exists() else [])

    # Split summary — derived from config.py constants; gives the briefing
    # concrete date ranges to reference instead of vague "training window"
    split_summary = {
        'train': {'start': config.TRAIN_START, 'end': config.TRAIN_END},
        'val':   {'start': config.VAL_START,   'end': config.VAL_END},
        'test':  {'start': config.TEST_START,  'end': config.TEST_END},
        'horizon_hours': config.HORIZON_HOURS,
    }

    return {
        'metrics':         metrics,
        'shap_top10':      shap_top,
        'feature_columns': feature_cols,
        'split_summary':   split_summary,
        'generated_at':    datetime.now().strftime('%Y-%m-%d %H:%M'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default=DEFAULT_MODEL,
                        help=f'Claude model id (default {DEFAULT_MODEL})')
    parser.add_argument('--output', default=None,
                        help='Output path (default artifacts/executive_briefing.md)')
    args = parser.parse_args()

    if not os.getenv('ANTHROPIC_API_KEY'):
        raise EnvironmentError(
            'ANTHROPIC_API_KEY not set. Set it via one of:\n'
            f'  1. ~/.anthropic/.env  (preferred — single key shared across projects)\n'
            f'  2. {_REPO_ROOT / ".env"}\n'
            f'  3. export ANTHROPIC_API_KEY=... in your shell\n'
            f'\nFile contents should be:  ANTHROPIC_API_KEY=sk-ant-api03-...'
        )

    artifacts = load_artifacts()
    user_msg  = build_user_message(artifacts)

    print(f'Loaded artifacts (metrics phase: {artifacts["metrics"]["phase"]})')
    print(f'Calling Claude — model: {args.model}')

    client = anthropic.Anthropic()

    # cache_control on the user content marks it as cacheable. First run
    # writes the cache (~25% input-token surcharge); subsequent runs hit
    # cache (~10% of normal input-token cost on the cached portion).
    response = client.messages.create(
        model=args.model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[
            {
                'role': 'user',
                'content': [
                    {
                        'type': 'text',
                        'text': user_msg,
                        'cache_control': {'type': 'ephemeral'},
                    }
                ],
            }
        ],
    )

    # Cost transparency — useful for portfolio narrative + iteration tracking
    u = response.usage
    print(f'\nUsage stats:')
    print(f'  input_tokens:               {u.input_tokens:,}')
    print(f'  output_tokens:              {u.output_tokens:,}')
    print(f'  cache_creation_input_tokens: {getattr(u, "cache_creation_input_tokens", 0) or 0:,}')
    print(f'  cache_read_input_tokens:     {getattr(u, "cache_read_input_tokens",     0) or 0:,}')

    briefing_text = response.content[0].text

    output_path = Path(args.output) if args.output \
                  else config.ARTIFACTS / 'executive_briefing.md'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Stamp model + timestamp at the top so reviewers know when it was generated
    header = (
        f'<!-- Generated {artifacts["generated_at"]} | '
        f'model: {args.model} | '
        f'metrics phase: {artifacts["metrics"]["phase"]} -->\n\n'
    )
    output_path.write_text(header + briefing_text)
    print(f'\nSaved briefing: {output_path}')


if __name__ == '__main__':
    main()
