"""
snowflake_conn.py
-----------------
Shared Snowflake connection helper for portfolio_nyc_tlc loaders.

Reads config_snowflake.yaml (same ${ENV_VAR} format as dbprofile), resolves
environment variables, and returns a SnowflakeConnector instance imported
directly from dbprofile — so connection logic lives in exactly one place.

Usage:
    from snowflake_conn import get_connector

    connector = get_connector()
    try:
        connector.execute("SELECT CURRENT_VERSION()")
        # For write_pandas / PUT commands, access the raw connection:
        raw_conn = connector._conn
    finally:
        connector.close()

Pre-requisite (one-time):
    pip install -e /Users/marcalexander/projects/ai_orchestrator_claude/dbprofile

Why import from dbprofile?
    dbprofile's SnowflakeConnector handles both private-key and password auth,
    normalises column names to lowercase, and is already tested. No reason to
    duplicate that logic here.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

# ── Optional .env loading ─────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # python-dotenv not installed — rely on shell environment

# ── dbprofile import ──────────────────────────────────────────────────────────
try:
    from dbprofile.connectors.base import SnowflakeConnector
except ImportError as exc:
    raise ImportError(
        "dbprofile is not installed. Run:\n"
        "  pip install -e /Users/marcalexander/projects/ai_orchestrator_claude/dbprofile\n"
        "Then retry."
    ) from exc

# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_CONFIG = Path(__file__).parent / "config_snowflake.yaml"


def _resolve_env_vars(value: str) -> str:
    """Replace ${VAR_NAME} placeholders with environment variable values."""
    def _sub(match: re.Match) -> str:
        var = match.group(1)
        val = os.environ.get(var, "")
        if not val:
            # Warn but don't crash — missing optional vars (e.g. ROLE) are fine
            import warnings
            warnings.warn(f"snowflake_conn: env var '{var}' is not set", stacklevel=3)
        return val

    return re.sub(r"\$\{(\w+)\}", _sub, value)


def _resolve(obj):
    """Recursively resolve ${ENV_VAR} in all string values of a parsed YAML dict."""
    if isinstance(obj, dict):
        return {k: _resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve(v) for v in obj]
    if isinstance(obj, str):
        return _resolve_env_vars(obj)
    return obj


def get_connector(config_path: Path = _DEFAULT_CONFIG) -> SnowflakeConnector:
    """
    Read config_snowflake.yaml, resolve env vars, and return a connected
    SnowflakeConnector.

    Parameters
    ----------
    config_path : Path
        Path to a dbprofile-format YAML config. Defaults to
        portfolio_nyc_tlc/config_snowflake.yaml.

    Returns
    -------
    SnowflakeConnector
        Connected instance. Caller is responsible for calling .close().
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}\n"
            "Expected a file with 'connection:' and 'scope:' sections."
        )

    with open(config_path) as f:
        cfg = _resolve(yaml.safe_load(f))

    conn_cfg  = cfg.get("connection", {})
    scope_cfg = cfg.get("scope", {})

    # Treat empty strings as None so SnowflakeConnector's optional params work
    def _or_none(v):
        return v if v else None

    return SnowflakeConnector(
        account           = conn_cfg["account"],
        user              = conn_cfg["user"],
        database          = scope_cfg.get("database", "TAXI_PORTFOLIO"),
        warehouse         = _or_none(conn_cfg.get("warehouse")),
        role              = _or_none(conn_cfg.get("role")),
        private_key_path  = _or_none(conn_cfg.get("private_key_path")),
        private_key_passphrase = _or_none(conn_cfg.get("private_key_passphrase")),
        password          = _or_none(conn_cfg.get("password")),
    )
