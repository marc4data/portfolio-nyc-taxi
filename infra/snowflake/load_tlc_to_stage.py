"""
load_tlc_to_stage.py
--------------------
PUTs local NYC TLC Yellow Taxi parquet files into the Snowflake stage:
    @TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE/yellow/

Reads all files matching yellow_tripdata_2024-*.parquet and
yellow_tripdata_2025-*.parquet from LOCAL_DATA_DIR.

Usage:
    python load_tlc_to_stage.py

Connection:
    Reads config_snowflake.yaml — credentials via .env (see .env.example).
    Uses dbprofile's SnowflakeConnector (key-pair preferred, password fallback).
    Run `pip install -e /path/to/dbprofile` once if not already installed.
"""

import sys
from pathlib import Path

from snowflake_conn import get_connector

# ── Optional tqdm ─────────────────────────────────────────────────────────────
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

LOCAL_DATA_DIR = Path("/Users/marcalexander/nyc_tlc_data/yellow")
STAGE_PATH     = "@TAXI_PORTFOLIO.RAW.NYC_TLC_STAGE/yellow/"
YEARS          = [2024, 2025]

# ─────────────────────────────────────────────────────────────────────────────


def find_local_files() -> list[Path]:
    """Return sorted list of matching parquet files for the target years."""
    files = []
    for year in YEARS:
        files.extend(sorted(LOCAL_DATA_DIR.glob(f"yellow_tripdata_{year}-*.parquet")))
    return files


def list_staged_files(connector) -> set[str]:
    rows = connector.execute(f"LIST {STAGE_PATH}")
    # LIST returns rows with 'name' column; strip path prefix to get filename
    return {r["name"].split("/")[-1] for r in rows}


def put_file(connector, local_path: Path) -> str:
    """PUT a single file; returns Snowflake's status string (e.g. UPLOADED)."""
    rows = connector.execute(
        f"PUT 'file://{local_path}' {STAGE_PATH} "
        "AUTO_COMPRESS=FALSE "
        "OVERWRITE=FALSE"
    )
    # PUT result columns: source, target, source_size, target_size, status, ...
    return rows[0].get("status", "UNKNOWN") if rows else "UNKNOWN"


def main():
    if not LOCAL_DATA_DIR.is_dir():
        sys.exit(f"ERROR: LOCAL_DATA_DIR not found: {LOCAL_DATA_DIR}")

    local_files = find_local_files()
    if not local_files:
        sys.exit(f"ERROR: No matching parquet files found in {LOCAL_DATA_DIR}")

    print(f"Found {len(local_files)} local file(s) to consider.\n")

    print("Connecting to Snowflake …")
    connector = get_connector()
    print("  Connected.\n")

    try:
        print("Checking stage for existing files …")
        already_staged = list_staged_files(connector)
        if already_staged:
            print(f"  {len(already_staged)} file(s) already in stage — will skip.\n")

        results = []
        iterator = tqdm(local_files, unit="file") if HAS_TQDM else local_files

        for path in iterator:
            name    = path.name
            size_mb = path.stat().st_size / (1 << 20)

            if not HAS_TQDM:
                print(f"  {name}  ({size_mb:.1f} MB) … ", end="", flush=True)

            if name in already_staged:
                status = "SKIPPED"
            else:
                try:
                    status = put_file(connector, path)
                except Exception as exc:
                    status = f"ERROR: {exc}"

            if not HAS_TQDM:
                print(status)

            results.append((name, size_mb, status))

    finally:
        connector.close()

    print("\n" + "─" * 65)
    print(f"{'FILE':<42}  {'MB':>6}  STATUS")
    print("─" * 65)
    for fname, mb, status in results:
        print(f"{fname:<42}  {mb:>6.1f}  {status}")

    uploaded = sum(1 for _, _, s in results if s == "UPLOADED")
    skipped  = sum(1 for _, _, s in results if s == "SKIPPED")
    errors   = sum(1 for _, _, s in results if "ERROR" in s)

    print("─" * 65)
    print(f"Uploaded: {uploaded}  |  Skipped (already staged): {skipped}  |  Errors: {errors}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
