"""Quick diagnostic — run this to see exactly what's in the NOAA CSV file."""
import gzip, os
from pathlib import Path
import pandas as pd

path = Path(os.environ.get("WEATHER_CSV_GZ", Path.home() / "Downloads" / "USW00094728.csv.gz"))
opener = gzip.open if str(path).endswith(".gz") else open

with opener(path, "rt", encoding="utf-8") as f:
    df = pd.read_csv(f, low_memory=False, nrows=3)

print("=== COLUMNS (raw, before any normalisation) ===")
for c in df.columns:
    print(f"  {repr(c)}")

print(f"\n=== FIRST 3 ROWS ===")
with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 40):
    print(df.to_string())
