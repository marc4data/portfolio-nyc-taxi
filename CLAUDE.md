# CLAUDE.md — NYC TLC Portfolio Project

Project-specific instructions layered on top of the root CLAUDE.md.

---

## Key Files

| File | Purpose |
|---|---|
| `notebooks/eda_helpers.py` | Reusable chart helpers — all visualization functions |
| `notebooks/eda_helpers_call_templates.py` | Copy-paste templates with all params for each helper |
| `notebooks/eda_fct_trips.ipynb` | Main EDA notebook |

---

## Call Templates Maintenance

`eda_helpers_call_templates.py` must stay in sync with `eda_helpers.py`.

**When building or modifying a helper function in eda_helpers.py:**
1. **New function:** Add a template to the appropriate class in the templates file. Include ALL parameters with defaults, commented out except required ones.
2. **Modified parameters:** Update the affected template to match the new signature.
3. **Full rebuild:** Rewrite the entire templates file — scan all `def plot_*` and `def _draw_*` functions in eda_helpers.py, regenerate every template.

**Classes in the templates file (VS Code Outline categories):**
- `TIME_SERIES` — plot_daily_trips, plot_borough_detail
- `DISTRIBUTIONS` — plot_distribution, plot_histogram, plot_boxplot
- `STRING_PROFILING` — plot_string_profile, plot_string_profile_hc
- `DATA_QUALITY` — plot_indicators

**Template naming convention:** `template_<function_name>`

---

## Autoreload Behavior

- Changes to existing functions in `eda_helpers.py` → picked up automatically by `%autoreload 2`
- **New functions** added to `eda_helpers.py` → require kernel restart
- New constants → require kernel restart
