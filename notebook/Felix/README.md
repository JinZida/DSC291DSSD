# Felix Yu · Maize data cleaning

Start with [the cleaning notebook](seasonal_contrast.ipynb). It produces one row per eligible, unlocked field-season for the team’s yield models. It uses clear monthly Sentinel-2 medians, previous-year canopy comparisons, and valid rain, temperature, soil and terrain measurements. Fields with sparse imagery stay in the table.

## Reproduce the dataset

1. Place the team’s `team_data_full_v1.duckdb` in the repository’s `data/` folder.
2. From the repository root, install the shared dependencies in your Python 3.12 environment with `python -m pip install -r requirements.txt`.
3. Open `notebook/Felix/seasonal_contrast.ipynb` and run through **section 5**. The cleaned table is written to `data/clean/felix_seasonal_contrast.parquet`. Run the optional Ridge section to reproduce the comparison.

## What Zida can use

The shared release yields 25,579 rows from eight countries. The notebook audits all nine countries, including Nigeria, where the archive has no eligible field locations. The target is `yield_t_ha`. Each row has 140 predictors, including the categorical `survey_year`. Missing numeric values are preserved for training-fold imputation.

From the repository root, this example loads the export and selects the team’s first spatial fold.

```python
import pandas as pd
from scripts.spatial_folds import train_test

table = pd.read_parquet("data/clean/felix_seasonal_contrast.parquet")
train, test = train_test(table, 1)
metadata = [
    "field_season_id", "release", "country_iso3", "data_set", "season_year",
    "yield_t_ha", "fold", "locked", "gap_1", "gap_2", "gap_3", "gap_4",
]
X = table.drop(columns=metadata)
y = table["yield_t_ha"]
```

Fit the numeric imputer, scaler and survey/year encoder on `X.loc[train]`. Score on `X.loc[test]`. The executed notebook reports pooled Ridge RMSE of **1.5667 t/ha** for this table and **1.6037 t/ha** for a clearest-pass reference evaluated on the same fields. Most of the improvement came from the environmental measurements.
