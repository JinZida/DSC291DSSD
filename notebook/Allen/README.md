# Allen: Sentinel-2 top-15 image-quality screen

[Executed notebook](top15_cleaning.ipynb) | [Screening code](quality_screen.py) | [Feature assembly](top15_features.py)

This submission uses only the private team_data_full_v1.duckdb. Its raw_v1 and raw_oaf5 schemas contain the individual Sentinel-2 passes. The new database includes the old three-country archive and additional countries; the old database is not required.

## One cleaning method

For each eligible maize field and month, inspect every sampled Level-2A scene. Reject missing or invalid model bands, cloud/shadow at a point, or a polygon with less than 80% clear SCL pixels. Review uncertain SCL, local cloud probability of at least 60%, or sun elevation below 30 degrees. Qualifying scenes receive the original 100-point score: 50 local clear fraction, 25 local cloud probability, 10 scene cloud cover, 10 crop timing, and 5 sun elevation. Prefer independent overflights and retain up to 15 scenes per field-month. Never fill with review or rejected scenes.

This rule gives local field conditions more weight than whole-scene cloud and preserves several usable dates when available. The weights are a transparent proposal, not tuned to yield. Yield labels and model predictions do not enter scene ranking.

The model handoff takes the median of the **selected** scenes' bands within each month, then averages the resulting month features over the crop season. This is feature aggregation after the top-15 selection, not a second screening option. Missing months remain missing.

## Scope and files

The screen uses the official kept, unlocked maize fields in folds 1–4 only. It covers all eligible years in Burundi, Kenya, Malawi, Rwanda, Tanzania, Uganda, Zambia, and Zimbabwe. Nigeria has no eligible development fields. No locked field is screened, summarized, or included in the handoff.

Put the private database at data/team_data_full_v1.duckdb, install notebook/Allen/requirements.txt, and run notebook/Allen/top15_cleaning.ipynb from the repository. Outputs under ignored data/clean/:

- top15_dev_raw_v1/ and top15_dev_raw_oaf5/: candidate decisions, ranked scene selections, monthly coverage, summaries.
- top15_dev_features_raw_v1/ and top15_dev_features_raw_oaf5/: medians of selected scenes and season features.
- top15_model_ready.parquet: one row per eligible development field, with official fold and yield metadata.

**Preprocessed data:** [Download the top-15 model table from Google Drive](https://drive.google.com/file/d/18nHU4yGQfF2Qzli_PNbUTlDgcawmcdj4/view?usp=sharing). This link is for top15_model_ready.parquet, not the private source database. Do not upload the private source database to GitHub or a public Drive location.

## Limits

Most locations are GPS points, so a clear sampled pixel does not prove the field is uncontaminated by nearby land cover. Crop dates are incomplete in some sources. Historical cloud probability is frequently missing and receives neutral score rather than causing data loss. The notebook reports out-of-range converted reflectance without silently changing the selected scenes. Imputation and scaling must be fitted inside each training fold.
