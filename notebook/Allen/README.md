# Full database maize cleaning

Open [the executed notebook](full_database_cleaning.ipynb). It reads only the private team_data_full_v1.duckdb and produces two matched season-level feature arms from the monthly Sentinel-2 summaries already in that file: the clearest image and the median of clear images.

## Data and scope

Place team_data_full_v1.duckdb in the repository's ignored data/ folder. The file supplied for this submission has SHA-256 17fe82d849c7afff71ff20152fff42b912458ffecc75dec1f54d2d67e6149fd9. Its own readme marks it private: **do not commit or publicly share this database**.

The notebook inventories every maize country and year. It uses the database's official keep decision and folds_all table, then excludes locked fields and fold 0. The development cohort has **25,579 fields across eight countries** (Burundi, Kenya, Malawi, Rwanda, Tanzania, Uganda, Zambia and Zimbabwe). Nigeria appears in the inventory but has no eligible fields. The export contains **51,158 rows**, one per field and aggregation arm. No source labels, folds or imagery tables are altered.

## Run

From the repository root:

    python -m pip install -r notebook/Allen/requirements.txt

Open notebook/Allen/full_database_cleaning.ipynb and run all cells. The notebook writes data/clean/allen_model_ready.parquet. The entire data/ folder is ignored by Git, so neither the private database nor the derived field table is committed.

## Cleaning and model handoff

For this_season, a month is used in both arms only if monthly_clearest marks it clear and both monthly_clearest and monthly_median have all ten reflectance bands. Missing months stay missing. The notebook reports out-of-range reflectance values but does not silently clip them. It computes ten season-mean bands, mean and maximum NDVI, scheduled and observed month counts, clear-pass support and coverage fraction.

The output preserves field_season_id, release, country_iso3, data_set, season_year, yield_t_ha, cleaning_arm and the official fold, locked and gap_1 through gap_4 columns. Filter to one cleaning_arm before training. Use yield_t_ha as the target. IDs, labels, country/source identifiers and fold/lock/gap columns are metadata; fit imputers and scalers only on each training fold. Compare arms with the same model and spatial splits.

The full database has monthly summaries but not every raw satellite scene or native field pixel. The earlier top-15 raw-image selection **cannot be reconstructed from this file** and is not claimed here.

**Preprocessed data link:** pending upload of data/clean/allen_model_ready.parquet to a team-accessible, restricted Drive location.
