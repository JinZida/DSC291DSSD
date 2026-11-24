# Maize cleaning submission

Start with **maize_cleaning.ipynb**. It contains the cleaning rationale, all-country/year inventory, label audit, 50/70/90% quality comparison, reproduction steps, growing-season policy, eligibility rules and aggregate validation results.

Author: **Pamely**. Submission folder: `notebook/Pamely/`.

## Run

Use Python 3.12 and install `requirements.txt` from this directory. The existing `DSC 291 Project (.venv)` kernel also has the needed packages. Restart the kernel before running, because this submission includes its own small `cropyield` helper package.

Run the notebook from its directory. Put the source `passarchive_v1.duckdb` in the repository's ignored `data/` directory or set `PASSARCHIVE_SOURCE`. Set `CLEANING_HOME` if running from another directory. The old `cropyield.duckdb` cannot replace the acquisition/pixel archive.

For the local original project copy, use these environment variables before launching the notebook, or set the paths in its first cell:

```text
PASSARCHIVE_SOURCE=/Users/speakeasy/Documents/workspace/DSC-291-Final-Project/data/passarchive_v1.duckdb
PASSARCHIVE_AUDIT=/Users/speakeasy/Documents/workspace/DSC-291-Final-Project/data/processed/passarchive_quality_audit_v1/quality_audit.duckdb
PASSARCHIVE_FILTERED=/Users/speakeasy/Documents/workspace/DSC-291-Final-Project/data/processed/passarchive_filtered_50_v1
```

With no cached audit, the notebook creates a fresh audit from the read-only source. Set `EXPORT=True` to reproduce the masked full-window and estimated-season exports. Outputs can consume several GB. Existing output folders are never overwritten.

## Results

All 11 maize country-years present in the archive are covered: Kenya 2016–2020, Malawi 2020/2022/2023, Zimbabwe 2022–2024. These are 34,777 field-seasons and 683,833 seasonal acquisition rows. Rwanda, Tanzania and Nigeria are absent from this archive; other Zimbabwe crops are outside the maize scope. The proposed supervised eligibility is an explicit additional flag, not deletion of the original labels.

Nine tests cover native-grid masking, missing-band handling, label and zero preservation, CSV null round trips, season boundaries and supervised eligibility:

```bash
PYTHONPATH=src python -m pytest tests -q
```

The notebook was executed against the existing full export. Rebuilding the multi-GB source/pixel export was not repeated; the unchanged helpers are checked by provenance hashes and synthetic tests.

## Dataset and submission status

[Download the preprocessed dataset from Google Drive](https://drive.google.com/file/d/1BqBGkxKgYLh2fat50dKRpOfeakgG6vPu/view?usp=sharing)

The archive is `maize_cleaned_submission.tar.gz` (about 50 MB). This link points to the preprocessed dataset bundle.

The bundle contains country Parquet observations, all-label eligibility audits, seasonal calendar coverage, unchanged split tables, a country-year summary and SHA-256 manifests. Data files and archives are ignored by Git. Only code, documentation, tests and aggregate notebook outputs are submitted.

Submit changes on the `pamely` branch and open a pull request targeting `main`. Dataset files must remain outside Git.
