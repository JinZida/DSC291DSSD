"""Package seasonal observations plus transparent supervised-label eligibility."""
import hashlib
import json
import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

COUNTRIES = {'malawi': 'MWI', 'zimbabwe': 'ZWE', 'kenya': 'KEN'}


def prepare(filtered, quality, output):
    filtered, quality, output = map(Path, (filtered, quality, output))
    seasons = filtered / 'growing_seasons'
    season_manifest = json.loads((seasons / 'manifest.json').read_text())
    assert season_manifest['complete']
    if output.exists():
        raise FileExistsError(f'Choose a new output folder: {output}')
    output.mkdir(parents=True)
    summaries = []
    with duckdb.connect() as c:
        for country, iso in COUNTRIES.items():
            dest = output / country
            dest.mkdir()
            fields = pd.read_csv(seasons / f'{country}_field_seasons.csv',
                                 na_values=[r'\N'], keep_default_na=False)
            y = pd.to_numeric(fields.yield_t_ha, errors='coerce')
            lat = pd.to_numeric(fields.latitude, errors='coerce')
            lon = pd.to_numeric(fields.longitude, errors='coerce')
            issues = pd.DataFrame({
                'missing_or_nonfinite_target': ~np.isfinite(y),
                'negative_target': y.lt(0),
                'location_not_field_usable': ~fields.usable_at_field_scale.eq(True),
                'invalid_wgs84_coordinates': ~lat.between(-90, 90) | ~lon.between(-180, 180),
                'unresolved_identity': ~fields.identity_status.isin(['single_source', 'linked_confident']),
                'no_retained_seasonal_imagery': fields.seasonal_retained_dates.eq(0),
            })
            fields['supervised_eligible'] = ~issues.any(axis=1)
            fields['supervised_exclusion_reasons'] = issues.apply(
                lambda row: ';'.join(row.index[row.to_numpy()]), axis=1)
            fields['yield_above_15_t_ha_review'] = y.gt(15)
            assert not fields[['fs_key', 'field_season_id']].duplicated().any()
            assert (fields.country_iso3 == iso).all()
            assert np.isfinite(y[fields.supervised_eligible]).all()
            assert y[fields.supervised_eligible].ge(0).all()
            fields.to_parquet(dest / 'fields.parquet', index=False)
            # Preserve source observation tables and split definitions verbatim.
            shutil.copy2(seasons / f'{country}_growing_season.parquet', dest / 'observations.parquet')
            shutil.copy2(seasons / f'{country}_season_coverage.csv', dest / 'season_coverage.csv')
            for name in ['split_membership.parquet', 'split_specs.parquet']:
                shutil.copy2(filtered / country / name, dest / name)
            obs = str(dest / 'observations.parquet')
            invalid = c.execute('''SELECT count(*) FROM read_parquet(?) WHERE usable_percent<50
                OR usable_percent IS NULL OR CAST(sensing_time_utc AS DATE)<season_start
                OR CAST(sensing_time_utc AS DATE)>season_end''', [obs]).fetchone()[0]
            assert invalid == 0
            total, keys = c.execute('''SELECT count(*),count(DISTINCT(fs_key,window_kind,acquisition_id))
                FROM read_parquet(?)''', [obs]).fetchone()
            assert total == keys
            c.register('field_audit', fields)
            orphaned = c.execute('''SELECT count(*) FROM read_parquet(?) o
                ANTI JOIN field_audit f USING(fs_key,field_season_id)''', [obs]).fetchone()[0]
            assert orphaned == 0
            counts = c.execute('''SELECT country_iso3,harvest_year,count(*) AS seasonal_observations
                FROM read_parquet(?) GROUP BY ALL''', [obs]).df()
            group = fields.groupby(['country_iso3', 'harvest_year']).agg(
                fields_total=('fs_key', 'size'),
                fields_with_imagery=('has_seasonal_imagery', 'sum'),
                supervised_fields=('supervised_eligible', 'sum'),
                high_yield_review=('yield_above_15_t_ha_review', 'sum')).reset_index()
            group = group.merge(counts, on=['country_iso3', 'harvest_year'], how='left')
            group['seasonal_observations'] = group.seasonal_observations.fillna(0).astype(int)
            summaries.append(group)
    pd.concat(summaries).sort_values(['country_iso3', 'harvest_year']).to_csv(
        output / 'country_year_summary.csv', index=False)
    with duckdb.connect(str(quality), read_only=True) as c:
        c.execute("SELECT * FROM retention_by_dataset WHERE crop='maize'").df().to_csv(
            output / 'threshold_sensitivity.csv', index=False)
    shutil.copy2(filtered / 'manifest.json', output / 'imagery_manifest.json')
    shutil.copy2(seasons / 'manifest.json', output / 'season_manifest.json')
    (output / 'README.md').write_text('''# Cleaned maize sequences

Countries: Malawi, Zimbabwe, Kenya; all maize years in the input archive.
See country_year_summary.csv. Each country has observations.parquet,
fields.parquet (all labels and explicit supervised_eligible/exclusion reasons),
season_coverage.csv and unchanged split definitions. Join using fs_key and
field_season_id. Yields repeat across dates; dates are not independent labels.

Imagery uses SCL 4/5, complete native bands, known offsets, at least 50% usable
footprint, masked-only medians and estimated growing seasons. Reflectance is
already scaled. Missing values remain null. Zero targets are retained; missing,
negative/nonfinite targets, unusable coordinates, unresolved identity and no
retained imagery are excluded only from the proposed supervised subset.
No target imputation, clipping, global scaling or model fitting was performed.
Unknown moisture and protocol differences remain. Apply a forecast cutoff if
needed; recorded harvest dates and fixed windows are retrospective. Group
source overlap and fields across partitions; choose one frozen split scheme.
''')
    files = [{'path': str(p.relative_to(output)), 'bytes': p.stat().st_size,
              'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
             for p in sorted(output.rglob('*')) if p.is_file()]
    manifest = {'complete': True, 'crop': 'maize',
                'season_manifest_sha256': hashlib.sha256((seasons / 'manifest.json').read_bytes()).hexdigest(),
                'packaging_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'files': files}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest
