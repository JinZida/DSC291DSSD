"""Create pandas-ready country CSVs and faithful CSV copies of filtered Parquet files."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from cropyield.satellite.passarchive_quality import quote

COUNTRIES = ['malawi', 'zimbabwe', 'kenya']
TABLES = ['fields', 'observations', 'masked_pixels', 'coverage_months',
          'split_membership', 'split_specs']
FIELD_COLUMNS = ['latitude','longitude','location_quality','usable_at_field_scale',
    'unusable_reason','identity_status','overlap_group_id','primary_source_observation_id',
    'source_observation_id','label_status','yield_raw','yield_raw_unit','yield_kg_ha',
    'yield_t_ha','yield_conversion','moisture_basis','n_cuts','cut_area_m2','protocol',
    'zero_evidence','planting_date','harvest_date','survey_date','intercropped','irrigated',
    'retained_dates']


def convert(export_dir, output, progress=print):
    export_dir, output = Path(export_dir), Path(output)
    source_manifest = json.loads((export_dir/'manifest.json').read_text())
    if not source_manifest.get('complete'):
        raise ValueError('The filtered export is incomplete')
    if output.exists():
        raise FileExistsError(f'CSV output already exists: {output}')
    output.mkdir(parents=True)
    reports = []
    with duckdb.connect() as c:
        c.execute("SET memory_limit='1200MB'")
        for country in COUNTRIES:
            folder = output/country
            folder.mkdir()
            for table in TABLES:
                parquet = export_dir/country/f'{table}.parquet'
                csv = folder/f'{table}.csv'
                relation = c.read_parquet(str(parquet))
                relation.create_view('source_table', replace=True)
                columns = {
                    row[0]: row[1] for row in c.sql('DESCRIBE source_table').fetchall()}
                # Use an explicit null token so legitimate empty text and IDs such as NA survive.
                text_cols = [name for name, dtype in columns.items() if dtype=='VARCHAR']
                if text_cols:
                    collision = ' OR '.join(f'"{name}"={quote(chr(92)+"N")}' for name in text_cols)
                    if c.sql(f'SELECT count(*) FROM source_table WHERE {collision}').fetchone()[0]:
                        raise ValueError('Null token collision in source text')
                c.execute(f'''COPY source_table TO {quote(csv)}
                    (FORMAT CSV,HEADER TRUE,NULL {quote(chr(92)+'N')})''')
                schema = '{'+','.join(quote(k)+':'+quote(v) for k,v in columns.items())+'}'
                c.execute(f"CREATE OR REPLACE TEMP VIEW csv_table AS SELECT * FROM read_csv({quote(csv)},header=true,columns={schema},nullstr={quote(chr(92)+'N')},auto_detect=false)")
                source_n = c.sql('SELECT count(*) FROM source_table').fetchone()[0]
                csv_n = c.sql('SELECT count(*) FROM csv_table').fetchone()[0]
                if csv_n != source_n:
                    raise AssertionError(f'{csv}: row count changed')
                nulls = ','.join(f'count(*) FILTER(WHERE "{col}" IS NULL)' for col in columns)
                if c.sql(f'SELECT {nulls} FROM source_table').fetchone()!=c.sql(f'SELECT {nulls} FROM csv_table').fetchone():
                    raise AssertionError(f'{csv}: null counts changed')
                reports.append({'country': country,'table': table,'path': str(csv.relative_to(output)),
                                    'rows': csv_n,'columns': columns,'bytes': csv.stat().st_size})
            c.read_parquet(str(export_dir/country/'observations.parquet')).create_view('observations',replace=True)
            c.read_parquet(str(export_dir/country/'fields.parquet')).create_view('fields',replace=True)
            if c.sql('SELECT count(*)-count(DISTINCT(fs_key,field_season_id)) FROM fields').fetchone()[0]:
                raise ValueError('Ambiguous field-label join')
            additions = ','.join(f'f."{col}"' for col in FIELD_COLUMNS)
            c.execute(f'''CREATE OR REPLACE TEMP VIEW country_data AS
                SELECT o.*,{additions} FROM observations o LEFT JOIN fields f USING(fs_key,field_season_id)''')
            csv = output/f'{country}.csv'
            c.execute(f'''COPY country_data TO {quote(csv)}
                (FORMAT CSV,HEADER TRUE,NULL {quote(chr(92)+'N')})''')
            columns = {r[0]:r[1] for r in c.sql('DESCRIBE country_data').fetchall()}
            schema = '{'+','.join(quote(k)+':'+quote(v) for k,v in columns.items())+'}'
            c.execute(f"CREATE OR REPLACE TEMP VIEW joined_csv AS SELECT * FROM read_csv({quote(csv)},header=true,columns={schema},nullstr={quote(chr(92)+'N')},auto_detect=false)")
            count, fields, unique = c.sql('''SELECT count(*),count(DISTINCT field_season_id),
                count(DISTINCT(fs_key,window_kind,acquisition_id)) FROM joined_csv''').fetchone()
            expected = next(row for row in source_manifest['countries'] if row['country']==country)
            if count != expected['observations'] or fields != expected['fields_with_imagery'] or unique != count:
                raise AssertionError('CSV country counts disagree with filtered export')
            missing, zeros = c.sql('SELECT count(*) FILTER(WHERE yield_kg_ha IS NULL),count(*) FILTER(WHERE yield_kg_ha=0) FROM country_data').fetchone()
            if (missing,zeros)!=c.sql('SELECT count(*) FILTER(WHERE yield_kg_ha IS NULL),count(*) FILTER(WHERE yield_kg_ha=0) FROM joined_csv').fetchone():
                raise AssertionError('CSV changed zero/missing yields')
            reports.append({'country': country,'table': 'country_data','path': csv.name,'rows': count,
                'field_seasons': fields,'columns': columns,'bytes': csv.stat().st_size})
            progress(f'{country}: {count:,} CSV rows across {fields:,} field-seasons',flush=True)
    manifest = {'complete': True,'null_token': '\\N','encoding': 'UTF-8','delimiter': ',',
        'source_manifest_sha256': hashlib.sha256((export_dir/'manifest.json').read_bytes()).hexdigest(),
        'conversion_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'threshold_percent': source_manifest['threshold_percent'],'crop': source_manifest['crop'],
        'window_kinds': source_manifest['window_kinds'],'files': reports}
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export-dir',type=Path,default=ROOT/'data/processed/passarchive_filtered_50_v1')
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    convert(args.export_dir,args.output or args.export_dir/'csv')
