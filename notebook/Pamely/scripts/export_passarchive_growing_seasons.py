"""Export estimated maize growing seasons without changing full-window data.

Use recorded planting/harvest dates passing explicit checks; otherwise reuse the
project's fixed Oct–Jun (MWI/ZWE) or Feb–Sep (KEN) extraction windows.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from cropyield.satellite.passarchive_quality import quote

COUNTRIES = {'malawi':'MWI','zimbabwe':'ZWE','kenya':'KEN'}
MAX_RECORDED_DAYS = 400


def season_bounds(fields):
    """Return estimates plus reasons; original date columns remain unchanged."""
    frame=fields[['fs_key','field_season_id','country_iso3','harvest_year',
                  'planting_date','harvest_date']].copy()
    year=pd.to_numeric(frame.harvest_year,errors='raise').astype(int)
    south=frame.country_iso3.isin(['MWI','ZWE'])
    if not frame.country_iso3.isin(COUNTRIES.values()).all():
        raise ValueError('Unsupported country')
    planting=pd.to_datetime(frame.planting_date,errors='coerce')
    harvest=pd.to_datetime(frame.harvest_date,errors='coerce')
    duration=(harvest-planting).dt.days
    issues=pd.DataFrame({
        'missing_planting_date':planting.isna(),
        'missing_harvest_date':harvest.isna(),
        'planting_year_outside_label_year_or_previous':planting.notna() & ~planting.dt.year.between(year-1,year),
        'harvest_year_outside_label_year_or_next':harvest.notna() & ~harvest.dt.year.between(year,year+1),
        'nonpositive_recorded_duration':duration.le(0),
        'recorded_duration_over_400_days':duration.gt(MAX_RECORDED_DAYS),
    })
    accepted=~issues.any(axis=1)
    fixed_start=pd.to_datetime({'year': year-south.astype(int),
        'month': south.map({True:10,False:2}),'day': 1})
    fixed_end=pd.to_datetime({'year': year,'month': south.map({True:6,False:9}),'day': 30})
    frame['season_start']=planting.where(accepted,fixed_start)
    frame['season_end']=harvest.where(accepted,fixed_end)
    frame['season_basis']=accepted.map({True:'recorded_planting_to_harvest',False:'fixed_project_window_estimate'})
    frame['season_date_review_reason']=issues.apply(
        lambda row:';'.join(name for name,flag in row.items() if flag),axis=1)
    frame['season_duration_days']=(frame.season_end-frame.season_start).dt.days+1
    frame['season_window_complete_in_archive']=(
        frame.season_start.ge(pd.to_datetime({'year': year-1,'month': 1,'day': 1})) &
        frame.season_end.le(pd.to_datetime({'year': year,'month': 12,'day': 31})))
    assert frame.season_end.gt(frame.season_start).all()
    return frame.drop(columns=['country_iso3','harvest_year','planting_date','harvest_date'])


def export_seasons(export_dir,output,progress=print):
    export_dir,output=Path(export_dir),Path(output)
    if output.exists():
        raise FileExistsError(f'Choose a new output folder: {output}')
    source_manifest=json.loads((export_dir/'manifest.json').read_text())
    csv_manifest=json.loads((export_dir/'csv/manifest.json').read_text())
    if not source_manifest['complete'] or not csv_manifest['complete']:
        raise ValueError('Incomplete input export')
    if source_manifest['threshold_percent']!=50:
        raise ValueError('This extraction retains the approved 50% quality rule')
    output.mkdir(parents=True)
    summaries=[]
    with duckdb.connect() as c:
        c.execute("SET memory_limit='1200MB'")
        for country in COUNTRIES:
            original=next(row for row in source_manifest['countries'] if row['country']==country)
            path=export_dir/country
            c.read_parquet(str(path/'fields.parquet')).create_view('fields',replace=True)
            c.read_parquet(str(path/'observations.parquet')).create_view('observations',replace=True)
            metadata=season_bounds(c.sql('SELECT * FROM fields').df())
            c.register('season_metadata',metadata)
            c.execute('''CREATE OR REPLACE TEMP VIEW seasonal_observations AS
                SELECT o.*,s.* EXCLUDE(fs_key,field_season_id)
                FROM observations o JOIN season_metadata s USING(fs_key,field_season_id)
                WHERE CAST(o.sensing_time_utc AS DATE) BETWEEN s.season_start AND s.season_end''')
            csv_columns=next(row['columns'] for row in csv_manifest['files']
                if row['country']==country and row['table']=='country_data')
            observation_columns={row[0] for row in c.sql('DESCRIBE observations').fetchall()}
            additions=[]
            for col in csv_columns:
                if col not in observation_columns:
                    alias=' AS full_window_retained_dates' if col=='retained_dates' else ''
                    additions.append(f'f."{col}"{alias}')
            c.execute('''CREATE OR REPLACE TEMP VIEW seasonal_counts AS
                SELECT fs_key,field_season_id,count(*) AS seasonal_retained_dates
                FROM seasonal_observations GROUP BY ALL''')
            c.execute(f'''CREATE OR REPLACE TEMP VIEW country_data AS
                SELECT o.*,{','.join(additions)},n.seasonal_retained_dates
                FROM seasonal_observations o JOIN fields f USING(fs_key,field_season_id)
                JOIN seasonal_counts n USING(fs_key,field_season_id)''')
            csv=output/f'{country}_growing_season.csv'
            c.execute(f'''COPY country_data TO {quote(csv)}
                (FORMAT CSV,HEADER TRUE,NULL {quote(chr(92)+'N')})''')
            c.execute(f'''COPY country_data TO {quote(output/f'{country}_growing_season.parquet')}
                (FORMAT PARQUET,COMPRESSION ZSTD)''')
            schema={row[0]:row[1] for row in c.sql('DESCRIBE country_data').fetchall()}
            schema_sql='{'+','.join(quote(k)+':'+quote(v) for k,v in schema.items())+'}'
            c.execute(f'''CREATE OR REPLACE TEMP VIEW readback AS SELECT * FROM read_csv(
                {quote(csv)},header=true,columns={schema_sql},auto_detect=false,nullstr={quote(chr(92)+'N')})''')
            rows,fields,keys,outside,low_quality=c.sql('''SELECT count(*),count(DISTINCT field_season_id),
                count(DISTINCT(fs_key,window_kind,acquisition_id)),
                count(*) FILTER(WHERE CAST(sensing_time_utc AS DATE)<season_start OR CAST(sensing_time_utc AS DATE)>season_end),
                count(*) FILTER(WHERE usable_percent<50) FROM readback''').fetchone()
            assert rows==keys==c.sql('SELECT count(*) FROM country_data').fetchone()[0]
            assert outside==low_quality==0
            checks=','.join(f'count(*) FILTER(WHERE "{name}" IS NULL)' for name in schema)
            assert c.sql(f'SELECT {checks} FROM readback').fetchone()==c.sql(f'SELECT {checks} FROM country_data').fetchone()
            c.execute(f'''COPY (
                SELECT f.*,s.* EXCLUDE(fs_key,field_season_id),coalesce(n.seasonal_retained_dates,0) AS seasonal_retained_dates,
                    coalesce(n.seasonal_retained_dates,0)>0 AS has_seasonal_imagery
                FROM fields f JOIN season_metadata s USING(fs_key,field_season_id)
                LEFT JOIN seasonal_counts n USING(fs_key,field_season_id)
            ) TO {quote(output/f'{country}_field_seasons.csv')} (FORMAT CSV,HEADER TRUE,NULL {quote(chr(92)+'N')})''')
            c.read_parquet(str(path/'coverage_months.parquet')).create_view('calendar',replace=True)
            c.execute(f'''COPY (
                WITH n AS (SELECT fs_key,window_kind,year(sensing_time_utc) AS year,
                    month(sensing_time_utc) AS month,count(*) AS seasonal_retained_dates
                    FROM seasonal_observations GROUP BY ALL)
                SELECT c.* EXCLUDE(retained_dates),c.retained_dates AS full_window_retained_dates,
                    s.season_start,s.season_end,s.season_basis,
                    coalesce(n.seasonal_retained_dates,0) AS seasonal_retained_dates
                FROM calendar c JOIN season_metadata s USING(fs_key,field_season_id)
                LEFT JOIN n USING(fs_key,window_kind,year,month)
                WHERE make_date(c.year,c.month,1)<=s.season_end
                    AND last_day(make_date(c.year,c.month,1))>=s.season_start
            ) TO {quote(output/f'{country}_season_coverage.csv')} (FORMAT CSV,HEADER TRUE,NULL {quote(chr(92)+'N')})''')
            basis=c.sql('''SELECT season_basis,count(*) AS fields FROM season_metadata GROUP BY season_basis''').df()
            basis.to_csv(output/f'{country}_season_basis.csv',index=False)
            recorded_rows=c.sql("SELECT count(*) FROM country_data WHERE season_basis='recorded_planting_to_harvest'").fetchone()[0]
            stats={'country':country,'original_observations':original['observations'],
                'seasonal_observations':rows,'original_field_seasons_with_imagery':original['fields_with_imagery'],
                'seasonal_field_seasons_with_imagery':fields,
                'recorded_date_observations':recorded_rows,'fixed_window_observations':rows-recorded_rows,
                'file':csv.name,'columns':schema,'bytes':csv.stat().st_size}
            summaries.append(stats)
            progress(f'{country}: {rows:,} growing-season observations across {fields:,} field-seasons',flush=True)
    pd.DataFrame([{k:v for k,v in row.items() if k!='columns'} for row in summaries]).to_csv(output/'summary.csv',index=False)
    manifest={'complete':True,'quality_threshold_percent':50,'crop':'maize',
        'season_policy':'Recorded planting-to-harvest dates passing basic checks; otherwise fixed project estimates',
        'maximum_recorded_duration_days':MAX_RECORDED_DAYS,
        'recorded_date_checks':['Both dates present','Planting year is label year or previous',
            'Harvest year is label year or next','Harvest follows planting by at most 400 days'],
        'fallback_windows':{'MWI':'October 1 of prior year through June 30 of label year',
            'ZWE':'October 1 of prior year through June 30 of label year',
            'KEN':'February 1 through September 30 of label year; may truncate late regional harvests'},
        'date_bounds_inclusive':True,'null_token':chr(92)+'N',
        'parent_manifest_sha256':hashlib.sha256((export_dir/'manifest.json').read_bytes()).hexdigest(),
        'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'limitations':['Fallback windows are estimates, not verified planting/harvest dates',
            'Basic date checks do not independently validate source dates',
            'Actual harvest dates are retrospective and cannot define an operational forecast cutoff',
            'Parent archive lacks imagery after December of label year even when recorded season extends later'],
        'countries':summaries}
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export-dir',type=Path,default=ROOT/'data/processed/passarchive_filtered_50_v1')
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    export_seasons(args.export_dir,args.output or args.export_dir/'growing_seasons')
