"""Growing-season exports respect cross-year dates, uncertainty and inclusive endpoints."""
import importlib.util
import json
from pathlib import Path

import duckdb
import pandas as pd


def load_module():
    spec=importlib.util.spec_from_file_location('seasons',Path(__file__).resolve().parents[1]/'scripts/export_passarchive_growing_seasons.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_recorded_and_fallback_bounds():
    module=load_module()
    fields=pd.DataFrame({
        'fs_key':[1,2,3,4],'field_season_id':['a','b','c','d'],
        'country_iso3':['MWI','ZWE','KEN','KEN'],'harvest_year':[2023]*4,
        'planting_date':['2022-11-15',None,'2100-03-01','2022-01-01'],
        'harvest_date':['2023-05-31',None,'2023-08-30','2023-08-30'],
    })
    result=module.season_bounds(fields)
    assert result.loc[0,'season_start']==pd.Timestamp('2022-11-15')
    assert result.loc[0,'season_end']==pd.Timestamp('2023-05-31')
    assert result.loc[0,'season_basis']=='recorded_planting_to_harvest'
    assert result.loc[1,'season_start']==pd.Timestamp('2022-10-01')
    assert result.loc[1,'season_end']==pd.Timestamp('2023-06-30')
    assert result.loc[2,'season_start']==pd.Timestamp('2023-02-01')
    assert result.loc[2,'season_end']==pd.Timestamp('2023-09-30')
    assert 'planting_year_outside' in result.loc[2,'season_date_review_reason']
    assert 'over_400_days' in result.loc[3,'season_date_review_reason']


def test_season_csv_inclusive_dates_and_coverage(tmp_path):
    module=load_module();module.COUNTRIES={'malawi':'MWI'}
    root=tmp_path/'source';country=root/'malawi';country.mkdir(parents=True)
    (root/'csv').mkdir()
    source_manifest={'complete':True,'threshold_percent':50,'countries':[
        {'country':'malawi','observations':6,'fields_with_imagery':2}]}
    (root/'manifest.json').write_text(json.dumps(source_manifest))
    csv_manifest={'complete':True,'files':[{'country':'malawi','table':'country_data','columns':{
        'fs_key':'INTEGER','field_season_id':'VARCHAR','window_kind':'VARCHAR','acquisition_id':'VARCHAR',
        'sensing_time_utc':'TIMESTAMP','usable_percent':'DOUBLE','yield_kg_ha':'DOUBLE','retained_dates':'BIGINT'}}]}
    (root/'csv/manifest.json').write_text(json.dumps(csv_manifest))
    with duckdb.connect() as c:
        c.execute("""CREATE TABLE fields AS SELECT * FROM (VALUES
            (1,'f1','MWI',2023,date '2022-11-15',date '2023-05-31',0.0,5),
            (2,'f2','MWI',2023,NULL,NULL,NULL,1))
            t(fs_key,field_season_id,country_iso3,harvest_year,planting_date,harvest_date,yield_kg_ha,retained_dates);
            CREATE TABLE observations AS SELECT *,80.0 AS usable_percent FROM (VALUES
            (1,'f1','previous','a0',timestamp '2022-09-30'),
            (2,'f2','previous','a1',timestamp '2022-10-01'),
            (1,'f1','previous','a2',timestamp '2022-11-10'),
            (1,'f1','previous','a3',timestamp '2022-11-15'),
            (1,'f1','current','a4',timestamp '2023-05-31 23:59:59'),
            (1,'f1','current','a5',timestamp '2023-06-01'))
            t(fs_key,field_season_id,window_kind,acquisition_id,sensing_time_utc);
            CREATE TABLE coverage_months AS SELECT * FROM (VALUES
            (1,'f1','previous',2022,11,2),(1,'f1','current',2023,5,1),
            (1,'f1','current',2023,6,1),(2,'f2','previous',2022,10,1))
            t(fs_key,field_season_id,window_kind,year,month,retained_dates)""")
        for table in ['fields','observations','coverage_months']:
            c.execute(f"COPY {table} TO '{country/table}.parquet' (FORMAT PARQUET)")
    output=tmp_path/'output'
    result=module.export_seasons(root,output,progress=lambda *a,**kw:None)
    assert result['complete']
    df=pd.read_csv(output/'malawi_growing_season.csv',na_values=[r'\N'],keep_default_na=False)
    assert set(df.acquisition_id)=={'a1','a3','a4'}
    assert len(df)==3
    assert df.loc[df.field_season_id.eq('f1'),'yield_kg_ha'].eq(0).all()
    assert df.loc[df.field_season_id.eq('f2'),'yield_kg_ha'].isna().all()
    assert df.loc[df.field_season_id.eq('f1'),'seasonal_retained_dates'].eq(2).all()
    months=pd.read_csv(output/'malawi_season_coverage.csv')
    assert months.seasonal_retained_dates.sum()==3
    assert not ((months.fs_key==1)&(months.month==6)).any()
