"""Eligibility never imputes targets or discards recorded zero yields."""
import importlib.util
import json
from pathlib import Path

import duckdb
import pandas as pd


def test_packaging_eligibility_and_all_countries(tmp_path):
    spec = importlib.util.spec_from_file_location('prepare', Path(__file__).parents[1]/'scripts/prepare_dataset.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    filtered = tmp_path/'filtered'
    seasons = filtered/'growing_seasons'
    seasons.mkdir(parents=True)
    (filtered/'manifest.json').write_text('{}')
    (seasons/'manifest.json').write_text(json.dumps({'complete': True}))
    for country,iso in module.COUNTRIES.items():
        folder=filtered/country
        folder.mkdir()
        fields=pd.DataFrame({'fs_key':range(1,7),'field_season_id':list('abcdef'),
            'country_iso3':[iso]*6,'harvest_year':[2023]*6,
            'yield_t_ha':[0,None,-1,3,4,20], 'latitude':[-15]*6,'longitude':[30]*6,
            'usable_at_field_scale':[True]*6,
            'identity_status':['single_source']*3+['ambiguous_overlap','single_source','single_source'],
            'seasonal_retained_dates':[1,1,1,1,0,1],
            'has_seasonal_imagery':[True]*4+[False,True]})
        fields.to_csv(seasons/f'{country}_field_seasons.csv',index=False,na_rep=r'\N')
        observations=pd.DataFrame({'fs_key':[1],'field_season_id':['a'],'window_kind':['current'],
            'acquisition_id':['date1'],'country_iso3':[iso],'harvest_year':[2023],
            'usable_percent':[100.], 'sensing_time_utc':pd.to_datetime(['2023-03-01']),
            'season_start':pd.to_datetime(['2023-01-01']), 'season_end':pd.to_datetime(['2023-06-01'])})
        observations.to_parquet(seasons/f'{country}_growing_season.parquet',index=False)
        (seasons/f'{country}_season_coverage.csv').write_text('coverage\n1\n')
        for filename in ['split_membership.parquet','split_specs.parquet']:
            pd.DataFrame({'scheme':['frozen']}).to_parquet(folder/filename,index=False)
    quality=tmp_path/'audit.duckdb'
    with duckdb.connect(str(quality)) as c:
        c.execute("CREATE TABLE retention_by_dataset AS SELECT 'maize' crop,50 threshold_percent")
    out=tmp_path/'bundle'
    result=module.prepare(filtered,quality,out)
    assert result['complete']
    for country in module.COUNTRIES:
        audit=pd.read_parquet(out/country/'fields.parquet')
        assert audit.supervised_eligible.tolist()==[True,False,False,False,False,True]
        assert audit.yield_above_15_t_ha_review.tolist()==[False]*5+[True]
        assert audit.iloc[0].yield_t_ha==0
        assert len(audit)==6
    summary=pd.read_csv(out/'country_year_summary.csv')
    assert set(summary.country_iso3)=={'KEN','MWI','ZWE'}
    assert summary.supervised_fields.eq(2).all()
