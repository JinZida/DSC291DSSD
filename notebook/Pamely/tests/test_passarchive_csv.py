"""CSV conversion preserves missing values, empty text, NA-like strings and zero yields."""
import importlib.util
import json
from pathlib import Path

import duckdb
import pandas as pd


def test_csv_roundtrip(tmp_path):
    script=Path(__file__).resolve().parents[1]/'scripts/export_passarchive_csv.py'
    spec=importlib.util.spec_from_file_location('csv_export',script)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.COUNTRIES=['malawi']
    module.FIELD_COLUMNS=['yield_kg_ha','label_status']
    root=tmp_path/'parquet'; folder=root/'malawi'; folder.mkdir(parents=True)
    manifest={'complete': True,'threshold_percent': 50,'crop': 'maize','window_kinds': ['previous','current'],
        'countries': [{'country': 'malawi','observations': 3,'fields_with_imagery': 3}]}
    (root/'manifest.json').write_text(json.dumps(manifest))
    with duckdb.connect() as c:
        c.execute("""CREATE TABLE fields AS SELECT * FROM (VALUES
            (1,'f1',0.0,'recorded_zero'),(2,'f2',NULL,'NA'),
            (3,'f3',2.0,'')) t(fs_key,field_season_id,yield_kg_ha,label_status);
            CREATE TABLE observations AS SELECT fs_key,field_season_id,'current' AS window_kind,
            'a1' AS acquisition_id FROM fields""")
        for table in module.TABLES:
            source=table if table in ['fields','observations'] else 'fields'
            c.execute(f"COPY {source} TO '{folder/table}.parquet' (FORMAT PARQUET)")
    out=tmp_path/'csv'
    result=module.convert(root,out,progress=lambda *args,**kwargs:None)
    assert result['null_token']==r'\N'
    df=pd.read_csv(out/'malawi.csv',na_values=[r'\N'],keep_default_na=False)
    assert len(df)==3
    assert df.loc[0,'yield_kg_ha']==0
    assert pd.isna(df.loc[1,'yield_kg_ha'])
    assert df.loc[1,'label_status']=='NA'
    assert df.loc[2,'label_status']==''
