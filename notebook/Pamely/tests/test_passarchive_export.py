"""Filtered export preserves labels, masks contaminated pixels, and keeps calendars."""
import json

import duckdb
import pytest
from test_passarchive_quality import make_archive

from cropyield.satellite.passarchive_export import export_filtered
from cropyield.satellite.passarchive_quality import audit


def test_export_masks_bad_pixels_and_preserves_labels(tmp_path):
    source=tmp_path/'source.duckdb'
    make_archive(source)
    with duckdb.connect(str(source)) as c:
        c.execute("INSERT INTO pixel_samples VALUES (1,1,20,0,0,'B12',1000)")
        c.execute('ALTER TABLE fields ADD COLUMN loc_key INTEGER DEFAULT 1')
        c.execute("CREATE TABLE labels AS SELECT 1 AS fs_key,'f1' AS field_season_id,'maize' AS crop,0.0 AS yield_kg_ha,'recorded_zero' AS label_status")
        c.execute("CREATE TABLE splits AS SELECT 1 AS fs_key,'split' AS split_name,'f1' AS field_season_id,0 AS fold")
        c.execute("CREATE TABLE split_specs AS SELECT 'split' AS split_name,true AS frozen")
    before=(source.stat().st_size,source.stat().st_mtime_ns)
    audit_db=tmp_path/'audit.duckdb'
    audit(source,audit_db,progress=lambda _:None).close()
    out=tmp_path/'export'
    result=export_filtered(source,audit_db,out,progress=lambda _:None)
    assert result['complete']
    assert result['threshold_percent']==50
    assert (source.stat().st_size,source.stat().st_mtime_ns)==before
    with duckdb.connect() as c:
        pixels=c.execute('SELECT acquisition_id,px_col,pixel_usable,scl,b02,masking_reason FROM read_parquet(?) ORDER BY 1,2',[str(out/'malawi/masked_pixels.parquet')]).fetchall()
        assert len(pixels)==6
        assert pixels[2]==('a1',4,False,9.0,None,'cloud_or_cirrus')
        assert pixels[0][4]==pytest.approx(.1)
        assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(out/'malawi/observations.parquet')]).fetchone()[0]==2
        assert c.execute('SELECT yield_t_ha FROM read_parquet(?)',[str(out/'malawi/fields.parquet')]).fetchone()[0]==0
        assert c.execute('SELECT retained_dates FROM read_parquet(?)',[str(out/'malawi/coverage_months.parquet')]).fetchone()[0]==2
        assert c.execute('SELECT fold FROM read_parquet(?)',[str(out/'malawi/split_membership.parquet')]).fetchone()[0]==0
        assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(out/'kenya/observations.parquet')]).fetchone()[0]==0
    assert json.loads((out/'manifest.json').read_text())['complete']
    with pytest.raises(FileExistsError):
        export_filtered(source,audit_db,out)
