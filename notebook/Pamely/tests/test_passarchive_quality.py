"""Pixel audit: native grid alignment, missing-band gating and unsampled dates."""

import duckdb
import pytest

from cropyield.satellite.passarchive_quality import audit


def make_archive(path):
    c=duckdb.connect(str(path))
    c.execute("""
    CREATE TABLE fields AS SELECT 1 AS fs_key,'f1' AS field_season_id,'test' AS dataset,
        'MWI' AS country_iso3,'maize' AS crop,2023 AS harvest_year,'r1' AS admin_unit,
        'polygon' AS geometry_type,false AS fold0_reserved;
    CREATE TABLE scenes AS SELECT i AS scene_key,i AS granule_key,'tile' AS mgrs_tile,
        'L2A' AS level,'5.0' AS processing_baseline,
        timestamp '2023-01-01' + i * interval '1 day' AS sensing_time_utc,
        'scene' || i AS scene_id FROM range(1,4) t(i);
    CREATE TABLE footprint_pixels AS SELECT 1 AS loc_key,'tile' AS mgrs_tile,10 AS res_m,
        0 AS px_row,i*2 AS px_col,true AS in_default_summary,false AS is_point_pixel FROM range(3) t(i);
    CREATE TABLE scene_items AS SELECT scene_key,'item' || scene_key AS item_id,
        0.0 AS dn_add_offset FROM scenes;
    CREATE TABLE pass_samples AS SELECT 1 AS loc_key,scene_key,granule_key,
        'item' || scene_key AS item_id FROM scenes WHERE scene_key<3;
    CREATE TABLE passes AS SELECT 1 AS fs_key,1 AS loc_key,granule_key,
        'a' || scene_key AS acquisition_id,mgrs_tile,'current' AS window_kind,
        sensing_time_utc,'sampled_all_bands' AS sample_status,true AS in_catalogue_footprint
        FROM scenes;
    CREATE TABLE coverage_months AS SELECT 1 AS fs_key,'current' AS window_kind,
        2023 AS year,1 AS month,'sampled' AS month_status;
    CREATE TABLE pixel_samples(loc_key INTEGER,scene_key INTEGER,res_m INTEGER,
        px_row INTEGER,px_col INTEGER,band VARCHAR,dn INTEGER);
    """)
    for col in ['b02','b03','b04','b08','b05','b06','b07','b8a','b11','b12']:
        c.execute(f'ALTER TABLE pass_samples ADD COLUMN {col} DOUBLE DEFAULT 1000')
    c.execute('ALTER TABLE pass_samples ADD COLUMN n_pixels INTEGER DEFAULT 1')
    c.execute('ALTER TABLE pass_samples ADD COLUMN scl DOUBLE DEFAULT 4')
    values=[]
    for scene in [1,2]:
        for pos in range(3):
            for band in ['B02','B03','B04','B08']:
                values.append((1,scene,10,0,pos*2,band,1000))
            for band in ['B05','B06','B07','B8A','B11','B12']:
                if scene==1 and pos==0 and band=='B12':
                    continue
                values.append((1,scene,20,0,pos,band,1000))
            values.append((1,scene,20,0,pos,'SCL',9 if scene==1 and pos==2 else 4))
    c.executemany('INSERT INTO pixel_samples VALUES (?,?,?,?,?,?,?)',values)
    c.close()


def test_pixel_quality_and_thresholds(tmp_path):
    src=tmp_path/'source.duckdb'; out=tmp_path/'audit.duckdb'
    make_archive(src)
    stat=(src.stat().st_size,src.stat().st_mtime_ns)
    c=audit(src,out,progress=lambda _:None)
    rows=c.sql('SELECT acquisition_id,expected_pixels,usable_pixels,cloud_pixels,unknown_pixels,missing_band_pixels FROM field_date_quality ORDER BY acquisition_id').fetchall()
    assert rows==[('a1',3,1,1,0,1),('a2',3,3,0,0,0),('a3',3,0,0,3,3)]
    assert c.sql('SELECT threshold_percent,retained_dates FROM retention_by_dataset ORDER BY 1').fetchall()==[(50,1),(70,1),(90,1)]
    assert c.sql('SELECT count(*) FROM field_month_coverage').fetchone()[0]==3
    assert all(r[0] for r in c.sql('SELECT passed FROM validation').fetchall())
    c.close()
    assert (src.stat().st_size,src.stat().st_mtime_ns)==stat
    with pytest.raises(FileExistsError):
        audit(src,out)


def test_reject_source_as_output(tmp_path):
    with pytest.raises(ValueError):
        audit(tmp_path/'x.duckdb',tmp_path/'x.duckdb')


def test_point_records_use_single_pixel_samples(tmp_path):
    src=tmp_path/'point.duckdb'
    make_archive(src)
    with duckdb.connect(str(src)) as c:
        c.execute('DELETE FROM pixel_samples')
        c.execute('DELETE FROM footprint_pixels WHERE px_col>0')
        c.execute('UPDATE footprint_pixels SET is_point_pixel=true')
        c.execute('UPDATE pass_samples SET scl=9 WHERE scene_key=2')
    c=audit(src,tmp_path/'point_audit.duckdb',progress=lambda _:None)
    rows=c.sql('SELECT acquisition_id,usable_pixels,cloud_pixels,unknown_pixels,quality_source FROM field_date_quality ORDER BY acquisition_id').fetchall()
    assert rows==[('a1',1,0,0,'single_pixel_sample'),('a2',0,1,0,'single_pixel_sample'),('a3',0,0,1,'unavailable')]
    c.close()


def test_thresholds_use_complete_field_footprint(tmp_path):
    src=tmp_path/'thresholds.duckdb'
    make_archive(src)
    with duckdb.connect(str(src)) as c:
        # Restore the missing band: acquisition a1 now has 2/3 usable field pixels.
        c.execute("INSERT INTO pixel_samples VALUES (1,1,20,0,0,'B12',1000)")
    c=audit(src,tmp_path/'threshold_audit.duckdb',progress=lambda _:None)
    assert c.sql('SELECT threshold_percent,retained_dates FROM retention_by_dataset ORDER BY 1').fetchall()==[(50,2),(70,1),(90,1)]
    assert c.sql("SELECT usable_percent FROM field_date_quality WHERE acquisition_id='a1'").fetchone()[0]==pytest.approx(200/3)
    c.close()
