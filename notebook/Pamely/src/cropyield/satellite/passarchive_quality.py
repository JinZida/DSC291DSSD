"""Read-only pixel quality audit. Derived tables live in a separate DuckDB file.

Coverage uses the archive's default 10 m field footprint. Native 20 m SCL and
red-edge/SWIR cells map to their 10 m children; this adds no spatial information.
"""
import hashlib
from pathlib import Path

import duckdb

TEN_BANDS = ['B02', 'B03', 'B04', 'B08']
TWENTY_BANDS = ['B05', 'B06', 'B07', 'B8A', 'B11', 'B12']


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def audit(source: Path, output: Path, progress=print):
    """Return a connection to derived audit tables; never write to source."""
    source, output = Path(source).resolve(), Path(output).resolve()
    if source == output:
        raise ValueError('Audit output must differ from source')
    if output.exists():
        raise FileExistsError(f'{output} already exists; choose a new audit output')
    if not source.is_file():
        raise FileNotFoundError(source)
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output))
    con.execute("SET memory_limit='1500MB'")
    con.execute('SET threads=4')
    con.execute(f'ATTACH {quote(source)} AS src (READ_ONLY)')
    try:
        _build(con, progress)
        con.execute("CREATE TABLE audit_provenance(source_name VARCHAR, source_bytes BIGINT, source_mtime_ns BIGINT, algorithm_sha256 VARCHAR)")
        con.execute('INSERT INTO audit_provenance VALUES (?,?,?,?)', [source.name, source.stat().st_size, source.stat().st_mtime_ns, hashlib.sha256(Path(__file__).read_bytes()).hexdigest()])
        con.execute('CHECKPOINT')
    except Exception:
        con.close()
        raise
    return con


def _build(c, progress):
    def run(label, sql):
        progress(label)
        c.execute(sql)

    run('1/7 Validate spatial and product keys', '''
        CREATE TABLE input_checks AS
        SELECT 'field IDs' AS check_name, count(*)-count(DISTINCT fs_key) AS duplicate_keys FROM src.fields
        UNION ALL SELECT 'scene keys', count(*)-count(DISTINCT scene_key) FROM src.scenes
        UNION ALL SELECT 'footprint keys', count(*)-count(DISTINCT (loc_key,mgrs_tile,res_m,px_row,px_col)) FROM src.footprint_pixels
        UNION ALL SELECT 'sample products', count(*)-count(DISTINCT (loc_key,scene_key)) FROM src.pass_samples;
    ''')
    if c.sql('SELECT sum(duplicate_keys) FROM input_checks').fetchone()[0]:
        raise ValueError('Duplicate spatial/product keys; inspect input_checks before joining')
    run('2/7 Select expected field pixels and inspect offset metadata', '''
        CREATE TABLE footprint10 AS
        SELECT loc_key,mgrs_tile,px_row,px_col,is_point_pixel FROM src.footprint_pixels
        WHERE res_m=10 AND in_default_summary;
        CREATE TABLE support20 AS
        SELECT DISTINCT loc_key,mgrs_tile,px_row//2 AS px_row,px_col//2 AS px_col FROM footprint10;
        CREATE TABLE footprint_sizes AS
        SELECT loc_key,mgrs_tile,count(*) AS expected_pixels FROM footprint10 GROUP BY ALL;
        CREATE TABLE offsets AS
        SELECT scene_key,item_id,min(dn_add_offset) AS dn_add_offset,
            count(DISTINCT dn_add_offset)=1 AS known_offset
        FROM src.scene_items GROUP BY scene_key,item_id;
    ''')
    bits10 = ' | '.join(f"CASE WHEN band='{b}' AND dn>0 THEN {1 << i} ELSE 0 END" for i,b in enumerate(TEN_BANDS))
    bits20 = ' | '.join(f"CASE WHEN band='{b}' AND dn>0 THEN {1 << i} ELSE 0 END" for i,b in enumerate(TWENTY_BANDS))
    run('3/7 Check required bands at their native pixel grids', f'''
        CREATE TEMP TABLE q10 AS
        SELECT p.loc_key,p.scene_key,p.px_row,p.px_col,
            bit_or({bits10}) AS valid_band_mask
        FROM src.pixel_samples p JOIN src.scenes s USING(scene_key)
        JOIN footprint10 f ON p.loc_key=f.loc_key AND s.mgrs_tile=f.mgrs_tile
            AND p.px_row=f.px_row AND p.px_col=f.px_col
        WHERE p.res_m=10 AND p.band IN ('B02','B03','B04','B08') GROUP BY ALL;
        CREATE TEMP TABLE q20 AS
        SELECT p.loc_key,p.scene_key,p.px_row,p.px_col,
            bit_or({bits20}) AS valid_band_mask,
            max(dn) FILTER(WHERE band='SCL') AS scl,
            count(DISTINCT dn) FILTER(WHERE band='SCL') AS scl_versions
        FROM src.pixel_samples p JOIN src.scenes s USING(scene_key)
        JOIN support20 f ON p.loc_key=f.loc_key AND s.mgrs_tile=f.mgrs_tile
            AND p.px_row=f.px_row AND p.px_col=f.px_col
        WHERE p.res_m=20 AND p.band IN ('B05','B06','B07','B8A','B11','B12','SCL') GROUP BY ALL;
    ''')
    point_bands = ' AND '.join(f'p.{band.lower()}>0 AND isfinite(p.{band.lower()})' for band in TEN_BANDS + TWENTY_BANDS)
    run('4/7 Count contamination, uncertainty and complete-band pixels', f'''
        CREATE TABLE product_quality AS
        WITH px AS (
            SELECT p.loc_key,p.scene_key,p.granule_key,p.item_id,s.mgrs_tile,
                s.level,s.processing_baseline,s.sensing_time_utc,s.scene_id,
                f.px_row,f.px_col,
                CASE WHEN f.is_point_pixel AND p.n_pixels=1 THEN 'single_pixel_sample'
                     ELSE 'raw_pixels' END AS quality_source,
                CASE WHEN s.level='L2A' THEN
                    CASE WHEN f.is_point_pixel AND p.n_pixels=1 THEN p.scl
                         WHEN q.scl_versions=1 THEN q.scl END END AS scl,
                coalesce((CASE WHEN f.is_point_pixel AND p.n_pixels=1 THEN ({point_bands})
                    ELSE a.valid_band_mask=15 AND q.valid_band_mask=63 END)
                    AND o.known_offset AND isfinite(o.dn_add_offset),false) AS bands_complete
            FROM src.pass_samples p JOIN src.scenes s USING(scene_key)
            JOIN footprint10 f ON p.loc_key=f.loc_key AND s.mgrs_tile=f.mgrs_tile
            LEFT JOIN q10 a ON p.loc_key=a.loc_key AND p.scene_key=a.scene_key
                AND f.px_row=a.px_row AND f.px_col=a.px_col
            LEFT JOIN q20 q ON p.loc_key=q.loc_key AND p.scene_key=q.scene_key
                AND f.px_row//2=q.px_row AND f.px_col//2=q.px_col
            LEFT JOIN offsets o ON p.scene_key=o.scene_key AND p.item_id=o.item_id
        ) SELECT loc_key,scene_key,granule_key,item_id,mgrs_tile,level,processing_baseline,
            sensing_time_utc,scene_id,quality_source,count(*) AS expected_pixels,
            count(*) FILTER(WHERE scl IN(8,9,10)) AS cloud_pixels,
            count(*) FILTER(WHERE scl=3) AS cloud_shadow_pixels,
            count(*) FILTER(WHERE scl=11) AS snow_pixels,
            count(*) FILTER(WHERE scl IN(0,1)) AS invalid_pixels,
            count(*) FILTER(WHERE scl IN(2,7)) AS review_pixels,
            count(*) FILTER(WHERE scl=6) AS water_pixels,
            count(*) FILTER(WHERE scl IS NULL OR scl NOT BETWEEN 0 AND 11 OR scl<>round(scl)) AS unknown_pixels,
            count(*) FILTER(WHERE NOT bands_complete) AS missing_band_pixels,
            count(*) FILTER(WHERE scl IN(4,5) AND bands_complete) AS usable_pixels
        FROM px GROUP BY ALL;
        DROP TABLE q10; DROP TABLE q20;
    ''')
    run('5/7 Build one row per field-season/window/acquisition', '''
        CREATE TABLE field_date_quality AS
        WITH candidates AS (
            SELECT f.fs_key,f.field_season_id,f.dataset,f.country_iso3,f.crop,
                f.harvest_year,f.admin_unit,f.geometry_type,f.fold0_reserved,
                p.window_kind,p.acquisition_id,p.sensing_time_utc,
                p.mgrs_tile,p.sample_status,p.in_catalogue_footprint,
                q.scene_key,q.item_id,q.level,coalesce(q.quality_source,'unavailable') AS quality_source,
                coalesce(z.expected_pixels,0) AS expected_pixels,
                coalesce(q.cloud_pixels,0) AS cloud_pixels,
                coalesce(q.cloud_shadow_pixels,0) AS cloud_shadow_pixels,
                coalesce(q.snow_pixels,0) AS snow_pixels,
                coalesce(q.invalid_pixels,0) AS invalid_pixels,
                coalesce(q.review_pixels,0) AS review_pixels,
                coalesce(q.water_pixels,0) AS water_pixels,
                CASE WHEN q.scene_key IS NULL THEN coalesce(z.expected_pixels,0)
                     ELSE q.unknown_pixels END AS unknown_pixels,
                CASE WHEN q.scene_key IS NULL THEN coalesce(z.expected_pixels,0)
                     ELSE q.missing_band_pixels END AS missing_band_pixels,
                coalesce(q.usable_pixels,0) AS usable_pixels,
                count(*) OVER w AS candidate_products_or_tiles,
                row_number() OVER (w ORDER BY (q.level='L2A') DESC NULLS LAST,
                    (q.scene_key IS NOT NULL) DESC, z.expected_pixels DESC NULLS LAST,
                    try_cast(q.processing_baseline AS DOUBLE) DESC NULLS LAST,
                    p.mgrs_tile,q.scene_id,q.item_id) AS product_rank
            FROM src.passes p JOIN src.fields f USING(fs_key)
            LEFT JOIN product_quality q ON p.loc_key=q.loc_key AND p.granule_key=q.granule_key
                AND p.mgrs_tile=q.mgrs_tile
            LEFT JOIN footprint_sizes z ON p.loc_key=z.loc_key AND p.mgrs_tile=z.mgrs_tile
            WINDOW w AS (PARTITION BY p.fs_key,p.window_kind,p.acquisition_id)
        ) SELECT * EXCLUDE(product_rank),
            100.0*usable_pixels/nullif(expected_pixels,0) AS usable_percent,
            100.0*(cloud_pixels+cloud_shadow_pixels+snow_pixels+invalid_pixels)/nullif(expected_pixels,0) AS contaminated_percent,
            100.0*(review_pixels+unknown_pixels)/nullif(expected_pixels,0) AS uncertain_percent,
            100.0*missing_band_pixels/nullif(expected_pixels,0) AS missing_band_percent
        FROM candidates WHERE product_rank=1;
    ''')
    run('6/7 Compare coverage thresholds by dataset, year and region', '''
        CREATE TABLE thresholds AS SELECT * FROM (VALUES (50),(70),(90)) t(threshold_percent);
        CREATE TABLE retention_by_dataset AS
        SELECT t.threshold_percent,q.dataset,q.country_iso3,q.crop,q.harvest_year,
            count(*) AS candidate_dates,count(DISTINCT q.fs_key) AS fields_with_candidates,
            count(*) FILTER(WHERE q.expected_pixels=0) AS dates_without_footprint,
            count(*) FILTER(WHERE q.usable_percent>=t.threshold_percent) AS retained_dates,
            count(DISTINCT q.fs_key) FILTER(WHERE q.usable_percent>=t.threshold_percent) AS fields_with_retained_dates,
            100.0*retained_dates/count(*) AS retained_percent
        FROM field_date_quality q CROSS JOIN thresholds t GROUP BY ALL;
        CREATE TABLE retention_by_region AS
        SELECT t.threshold_percent,q.dataset,q.country_iso3,q.harvest_year,q.crop,q.admin_unit,
            count(*) AS candidate_dates,
            count(*) FILTER(WHERE q.usable_percent>=t.threshold_percent) AS retained_dates,
            100.0*retained_dates/count(*) AS retained_percent
        FROM field_date_quality q CROSS JOIN thresholds t GROUP BY ALL;
        CREATE TABLE field_month_coverage AS
        WITH dates AS (
            SELECT q.fs_key,q.window_kind,year(q.sensing_time_utc) AS year,
                month(q.sensing_time_utc) AS month,t.threshold_percent,
                count(*) AS candidate_dates,
                count(*) FILTER(WHERE q.usable_percent>=t.threshold_percent) AS retained_dates
            FROM field_date_quality q CROSS JOIN thresholds t GROUP BY ALL
        ) SELECT c.fs_key,f.field_season_id,f.dataset,f.country_iso3,f.crop,f.harvest_year,
            f.admin_unit,c.window_kind,c.year,c.month,c.month_status,t.threshold_percent,
            coalesce(d.candidate_dates,0) AS candidate_dates,
            coalesce(d.retained_dates,0) AS retained_dates
        FROM src.coverage_months c JOIN src.fields f USING(fs_key) CROSS JOIN thresholds t
        LEFT JOIN dates d ON c.fs_key=d.fs_key AND c.window_kind=d.window_kind
            AND c.year=d.year AND c.month=d.month AND t.threshold_percent=d.threshold_percent;
        CREATE TABLE retention_by_month AS
        SELECT threshold_percent,dataset,country_iso3,crop,harvest_year,window_kind,year,month,
            count(*) AS field_months,sum(candidate_dates) AS candidate_dates,
            sum(retained_dates) AS retained_dates,
            count(*) FILTER(WHERE retained_dates>0) AS fields_with_retained_dates,
            100.0*fields_with_retained_dates/count(*) AS percent_field_months_covered
        FROM field_month_coverage GROUP BY ALL;
        CREATE TABLE field_coverage AS
        SELECT threshold_percent,fs_key,field_season_id,dataset,country_iso3,crop,harvest_year,
            window_kind,count(*) AS calendar_months,
            sum(retained_dates) AS retained_dates,
            count(*) FILTER(WHERE retained_dates>0) AS months_with_retained_dates
        FROM field_month_coverage GROUP BY ALL;
    ''')
    run('7/7 Verify counts and threshold monotonicity', '''
        CREATE TABLE validation AS
        SELECT 'unique field acquisition keys' AS check_name,
            count(*)=count(DISTINCT (fs_key,window_kind,acquisition_id)) AS passed FROM field_date_quality
        UNION ALL SELECT 'usable counts within footprint',
            count(*) FILTER(WHERE usable_pixels<0 OR usable_pixels>expected_pixels)=0 FROM field_date_quality
        UNION ALL SELECT 'flag categories within footprint',count(*) FILTER(WHERE
            cloud_pixels+cloud_shadow_pixels+snow_pixels+invalid_pixels+review_pixels+
            water_pixels+unknown_pixels>expected_pixels)=0 FROM field_date_quality
        UNION ALL SELECT 'threshold monotonicity',count(*) FILTER(WHERE a.retained_dates<b.retained_dates)=0
            FROM retention_by_dataset a JOIN retention_by_dataset b
                USING(dataset,country_iso3,crop,harvest_year)
            WHERE a.threshold_percent<b.threshold_percent;
    ''')
    if not all(row[0] for row in c.sql('SELECT passed FROM validation').fetchall()):
        raise AssertionError('Quality audit validation failed; inspect validation table')
    c.execute('CHECKPOINT')
    progress('Complete. Source unchanged; thresholds are audit scenarios, not chosen filters.')
