"""Export 50%-coverage maize observations with only usable spectral pixels.

Source and audit databases are read-only. All products retain their field-season,
window and acquisition keys; labels are exported once per field-season.
"""
import hashlib
import json
from pathlib import Path

import duckdb

from cropyield.satellite.passarchive_quality import TEN_BANDS, TWENTY_BANDS, quote

BANDS = TEN_BANDS + TWENTY_BANDS
COUNTRIES = {'MWI': 'malawi', 'ZWE': 'zimbabwe', 'KEN': 'kenya'}


def export_filtered(source, audit_db, output, threshold=50, progress=print):
    source, audit_db, output = (Path(p).resolve() for p in [source, audit_db, output])
    if not 0 < threshold <= 100:
        raise ValueError('Threshold must be in (0,100]')
    if output.exists():
        raise FileExistsError(f'Choose a new output folder: {output}')
    stat = (source.stat().st_size, source.stat().st_mtime_ns)
    c = duckdb.connect()
    c.execute("SET memory_limit='1500MB'")
    c.execute('SET threads=4')
    c.execute(f'ATTACH {quote(source)} AS src (READ_ONLY)')
    c.execute(f'ATTACH {quote(audit_db)} AS qa (READ_ONLY)')
    prov = c.sql('SELECT * FROM qa.audit_provenance').fetchone()
    audit_hash = hashlib.sha256(Path(__file__).with_name('passarchive_quality.py').read_bytes()).hexdigest()
    if prov != (source.name, *stat, audit_hash):
        c.close()
        raise ValueError('Audit provenance does not match source/algorithm; rebuild the audit')
    if not all(row[0] for row in c.sql('SELECT passed FROM qa.validation').fetchall()):
        c.close()
        raise ValueError('Audit checks failed')
    output.mkdir(parents=True)
    c.execute(f"SET temp_directory={quote(output / '.duckdb_spill')}")
    try:
        manifest = _export(c, output, threshold, progress)
        if (source.stat().st_size, source.stat().st_mtime_ns) != stat:
            raise AssertionError('Source file changed during export')
        manifest.update(source=source.name, source_bytes=stat[0], source_mtime_ns=stat[1],
                        audit_algorithm_sha256=audit_hash,
                        export_algorithm_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                        complete=True)
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    finally:
        c.close()
    return manifest


def _export(c, output, threshold, progress):
    progress('1/5 Select maize observations at the approved coverage threshold')
    for table, key in [('src.fields', 'fs_key'), ('src.labels', 'fs_key')]:
        rows, unique = c.sql(f'SELECT count(*),count(DISTINCT {key}) FROM {table}').fetchone()
        if rows != unique:
            raise ValueError(f'Duplicate {table} keys; refusing an ambiguous label join')
    c.execute('''CREATE TEMP TABLE selected AS
        SELECT q.*,f.loc_key FROM qa.field_date_quality q JOIN src.fields f USING(fs_key)
        WHERE q.country_iso3 IN ('MWI','ZWE','KEN') AND q.crop='maize'
          AND q.usable_percent>=?''', [threshold])
    c.execute('''CREATE TEMP TABLE products AS SELECT DISTINCT loc_key,scene_key,item_id,mgrs_tile,
        quality_source FROM selected''')
    c.execute('''CREATE TEMP TABLE polygons AS SELECT * FROM products WHERE quality_source='raw_pixels' ''')
    progress('2/5 Read only the selected polygon pixels on their native grids')
    def pivot(bands):
        return ','.join(f"max(p.dn) FILTER(WHERE p.band='{b}') AS {b.lower()}" for b in bands)
    c.execute(f'''CREATE TEMP TABLE dn10 AS
        SELECT p.loc_key,p.scene_key,p.px_row,p.px_col,{pivot(TEN_BANDS)}
        FROM src.pixel_samples p JOIN polygons k USING(loc_key,scene_key)
        JOIN qa.footprint10 f ON p.loc_key=f.loc_key AND k.mgrs_tile=f.mgrs_tile
            AND p.px_row=f.px_row AND p.px_col=f.px_col
        WHERE p.res_m=10 AND p.band IN ('B02','B03','B04','B08') GROUP BY ALL;
        CREATE TEMP TABLE dn20 AS
        SELECT p.loc_key,p.scene_key,p.px_row,p.px_col,{pivot(TWENTY_BANDS + ['SCL'])}
        FROM src.pixel_samples p JOIN polygons k USING(loc_key,scene_key)
        JOIN qa.support20 f ON p.loc_key=f.loc_key AND k.mgrs_tile=f.mgrs_tile
            AND p.px_row=f.px_row AND p.px_col=f.px_col
        WHERE p.res_m=20 AND p.band IN ('B05','B06','B07','B8A','B11','B12','SCL') GROUP BY ALL;
    ''')
    polygon_values = ','.join(f"{'a' if b in TEN_BANDS else 'b'}.{b.lower()}" for b in BANDS)
    point_values = ','.join('p.'+b.lower() for b in BANDS)
    valid = ' AND '.join(f'{b.lower()}>0 AND isfinite({b.lower()})' for b in BANDS)
    reflectance = ','.join(f'CAST(({b.lower()}+dn_add_offset)/10000.0 AS FLOAT) AS {b.lower()}' for b in BANDS)
    progress('3/5 Mask unusable pixels, then compute field medians from usable pixels only')
    c.execute(f'''CREATE TEMP TABLE usable_pixels AS
        WITH dn AS (
            SELECT k.loc_key,k.scene_key,k.item_id,f.px_row,f.px_col,b.scl,{polygon_values}
            FROM polygons k JOIN qa.footprint10 f USING(loc_key,mgrs_tile)
            JOIN dn10 a ON k.loc_key=a.loc_key AND k.scene_key=a.scene_key
                AND f.px_row=a.px_row AND f.px_col=a.px_col
            JOIN dn20 b ON k.loc_key=b.loc_key AND k.scene_key=b.scene_key
                AND f.px_row//2=b.px_row AND f.px_col//2=b.px_col
            UNION ALL
            SELECT k.loc_key,k.scene_key,k.item_id,f.px_row,f.px_col,p.scl,{point_values}
            FROM products k JOIN src.pass_samples p USING(loc_key,scene_key,item_id)
            JOIN qa.footprint10 f USING(loc_key,mgrs_tile)
            WHERE k.quality_source='single_pixel_sample' AND p.n_pixels=1 AND f.is_point_pixel
        ) SELECT d.loc_key,d.scene_key,d.item_id,d.px_row,d.px_col,d.scl,{reflectance}
        FROM dn d JOIN qa.offsets o USING(scene_key,item_id)
        WHERE scl IN(4,5) AND {valid} AND o.known_offset AND isfinite(o.dn_add_offset);
    ''')
    c.execute("""CREATE TEMP TABLE pixel_status AS
        SELECT k.loc_key,k.scene_key,k.item_id,f.px_row,f.px_col,b.scl
        FROM polygons k JOIN qa.footprint10 f USING(loc_key,mgrs_tile)
        LEFT JOIN dn20 b ON k.loc_key=b.loc_key AND k.scene_key=b.scene_key
            AND f.px_row//2=b.px_row AND f.px_col//2=b.px_col
        UNION ALL
        SELECT k.loc_key,k.scene_key,k.item_id,f.px_row,f.px_col,p.scl
        FROM products k JOIN src.pass_samples p USING(loc_key,scene_key,item_id)
        JOIN qa.footprint10 f USING(loc_key,mgrs_tile)
        WHERE k.quality_source='single_pixel_sample' AND f.is_point_pixel AND p.n_pixels=1
    """)
    c.execute('DROP TABLE dn10; DROP TABLE dn20')
    medians = ','.join(f'median({b.lower()}) AS {b.lower()}' for b in BANDS)
    c.execute(f'''CREATE TEMP TABLE pixel_summaries AS SELECT loc_key,scene_key,item_id,
        count(*) AS exported_usable_pixels,{medians} FROM usable_pixels GROUP BY ALL''')
    mismatches = c.sql('''SELECT count(*) FROM selected q LEFT JOIN pixel_summaries p
        USING(loc_key,scene_key,item_id)
        WHERE p.exported_usable_pixels IS NULL OR p.exported_usable_pixels<>q.usable_pixels''').fetchone()[0]
    if mismatches:
        raise AssertionError(f'{mismatches} observation pixel counts differ from quality audit')
    duplicate_pixels = c.sql('''SELECT count(*)-count(DISTINCT(loc_key,scene_key,item_id,px_row,px_col))
        FROM usable_pixels''').fetchone()[0]
    if duplicate_pixels:
        raise AssertionError('Duplicate usable pixel keys')
    c.execute('''CREATE TEMP TABLE filtered_observations AS SELECT q.*,
        p.* EXCLUDE(loc_key,scene_key,item_id) FROM selected q JOIN pixel_summaries p
        USING(loc_key,scene_key,item_id)''')
    countries = []
    progress('4/5 Export countries separately, including labels, masks and full-calendar coverage')
    for iso, name in COUNTRIES.items():
        folder = output/name
        folder.mkdir()
        predicate = f"country_iso3={quote(iso)} AND crop='maize'"
        field_predicate = f"f.country_iso3={quote(iso)} AND f.crop='maize'"
        def copy(sql, filename, folder=folder):
            c.execute(f'COPY ({sql}) TO {quote(folder/filename)} (FORMAT PARQUET,COMPRESSION ZSTD)')
        copy(f'''SELECT f.*,l.* EXCLUDE(fs_key,field_season_id,crop),
            l.yield_kg_ha/1000.0 AS yield_t_ha,
            coalesce(r.retained_dates,0) AS retained_dates,
            coalesce(r.retained_dates,0)>0 AS has_retained_imagery
            FROM src.fields f LEFT JOIN src.labels l USING(fs_key,field_season_id)
            LEFT JOIN (SELECT fs_key,count(*) AS retained_dates FROM selected GROUP BY fs_key) r USING(fs_key)
            WHERE {field_predicate}''', 'fields.parquet')
        copy(f'SELECT * FROM filtered_observations WHERE {predicate}', 'observations.parquet')
        # Complete footprint rows, including masked positions represented by null spectral values.
        columns = ','.join('p.'+b.lower() for b in BANDS)
        copy(f'''SELECT q.field_season_id,q.fs_key,q.window_kind,q.acquisition_id,q.sensing_time_utc,
            q.loc_key,q.scene_key,q.item_id,q.mgrs_tile,f.px_row,f.px_col,
            10 AS support_grid_m,q.quality_source,s.scl,
            p.scene_key IS NOT NULL AS pixel_usable,
            CASE WHEN p.scene_key IS NOT NULL THEN ''
                 WHEN s.scl IS NULL THEN 'missing_classification'
                 WHEN s.scl IN(0,1) THEN 'invalid'
                 WHEN s.scl=3 THEN 'cloud_shadow'
                 WHEN s.scl IN(8,9,10) THEN 'cloud_or_cirrus'
                 WHEN s.scl=11 THEN 'snow_or_ice'
                 WHEN s.scl IN(2,7) THEN 'review_classification'
                 WHEN s.scl=6 THEN 'water'
                 WHEN s.scl NOT IN(4,5) THEN 'unknown_classification'
                 ELSE 'missing_bands_or_offset' END AS masking_reason,{columns}
            FROM selected q JOIN qa.footprint10 f USING(loc_key,mgrs_tile)
            LEFT JOIN pixel_status s ON q.loc_key=s.loc_key AND q.scene_key=s.scene_key
                AND q.item_id=s.item_id AND f.px_row=s.px_row AND f.px_col=s.px_col
            LEFT JOIN usable_pixels p ON q.loc_key=p.loc_key AND q.scene_key=p.scene_key
                AND q.item_id=p.item_id AND f.px_row=p.px_row AND f.px_col=p.px_col
            WHERE q.{predicate}''', 'masked_pixels.parquet')
        copy(f'''SELECT * FROM qa.field_month_coverage
            WHERE threshold_percent={float(threshold)} AND {predicate}''', 'coverage_months.parquet')
        copy(f'''SELECT s.* FROM src.splits s JOIN src.fields f USING(fs_key)
            WHERE {field_predicate}''', 'split_membership.parquet')
        copy(f'''SELECT DISTINCT s.* FROM src.split_specs s JOIN src.splits m USING(split_name)
            JOIN src.fields f USING(fs_key) WHERE {field_predicate}''', 'split_specs.parquet')
        stats = c.execute(f'''SELECT count(*) AS observations,count(DISTINCT fs_key) AS fields_with_imagery,
            coalesce(sum(usable_pixels),0) AS usable_pixel_records,coalesce(sum(expected_pixels),0) AS footprint_pixel_records,
            min(sensing_time_utc) AS first_acquisition,max(sensing_time_utc) AS last_acquisition
            FROM selected WHERE {predicate}''').df().iloc[0].to_dict()
        total_fields = c.sql(f'SELECT count(*) FROM src.fields WHERE {predicate}').fetchone()[0]
        stats = {k: (str(v) if k.endswith('acquisition') else int(v)) for k,v in stats.items()}
        stats.update(country=name,country_iso3=iso,fields_total=total_fields)
        stats['fields_without_retained_imagery'] = total_fields-stats['fields_with_imagery']
        countries.append(stats)
        # Read back the exported masks to ensure rejected pixels have no spectral values.
        any_present = ' OR '.join(f'{b.lower()} IS NOT NULL' for b in BANDS)
        any_missing = ' OR '.join(f'{b.lower()} IS NULL OR NOT isfinite({b.lower()})' for b in BANDS)
        failures = c.sql(f'''SELECT count(*) FROM read_parquet({quote(folder/'masked_pixels.parquet')})
            WHERE (NOT pixel_usable AND ({any_present})) OR (pixel_usable AND ({any_missing}))''').fetchone()[0]
        if failures:
            raise AssertionError(f'{name}: invalid exported masks')
        progress(f'{name}: {stats["observations"]:,} observations, {stats["fields_with_imagery"]:,} fields with imagery')
    progress('5/5 Write provenance and export summary')
    import pandas as pd
    pd.DataFrame(countries).to_csv(output/'summary.csv',index=False)
    return {'threshold_percent': threshold,'crop': 'maize','window_kinds': ['previous','current'],
        'acquisition_date_cutoff': None,'usable_scl_codes': [4,5],'required_bands': BANDS,
        'reflectance_formula': '(DN + product dn_add_offset) / 10000; offset applied once',
        'support_grid_m': 10,'native_10m_bands': TEN_BANDS,'native_20m_bands': TWENTY_BANDS,
        'scl_native_grid_m': 20,'quality_rule': 'Matches approved coverage audit; required bands and known offset',
        'spectral_summary': 'Median of usable pixels only; masked pixel positions have NULL spectral values',
        'labels_policy': 'Source labels unchanged; missing yields remain missing and recorded zeros remain zero',
        'field_policy': 'All maize field-seasons retained in fields table, even when no imagery passes',
        'split_policy': 'All source split versions preserved separately; no training or scoring',
        'limitations': ['Full archive windows may include postharvest imagery; not an early-forecast dataset',
            'Default footprint pixels, not area-weighted boundaries',
            '20m bands/classification repeated on a 10m support grid; no new spatial detail',
            'One metadata-selected product/tile per acquisition; no polygon mosaicking',
            'Point SCL/bands come from single-pixel pass_samples; no point raw-raster reconstruction'],
        'countries': countries}
