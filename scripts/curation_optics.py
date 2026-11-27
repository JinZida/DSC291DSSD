"""Monthly Sentinel-2 features for curate_dataset.ipynb.

Adapted from Pamely's native-grid quality audit and masked field reflectances.
The input is the shared team database. Each raw schema has its own field and
product keys. Only field_season_id is used to combine the two archives.
"""
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd

NATIVE_10M = ("b02", "b03", "b04", "b08")
NATIVE_20M = ("b05", "b06", "b07", "b8a", "b11", "b12")
BANDS = ("b02", "b03", "b04", "b05", "b06", "b07", "b08", "b8a", "b11", "b12")
SCHEMAS = ("raw_v1", "raw_oaf5")


def _load_passes(con, schema, keys):
    """Restrict work to the requested field-seasons and their harvest calendar year."""
    con.register("field_keys", keys)
    con.execute(f"""
        CREATE TEMP TABLE passes AS
        SELECT f.field_season_id, p.*
        FROM archive.{schema}.fields f JOIN field_keys k USING(field_season_id)
        JOIN archive.{schema}.passes p USING(fs_key)
        WHERE year(p.sensing_time_utc) = k.harvest_year
    """)
    con.execute(f"""
        CREATE TEMP TABLE products AS
        SELECT p.* FROM archive.{schema}.pass_samples p
        SEMI JOIN passes USING(loc_key, granule_key)
    """)
    con.execute(f"""
        CREATE TEMP TABLE footprint AS
        SELECT loc_key, mgrs_tile, px_row, px_col, is_point_pixel
        FROM archive.{schema}.footprint_pixels
        WHERE res_m = 10 AND in_default_summary
    """)
    con.execute(f"""
        CREATE TEMP VIEW scenes AS SELECT * FROM archive.{schema}.scenes;
        CREATE TEMP VIEW pixel_samples AS
        SELECT p.* FROM archive.{schema}.pixel_samples p SEMI JOIN products USING(loc_key, scene_key);
        CREATE TEMP TABLE offsets AS
        SELECT scene_key, item_id, min(dn_add_offset) AS dn_offset,
               count(DISTINCT dn_add_offset) = 1 AS known_offset
        FROM archive.{schema}.scene_items GROUP BY scene_key, item_id
    """)
    for table, key in [("products", "(loc_key,scene_key)"), ("scenes", "scene_key"),
                       ("footprint", "(loc_key,mgrs_tile,px_row,px_col)")]:
        if con.execute(f"SELECT count(*) <> count(DISTINCT {key}) FROM {table}").fetchone()[0]:
            raise ValueError(f"Ambiguous {table} keys in {schema}")


def _pixel_summary(con):
    """Compute usable counts and median reflectance over the expected footprint of each product."""
    # A 20 m cell supplies the red-edge/SWIR bands and SCL for its four 10 m children.
    for resolution, bands in [(10, NATIVE_10M), (20, NATIVE_20M)]:
        scale = resolution // 10
        values = [f"max(p.dn) FILTER (WHERE p.band='{b.upper()}') AS {b}" for b in bands]
        if resolution == 20:
            values.append("""CASE WHEN count(DISTINCT p.dn) FILTER (WHERE p.band='SCL') = 1
                                  THEN max(p.dn) FILTER (WHERE p.band='SCL') END AS scl""")
        band_names = ", ".join(f"'{b.upper()}'" for b in (*bands, "scl"))
        con.execute(f"""
            CREATE TEMP TABLE dn{resolution} AS
            SELECT p.loc_key, p.scene_key, p.px_row, p.px_col, {', '.join(values)}
            FROM pixel_samples p JOIN scenes s USING(scene_key)
            SEMI JOIN footprint f ON p.loc_key=f.loc_key AND s.mgrs_tile=f.mgrs_tile
                 AND p.px_row=f.px_row//{scale} AND p.px_col=f.px_col//{scale}
            WHERE p.res_m={resolution} AND p.band IN ({band_names})
            GROUP BY p.loc_key, p.scene_key, p.px_row, p.px_col
        """)

    # For point fields the archive supplies a single sample. Polygons use their native pixel grids.
    bands_sql = ", ".join(
        f"CASE WHEN f.is_point_pixel AND p.n_pixels=1 THEN p.{b} "
        f"ELSE {'d10' if b in NATIVE_10M else 'd20'}.{b} END AS {b}" for b in BANDS
    )
    valid_bands = " AND ".join(f"{b} > 0 AND isfinite({b})" for b in BANDS)
    medians = ", ".join(
        f"median(CAST(({b} + dn_offset)/10000.0 AS FLOAT)) FILTER (WHERE usable) AS {b}" for b in BANDS
    )
    con.execute(f"""
        CREATE TEMP TABLE product_summary AS
        WITH native_pixels AS (
            SELECT p.loc_key, p.scene_key, p.item_id, p.granule_key, s.mgrs_tile,
                   s.level, s.processing_baseline, s.scene_id, o.dn_offset, o.known_offset,
                   CASE WHEN f.is_point_pixel AND p.n_pixels=1 THEN p.scl ELSE d20.scl END AS scl,
                   {bands_sql}
            FROM products p JOIN scenes s USING(scene_key)
            JOIN footprint f ON f.loc_key=p.loc_key AND f.mgrs_tile=s.mgrs_tile
            LEFT JOIN offsets o ON p.scene_key=o.scene_key AND p.item_id=o.item_id
            LEFT JOIN dn10 d10 ON p.loc_key=d10.loc_key AND p.scene_key=d10.scene_key
                 AND f.px_row=d10.px_row AND f.px_col=d10.px_col
            LEFT JOIN dn20 d20 ON p.loc_key=d20.loc_key AND p.scene_key=d20.scene_key
                 AND f.px_row//2=d20.px_row AND f.px_col//2=d20.px_col
        ), flagged AS (
            SELECT *, level='L2A' AND scl IN (4,5) AND known_offset AND isfinite(dn_offset)
                      AND {valid_bands} AS usable
            FROM native_pixels
        )
        SELECT loc_key, scene_key, item_id, granule_key, mgrs_tile, level, processing_baseline,
               scene_id, count(*) AS expected_pixels, count(*) FILTER (WHERE usable) AS usable_pixels,
               {medians}
        FROM flagged GROUP BY ALL
    """)


def _monthly_medians(con):
    """Choose one product per field/window/acquisition before applying the 50% rule."""
    con.execute("""
        CREATE TEMP TABLE acquisitions AS
        SELECT p.field_season_id, p.sensing_time_utc, q.* EXCLUDE(loc_key,granule_key,mgrs_tile)
        FROM passes p LEFT JOIN product_summary q USING(loc_key,granule_key,mgrs_tile)
        LEFT JOIN (SELECT loc_key,mgrs_tile,count(*) AS footprint_size FROM footprint GROUP BY ALL) f
             USING(loc_key,mgrs_tile)
        QUALIFY row_number() OVER (PARTITION BY p.fs_key,p.window_kind,p.acquisition_id
            ORDER BY (q.level='L2A') DESC NULLS LAST, (q.scene_key IS NOT NULL) DESC,
                     f.footprint_size DESC NULLS LAST, try_cast(q.processing_baseline AS DOUBLE) DESC NULLS LAST,
                     p.mgrs_tile,q.scene_id,q.item_id) = 1
    """)
    medians = ", ".join(f"median({b}) AS {b}" for b in BANDS)
    return con.sql(f"""
        SELECT field_season_id, month(sensing_time_utc) AS month, {medians},
               median((b08-b04)/nullif(b08+b04,0)) AS ndvi, count(*) AS acquisitions
        FROM acquisitions WHERE usable_pixels * 2 >= expected_pixels
        GROUP BY field_season_id, month(sensing_time_utc)
    """).df()


def monthly_optics(database: Path, keys: pd.DataFrame) -> pd.DataFrame:
    """Read the shared archive and return one row per observed field-season/calendar month."""
    keys = keys[["field_season_id", "harvest_year"]]
    if keys.isna().any().any() or not keys.field_season_id.is_unique:
        raise ValueError("Each input row needs a unique field-season ID and a harvest year")
    database_sql = "'" + str(database.resolve()).replace("'", "''") + "'"
    parts = []
    for schema in SCHEMAS:
        with TemporaryDirectory() as scratch, duckdb.connect(str(Path(scratch) / "pixels.duckdb")) as con:
            con.execute("SET threads=2")
            con.execute("SET memory_limit='4GB'")
            con.execute("SET preserve_insertion_order=false")
            con.execute(f"ATTACH {database_sql} AS archive (READ_ONLY)")
            _load_passes(con, schema, keys)
            _pixel_summary(con)
            parts.append(_monthly_medians(con))
    return pd.concat(parts, ignore_index=True)
