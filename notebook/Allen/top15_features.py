"""Aggregate only the selected top-15 scenes using the full team database.

Run after quality_screen.py --window-kind both --crop maize for each raw schema.
The full database is never changed. Model scaling/imputation belongs inside CV.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


BANDS = ("b02", "b03", "b04", "b05", "b06", "b07", "b08", "b8a", "b11", "b12")


def q(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def build(database: Path, schema: str, top15: Path, output: Path) -> dict:
    database, top15, output = map(Path, (database, top15, output))
    if schema not in ("raw_v1", "raw_oaf5"):
        raise ValueError("schema must be raw_v1 or raw_oaf5")
    if not database.is_file() or not top15.is_file():
        raise FileNotFoundError("Need the release DuckDB and two-year maize monthly_top15.parquet")
    summary_file = top15.parent / "summary.json"
    if summary_file.is_file():
        source_summary = json.loads(summary_file.read_text(encoding="utf-8"))
        if (source_summary.get("window_kind") != "both"
                or source_summary.get("crop") != "maize"
                or source_summary.get("schema") != schema
                or source_summary.get("cohort") != "official kept, unlocked development folds 1-4"):
            raise ValueError("Screen must use the selected schema, development cohort, both years and maize")
    output.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(database), read_only=True)
    try:
        for table in ("fields", "coverage_months"):
            con.execute(f"CREATE TEMP VIEW {table} AS SELECT * FROM {schema}.{table}")
        release = "passarchive_v1" if schema == "raw_v1" else "passarchive_oaf5_v1"
        con.execute(f"""
            CREATE TEMP TABLE dev_fields AS
            SELECT l.field_season_id FROM main.labels l
            JOIN main.folds_all fa ON fa.field_season_id=l.field_season_id
                                  AND fa.release=l.release
            WHERE l.release='{release}' AND l.keep AND NOT fa.locked
              AND fa.fold BETWEEN 1 AND 4
        """)
        reflectance = ",\n                   ".join(
            f"(d.{b} + coalesce(d.dn_add_offset,0))/10000.0 AS {b}" for b in BANDS
        )
        con.execute(f"""
            CREATE TEMP TABLE converted AS
            SELECT d.field_season_id, d.fs_key, d.country_iso3, d.dataset,
                   d.year, d.month, d.scene_id, d.acquisition_id,
                   d.sensing_time_utc, d.quality_rank,
                   d.same_overflight_rank,
                   {reflectance}
            FROM read_parquet({q(top15)}) d
            WHERE d.crop='maize' AND d.quality_rank<=15
        """)
        valid = " AND ".join(f"{b} BETWEEN 0 AND 1" for b in BANDS)
        con.execute(f"""
            CREATE TEMP TABLE valid AS SELECT * FROM converted WHERE {valid}
        """)
        con.execute("CREATE TEMP TABLE chosen AS SELECT * FROM converted")
        median_bands = ",\n                   ".join(f"median(c.{b}) AS {b}" for b in BANDS)
        con.execute(f"""
            CREATE TEMP TABLE monthly AS
            SELECT f.field_season_id, f.country_iso3, f.dataset, f.harvest_year,
                   cm.window_kind, cm.year, cm.month,
                   printf('%04d-%02d',cm.year,cm.month) AS month_id,
                   count(c.scene_id) AS n_selected_scenes,
                   count(DISTINCT c.acquisition_id) AS n_independent_overflights,
                   {median_bands}
            FROM coverage_months cm JOIN fields f USING(fs_key)
            JOIN dev_fields d ON d.field_season_id=f.field_season_id
            LEFT JOIN chosen c ON c.fs_key=cm.fs_key
                 AND c.year=cm.year AND c.month=cm.month
            WHERE f.crop='maize' AND cm.window_kind IN ('current','previous')
            GROUP BY ALL
        """)
        con.execute("""
            CREATE TEMP TABLE in_scope AS
            SELECT m.*, f.planting_date, f.harvest_date,
                CASE WHEN f.planting_date IS NOT NULL AND f.harvest_date IS NOT NULL
                       AND f.planting_date<=f.harvest_date
                       AND date_diff('day',f.planting_date,f.harvest_date)<=550
                     THEN 'recorded_dates' ELSE 'harvest_calendar_year' END AS window_rule,
                CASE WHEN f.planting_date IS NOT NULL AND f.harvest_date IS NOT NULL
                       AND f.planting_date<=f.harvest_date
                       AND date_diff('day',f.planting_date,f.harvest_date)<=550
                     THEN make_date(m.year,m.month,1)<=f.harvest_date
                          AND last_day(make_date(m.year,m.month,1))>=f.planting_date
                     ELSE m.year=f.harvest_year END AS in_model_window
            FROM monthly m JOIN fields f USING(field_season_id)
        """)
        season_bands = ",\n                   ".join(
            f"avg({b}) FILTER (WHERE in_model_window) AS {b}_mean" for b in BANDS
        )
        con.execute(f"""
            CREATE TEMP TABLE seasonal AS
            SELECT field_season_id, any_value(country_iso3) AS country_iso3,
                   any_value(dataset) AS dataset, any_value(harvest_year) AS harvest_year,
                   any_value(window_rule) AS window_rule,
                   count(*) FILTER (WHERE in_model_window) AS months_in_window,
                   count(*) FILTER (WHERE in_model_window AND n_selected_scenes>0)
                       AS months_observed,
                   sum(n_selected_scenes) FILTER (WHERE in_model_window) AS scene_count,
                   {season_bands},
                   max((b08-b04)/nullif(b08+b04,0)) FILTER (WHERE in_model_window)
                       AS ndvi_max,
                   avg((b08-b04)/nullif(b08+b04,0)) FILTER (WHERE in_model_window)
                       AS ndvi_mean
            FROM in_scope GROUP BY 1
        """)
        con.execute("""
            CREATE TEMP TABLE model_ready AS
            SELECT s.*, l.release, l.data_set, l.season_year, l.geometry_type,
                   l.location_quality, l.label_status, l.yield_t_ha,
                   fa.fold, fa.locked, fa.gap_1, fa.gap_2, fa.gap_3, fa.gap_4
            FROM seasonal s
            JOIN main.labels l USING(field_season_id)
            JOIN main.folds_all fa ON fa.field_season_id=l.field_season_id
                                  AND fa.release=l.release
            WHERE l.keep AND NOT fa.locked AND fa.fold BETWEEN 1 AND 4
        """)
        con.execute(f"COPY monthly TO {q(output / 'monthly.parquet')} (FORMAT PARQUET)")
        con.execute(f"COPY chosen TO {q(output / 'selected_scenes.parquet')} (FORMAT PARQUET)")
        con.execute(f"COPY model_ready TO {q(output / 'model_ready.parquet')} (FORMAT PARQUET)")
        results = {
            "schema": schema,
            "source_top15": str(top15.resolve()),
            "scope": "maize; previous and current archive years",
            "converted_scenes": con.execute("SELECT count(*) FROM converted").fetchone()[0],
            "valid_reflectance_scenes": con.execute("SELECT count(*) FROM valid").fetchone()[0],
            "out_of_range_reflectance_scenes": con.execute("SELECT count(*) FROM converted").fetchone()[0] - con.execute("SELECT count(*) FROM valid").fetchone()[0],
            "selected_scenes": con.execute("SELECT count(*) FROM chosen").fetchone()[0],
            "monthly_rows": con.execute("SELECT count(*) FROM monthly").fetchone()[0],
            "field_seasons": con.execute("SELECT count(DISTINCT field_season_id) FROM monthly").fetchone()[0],
            "model_rows": con.execute("SELECT count(*) FROM model_ready").fetchone()[0],
            "window_rule_counts": dict(con.execute("SELECT window_rule,count(*) FROM model_ready GROUP BY 1").fetchall()),
            "model_month_coverage": con.execute("""
                SELECT country_iso3, count(*) AS fields,
                       count(*) FILTER(WHERE months_observed>0) AS with_image,
                       round(avg(months_observed),2) AS mean_months
                FROM model_ready GROUP BY 1 ORDER BY 1
            """).fetchall(),
        }
        (output / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
        return results
    finally:
        con.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--database", type=Path, required=True)
    p.add_argument("--schema", choices=("raw_v1", "raw_oaf5"), required=True)
    p.add_argument("--top15", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(build(a.database, a.schema, a.top15, a.output), indent=2))
