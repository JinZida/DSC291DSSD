"""Spatial folds for any cleaned table: the team's frozen folds, the lock and the 2 km gap.

    python scripts/spatial_folds.py --check      # rebuild lock and gaps from folds and points: must equal the database

In a notebook:
    import sys; sys.path.insert(0, "../../scripts")
    from spatial_folds import add_folds, train_test
    table = add_folds(table, con)        # table needs field_season_id; adds fold, locked, gap_1..gap_4, fold_rule
    train, test = train_test(table, k)   # boolean masks for fold k = 1..4

Rules (the project's folds_v3):
- A kept field-season keeps its frozen fold (folds table of team_data_v1.duckdb).
- Another row with a point: within 2 km of a frozen field of the same country it takes the nearest one's fold;
  else its village group (fields chained by gaps under 2 km) takes the group's most common fold; else whole new
  groups are spread over the folds, largest first into the smallest fold of the data set (seed 20261029).
- Locked = fold 0, or `labels.in_lock_area` (within 2 km of a kept fold-0 field), or within 2 km of any fold-0 row
  of the same country in THIS table (a new row can get fold 0). Never train, tune or score on it.
- gap_k = not fold k, and within 2 km of a fold-k row of the same country in THIS table: such rows leave the
  training set when fold k is tested.
- A row without a point gets no fold and counts as locked.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from scipy.spatial import cKDTree

DATABASE = Path(__file__).resolve().parents[1] / "data" / "team_data_v1.duckdb"
EARTH_RADIUS_KM = 6371.0088
GAP_KM = 2.0
FOLDS = 5
SEED = 20261029
TEST_FOLDS = range(1, FOLDS)


def xyz(lat, lon) -> np.ndarray:
    """Points on the sphere in km: straight-line distances are chords."""
    lat, lon = np.radians(np.asarray(lat, float)), np.radians(np.asarray(lon, float))
    return np.c_[np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)] * EARTH_RADIUS_KM


def chord(km: float) -> float:
    return 2 * EARTH_RADIUS_KM * np.sin(km / (2 * EARTH_RADIUS_KM))


def near(points: np.ndarray, others: np.ndarray, km: float = GAP_KM) -> np.ndarray:
    """For each point: is any of `others` within `km`?"""
    if len(others) == 0 or len(points) == 0:
        return np.zeros(len(points), dtype=bool)
    distance, _ = cKDTree(others).query(points, distance_upper_bound=chord(km))
    return np.isfinite(distance)


def chains(points: np.ndarray, km: float = GAP_KM) -> np.ndarray:
    """Village groups: two points within `km` are always in the same group."""
    pairs = cKDTree(points).query_pairs(r=chord(km), output_type="ndarray")
    if len(pairs) == 0:
        return np.arange(len(points))
    graph = coo_matrix((np.ones(len(pairs)), (pairs[:, 0], pairs[:, 1])), shape=(len(points), len(points)))
    return connected_components(graph, directed=False)[1]


def frozen(con) -> pd.DataFrame:
    """Every label row with its point, frozen fold and lock area (one row per field_season_id)."""
    return con.execute("""
        select l.field_season_id, l.release, l.data_set, l.country_iso3, l.season_year, l.latitude, l.longitude,
               l.in_lock_area, f.fold, f.locked
        from labels l left join folds f using (release, field_season_id)""").df()


def assign_new(rows: pd.DataFrame) -> pd.DataFrame:
    """Folds for rows that are not kept, by the folds_v3 rules, on top of the frozen ones."""
    rows = rows.copy()
    rows["fold_rule"] = np.where(rows.fold.notna(), "frozen", None)
    has_point = rows.latitude.notna() & rows.longitude.notna()
    points = np.zeros((len(rows), 3))
    points[has_point.to_numpy()] = xyz(rows.latitude[has_point], rows.longitude[has_point])
    for _, part in rows[has_point].groupby("country_iso3"):
        old = part.fold.notna().to_numpy()
        if old.all() or not old.any():
            continue
        new_index = part.index[~old]
        distance, nearest = cKDTree(points[part.index[old]]).query(points[new_index], distance_upper_bound=chord(GAP_KM))
        hit = np.isfinite(distance)
        rows.loc[new_index[hit], "fold"] = part.fold.to_numpy()[old][nearest[hit]]
        rows.loc[new_index[hit], "fold_rule"] = "nearest_frozen_within_2km"
    open_rows = rows.fold.isna() & has_point
    if open_rows.any():
        by_year = rows.release == "passarchive_v1"          # v1 villages are per year, the other release across years
        key = rows.data_set + np.where(by_year, "_" + rows.season_year.astype(str), "")
        rows["village"] = None
        for name, part in rows[has_point].groupby(key[has_point]):
            rows.loc[part.index, "village"] = [f"{name}_{c}" for c in chains(points[part.index])]
        known = rows[rows.fold.notna() & rows.village.notna()].groupby("village").fold.agg(lambda s: s.mode().iloc[0])
        take = open_rows & rows.village.isin(known.index)
        rows.loc[take, "fold"] = rows.loc[take, "village"].map(known)
        rows.loc[take, "fold_rule"] = "village_group_fold"
        for name, part in rows[rows.fold.isna() & has_point].groupby("data_set"):
            have = rows[(rows.data_set == name) & rows.fold.notna()].fold.astype(int).value_counts()
            filled = np.array([have.get(f, 0) for f in range(FOLDS)], dtype=float)
            sizes = part.village.value_counts()
            order = np.array(sizes.index, dtype=object)
            np.random.default_rng(SEED).shuffle(order)
            fold_of = {}
            for group in sorted(order, key=lambda g: -sizes[g]):
                fold_of[group] = int(np.argmin(filled))
                filled[fold_of[group]] += sizes[group]
            rows.loc[part.index, "fold"] = part.village.map(fold_of)
            rows.loc[part.index, "fold_rule"] = "new_group_balanced"
        rows = rows.drop(columns="village")
    rows.loc[rows.fold.isna() & ~has_point, "fold_rule"] = "no_point"
    rows["fold"] = rows.fold.astype("Int64")
    new = rows.locked.isna()
    rows.loc[new, "locked"] = (rows.fold[new] == 0).fillna(True) | rows.in_lock_area[new].fillna(True).astype(bool)
    rows["locked"] = rows.locked.astype(bool)
    return rows


def ring_and_gaps(table: pd.DataFrame) -> pd.DataFrame:
    """For the rows of this table, by country: lock every row within 2 km of a fold-0 row (the ring), and set gap_k =
    not fold k and within 2 km of a fold-k row. A lock is only ever added here, never removed."""
    table = table.copy()
    table["ring"] = False
    has_point = (table.latitude.notna() & table.longitude.notna() & table.fold.notna()).to_numpy()
    for k in TEST_FOLDS:
        table[f"gap_{k}"] = False
    for _, part in table[has_point].groupby("country_iso3"):
        points, fold = xyz(part.latitude, part.longitude), part.fold.to_numpy()
        table.loc[part.index[near(points, points[fold == 0])], "ring"] = True
        for k in TEST_FOLDS:
            hit = near(points, points[fold == k]) & (fold != k)
            table.loc[part.index[hit], f"gap_{k}"] = True
    table["locked"] = table.locked | table.ring
    return table.drop(columns="ring")


def add_folds(table: pd.DataFrame, con) -> pd.DataFrame:
    """Fold, lock, fold rule and the 2 km gap for each row of a cleaned table (key: field_season_id)."""
    rows = assign_new(frozen(con))
    keep = ["field_season_id", "country_iso3", "latitude", "longitude", "fold", "locked", "fold_rule"]
    out = table.drop(columns=[c for c in keep[1:] if c in table], errors="ignore").merge(
        rows[keep], on="field_season_id", how="left", validate="many_to_one", indicator=True)
    if (out._merge != "both").any():
        raise ValueError(f"{int((out._merge != 'both').sum())} rows have an id that is not in labels")
    return ring_and_gaps(out.drop(columns="_merge"))


def train_test(table: pd.DataFrame, k: int) -> tuple[np.ndarray, np.ndarray]:
    """Training and test rows when fold k (1-4) is tested: never locked rows, never the 2 km gap."""
    usable = ~table.locked.to_numpy() & table.fold.isin(TEST_FOLDS).to_numpy()
    fold = table.fold.to_numpy()
    return usable & (fold != k) & ~table[f"gap_{k}"].to_numpy(), usable & (fold == k)


def leaks(table: pd.DataFrame) -> int:
    """Rows used for fold 1-4 (train or test) within 2 km of a fold-0 row of the same country in this table."""
    used = np.zeros(len(table), dtype=bool)
    for k in TEST_FOLDS:
        train, test = train_test(table, k)
        used |= train | test
    out = 0
    for _, part in table[table.fold.notna() & table.latitude.notna()].groupby("country_iso3"):
        points, fold = xyz(part.latitude, part.longitude), part.fold.to_numpy()
        out += int((used[table.index.get_indexer(part.index)] & near(points, points[fold == 0])).sum())
    return out


def check(database: Path = DATABASE) -> int:
    """Rebuild the lock (from fold and points only) and the gaps of the kept rows: they must equal the database.
    Then two wider tables must have no used row within 2 km of a fold-0 row."""
    con = duckdb.connect(str(database), read_only=True)
    con.execute("set memory_limit = '2GB'; set threads = 2")
    truth = con.execute("select field_season_id, fold, locked, gap_1, gap_2, gap_3, gap_4 from folds").df()
    rebuilt = frozen(con).merge(truth[["field_season_id"]], on="field_season_id")
    rebuilt = ring_and_gaps(rebuilt.assign(locked=rebuilt.fold == 0, fold=rebuilt.fold.astype("Int64")))
    both = rebuilt.merge(truth, on="field_season_id", suffixes=("", "_db"))
    bad = {c: int((both[c].astype(int) != both[f"{c}_db"].astype(int)).sum())
           for c in ["fold", "locked", *[f"gap_{k}" for k in TEST_FOLDS]]}
    print(f"kept rows: {len(both):,}; lock and gaps rebuilt from folds and points, differences: {bad}")
    labels = con.execute("select field_season_id, data_set, label_status, keep, latitude from labels").df()
    wider = {"kept + Zimbabwe 2024 zeros": labels.keep | ((labels.data_set == "zimbabwe_2024")
                                                          & (labels.label_status == "recorded_zero")),
             "every row with a point": labels.latitude.notna()}
    found = {}
    for name, mask in wider.items():
        table = add_folds(labels.loc[mask, ["field_season_id"]].reset_index(drop=True), con)
        found[name] = leaks(table)
        print(f"{name}: {len(table):,} rows, {int(table.locked.sum()):,} locked, used rows within 2 km of fold 0: "
              f"{found[name]}")
    con.close()
    return 0 if not any(bad.values()) and len(both) == len(truth) and not any(found.values()) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="compare with the frozen folds of the database")
    parser.add_argument("--database", type=Path, default=DATABASE)
    args = parser.parse_args()
    sys.exit(check(args.database) if args.check else parser.print_help() or 0)
