"""Build dimensions first, then facts with verified foreign keys."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from .config import Design, Target
from .storage import output_path, partition_day, stable_key, write_parquet
from .validate import duplicate_mask, foreign_key_mask, required_mask


def _date_dimension(frame: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(frame["date_key"].astype(str), format="%Y%m%d")
    calendar = pd.date_range(dates.min(), dates.max(), freq="D")
    result = pd.DataFrame({"full_date": calendar})
    result["date_key"] = result.full_date.dt.strftime("%Y%m%d").astype("int64")
    result["day_of_month"] = result.full_date.dt.day
    result["day_of_week"] = result.full_date.dt.day_name()
    result["month"] = result.full_date.dt.month
    result["month_name"] = result.full_date.dt.month_name()
    result["quarter"] = result.full_date.dt.quarter
    result["year"] = result.full_date.dt.year
    result["full_date"] = result.full_date.dt.date
    return result[["date_key", "full_date", "day_of_month", "day_of_week", "month", "month_name", "quarter", "year"]]


def _unique_keys(frame: pd.DataFrame, target: Target) -> None:
    missing = required_mask(frame, target.natural_key)
    if missing.any():
        raise ValueError(f"{target.name}: missing natural key in {int(missing.sum())} rows")
    duplicates = duplicate_mask(frame, target.natural_key)
    if duplicates.any():
        raise ValueError(f"{target.name}: duplicate natural key in {int(duplicates.sum())} rows")


def _write_target(root: Path, design: Design, target: Target, frame: pd.DataFrame, snapshot_day: date) -> list[Path]:
    if frame.empty:
        raise ValueError(f"{target.name}: mapped dataset is empty")
    day_values = frame[target.date_column].map(partition_day) if target.date_column else pd.Series(snapshot_day, index=frame.index)
    if target.time_column:
        time_values = pd.to_datetime(frame[target.time_column], errors="raise").dt.strftime("%H%M")
    else:
        time_values = pd.Series("0000", index=frame.index)
    written = []
    for (day, hhmm), indexes in frame.groupby([day_values, time_values], sort=True).groups.items():
        part = frame.loc[indexes].copy()
        path = output_path(root, "landing", design.source, target.name, day, hhmm)
        write_parquet(path, part)
        written.append(path)
    return written


def _quarantine_orphans(root: Path, design: Design, target: Target, frame: pd.DataFrame, reasons: dict[int, list[str]], snapshot_day: date) -> None:
    if not reasons:
        return
    bad = frame.loc[sorted(reasons)].copy()
    bad["_failed_rule"] = "foreign_key"
    bad["_error_code"] = "orphan_foreign_key"
    bad["_error_message"] = [json.dumps(reasons[index], ensure_ascii=False) for index in bad.index]
    path = root / "data" / "landing" / "bad_data" / target.name / f"year={snapshot_day:%Y}" / f"month={snapshot_day:%m}" / f"day={snapshot_day:%d}" / f"l.{design.source}.{target.name}.0000.parquet"
    write_parquet(path, bad.reset_index(drop=True))


def run_landing(design: Design, ingress_paths: dict[str, Path], root: Path, snapshot_day: date) -> dict[str, list[Path]]:
    outputs: dict[str, list[Path]] = {}
    dimensions: dict[str, pd.DataFrame] = {}
    ordered = sorted(design.targets, key=lambda item: 0 if item.kind == "dimension" else 1)
    with duckdb.connect() as connection:
        for alias, path in ingress_paths.items():
            quoted = str(path).replace("'", "''")
            connection.execute(f'CREATE VIEW "{alias}" AS SELECT * FROM read_parquet(\'{quoted}\')')
        for target in ordered:
            frame = connection.execute(target.query).df()
            frame.columns = [str(column).lower() for column in frame.columns]
            if target.name == "dim_date":
                frame = _date_dimension(frame)
            _unique_keys(frame, target)
            if target.key_column not in frame:
                frame.insert(0, target.key_column, [
                    stable_key(design.source, target.name, *(row[column] for column in target.natural_key))
                    for _, row in frame.iterrows()
                ])
            if target.kind == "fact":
                reasons: dict[int, list[str]] = defaultdict(list)
                for foreign_key, mapping in target.foreign_keys.items():
                    dimension = dimensions[mapping["dimension"]]
                    fact_columns = mapping["fact_columns"]
                    dimension_columns = mapping["dimension_columns"]
                    lookup = {
                        tuple(str(row[column]) for column in dimension_columns): row[next(item.key_column for item in ordered if item.name == mapping["dimension"])]
                        for _, row in dimension.iterrows()
                    }
                    keys = [lookup.get(tuple(str(row[column]) for column in fact_columns)) for _, row in frame.iterrows()]
                    frame[foreign_key] = pd.Series(keys, index=frame.index, dtype="Int64")
                    for index in frame.index[foreign_key_mask(frame, foreign_key, dimension[next(item.key_column for item in ordered if item.name == mapping["dimension"])])]:
                        reasons[int(index)].append(f"{foreign_key}: dimension key not found")
                _quarantine_orphans(root, design, target, frame, reasons, snapshot_day)
                if reasons:
                    frame = frame.drop(index=list(reasons)).reset_index(drop=True)
            else:
                dimensions[target.name] = frame
            outputs[target.name] = _write_target(root, design, target, frame, snapshot_day)
    return outputs
