"""Canonical paths, stable keys, checksums, and guarded Parquet writes."""
from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def stable_key(source: str, entity: str, *natural_values: object) -> int:
    if any(pd.isna(value) or str(value).strip() == "" for value in natural_values):
        raise ValueError(f"Missing natural key for {source}.{entity}")
    payload = "\x1f".join([source, entity, *(str(value) for value in natural_values)]).encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def partition_day(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    return pd.to_datetime(value, errors="raise").date()


def output_path(root: Path, zone: str, source: str, table: str, day: date, hhmm: str = "0000") -> Path:
    code = {"landing": "l", "staging": "s", "integration": "i"}[zone]
    if not re.fullmatch(r"(?:[01]\d|2[0-3])[0-5]\d", hhmm):
        raise ValueError(f"Invalid HHMM: {hhmm}")
    return root / "data" / zone / table / f"year={day:%Y}" / f"month={day:%m}" / f"day={day:%d}" / f"{code}.{source}.{table}.{hhmm}.parquet"


def clean_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [re.sub(r"[^a-z0-9]+", "_", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(column)).lower()).strip("_") for column in frame.columns]
    if frame.columns.duplicated().any():
        raise ValueError("Column names collide after snake_case normalization")
    return frame


def same_values(first: pd.DataFrame, second: pd.DataFrame) -> bool:
    return first.reset_index(drop=True).equals(second.reset_index(drop=True))


def write_parquet(path: Path, frame: pd.DataFrame, metadata: dict[bytes, bytes] | None = None, *, allow_identical: bool = True) -> bool:
    """Write once; identical reruns skip, changed content at same path fails."""
    frame = frame.reset_index(drop=True)
    if path.exists():
        old = pq.read_table(path, partitioning=None).to_pandas()
        if allow_identical and same_values(old, frame):
            return False
        raise FileExistsError(f"Path collision with different data: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    if metadata:
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), **metadata})
    temporary = path.with_suffix(".tmp")
    pq.write_table(table, temporary, compression="snappy")
    temporary.replace(path)
    return True


def read_parquet(path: Path) -> pd.DataFrame:
    return pq.ParquetFile(path).read().to_pandas()


def dataframe_hash(frame: pd.DataFrame) -> str:
    values = pd.util.hash_pandas_object(frame.reset_index(drop=True), index=False).values.tobytes()
    return hashlib.sha256("|".join(frame.columns).encode() + values).hexdigest()
