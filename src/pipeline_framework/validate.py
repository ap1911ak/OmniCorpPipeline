"""Reusable frame-level validation without external database side effects."""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def required_mask(frame: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    invalid = pd.Series(False, index=frame.index)
    for column in columns:
        if column not in frame:
            raise KeyError(f"Required column missing: {column}")
        values = frame[column]
        invalid |= values.isna() | values.astype("string").str.strip().eq("").fillna(False)
    return invalid


def foreign_key_mask(frame: pd.DataFrame, column: str, allowed_keys: Iterable[object]) -> pd.Series:
    if column not in frame:
        raise KeyError(f"Foreign key column missing: {column}")
    return ~frame[column].isin(set(allowed_keys))


def duplicate_mask(frame: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    return frame.duplicated(list(columns), keep=False)
