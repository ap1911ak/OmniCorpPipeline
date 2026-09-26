"""Read external formats and materialize canonical raw Parquet."""
from __future__ import annotations

import hashlib
import io
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Design, IDENTIFIER
from .storage import clean_frame


def _session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def _source_path(uri: str, project_root: Path) -> Path:
    path = Path(uri).expanduser()
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _bytes_for_uri(uri: str, project_root: Path) -> tuple[bytes, str]:
    if uri.startswith(("https://", "http://")):
        response = _session().get(uri, timeout=60)
        response.raise_for_status()
        return response.content, uri
    if "://" in uri:
        try:
            import fsspec
            with fsspec.open(uri, "rb") as stream:
                return stream.read(), uri
        except ImportError as exc:
            raise RuntimeError("Cloud URI needs fsspec and provider extra") from exc
    path = _source_path(uri, project_root)
    return path.read_bytes(), str(path)


def _read_json(raw: bytes, item: dict[str, Any]) -> pd.DataFrame:
    if item.get("lines"):
        return pd.read_json(io.BytesIO(raw), lines=True)
    obj = json.loads(raw)
    record_path = item.get("record_path")
    if record_path:
        for part in record_path.split("."):
            obj = obj[part]
    return pd.json_normalize(obj)


def _read_html(raw: bytes, item: dict[str, Any]) -> pd.DataFrame:
    soup = BeautifulSoup(raw, "html.parser")
    tables = soup.select(item.get("selector", "table"))
    index = int(item.get("table_index", 0))
    if index >= len(tables):
        raise ValueError(f"HTML table index {index} does not exist")
    rows = []
    for tr in tables[index].select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"], recursive=False)]
        if cells:
            rows.append(cells)
    if not rows:
        raise ValueError("HTML table contains no rows")
    return pd.DataFrame(rows[1:], columns=rows[0])


def _read_excel(raw: bytes, item: dict[str, Any]) -> dict[str, pd.DataFrame]:
    try:
        workbook = pd.ExcelFile(io.BytesIO(raw))
    except ImportError as exc:
        raise RuntimeError("Excel ingestion needs optional excel dependencies") from exc
    sheets = item.get("sheets") or workbook.sheet_names
    result = {}
    for sheet in sheets:
        frame = pd.read_excel(workbook, sheet_name=sheet, header=item.get("header", 0))
        if item.get("pivot"):
            spec = item["pivot"]
            frame = frame.melt(
                id_vars=spec["id_vars"], value_vars=spec.get("value_vars"),
                var_name=spec.get("var_name", "attribute"),
                value_name=spec.get("value_name", "value"),
            )
        suffix = re.sub(r"[^a-z0-9]+", "_", str(sheet).lower()).strip("_")
        alias = item["alias"] if len(sheets) == 1 else f"{item['alias']}_{suffix}"
        result[alias] = frame
    return result


def _read_api(uri: str, item: dict[str, Any]) -> pd.DataFrame:
    session = _session()
    url = urljoin(uri.rstrip("/") + "/", item.get("endpoint", ""))
    records = []
    max_pages = int(item.get("max_pages", 100))
    for _ in range(max_pages):
        response = session.get(url, params=item.get("params"), timeout=60)
        response.raise_for_status()
        payload = response.json()
        selected = payload
        for part in item.get("record_path", "").split("."):
            if part:
                selected = selected[part]
        records.extend(selected if isinstance(selected, list) else [selected])
        next_path = item.get("next_path")
        if not next_path:
            break
        next_value = payload
        for part in next_path.split("."):
            next_value = next_value.get(part) if isinstance(next_value, dict) else None
        if not next_value:
            break
        url = urljoin(url, str(next_value))
    else:
        raise RuntimeError("API max_pages reached before pagination ended")
    return pd.json_normalize(records)


def _frame_map(design: Design, project_root: Path) -> tuple[dict[str, pd.DataFrame], str]:
    fmt = design.source_format.lower()
    uri = design.source_uri
    if fmt == "sqlite":
        path = _source_path(uri, project_root)
        frames = {}
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
            for item in design.tables:
                alias = item["alias"]
                table = item["table"].replace('"', '""')
                frames[alias] = pd.read_sql_query(f'SELECT * FROM "{table}"', connection)
        return frames, hashlib.sha256(path.read_bytes()).hexdigest()

    if fmt == "api":
        frames = {item["alias"]: _read_api(uri, item) for item in design.tables}
        digest = hashlib.sha256()
        for alias, frame in sorted(frames.items()):
            digest.update(alias.encode())
            digest.update(frame.to_json(orient="records", date_format="iso").encode())
        return frames, digest.hexdigest()

    raw, _ = _bytes_for_uri(uri, project_root)
    fingerprint = hashlib.sha256(raw).hexdigest()
    frames = {}
    items = design.tables or ({"alias": design.source, "table": design.source},)
    for item in items:
        alias = item["alias"]
        if fmt == "csv":
            frames[alias] = pd.read_csv(io.BytesIO(raw), encoding=item.get("encoding", "utf-8"))
        elif fmt in ("excel", "xlsx", "xls"):
            frames.update(_read_excel(raw, item))
        elif fmt in ("json", "jsonl"):
            frames[alias] = _read_json(raw, {**item, "lines": fmt == "jsonl" or item.get("lines", False)})
        elif fmt == "parquet":
            frames[alias] = pq.read_table(io.BytesIO(raw)).to_pandas()
        elif fmt in ("html", "web"):
            frames[alias] = _read_html(raw, item)
        else:
            raise ValueError(f"Unsupported ingress format: {fmt}")
    return frames, fingerprint


def run_ingress(design: Design, root: Path, run_id: str, *, source_uri: str | None = None) -> tuple[dict[str, Path], str]:
    if source_uri:
        from dataclasses import replace
        design = replace(design, source_uri=source_uri)
    frames, fingerprint = _frame_map(design, root)
    outputs = {}
    timestamp = datetime.now(timezone.utc).isoformat()
    for alias, frame in frames.items():
        if not IDENTIFIER.fullmatch(alias):
            raise ValueError(f"Invalid ingress table alias: {alias}")
        frame = clean_frame(frame)
        item = next((candidate for candidate in design.tables if candidate["alias"] == alias), None)
        if item and item.get("expected_columns"):
            expected = set(item["expected_columns"])
            actual = set(frame.columns)
            if expected != actual:
                raise ValueError(f"Schema drift in {alias}: missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}")
        frame["_ingested_at"] = timestamp
        frame["_source_uri"] = design.source_uri
        frame["_source_file"] = Path(design.source_uri).name
        frame["_source_sheet"] = alias if design.source_format in ("excel", "xlsx", "xls") else None
        frame["_run_id"] = run_id
        path = root / "data" / "ingress" / design.source / run_id / f"{alias}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path, compression="snappy")
        outputs[alias] = path
    return outputs, fingerprint
