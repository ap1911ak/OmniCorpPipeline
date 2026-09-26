"""EDA report and DBML draft from ingested Parquet."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from .config import Design
from .storage import read_parquet


def _duckdb_type(value: str) -> str:
    name = value.upper()
    if "INT" in name:
        return "bigint"
    if any(item in name for item in ("DOUBLE", "FLOAT", "DECIMAL")):
        return "decimal(18,4)"
    if "DATE" in name or "TIMESTAMP" in name:
        return "datetime"
    return "varchar"


def profile(design: Design, ingress_paths: dict[str, Path], root: Path, run_id: str) -> tuple[Path, Path]:
    report = {"source": design.source, "run_id": run_id, "tables": {}, "candidate_relationships": []}
    for alias, path in ingress_paths.items():
        frame = read_parquet(path)
        columns = {}
        for name in frame.columns:
            series = frame[name]
            sensitive = any(token in name for token in ("email", "phone", "address", "birth_date", "tax_id", "citizen_id"))
            columns[name] = {
                "type": str(series.dtype),
                "nulls": int(series.isna().sum()),
                "null_rate": round(float(series.isna().mean()), 6) if len(frame) else 0,
                "distinct": int(series.nunique(dropna=True)),
                "sample": [] if sensitive else [str(value) for value in series.dropna().head(3)],
                "sensitive_candidate": sensitive,
            }
        report["tables"][alias] = {
            "rows": len(frame), "columns": columns,
            "duplicate_rows": int(frame.duplicated().sum()),
            "candidate_primary_keys": [name for name, item in columns.items() if item["distinct"] == len(frame) and item["nulls"] == 0],
        }
    aliases = list(report["tables"])
    for left in aliases:
        left_cols = report["tables"][left]["columns"]
        for right in aliases:
            if left == right:
                continue
            right_keys = report["tables"][right]["candidate_primary_keys"]
            for column in right_keys:
                if column in left_cols:
                    report["candidate_relationships"].append({"from": f"{left}.{column}", "to": f"{right}.{column}"})

    directory = root / "reports" / run_id
    directory.mkdir(parents=True, exist_ok=True)
    report_path = directory / f"{design.source}-eda.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    dbml_lines = [f"// Draft from {design.source}; approved design: {design.dbml.name}"]
    with duckdb.connect() as connection:
        for alias, path in ingress_paths.items():
            quoted = str(path).replace("'", "''")
            connection.execute(f'CREATE VIEW "{alias}" AS SELECT * FROM read_parquet(\'{quoted}\')')
        for target in design.targets:
            cursor = connection.execute(f"SELECT * FROM ({target.query}) AS mapped LIMIT 0")
            dbml_lines.append(f"Table {target.name} {{")
            for name, type_name, *_ in cursor.description:
                dbml_lines.append(f"  {name} {_duckdb_type(str(type_name))}")
            if target.key_column not in [row[0] for row in cursor.description]:
                dbml_lines.append(f"  {target.key_column} bigint [pk]")
            dbml_lines.append("}")
    for target in design.targets:
        for foreign_key, mapping in target.foreign_keys.items():
            key = next(item.key_column for item in design.targets if item.name == mapping["dimension"])
            dbml_lines.append(f"Ref: {target.name}.{foreign_key} > {mapping['dimension']}.{key}")
    dbml_path = directory / f"{design.source}-draft.dbml"
    dbml_path.write_text("\n".join(dbml_lines) + "\n", encoding="utf-8")
    return report_path, dbml_path
