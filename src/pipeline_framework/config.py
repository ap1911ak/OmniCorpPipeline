"""Validated source-to-star design configuration."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Target:
    name: str
    kind: str
    query: str
    natural_key: tuple[str, ...]
    key_column: str
    required: tuple[str, ...]
    date_column: str | None
    time_column: str | None
    foreign_keys: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class Design:
    source: str
    source_uri: str
    source_format: str
    tables: tuple[dict[str, str], ...]
    targets: tuple[Target, ...]
    version: str
    dbml: Path
    path: Path
    options: dict[str, Any]


def load_design(path: str | Path, *, require_approved: bool = True) -> Design:
    path = Path(path).resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Design must be a YAML mapping")
    approval = raw.get("approval", {})
    if require_approved and approval.get("status") != "approved":
        raise ValueError(f"Design is not approved: {path}")
    dbml = (path.parent / approval.get("dbml", "")).resolve()
    if require_approved and (not dbml.is_file() or not dbml.read_text(encoding="utf-8").strip()):
        raise ValueError(f"Approved DBML is missing: {dbml}")
    source = raw["source"]["name"]
    if not IDENTIFIER.fullmatch(source):
        raise ValueError(f"Invalid source name: {source}")
    targets = []
    seen = set()
    for item in raw["targets"]:
        name = item["name"]
        if not IDENTIFIER.fullmatch(name) or name in seen:
            raise ValueError(f"Invalid or duplicate target: {name}")
        seen.add(name)
        kind = item["kind"]
        if kind not in ("dimension", "fact"):
            raise ValueError(f"Invalid target kind: {kind}")
        targets.append(Target(
            name=name, kind=kind, query=item["query"],
            natural_key=tuple(item["natural_key"]),
            key_column=item["key_column"],
            required=tuple(item.get("required", [])),
            date_column=item.get("date_column"),
            time_column=item.get("time_column"),
            foreign_keys=item.get("foreign_keys", {}),
        ))
    dimensions = {target.name for target in targets if target.kind == "dimension"}
    for target in targets:
        for mapping in target.foreign_keys.values():
            if mapping["dimension"] not in dimensions:
                raise ValueError(f"Unknown dimension in {target.name}: {mapping['dimension']}")
    source_config = raw["source"]
    return Design(
        source=source, source_uri=source_config["uri"],
        source_format=source_config["format"],
        tables=tuple(raw.get("tables", [])), targets=tuple(targets),
        version=str(approval.get("version", "draft")),
        dbml=dbml, path=path, options=raw.get("options", {}),
    )
