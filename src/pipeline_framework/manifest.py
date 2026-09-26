"""Immutable run manifest with source-to-sink lineage."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_manifest(root: Path, run_id: str, payload: dict) -> Path:
    path = root / "manifests" / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)
    return path
