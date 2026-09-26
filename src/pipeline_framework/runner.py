"""Orchestrate approved designs through six stages."""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from .config import Design, load_design
from .ingress import run_ingress
from .integration import run_integration
from .landing import run_landing
from .load import load_integration
from .manifest import now, save_manifest
from .profiling import profile
from .staging import run_staging


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _paths(value: dict[str, list[Path]] | dict[str, Path]) -> dict[str, list[str] | str]:
    return {name: [str(path) for path in paths] if isinstance(paths, list) else str(paths) for name, paths in value.items()}


def _stage_metrics(value: dict[str, list[Path]] | dict[str, Path]) -> dict[str, object]:
    paths = [path for item in value.values() for path in (item if isinstance(item, list) else [item])]
    digest = hashlib.sha256()
    rows = 0
    bytes_written = 0
    schemas = set()
    for path in sorted(paths):
        parquet = pq.ParquetFile(path)
        rows += parquet.metadata.num_rows
        bytes_written += path.stat().st_size
        schemas.add(f"{path.name}:{parquet.schema_arrow.remove_metadata()}")
        digest.update(str(path).encode())
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return {"files": len(paths), "rows": rows, "bytes": bytes_written,
            "output_checksum": digest.hexdigest(), "schema_hash": hashlib.sha256("\n".join(sorted(schemas)).encode()).hexdigest()}


def _prior_ingress_schema(root: Path, source: str, design_version: str, run_id: str) -> tuple[str, str] | None:
    candidates = []
    for path in (root / "manifests").glob("*.json"):
        if path.stem == run_id:
            continue
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("status") != "success" or manifest.get("design_versions", {}).get(source) != design_version:
            continue
        for stage in manifest.get("stages", []):
            if stage.get("source") == source and stage.get("zone") == "ingress" and stage.get("schema_hash"):
                candidates.append((manifest.get("started_at", ""), path.stem, stage["schema_hash"]))
    if not candidates:
        return None
    _, previous_run, schema_hash = max(candidates)
    return previous_run, schema_hash


def run(
    config_paths: list[str | Path], *, root: Path = PROJECT_ROOT,
    sink: str = "sqlite", mode: str = "incremental",
    run_id: str | None = None, source_uris: dict[str, str] | None = None,
    profile_only: bool = False, sink_settings: dict | None = None,
) -> Path:
    root = root.resolve()
    run_id = run_id or uuid.uuid4().hex
    manifest_path = root / "manifests" / f"{run_id}.json"
    designs: list[Design] = [load_design(path) for path in config_paths]
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("status") == "success":
            return manifest_path
        ready = {stage["source"]: stage for stage in existing.get("stages", []) if stage.get("zone") == "integration"}
        if set(ready) != {design.source for design in designs}:
            raise ValueError(f"Run ID cannot resume before all Integration stages finish: {run_id}")
        integrations = [
            {name: [Path(value) for value in paths] for name, paths in ready[design.source]["output_paths"].items()}
            for design in designs
        ]
        try:
            counts = load_integration(designs, integrations, root, existing["sink"], existing["mode"], sink_settings)
            existing["stages"] = [stage for stage in existing["stages"] if stage.get("zone") != "load"]
            existing["stages"].append({"zone": "load", "sink": existing["sink"], "rows_by_table": counts})
            existing["status"] = "success"
            existing.pop("error", None)
        except Exception as exc:
            existing["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            existing["completed_at"] = now()
            save_manifest(root, run_id, existing)
        return manifest_path
    if len({design.source for design in designs}) != len(designs):
        raise ValueError("Each source must appear once per run")
    manifest = {
        "run_id": run_id, "started_at": now(), "status": "running",
        "mode": mode, "sink": sink, "sources": [], "stages": [],
        "design_versions": {design.source: design.version for design in designs},
        "pipeline_version": "0.1.0", "ruleset_version": "rules-v1",
        "input_uris": {design.source: (source_uris or {}).get(design.source, design.source_uri) for design in designs},
        "parent_run_id": None, "watermark": None,
    }
    save_manifest(root, run_id, manifest)
    integrations = []
    try:
        for design in designs:
            source_uri = (source_uris or {}).get(design.source)
            ingressed, fingerprint = run_ingress(design, root, run_id, source_uri=source_uri)
            ingress_metrics = _stage_metrics(ingressed)
            manifest["stages"].append({"source": design.source, "zone": "ingress", "output_paths": _paths(ingressed), "source_fingerprint": fingerprint, **ingress_metrics})
            eda_path, draft_path = profile(design, ingressed, root, run_id)
            prior = _prior_ingress_schema(root, design.source, design.version, run_id)
            if prior:
                previous_run, previous_hash = prior
                report = json.loads(eda_path.read_text(encoding="utf-8"))
                report["schema_drift"] = {"baseline_run_id": previous_run,
                                          "baseline_hash": previous_hash,
                                          "current_hash": ingress_metrics["schema_hash"],
                                          "detected": previous_hash != ingress_metrics["schema_hash"]}
                eda_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            manifest["stages"].append({"source": design.source, "zone": "eda", "report": str(eda_path), "draft_dbml": str(draft_path)})
            manifest["sources"].append(design.source)
            save_manifest(root, run_id, manifest)
            if profile_only:
                continue
            if prior and previous_hash != ingress_metrics["schema_hash"]:
                raise ValueError(f"Schema drift in {design.source}; see {eda_path}")
            snapshot_day = date.today()
            landed = run_landing(design, ingressed, root, snapshot_day)
            manifest["stages"].append({"source": design.source, "zone": "landing", "output_paths": _paths(landed), **_stage_metrics(landed)})
            staged, rejected, reused = run_staging(design, landed, root, run_id)
            manifest["stages"].append({"source": design.source, "zone": "staging", "output_paths": _paths(staged), "rejected_rows": rejected, "reused_artifacts_by_run": reused, **_stage_metrics(staged)})
            integrated = run_integration(design, staged, root)
            manifest["stages"].append({"source": design.source, "zone": "integration", "output_paths": _paths(integrated), **_stage_metrics(integrated)})
            integrations.append(integrated)
            save_manifest(root, run_id, manifest)
        if not profile_only:
            counts = load_integration(designs, integrations, root, sink, mode, sink_settings)
            manifest["stages"].append({"zone": "load", "sink": sink, "rows_by_table": counts})
        manifest["status"] = "success"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        manifest["completed_at"] = now()
        save_manifest(root, run_id, manifest)
    return manifest_path
