"""Monad-based cleaning, quarantine, and staging_6 footer encoding."""
from __future__ import annotations

import io
import json
import hashlib
from collections import defaultdict
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import rules, validate
from .config import Design, Target
from .monadsquishy import SquishyEngine
from .storage import output_path, read_parquet, write_parquet


FOOTER_KEYS = (b"monad_results.zstd", b"monad_results.codec", b"monad_results.raw_size")


def _column_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_numeric_dtype(series):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "string"


def _monad_run(frame: pd.DataFrame, target: Target) -> tuple[pd.DataFrame, pd.DataFrame]:
    config = []
    for column in frame.columns:
        column_type = _column_type(frame[column])
        operations = [rules.must_exist] if column in target.required else []
        if column_type == "string":
            operations.append(rules.normalize_whitespace)
        config.append({"target": column, "source": column, "type": column_type, "pipeline": operations})
    engine = SquishyEngine(config, frame)
    with redirect_stdout(io.StringIO()):
        cleaned = engine.run()
    return cleaned, engine.logs


def build_footer(logs: pd.DataFrame, design: Design, target: Target, run_id: str, input_rows: int, accepted_rows: int, schema_hash: str) -> dict:
    fields = ["col", "step", "role", "role_type", "status", "count", "top_failed_values", "run_date"]
    records = []
    samples = []
    if not logs.empty:
        for (column, step, role, role_type, status), group in logs.groupby(["col", "step", "role", "role_type", "status"], dropna=False, sort=False):
            failures = group.loc[group.status == "failed", "value"].dropna().astype(str)
            records.append({
                "col": str(column), "step": int(step), "role": str(role), "role_type": str(role_type),
                "status": str(status), "count": len(group),
                "top_failed_values": failures.value_counts().head(5).index.tolist(),
                "run_date": date.today().isoformat(),
            })
        for (column, step, role, role_type), group in logs.groupby(["col", "step", "role", "role_type"], dropna=False, sort=False):
            samples.append({"column": str(column), "operate": [{
                "name": str(role), "step": int(step), "role_type": str(role_type),
                "passes_count": int((group.status == "passed").sum()),
                "failed": int((group.status == "failed").sum()),
                "skipped_count": int((group.status == "skipped").sum()),
                "sample_data": {
                    "passes": group.loc[group.status == "passed", "value"].astype(str).head(50).tolist(),
                    "failed": group.loc[group.status == "failed", "value"].astype(str).head(50).tolist(),
                },
            }]})
    return {
        "footer_schema_version": "1.1", "source": design.source, "table": target.name,
        "run_id": run_id, "run_date": date.today().isoformat(),
        "design_version": design.version, "ruleset_version": "rules-v1",
        "schema_hash": schema_hash, "input_rows": input_rows,
        "accepted_rows": accepted_rows, "rejected_rows": input_rows - accepted_rows,
        "quality_score": round(accepted_rows / input_rows, 6) if input_rows else 1.0,
        "graph_values": {
            "sample_data": {"columns": samples},
            "chart_chain": {"records": records, "fields": fields},
            "chart_recommender": {"records": records, "fields": fields},
        },
    }


def encode_footer(footer: dict) -> dict[bytes, bytes]:
    raw = json.dumps(footer, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    return {
        b"monad_results.zstd": pa.Codec("zstd").compress(raw).to_pybytes(),
        b"monad_results.codec": b"zstd-json-v1",
        b"monad_results.raw_size": str(len(raw)).encode("ascii"),
    }


def decode_footer(path: Path) -> dict:
    metadata = pq.ParquetFile(path).schema_arrow.metadata or {}
    if metadata.get(b"monad_results.codec") != b"zstd-json-v1":
        raise ValueError(f"Unsupported footer codec: {path}")
    raw_size = int(metadata[b"monad_results.raw_size"])
    raw = pa.Codec("zstd").decompress(metadata[b"monad_results.zstd"], raw_size).to_pybytes()
    return json.loads(raw)


def _bad_path(root: Path, design: Design, target: Target, day: date, hhmm: str) -> Path:
    return root / "data" / "staging" / "bad_data" / target.name / f"year={day:%Y}" / f"month={day:%m}" / f"day={day:%d}" / f"s.{design.source}.{target.name}.{hhmm}.parquet"


def run_staging(design: Design, landing_paths: dict[str, list[Path]], root: Path, run_id: str) -> tuple[dict[str, list[Path]], int, dict[str, int]]:
    outputs: dict[str, list[Path]] = {}
    rejected_total = 0
    reused: dict[str, int] = defaultdict(int)
    accepted_dimension_keys: dict[str, set[object]] = defaultdict(set)
    for target in sorted(design.targets, key=lambda item: 0 if item.kind == "dimension" else 1):
        paths = []
        target_input_rows = 0
        target_accepted_rows = 0
        for landing_path in landing_paths[target.name]:
            frame = read_parquet(landing_path)
            target_input_rows += len(frame)
            cleaned, logs = _monad_run(frame, target)
            bad_mask = validate.required_mask(cleaned, target.required)
            reasons: dict[int, list[dict[str, str]]] = defaultdict(list)
            if not logs.empty:
                for _, row in logs.loc[logs.status == "failed"].iterrows():
                    reasons[int(row["row"])].append({
                        "column": str(row["col"]), "rule": str(row["role"]),
                        "message": str(row.get("details") or "Rule failed"),
                    })
            for index in cleaned.index[bad_mask]:
                reasons[int(index)].append({"column": "required", "rule": "must_exist", "message": "Required value missing"})
            if target.kind == "fact":
                for column, mapping in target.foreign_keys.items():
                    dimension = mapping["dimension"]
                    failed = validate.foreign_key_mask(cleaned, column, accepted_dimension_keys[dimension])
                    for index in cleaned.index[failed]:
                        reasons[int(index)].append({
                            "column": column, "rule": "staged_foreign_key",
                            "message": f"Key absent from accepted {dimension}",
                        })
                    if failed.any():
                        logs = pd.concat([logs, pd.DataFrame({
                            "row": cleaned.index[failed], "col": column, "step": 0,
                            "role_type": "governance", "role": "staged_foreign_key",
                            "status": "failed", "value": cleaned.loc[failed, column].astype(str).to_numpy(),
                            "details": f"Key absent from accepted {dimension}",
                        })], ignore_index=True)
            bad_indexes = sorted(reasons)
            valid = cleaned.drop(index=bad_indexes).reset_index(drop=True)
            if target.kind == "dimension":
                accepted_dimension_keys[target.name].update(valid[target.key_column].dropna().tolist())
            target_accepted_rows += len(valid)
            rejected_total += len(bad_indexes)
            day = date(int(landing_path.parts[-4].split("=")[1]), int(landing_path.parts[-3].split("=")[1]), int(landing_path.parts[-2].split("=")[1]))
            hhmm = landing_path.stem.rsplit(".", 1)[-1]
            if bad_indexes:
                bad = frame.loc[bad_indexes].copy()
                bad["_run_id"] = run_id
                bad["_failed_column"] = [reasons[index][0]["column"] for index in bad_indexes]
                bad["_failed_rule"] = [reasons[index][0]["rule"] for index in bad_indexes]
                bad["_error_code"] = "rule_failed"
                bad["_error_message"] = [json.dumps(reasons[index], ensure_ascii=False) for index in bad_indexes]
                bad["_rejected_at"] = pd.Timestamp.utcnow().isoformat()
                write_parquet(_bad_path(root, design, target, day, hhmm), bad.reset_index(drop=True))
            schema_hash = hashlib.sha256(json.dumps([(column, str(valid[column].dtype)) for column in valid.columns], separators=(",", ":")).encode()).hexdigest()
            footer = build_footer(logs, design, target, run_id, len(frame), len(valid), schema_hash)
            output = output_path(root, "staging", design.source, target.name, day, hhmm)
            created = write_parquet(output, valid, encode_footer(footer))
            if not created:
                original_run = decode_footer(output).get("run_id", "unknown")
                reused[str(original_run)] += 1
            paths.append(output)
        outputs[target.name] = paths
        threshold = float(design.options.get("min_quality_pass_rate", 0))
        if target_input_rows and target_accepted_rows / target_input_rows < threshold:
            raise ValueError(f"Quality threshold failed for {target.name}: {target_accepted_rows}/{target_input_rows} < {threshold}")
    return outputs, rejected_total, dict(reused)
