"""Publish staging values to Integration without custom footer metadata."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from .config import Design
from .storage import dataframe_hash, output_path, read_parquet, write_parquet


def run_integration(design: Design, staging_paths: dict[str, list[Path]], root: Path) -> dict[str, list[Path]]:
    outputs = {}
    for target in design.targets:
        paths = []
        for staged in staging_paths[target.name]:
            day = date(int(staged.parts[-4].split("=")[1]), int(staged.parts[-3].split("=")[1]), int(staged.parts[-2].split("=")[1]))
            hhmm = staged.stem.rsplit(".", 1)[-1]
            frame = read_parquet(staged)
            output = output_path(root, "integration", design.source, target.name, day, hhmm)
            write_parquet(output, frame)
            integrated = read_parquet(output)
            if len(frame) != len(integrated) or list(frame.columns) != list(integrated.columns) or dataframe_hash(frame) != dataframe_hash(integrated):
                raise ValueError(f"Integration reconciliation failed: {output}")
            metadata = pq.ParquetFile(output).schema_arrow.metadata or {}
            if any(key.startswith(b"monad_results") for key in metadata):
                raise ValueError(f"Custom footer leaked to Integration: {output}")
            paths.append(output)
        outputs[target.name] = paths
    return outputs
