"""Command-line entry point."""
from __future__ import annotations

import argparse
from pathlib import Path

from .runner import PROJECT_ROOT, run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run approved pipeline designs")
    parser.add_argument("configs", nargs="+", type=Path)
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--sink", choices=["sqlite", "postgresql", "clickhouse", "sqlserver"], default="sqlite")
    parser.add_argument("--mode", choices=["incremental", "full_replace"], default="incremental")
    parser.add_argument("--run-id")
    parser.add_argument("--source-uri", action="append", default=[], metavar="SOURCE=URI")
    parser.add_argument("--profile-only", action="store_true")
    args = parser.parse_args()
    overrides = {}
    for value in args.source_uri:
        if "=" not in value:
            parser.error("--source-uri must be SOURCE=URI")
        name, uri = value.split("=", 1)
        overrides[name] = uri
    path = run(args.configs, root=args.root, sink=args.sink, mode=args.mode,
               run_id=args.run_id, source_uris=overrides, profile_only=args.profile_only)
    print(path)


if __name__ == "__main__":
    main()
