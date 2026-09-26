from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline_framework.config import load_design
from pipeline_framework.runner import run
from pipeline_framework.staging import decode_footer
from pipeline_framework.storage import stable_key


CONFIG = Path(__file__).resolve().parents[1] / "configs" / "chinook.yaml"


def _fixture_source(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE Customer(CustomerId INTEGER, FirstName TEXT, LastName TEXT, City TEXT, Country TEXT, SupportRepId INTEGER);
            CREATE TABLE Employee(EmployeeId INTEGER, FirstName TEXT, LastName TEXT, Title TEXT);
            CREATE TABLE Track(TrackId INTEGER, Name TEXT, GenreId INTEGER);
            CREATE TABLE Genre(GenreId INTEGER, Name TEXT);
            CREATE TABLE Invoice(InvoiceId INTEGER, CustomerId INTEGER, InvoiceDate TEXT);
            CREATE TABLE InvoiceLine(InvoiceLineId INTEGER, InvoiceId INTEGER, TrackId INTEGER, UnitPrice REAL, Quantity INTEGER);
            INSERT INTO Customer VALUES (1,'Ada','Lovelace','London','UK',2);
            INSERT INTO Employee VALUES (2,'Grace','Hopper','Engineer');
            INSERT INTO Genre VALUES (3,'Rock');
            INSERT INTO Track VALUES (4,'Sample Track',3);
            INSERT INTO Invoice VALUES (5,1,'2026-03-15 12:00:00');
            INSERT INTO InvoiceLine VALUES (6,5,4,9.5,2);
        """)


def test_full_pipeline_and_idempotent_incremental(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    _fixture_source(database)
    manifest_path = run([CONFIG], root=tmp_path, sink="sqlite", mode="full_replace",
                        run_id="test-full", source_uris={"chinook": str(database)})
    manifest = json.loads(manifest_path.read_text())
    assert manifest["status"] == "success"
    assert manifest["stages"][-1]["rows_by_table"]["fact_sales_line"] == 1

    staged = next((tmp_path / "data/staging/fact_sales_line").rglob("*.parquet"))
    integrated = next((tmp_path / "data/integration/fact_sales_line").rglob("*.parquet"))
    landing = next((tmp_path / "data/landing/fact_sales_line").rglob("*.parquet"))
    assert staged.name == "s.chinook.fact_sales_line.0000.parquet"
    assert "year=2026/month=03/day=15" in str(staged)
    assert int(pq.read_table(landing, partitioning=None).column("customer_key")[0].as_py()) == stable_key("chinook", "dim_customers", "1")
    footer = decode_footer(staged)
    assert footer["input_rows"] == 1
    assert footer["accepted_rows"] == 1
    assert footer["graph_values"]["chart_chain"]["records"]
    assert not any(key.startswith(b"monad_results") for key in (pq.ParquetFile(integrated).schema_arrow.metadata or {}))

    with sqlite3.connect(tmp_path / "warehouse/star.sqlite") as db:
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        assert db.execute("SELECT COUNT(*) FROM fact_sales_line").fetchone()[0] == 1

    assert run([CONFIG], root=tmp_path, run_id="test-full", source_uris={"chinook": str(database)}) == manifest_path
    second = run([CONFIG], root=tmp_path, sink="sqlite", mode="incremental",
                 run_id="test-incremental", source_uris={"chinook": str(database)})
    assert json.loads(second.read_text())["status"] == "success"
    with sqlite3.connect(tmp_path / "warehouse/star.sqlite") as db:
        assert db.execute("SELECT COUNT(*) FROM fact_sales_line").fetchone()[0] == 1


def test_unapproved_design_is_rejected(tmp_path: Path) -> None:
    config = tmp_path / "draft.yaml"
    config.write_text("approval:\n  status: draft\nsource:\n  name: sample\n  uri: file.csv\n  format: csv\ntargets: []\n")
    with pytest.raises(ValueError, match="not approved"):
        load_design(config)


def test_fact_is_quarantined_when_its_dimension_fails_staging(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    _fixture_source(database)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE Customer SET FirstName = '', LastName = '' WHERE CustomerId = 1")

    manifest_path = run([CONFIG], root=tmp_path, sink="sqlite", mode="full_replace",
                        run_id="bad-customer", source_uris={"chinook": str(database)})
    manifest = json.loads(manifest_path.read_text())
    assert manifest["status"] == "success"
    rejected = sorted((tmp_path / "data/staging/bad_data").rglob("*.parquet"))
    assert {"dim_customers", "fact_sales_line"} <= {path.parent.parent.parent.parent.name for path in rejected}
    fact_bad = next(path for path in rejected if "fact_sales_line" in path.parts)
    bad = pq.read_table(fact_bad, partitioning=None).to_pandas()
    assert bad["_failed_rule"].iloc[0] == "staged_foreign_key"
    with sqlite3.connect(tmp_path / "warehouse/star.sqlite") as db:
        assert db.execute("SELECT COUNT(*) FROM fact_sales_line").fetchone()[0] == 0
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_schema_drift_stops_before_landing(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    _fixture_source(database)
    run([CONFIG], root=tmp_path, sink="sqlite", mode="full_replace",
        run_id="baseline", source_uris={"chinook": str(database)})
    with sqlite3.connect(database) as db:
        db.execute("ALTER TABLE Employee ADD COLUMN extra_field TEXT")

    with pytest.raises(ValueError, match="Schema drift"):
        run([CONFIG], root=tmp_path, sink="sqlite", mode="incremental",
            run_id="drifted", source_uris={"chinook": str(database)})
    report = json.loads((tmp_path / "reports/drifted/chinook-eda.json").read_text())
    assert report["schema_drift"]["detected"] is True
    manifest = json.loads((tmp_path / "manifests/drifted.json").read_text())
    assert manifest["status"] == "failed"
    assert not any(stage["zone"] == "landing" for stage in manifest["stages"])
