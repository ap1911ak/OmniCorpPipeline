# Pipeline Framework

Python batch framework implementing `Ingress > EDA/Design > Landing > Staging > Integration > Load`.

## Quick start

From this project directory:

```bash
python -m venv .venv
.venv/bin/pip install -e '.[test,excel,postgresql,clickhouse]'
.venv/bin/pipeline-framework configs/chinook.yaml configs/northwind.yaml --sink sqlite --mode full_replace
```

Example configs read bundled local SQLite databases in `data/raw/`. Override a source:

```bash
.venv/bin/pipeline-framework configs/chinook.yaml \
  --source-uri chinook=/absolute/path/chinook.db \
  --sink sqlite --mode incremental
```

Profile without publishing data:

```bash
.venv/bin/pipeline-framework configs/chinook.yaml --profile-only
```

Outputs:

- `data/ingress/<source>/<run_id>/*.parquet`: canonical source tables
- `reports/<run_id>/*-eda.json` and `*-draft.dbml`: EDA and design draft
- `data/landing/<table>/year=YYYY/month=MM/day=DD/l.<source>.<table>.0000.parquet`
- `data/staging/<table>/year=YYYY/month=MM/day=DD/s.<source>.<table>.0000.parquet`
- `data/staging/bad_data/` and `data/landing/bad_data/`: rejected records
- `data/integration/<table>/year=YYYY/month=MM/day=DD/i.<source>.<table>.0000.parquet`
- `warehouse/star.sqlite`: default warehouse
- `manifests/<run_id>.json`: run lineage and counts

Fact partition uses event date. Dimension partition uses ingestion date. Date and product dimensions provide two or more hierarchy levels.

## Design approval

The YAML design must contain `approval.status: approved`, a version, and a nonempty DBML file. The runner refuses unapproved or missing design artifacts. Review changes in Git before changing status to `approved`.

`configs/chinook.yaml`, `configs/northwind.yaml`, and `schemas/omnicorp.dbml` are the approved reference design. The core framework has no Chinook/Northwind-specific mappings.

## Staging footer

Every valid Staging Parquet file contains:

```text
monad_results.codec = zstd-json-v1
monad_results.raw_size = uncompressed JSON byte count
monad_results.zstd = Zstandard-compressed minified JSON
```

The footer includes `graph_values.sample_data`, `chart_chain`, `chart_recommender`, and run counts. `sample_data` is capped at 50 values per operation/status. Use `pipeline_framework.staging.decode_footer(path)` to read it.

Integration preserves accepted values and column order. It removes `monad_results.*` metadata and verifies row count, schema, and value hash.

## Ingress adapters

The `source.format` field supports SQLite, CSV, Excel, JSON, JSON Lines, Parquet, REST API, and HTML tables. Local paths, HTTP(S) URLs, and cloud storage URIs are accepted. Cloud storage uses `fsspec` with provider extras (`s3fs`, `gcsfs`, `adlfs`). Excel needs `openpyxl` or `xlrd`. Excel configuration supports multiple sheets and `pivot`/cross-tab melt.

API sources accept `endpoint`, `record_path`, `next_path`, and `max_pages` in each table entry. HTTP requests use retries and timeouts. Web scraping selects an HTML table by CSS selector and index.

## Database sinks

| Sink | Setting | Driver |
|---|---|---|
| SQLite | `--sink sqlite` | built in |
| PostgreSQL | `PIPELINE_POSTGRESQL_URL` | psycopg2 |
| ClickHouse | `PIPELINE_CH_HOST`, `PIPELINE_CH_PORT`, `PIPELINE_CH_DATABASE`, `PIPELINE_CH_USER`, `PIPELINE_CH_PASSWORD` | clickhouse-connect |
| SQL Server | `PIPELINE_SQLSERVER_URL` | pyodbc |

`full_replace` replaces target tables. `incremental` updates dimensions and replaces fact rows by deterministic key. SQLite/PostgreSQL/SQL Server run in a database transaction with PK/FK constraints. ClickHouse uses synchronous DELETE mutations for incremental replacement; it does not provide cross-table transactions or FK enforcement.

## Idempotency and collisions

Run manifests are immutable by `run_id`. Reusing a successful `run_id` returns its manifest. Zone paths reject different data at the same date/source/table/HHMM. Identical reruns reuse existing files. Changed data in an occupied partition needs an explicit partition migration strategy before reprocessing; the framework reports a collision rather than silently overwriting.

## Tests

```bash
.venv/bin/pytest -q
```

The local integration test creates a small Chinook SQLite source and exercises all six stages plus SQLite load. PostgreSQL, ClickHouse, and SQL Server adapters are implemented but require their services and drivers for live integration tests.
