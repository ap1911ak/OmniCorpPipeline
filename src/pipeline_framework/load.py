"""Database sink adapters and ordered dimension/fact loads."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd
from datetime import date, datetime

from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, ForeignKey, MetaData, Table, Text, create_engine, delete, event, inspect, text, update

from .config import Design, Target
from .storage import read_parquet


class SinkAdapter(ABC):
    @abstractmethod
    def load(self, tables: list[tuple[Target, pd.DataFrame]], mode: str) -> dict[str, int]:
        """Load tables and return final row counts."""


class SqlAlchemyAdapter(SinkAdapter):
    """SQLite, PostgreSQL, and SQL Server using one transaction per run."""

    def __init__(self, url: str):
        self.engine = create_engine(url)
        if self.engine.dialect.name == "sqlite":
            @event.listens_for(self.engine, "connect")
            def _enable_foreign_keys(connection, _record):
                connection.execute("PRAGMA foreign_keys=ON")

    @staticmethod
    def _sql_type(series: pd.Series):
        if pd.api.types.is_integer_dtype(series):
            return BigInteger()
        if pd.api.types.is_float_dtype(series):
            return Float()
        if pd.api.types.is_bool_dtype(series):
            return Boolean()
        if pd.api.types.is_datetime64_any_dtype(series):
            return DateTime()
        sample = series.dropna().head(1)
        if not sample.empty and isinstance(sample.iloc[0], datetime):
            return DateTime()
        if not sample.empty and isinstance(sample.iloc[0], date):
            return Date()
        return Text()

    @classmethod
    def _table_model(cls, metadata: MetaData, target: Target, frame: pd.DataFrame, keys: dict[str, str]) -> Table:
        columns = []
        for name in frame.columns:
            foreign = target.foreign_keys.get(name)
            args = []
            if foreign:
                dimension = foreign["dimension"]
                args.append(ForeignKey(f"{dimension}.{keys[dimension]}"))
            columns.append(Column(name, cls._sql_type(frame[name]), *args, primary_key=name == target.key_column,
                                  nullable=name not in target.required and name != target.key_column))
        return Table(target.name, metadata, *columns)

    def load(self, tables: list[tuple[Target, pd.DataFrame]], mode: str) -> dict[str, int]:
        if mode not in ("full_replace", "incremental"):
            raise ValueError(f"Invalid load mode: {mode}")
        keys = {target.name: target.key_column for target, _ in tables}
        counts = {}
        with self.engine.begin() as connection:
            if mode == "full_replace":
                for name in reversed(list(keys)):
                    if inspect(connection).has_table(name):
                        Table(name, MetaData(), autoload_with=connection).drop(connection)
            metadata = MetaData()
            models = {target.name: self._table_model(metadata, target, frame, keys) for target, frame in tables}
            metadata.create_all(connection)
            for target, frame in tables:
                if frame.empty:
                    continue
                table = models[target.name]
                if mode == "incremental" and target.kind == "dimension":
                    existing = {row[0] for row in connection.execute(text(f'SELECT "{target.key_column}" FROM "{target.name}"'))}
                    updates = frame[frame[target.key_column].isin(existing)]
                    inserts = frame[~frame[target.key_column].isin(existing)]
                    for record in updates.to_dict("records"):
                        connection.execute(update(table).where(table.c[target.key_column] == record[target.key_column]).values(**record))
                    frame = inserts
                elif mode == "incremental":
                    values = frame[target.key_column].tolist()
                    for start in range(0, len(values), 900):
                        connection.execute(delete(table).where(table.c[target.key_column].in_(values[start:start + 900])))
                if not frame.empty:
                    chunksize = max(1, min(500, 2000 // len(frame.columns)))
                    frame.to_sql(target.name, connection, index=False, if_exists="append", chunksize=chunksize, method="multi")
            for name in keys:
                if inspect(connection).has_table(name):
                    counts[name] = int(connection.execute(text(f'SELECT COUNT(*) FROM "{name}"')).scalar_one())
        return counts


class ClickHouseAdapter(SinkAdapter):
    """ClickHouse adapter; incremental replacement uses synchronous DELETE mutations."""

    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        try:
            import clickhouse_connect
        except ImportError as exc:
            raise RuntimeError("ClickHouse adapter needs clickhouse extra") from exc
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
        self.client.command(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        self.client.close()
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password, database=database)

    @staticmethod
    def _column_type(series: pd.Series) -> str:
        if pd.api.types.is_integer_dtype(series):
            base = "Int64"
        elif pd.api.types.is_float_dtype(series):
            base = "Float64"
        elif pd.api.types.is_datetime64_any_dtype(series):
            base = "DateTime"
        else:
            sample = series.dropna().head(1)
            base = "Date" if not sample.empty and isinstance(sample.iloc[0], date) else "String"
        return f"Nullable({base})" if series.isna().any() else base

    def load(self, tables: list[tuple[Target, pd.DataFrame]], mode: str) -> dict[str, int]:
        if mode not in ("full_replace", "incremental"):
            raise ValueError(f"Invalid load mode: {mode}")
        counts = {}
        for target, frame in tables:
            name = target.name
            if mode == "full_replace":
                self.client.command(f"DROP TABLE IF EXISTS `{name}`")
            if int(self.client.command(f"EXISTS TABLE `{name}`")) == 0:
                cols = ", ".join(f"`{column}` {self._column_type(frame[column])}" for column in frame.columns)
                self.client.command(f"CREATE TABLE `{name}` ({cols}) ENGINE = MergeTree ORDER BY `{target.key_column}`")
            if mode == "incremental" and not frame.empty:
                keys = frame[target.key_column].tolist()
                for start in range(0, len(keys), 1000):
                    chunk = keys[start:start + 1000]
                    literals = ",".join(str(int(key)) for key in chunk)
                    self.client.command(f"ALTER TABLE `{name}` DELETE WHERE `{target.key_column}` IN ({literals}) SETTINGS mutations_sync=1")
            if not frame.empty:
                self.client.insert_df(name, frame)
            counts[name] = int(self.client.query(f"SELECT count() FROM `{name}`").first_item)
        return counts


def build_adapter(sink: str, root: Path, settings: dict[str, Any]) -> SinkAdapter:
    if sink == "sqlite":
        database = Path(settings.get("database", root / "warehouse" / "star.sqlite"))
        database.parent.mkdir(parents=True, exist_ok=True)
        return SqlAlchemyAdapter(f"sqlite:///{database.resolve()}")
    if sink in ("postgresql", "sqlserver"):
        env_key = "PIPELINE_POSTGRESQL_URL" if sink == "postgresql" else "PIPELINE_SQLSERVER_URL"
        import os
        url = settings.get("url") or os.getenv(env_key)
        if not url:
            raise ValueError(f"Set {env_key} or configure sink URL")
        return SqlAlchemyAdapter(url)
    if sink == "clickhouse":
        import os
        return ClickHouseAdapter(
            host=settings.get("host", os.getenv("PIPELINE_CH_HOST", "127.0.0.1")),
            port=int(settings.get("port", os.getenv("PIPELINE_CH_PORT", "8123"))),
            database=settings.get("database", os.getenv("PIPELINE_CH_DATABASE", "pipeline")),
            username=settings.get("username", os.getenv("PIPELINE_CH_USER", "default")),
            password=settings.get("password", os.getenv("PIPELINE_CH_PASSWORD", "")),
        )
    raise ValueError(f"Unknown sink: {sink}")


def load_integration(
    designs: list[Design],
    integrations: list[dict[str, list[Path]]],
    root: Path,
    sink: str,
    mode: str,
    settings: dict[str, Any] | None = None,
) -> dict[str, int]:
    grouped: dict[str, list[pd.DataFrame]] = {}
    targets: dict[str, Target] = {}
    for design, paths_by_target in zip(designs, integrations):
        for target in design.targets:
            grouped.setdefault(target.name, []).extend(read_parquet(path) for path in paths_by_target[target.name])
            targets.setdefault(target.name, target)
            if targets[target.name].key_column != target.key_column:
                raise ValueError(f"Key contract differs across sources: {target.name}")
    ordered = sorted(targets.values(), key=lambda item: 0 if item.kind == "dimension" else 1)
    tables = [(target, pd.concat(grouped[target.name], ignore_index=True)) for target in ordered]
    deduplicated = []
    for target, frame in tables:
        if target.name == "dim_date":
            frame = frame.drop_duplicates().reset_index(drop=True)
        deduplicated.append((target, frame))
    tables = deduplicated
    for target, frame in tables:
        if frame[target.key_column].duplicated().any():
            duplicate = int(frame[target.key_column].duplicated().sum())
            raise ValueError(f"Duplicate conformed key in {target.name}: {duplicate}")
    return build_adapter(sink, root, settings or {}).load(tables, mode)
