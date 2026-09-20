"""Resolve the shared queries.yaml templates against a loaded Config."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from benchmark.config import Config
from benchmark.db_client import DUCKDB_UC_ALIAS
from benchmark.dialect import to_databricks_dialect


@dataclass
class ResolvedQuery:
    id: str
    table: str
    description: str
    sql: str


def _context(config: Config, dialect: str) -> dict:
    ctx: dict = {}
    for name, table in config.tables.items():
        if table.format == "iceberg":
            if dialect == "duckdb":
                ctx[f"{name}_table"] = table.duckdb_managed_iceberg_ref(
                    DUCKDB_UC_ALIAS, config.managed_schema
                )
            else:
                ctx[f"{name}_table"] = table.full_name(config.catalog, config.managed_schema)
        elif dialect == "duckdb":
            ctx[f"{name}_table"] = table.duckdb_read_parquet(
                config.bucket, config.root_prefix, config.local_data_dir
            )
        else:
            ctx[f"{name}_table"] = table.full_name(config.catalog, config.schema)
    return ctx


def load_queries(
    config: Config,
    queries_file: Path | None = None,
    dialect: str = "duckdb",
) -> list[ResolvedQuery]:
    path = queries_file or config.queries_file
    raw_queries = yaml.safe_load(Path(path).read_text())
    ctx = _context(config, dialect)

    resolved = []
    for q in raw_queries:
        try:
            sql = q["sql"].format(**ctx)
        except KeyError as e:
            raise KeyError(
                f"Query '{q['id']}' references placeholder {e} that isn't "
                f"resolvable from config -- check config.yaml `tables` entries."
            ) from e

        if dialect == "databricks":
            sql = to_databricks_dialect(sql)
        elif dialect != "duckdb":
            raise ValueError(f"Unknown dialect: {dialect}")

        resolved.append(
            ResolvedQuery(
                id=q["id"],
                table=q["table"],
                description=q.get("description", ""),
                sql=sql,
            )
        )
    return resolved
