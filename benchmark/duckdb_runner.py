"""Query driver for local DuckDB: Parquet via read_parquet, Iceberg via the
attached Unity Catalog REST catalog. Runs the shared query set cold + warm.
Cold attempts open a fresh connection so DuckDB buffer/catalog state does
not carry warmth across attempts.
"""
from __future__ import annotations

import json
import logging
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

from benchmark.config import Config, TableConfig
from benchmark.db_client import duckdb_connection
from benchmark.query_loader import ResolvedQuery, load_queries
from benchmark.results_writer import ResultRow, ResultsWriter

log = logging.getLogger(__name__)

ENGINE = "duckdb"

_MISSING_DATA_EXCEPTIONS = (duckdb.IOException, duckdb.HTTPException, duckdb.CatalogException)
_MISSING_DATA_MARKERS = ("no files found", "404", "not found", "does not exist")


def _is_missing_data_error(exc: Exception) -> bool:
    return isinstance(exc, _MISSING_DATA_EXCEPTIONS) and any(
        marker in str(exc).lower() for marker in _MISSING_DATA_MARKERS
    )


@dataclass
class QueryOutcome:
    latency_s: float
    bytes_scanned: int | None


def _sum_byte_fields(node: object) -> int:
    total = 0
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, (int, float)) and "byte" in key.lower():
                total += int(value)
            else:
                total += _sum_byte_fields(value)
    elif isinstance(node, list):
        for item in node:
            total += _sum_byte_fields(item)
    return total


def _run_once(conn, query: ResolvedQuery) -> QueryOutcome:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        profile_path = f.name

    bytes_scanned: int | None = None
    try:
        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute(f"PRAGMA profiling_output='{profile_path}'")

        start = time.perf_counter()
        conn.execute(query.sql).fetchall()
        latency_s = time.perf_counter() - start

        try:
            profile = json.loads(Path(profile_path).read_text())
            bytes_scanned = _sum_byte_fields(profile) or None
        except Exception:
            log.debug("Could not parse DuckDB profiling output for '%s'", query.id, exc_info=True)
    finally:
        conn.execute("PRAGMA disable_profiling")
        Path(profile_path).unlink(missing_ok=True)

    return QueryOutcome(latency_s=latency_s, bytes_scanned=bytes_scanned)


def _estimate_cost_usd(config: Config, latency_s: float) -> float | None:
    if not config.duckdb_hourly_cost_usd:
        return None
    return (latency_s / 3600.0) * config.duckdb_hourly_cost_usd


def _row(
    config: Config,
    table_cfg: TableConfig,
    query: ResolvedQuery,
    run_type: str,
    attempt: int,
    outcome: QueryOutcome,
) -> ResultRow:
    return ResultRow(
        engine=ENGINE,
        format=table_cfg.format,
        table=query.table,
        query_id=query.id,
        run_type=run_type,
        attempt=attempt,
        latency_s=outcome.latency_s,
        bytes_scanned=outcome.bytes_scanned,
        cost_usd=_estimate_cost_usd(config, outcome.latency_s),
        warehouse_size=config.duckdb_warehouse_size,
    )


def run_benchmark(config: Config, queries_file=None, force: bool = False) -> list[ResultRow]:
    queries = load_queries(config, queries_file=queries_file, dialect="duckdb")
    rows: list[ResultRow] = []

    writer = ResultsWriter(config.results_path)
    if not force:
        already_run = writer.existing_query_ids(ENGINE)
        skipped = [q.id for q in queries if q.id in already_run]
        queries = [q for q in queries if q.id not in already_run]
        if skipped:
            log.info(
                "Skipping %d already-benchmarked quer%s (pass --force to re-run): %s",
                len(skipped),
                "y" if len(skipped) == 1 else "ies",
                ", ".join(skipped),
            )

    with duckdb_connection(config) as warm_conn:
        for query in queries:
            table_cfg = config.tables[query.table]
            if config.local_data_dir and table_cfg.format == "iceberg":
                log.info("Skipping '%s': Iceberg tables require a catalog attach, not local_data_dir", query.id)
                continue

            try:
                for attempt in range(1, config.cold_repetitions + 1):
                    with duckdb_connection(config) as cold_conn:
                        outcome = _run_once(cold_conn, query)
                    rows.append(_row(config, table_cfg, query, "cold", attempt, outcome))
                    log.info(
                        "[cold %d/%d] %s: %.3fs",
                        attempt,
                        config.cold_repetitions,
                        query.id,
                        outcome.latency_s,
                    )
            except Exception as e:
                if not _is_missing_data_error(e):
                    raise
                log.warning(
                    "Skipping '%s' (table %r): data not found -- run `generate` "
                    "or `setup-managed-iceberg` first: %s",
                    query.id,
                    query.table,
                    e,
                )
                continue

            try:
                for attempt in range(1, config.warm_repetitions + 1):
                    outcome = _run_once(warm_conn, query)
                    rows.append(_row(config, table_cfg, query, "warm", attempt, outcome))
                    log.info(
                        "[warm %d/%d] %s: %.3fs",
                        attempt,
                        config.warm_repetitions,
                        query.id,
                        outcome.latency_s,
                    )
            except Exception as e:
                if not _is_missing_data_error(e):
                    raise
                log.warning(
                    "Skipping remaining warm attempts for '%s' (table %r): data not found: %s",
                    query.id,
                    query.table,
                    e,
                )

    writer.append_rows(rows)
    return rows
