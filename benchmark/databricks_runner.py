"""Query driver for Databricks Serverless SQL / Unity Catalog.

Runs the shared query set cold + warm against the warehouse. Cold is
first-touch on a live warehouse by default (restart_warehouse_for_cold_run
is off); flip that flag to stop/start before each query.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from databricks.sql.exc import ServerOperationError

from benchmark.config import Config
from benchmark.db_client import sql_connection, workspace_client
from benchmark.query_loader import ResolvedQuery, load_queries
from benchmark.results_writer import ResultRow, ResultsWriter

log = logging.getLogger(__name__)

ENGINE = "databricks_uc_serverless"

_MISSING_DATA_ERROR_CODES = ("TABLE_OR_VIEW_NOT_FOUND", "PATH_NOT_FOUND", "DELTA_PATH_DOES_NOT_EXIST")


def _is_missing_data_error(exc: Exception) -> bool:
    return isinstance(exc, ServerOperationError) and any(
        code in str(exc) for code in _MISSING_DATA_ERROR_CODES
    )


@dataclass
class QueryOutcome:
    latency_s: float
    bytes_scanned: int | None
    query_id: str | None


def _run_once(cur, query: ResolvedQuery) -> QueryOutcome:
    start = time.perf_counter()
    cur.execute(query.sql)
    cur.fetchall()
    latency_s = time.perf_counter() - start
    query_id = getattr(cur, "query_id", None)
    return QueryOutcome(latency_s=latency_s, bytes_scanned=None, query_id=query_id)


def _fetch_bytes_scanned(config: Config, query_id: str | None) -> int | None:
    if not query_id:
        return None
    try:
        ws = workspace_client(config)
        info = ws.query_history.get(query_id)
        metrics = getattr(info, "metrics", None) or getattr(info, "query_metrics", None)
        if metrics is None:
            return None
        return getattr(metrics, "read_bytes", None) or getattr(metrics, "total_bytes_read", None)
    except Exception:
        log.debug("Could not fetch query metrics for %s", query_id, exc_info=True)
        return None


def _estimate_cost_usd(config: Config, latency_s: float) -> float | None:
    if not config.dbu_per_hour or not config.rate_usd_per_dbu_hour:
        return None
    return (latency_s / 3600.0) * config.dbu_per_hour * config.rate_usd_per_dbu_hour


def _maybe_restart_warehouse(config: Config) -> None:
    if not config.restart_warehouse_for_cold_run:
        return
    warehouse_id = config.http_path.rstrip("/").split("/")[-1]
    ws = workspace_client(config)
    log.info("Stopping warehouse %s for a true cold start", warehouse_id)
    ws.warehouses.stop_and_wait(warehouse_id)
    log.info("Starting warehouse %s", warehouse_id)
    ws.warehouses.start_and_wait(warehouse_id)


def run_benchmark(config: Config, queries_file=None, force: bool = False) -> list[ResultRow]:
    queries = load_queries(config, queries_file=queries_file, dialect="databricks")
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

    for query in queries:
        table_cfg = config.tables[query.table]
        _maybe_restart_warehouse(config)

        with sql_connection(config) as conn:
            try:
                for attempt in range(1, config.cold_repetitions + 1):
                    with conn.cursor() as cur:
                        outcome = _run_once(cur, query)
                    bytes_scanned = _fetch_bytes_scanned(config, outcome.query_id)
                    rows.append(
                        ResultRow(
                            engine=ENGINE,
                            format=table_cfg.format,
                            table=query.table,
                            query_id=query.id,
                            run_type="cold",
                            attempt=attempt,
                            latency_s=outcome.latency_s,
                            bytes_scanned=bytes_scanned,
                            cost_usd=_estimate_cost_usd(config, outcome.latency_s),
                            warehouse_size=config.warehouse_size,
                        )
                    )
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
                    "Skipping '%s' (table %r): table not found in Unity Catalog -- "
                    "run `setup` / `setup-managed-iceberg` first: %s",
                    query.id,
                    query.table,
                    e,
                )
                continue

            try:
                for attempt in range(1, config.warm_repetitions + 1):
                    with conn.cursor() as cur:
                        outcome = _run_once(cur, query)
                    bytes_scanned = _fetch_bytes_scanned(config, outcome.query_id)
                    rows.append(
                        ResultRow(
                            engine=ENGINE,
                            format=table_cfg.format,
                            table=query.table,
                            query_id=query.id,
                            run_type="warm",
                            attempt=attempt,
                            latency_s=outcome.latency_s,
                            bytes_scanned=bytes_scanned,
                            cost_usd=_estimate_cost_usd(config, outcome.latency_s),
                            warehouse_size=config.warehouse_size,
                        )
                    )
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
