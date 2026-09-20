"""Parquet -> write-time-sorted Iceberg converter.

`setup-managed-iceberg` does a plain CTAS with no ORDER BY. This script
writes a managed Iceberg table sorted by --sort-columns (used for ds10).

    python -m benchmark.scripts.convert_to_sorted_iceberg \\
        --config config/config.yaml \\
        --source-table ds9 \\
        --dest-table ds10 \\
        --sort-columns col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9
"""
from __future__ import annotations

import logging

import click

from benchmark.catalog_setup import ensure_managed_schema
from benchmark.config import Config, load_config, quote_ident
from benchmark.db_client import sql_connection

log = logging.getLogger(__name__)


def _parse_sort_columns(raw: str) -> list[str]:
    order_by = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":") if ":" in entry else entry.rsplit(" ", 1)
        if len(parts) == 2 and parts[1].strip().lower() in ("asc", "desc"):
            col, direction = parts[0].strip(), parts[1].strip().upper()
        else:
            col, direction = entry, "ASC"
        order_by.append(f"{quote_ident(col)} {direction}")
    if not order_by:
        raise click.BadParameter("--sort-columns must name at least one column")
    return order_by


def _table_exists(cur, catalog: str, schema: str, table_name: str) -> bool:
    escaped = table_name.replace("'", "''")
    cur.execute(
        f"SHOW TABLES IN {quote_ident(catalog)}.{quote_ident(schema)} "
        f"LIKE '{escaped}'"
    )
    return len(cur.fetchall()) > 0


def convert_to_sorted_iceberg(
    config: Config,
    source_uri: str,
    dest_table: str,
    sort_columns: list[str],
    dest_catalog: str | None = None,
    dest_schema: str | None = None,
    if_exists: str = "error",
    verify_row_count: bool = True,
    dry_run: bool = False,
) -> None:
    catalog = dest_catalog or config.catalog
    schema = dest_schema or config.managed_schema
    full_dest = f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(dest_table)}"
    order_by_clause = ", ".join(sort_columns)

    ctas_sql = (
        f"CREATE TABLE {full_dest} USING ICEBERG AS "
        f"SELECT * FROM parquet.`{source_uri}*.parquet` "
        f"ORDER BY {order_by_clause}"
    )

    if dry_run:
        click.echo(ctas_sql)
        return

    ensure_managed_schema(config)

    with sql_connection(config) as conn, conn.cursor() as cur:
        exists = _table_exists(cur, catalog, schema, dest_table)
        if exists:
            if if_exists == "skip":
                log.info("Skipping %s: already exists (--if-exists skip)", full_dest)
                return
            if if_exists == "error":
                raise RuntimeError(
                    f"{full_dest} already exists. Pass --if-exists skip or "
                    f"--if-exists overwrite to proceed."
                )
            log.info("Dropping existing %s (--if-exists overwrite)", full_dest)
            cur.execute(f"DROP TABLE {full_dest}")

        log.info("Creating %s <- %s (write-time sorted by %s)", full_dest, source_uri, order_by_clause)
        cur.execute(ctas_sql)

        if verify_row_count:
            cur.execute(f"SELECT count(*) FROM {full_dest}")
            (dest_count,) = cur.fetchone()
            cur.execute(f"SELECT count(*) FROM parquet.`{source_uri}*.parquet`")
            (source_count,) = cur.fetchone()
            if dest_count != source_count:
                raise RuntimeError(
                    f"Row count mismatch after conversion: source={source_count}, "
                    f"{full_dest}={dest_count}"
                )
            log.info("Verified %s: %d rows", full_dest, dest_count)


@click.command()
@click.option("--config", "config_path", default="config/config.yaml", show_default=True)
@click.option("--source-table", help="Name of a format: parquet entry in config.yaml.")
@click.option("--source-uri", help="Explicit s3://bucket/prefix/ to read *.parquet from.")
@click.option("--dest-table", required=True, help="Name of the new managed Iceberg table.")
@click.option("--dest-catalog", default=None)
@click.option("--dest-schema", default=None)
@click.option("--sort-columns", required=True, help="Comma-separated columns, e.g. col_0,col_1:desc.")
@click.option("--if-exists", type=click.Choice(["error", "skip", "overwrite"]), default="error")
@click.option("--verify-row-count/--no-verify-row-count", default=True)
@click.option("--dry-run", is_flag=True)
@click.option("-v", "--verbose", is_flag=True)
def main(
    config_path: str,
    source_table: str | None,
    source_uri: str | None,
    dest_table: str,
    dest_catalog: str | None,
    dest_schema: str | None,
    sort_columns: str,
    if_exists: str,
    verify_row_count: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not source_table and not source_uri:
        raise click.UsageError("Pass --source-table or --source-uri.")

    config = load_config(config_path)

    if source_uri:
        resolved_uri = source_uri.rstrip("/") + "/"
    else:
        table = config.tables.get(source_table)
        if table is None:
            raise click.UsageError(f"--source-table {source_table!r} not found in config.yaml.")
        if table.format != "parquet":
            raise click.UsageError(
                f"--source-table {source_table!r} has format {table.format!r}, expected 'parquet'."
            )
        resolved_uri = table.s3_uri(config.bucket, config.root_prefix)

    convert_to_sorted_iceberg(
        config,
        source_uri=resolved_uri,
        dest_table=dest_table,
        sort_columns=_parse_sort_columns(sort_columns),
        dest_catalog=dest_catalog,
        dest_schema=dest_schema,
        if_exists=if_exists,
        verify_row_count=verify_row_count,
        dry_run=dry_run,
    )
    if not dry_run:
        click.echo(
            f"Converted -> {dest_catalog or config.catalog}."
            f"{dest_schema or config.managed_schema}.{dest_table}"
        )


if __name__ == "__main__":
    main()
