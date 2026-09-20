"""Unity Catalog setup for the S3-backed benchmark tables.

Creates (idempotently, if config.create_if_missing) the storage credential
and external location, then the catalog/schema and one external table per
`format: parquet` entry. `format: iceberg` entries are handled by
run_setup_managed_iceberg -- UC-managed tables, not external tables over a
self-managed location.
"""
from __future__ import annotations

import logging

from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import AwsIamRole

from benchmark.config import Config, quote_ident
from benchmark.db_client import sql_connection, workspace_client

log = logging.getLogger(__name__)


def _find_storage_credential_by_role_arn(ws, role_arn: str):
    for cred in ws.storage_credentials.list():
        aws_role = getattr(cred, "aws_iam_role", None)
        if aws_role is not None and getattr(aws_role, "role_arn", None) == role_arn:
            return cred
    return None


def ensure_storage_credential(config: Config) -> None:
    ws = workspace_client(config)
    name = config.storage_credential_name

    try:
        ws.storage_credentials.get(name)
        log.info("Storage credential %s already exists", name)
        return
    except NotFound:
        pass

    if config.iam_role_arn:
        existing = _find_storage_credential_by_role_arn(ws, config.iam_role_arn)
        if existing is not None:
            log.info(
                "Found existing storage credential %r wrapping role %s",
                existing.name,
                config.iam_role_arn,
            )
            return

    if not config.create_if_missing:
        raise RuntimeError(
            f"No storage credential named {name!r} was found, and "
            f"create_if_missing is false."
        )
    if not config.iam_role_arn:
        raise RuntimeError("unity_catalog.iam_role_arn must be set to create the storage credential.")

    log.info("Creating storage credential %s (role %s)", name, config.iam_role_arn)
    ws.storage_credentials.create(
        name=name,
        aws_iam_role=AwsIamRole(role_arn=config.iam_role_arn),
        comment="Created by the DuckDB vs UC Serverless benchmark harness.",
    )


def _normalize_url(url: str) -> str:
    return url.rstrip("/")


def _find_external_location_by_url(ws, url: str):
    target = _normalize_url(url)
    for loc in ws.external_locations.list():
        if _normalize_url(getattr(loc, "url", "")) == target:
            return loc
    return None


def ensure_external_location(config: Config) -> None:
    ws = workspace_client(config)
    name = config.external_location_name

    try:
        ws.external_locations.get(name)
        log.info("External location %s already exists", name)
        return
    except NotFound:
        pass

    existing = _find_external_location_by_url(ws, config.external_location_url)
    if existing is not None:
        log.info(
            "Found existing external location %r covering %s",
            existing.name,
            config.external_location_url,
        )
        return

    if not config.create_if_missing:
        raise RuntimeError(
            f"No external location named {name!r} covering "
            f"{config.external_location_url!r} was found, and "
            f"create_if_missing is false."
        )

    log.info(
        "Creating external location %s -> %s (credential %s)",
        name,
        config.external_location_url,
        config.storage_credential_name,
    )
    ws.external_locations.create(
        name=name,
        url=config.external_location_url,
        credential_name=config.storage_credential_name,
        comment="Created by the DuckDB vs UC Serverless benchmark harness.",
    )


def ensure_catalog_and_schema(config: Config) -> None:
    catalog = quote_ident(config.catalog)
    schema = quote_ident(config.schema)
    with sql_connection(config) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _table_exists(cur, catalog: str, schema: str, table_name: str) -> bool:
    escaped = table_name.replace("'", "''")
    cur.execute(
        f"SHOW TABLES IN {quote_ident(catalog)}.{quote_ident(schema)} "
        f"LIKE '{escaped}'"
    )
    return len(cur.fetchall()) > 0


def register_external_tables(config: Config) -> None:
    with sql_connection(config) as conn, conn.cursor() as cur:
        for table in config.tables.values():
            if table.format != "parquet":
                continue

            full_name = table.full_name(config.catalog, config.schema)
            if _table_exists(cur, config.catalog, config.schema, table.name):
                log.info("Skipping %s: already registered", full_name)
                continue

            location = table.s3_uri(config.bucket, config.root_prefix)
            log.info("Registering %s -> %s (PARQUET)", full_name, location)
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {full_name} "
                f"USING PARQUET LOCATION '{location}'"
            )


def run_setup(config: Config) -> None:
    ensure_storage_credential(config)
    ensure_external_location(config)
    ensure_catalog_and_schema(config)
    register_external_tables(config)


def ensure_managed_schema(config: Config) -> None:
    catalog = quote_ident(config.catalog)
    schema = quote_ident(config.managed_schema)
    with sql_connection(config) as conn, conn.cursor() as cur:
        cur.execute(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} "
            f"MANAGED LOCATION '{config.managed_location}'"
        )


def register_managed_iceberg_tables(config: Config) -> None:
    """CTAS each `format: iceberg` table that names a `source_table` into
    config.managed_schema. Tables without source_table (ds10) are left
    alone -- they are created by the sorted-Iceberg converter.
    """
    with sql_connection(config) as conn, conn.cursor() as cur:
        for table in config.tables.values():
            if table.format != "iceberg":
                continue

            source_name = table.extra.get("source_table")
            if not source_name:
                log.info("Skipping %s: no source_table (create it with convert-sorted-iceberg)", table.name)
                continue
            source = config.tables.get(source_name)
            if source is None:
                log.warning("Skipping %s: source_table %r not found", table.name, source_name)
                continue

            full_name = table.full_name(config.catalog, config.managed_schema)
            if _table_exists(cur, config.catalog, config.managed_schema, table.name):
                log.info("Skipping %s: already registered", full_name)
                continue

            source_uri = source.s3_uri(config.bucket, config.root_prefix)
            log.info("Creating managed table %s <- %s", full_name, source_uri)
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {full_name} "
                f"USING ICEBERG AS SELECT * FROM parquet.`{source_uri}*.parquet`"
            )


def run_setup_managed_iceberg(config: Config) -> None:
    ensure_managed_schema(config)
    register_managed_iceberg_tables(config)
