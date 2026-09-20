"""Shared connection helpers for the SQL warehouse, workspace APIs, and
local DuckDB (httpfs + optional Iceberg REST attach).
"""
from __future__ import annotations

from contextlib import contextmanager

from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config as SdkConfig
from databricks.sdk.core import oauth_service_principal

from benchmark.config import Config


def _require_token(config: Config) -> str:
    if not config.token:
        raise RuntimeError(
            f"Env var {config.token_env} is not set. Export it or put it in "
            f"a .env file before running commands that talk to Databricks."
        )
    return config.token


def _require_oauth_secret(config: Config) -> str:
    if not config.oauth_client_secret:
        raise RuntimeError(
            f"databricks.client_id is set but env var "
            f"{config.oauth_client_secret_env} is not. Export it or put it "
            f"in a .env file, or unset client_id to use the PAT in "
            f"{config.token_env}."
        )
    return config.oauth_client_secret


def _oauth_credential_provider(config: Config):
    def provider():
        sdk_config = SdkConfig(
            host=f"https://{config.host}",
            client_id=config.oauth_client_id,
            client_secret=_require_oauth_secret(config),
        )
        return oauth_service_principal(sdk_config)

    return provider


@contextmanager
def sql_connection(config: Config):
    if config.use_oauth:
        conn = dbsql.connect(
            server_hostname=config.host,
            http_path=config.http_path,
            credentials_provider=_oauth_credential_provider(config),
        )
    else:
        conn = dbsql.connect(
            server_hostname=config.host,
            http_path=config.http_path,
            access_token=_require_token(config),
        )
    try:
        yield conn
    finally:
        conn.close()


def workspace_client(config: Config) -> WorkspaceClient:
    if config.use_oauth:
        return WorkspaceClient(
            host=f"https://{config.host}",
            client_id=config.oauth_client_id,
            client_secret=_require_oauth_secret(config),
            auth_type="oauth-m2m",
        )
    return WorkspaceClient(host=f"https://{config.host}", token=_require_token(config))


DUCKDB_UC_ALIAS = "uc"


@contextmanager
def duckdb_connection(config: Config):
    """DuckDB connection.

    Parquet tables are read via httpfs from S3 (or from local_data_dir).
    Iceberg tables attach Unity Catalog's Iceberg REST catalog under
    DUCKDB_UC_ALIAS. S3 credentials for direct Parquet reads come from the
    AWS credential chain (env, ~/.aws/credentials, instance role). Iceberg
    reads use vended credentials from the catalog attach.
    """
    import duckdb as ddb

    conn = ddb.connect()
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute(f"SET temp_directory='{config.duckdb_temp_directory}'")
        if config.duckdb_memory_limit:
            conn.execute(f"SET memory_limit='{config.duckdb_memory_limit}'")
        if config.duckdb_threads:
            conn.execute(f"SET threads={int(config.duckdb_threads)}")
        if not config.local_data_dir:
            conn.execute(
                f"""
                CREATE SECRET s3_bench (
                    TYPE S3,
                    PROVIDER CREDENTIAL_CHAIN,
                    REGION '{config.region}'
                )
                """
            )

        attach_iceberg = any(t.format == "iceberg" for t in config.tables.values()) and not config.local_data_dir
        if attach_iceberg:
            conn.execute("INSTALL iceberg")
            conn.execute("LOAD iceberg")
            if config.use_oauth:
                conn.execute(
                    f"""
                    CREATE SECRET uc_iceberg (
                        TYPE ICEBERG,
                        CLIENT_ID '{config.oauth_client_id}',
                        CLIENT_SECRET '{_require_oauth_secret(config)}',
                        OAUTH2_SERVER_URI 'https://{config.host}/oidc/v1/token',
                        OAUTH2_SCOPE 'all-apis'
                    )
                    """
                )
            else:
                conn.execute(
                    f"""
                    CREATE SECRET uc_iceberg (
                        TYPE ICEBERG,
                        TOKEN '{_require_token(config)}'
                    )
                    """
                )
            conn.execute(
                f"""
                ATTACH '{config.catalog}' AS {DUCKDB_UC_ALIAS} (
                    TYPE ICEBERG,
                    ENDPOINT 'https://{config.host}/api/2.1/unity-catalog/iceberg-rest',
                    ACCESS_DELEGATION_MODE 'vended_credentials'
                )
                """
            )

        yield conn
    finally:
        conn.close()
