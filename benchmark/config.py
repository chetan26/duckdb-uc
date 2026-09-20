"""Config loading for the benchmark harness.

Layering: config.yaml (copied from config.example.yaml) holds non-secret
workspace and table settings. Tokens and OAuth client secrets come from
environment variables named in that file, optionally loaded via a local
.env that is never checked in.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

DEFAULT_CONFIG_PATH = Path("config/config.yaml")


def quote_ident(name: str) -> str:
    """Backtick-quote a Databricks SQL identifier."""
    return f"`{name.replace('`', '``')}`"


def duckdb_quote_ident(name: str) -> str:
    """Double-quote a DuckDB SQL identifier."""
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


@dataclass
class TableConfig:
    name: str
    format: str
    path: str = ""
    extra: dict = field(default_factory=dict)

    def s3_uri(self, bucket: str, root_prefix: str) -> str:
        prefix = root_prefix.strip("/")
        return f"s3://{bucket}/{prefix}/{self.path.strip('/')}/"

    def full_name(self, catalog: str, schema: str) -> str:
        return f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(self.name)}"

    def duckdb_read_parquet(self, bucket: str, root_prefix: str, local_data_dir: str = "") -> str:
        """`read_parquet(...)` for this table. Uses a local directory when
        `local_data_dir` is set (offline smoke runs); otherwise the S3 glob."""
        if local_data_dir:
            local = Path(local_data_dir) / self.path.strip("/")
            return f"read_parquet('{local}/*.parquet')"
        return f"read_parquet('{self.s3_uri(bucket, root_prefix)}*.parquet')"

    def duckdb_managed_iceberg_ref(self, uc_alias: str, managed_schema: str) -> str:
        return f"{uc_alias}.{duckdb_quote_ident(managed_schema)}.{duckdb_quote_ident(self.name)}"


@dataclass
class Config:
    host: str
    http_path: str
    token: str | None
    token_env: str

    oauth_client_id: str
    oauth_client_secret: str | None
    oauth_client_secret_env: str

    catalog: str
    schema: str
    storage_credential_name: str
    external_location_name: str
    create_if_missing: bool
    iam_role_arn: str

    managed_schema: str
    managed_location: str

    bucket: str
    root_prefix: str
    region: str
    local_data_dir: str

    tables: dict[str, TableConfig]

    warehouse_size: str
    dbu_per_hour: float
    rate_usd_per_dbu_hour: float

    duckdb_threads: int | None
    duckdb_memory_limit: str | None
    duckdb_temp_directory: str
    duckdb_hourly_cost_usd: float
    duckdb_warehouse_size: str

    cold_repetitions: int
    warm_repetitions: int
    results_path: Path
    queries_file: Path
    restart_warehouse_for_cold_run: bool

    raw: dict = field(default_factory=dict, repr=False)

    @property
    def external_location_url(self) -> str:
        return f"s3://{self.bucket}/{self.root_prefix.strip('/')}/"

    @property
    def use_oauth(self) -> bool:
        return bool(self.oauth_client_id)


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> Config:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Copy config/config.example.yaml to "
            f"config/config.yaml and fill in your values."
        )

    load_dotenv()

    raw = yaml.safe_load(path.read_text())

    db = raw["databricks"]
    uc = raw["unity_catalog"]
    s3 = raw.get("s3", {})
    warehouse = raw.get("warehouse", {})
    duckdb = raw.get("duckdb", {})
    run = raw.get("run", {})

    token_env = db.get("token_env", "DATABRICKS_TOKEN")
    token = os.environ.get(token_env) or None

    oauth_client_id = db.get("client_id", "")
    oauth_client_secret_env = db.get("client_secret_env", "DATABRICKS_CLIENT_SECRET")
    oauth_client_secret = os.environ.get(oauth_client_secret_env) or None

    tables = {
        name: TableConfig(
            name=name,
            format=t["format"],
            path=t.get("path", ""),
            extra={k: v for k, v in t.items() if k not in ("format", "path")},
        )
        for name, t in raw.get("tables", {}).items()
    }

    bucket = s3["bucket"]
    root_prefix = s3.get("root_prefix", "").strip("/")

    return Config(
        host=db["host"],
        http_path=db["http_path"],
        token=token,
        token_env=token_env,
        oauth_client_id=oauth_client_id,
        oauth_client_secret=oauth_client_secret,
        oauth_client_secret_env=oauth_client_secret_env,
        catalog=uc["catalog"],
        schema=uc["schema"],
        storage_credential_name=uc["storage_credential_name"],
        external_location_name=uc["external_location_name"],
        create_if_missing=bool(uc.get("create_if_missing", False)),
        iam_role_arn=uc.get("iam_role_arn", ""),
        bucket=bucket,
        root_prefix=root_prefix,
        region=s3.get("region", "us-east-1"),
        local_data_dir=s3.get("local_data_dir", ""),
        managed_schema=uc.get("managed_schema", f"{uc['schema']}_managed"),
        managed_location=(
            f"s3://{bucket}/"
            f"{uc.get('managed_location_prefix', 'uc-managed').strip('/')}/"
        ),
        tables=tables,
        warehouse_size=warehouse.get("size", "unknown"),
        dbu_per_hour=float(warehouse.get("dbu_per_hour", 0.0)),
        rate_usd_per_dbu_hour=float(warehouse.get("rate_usd_per_dbu_hour", 0.0)),
        duckdb_threads=duckdb.get("threads"),
        duckdb_memory_limit=duckdb.get("memory_limit"),
        duckdb_temp_directory=duckdb.get("temp_directory", "/tmp/duckdb-bench-tmp"),
        duckdb_hourly_cost_usd=float(duckdb.get("hourly_cost_usd", 0.0)),
        duckdb_warehouse_size=str(duckdb.get("warehouse_size", "duckdb-local")),
        cold_repetitions=int(run.get("cold_repetitions", 3)),
        warm_repetitions=int(run.get("warm_repetitions", 3)),
        results_path=Path(run.get("results_path", "results/benchmark_results.csv")),
        queries_file=Path(run.get("queries_file", "benchmark/queries.yaml")),
        restart_warehouse_for_cold_run=bool(run.get("restart_warehouse_for_cold_run", False)),
        raw=raw,
    )
