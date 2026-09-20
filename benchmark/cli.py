"""CLI entrypoint:

    python -m benchmark.cli generate              --config config/config.yaml
    python -m benchmark.cli setup                 --config config/config.yaml
    python -m benchmark.cli setup-managed-iceberg --config config/config.yaml
    python -m benchmark.cli run                   --config config/config.yaml
    python -m benchmark.cli run-duckdb            --config config/config.yaml
"""
from __future__ import annotations

import logging

import click

from benchmark.catalog_setup import run_setup, run_setup_managed_iceberg
from benchmark.config import DEFAULT_CONFIG_PATH, load_config
from benchmark.databricks_runner import run_benchmark


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Debug logging.")
def cli(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@cli.command()
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH))
@click.option("--force", is_flag=True, help="Regenerate even if local or S3 already has data.")
@click.option("--no-upload", is_flag=True, help="Write local Parquet only; skip aws s3 sync.")
@click.option("--smoke", is_flag=True, help="Cap each table at 2000 rows / 1 file for a local dry run.")
@click.option("--local-dir", default=None, help="Local output root (default: s3.local_data_dir or ./data).")
@click.argument("tables", nargs=-1)
def generate(
    config_path: str,
    force: bool,
    no_upload: bool,
    smoke: bool,
    local_dir: str | None,
    tables: tuple[str, ...],
) -> None:
    """Generate every synthetic parquet table (ds1-ds7, ds9) and optionally upload to S3."""
    from pathlib import Path

    from benchmark.generate_synthetic import run_generate

    config = load_config(config_path)
    run_generate(
        config,
        only=list(tables) or None,
        force=force,
        upload=not no_upload,
        smoke=smoke,
        local_root=Path(local_dir) if local_dir else None,
    )
    click.echo("Synthetic data generation complete.")


@cli.command()
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH))
def setup(config_path: str) -> None:
    """Create the storage credential/external location, catalog, schema, and external Parquet tables."""
    config = load_config(config_path)
    run_setup(config)
    click.echo("Unity Catalog setup complete.")


@cli.command(name="setup-managed-iceberg")
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH))
def setup_managed_iceberg(config_path: str) -> None:
    """CTAS each format: iceberg table (with source_table) into the managed schema."""
    config = load_config(config_path)
    run_setup_managed_iceberg(config)
    click.echo("Managed Iceberg setup complete.")


@cli.command()
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH))
@click.option("--queries-file", default=None, help="Override run.queries_file from config.")
@click.option("--force", is_flag=True, help="Re-run queries that already have rows for this engine.")
def run(config_path: str, queries_file: str | None, force: bool) -> None:
    """Run the shared query set cold+warm against the Serverless SQL warehouse."""
    config = load_config(config_path)
    rows = run_benchmark(config, queries_file=queries_file, force=force)
    click.echo(f"Wrote {len(rows)} result rows to {config.results_path}")


@cli.command(name="run-duckdb")
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH))
@click.option("--queries-file", default=None, help="Override run.queries_file from config.")
@click.option("--force", is_flag=True, help="Re-run queries that already have rows for this engine.")
def run_duckdb(config_path: str, queries_file: str | None, force: bool) -> None:
    """Run the shared query set cold+warm via local DuckDB."""
    from benchmark.duckdb_runner import run_benchmark as run_duckdb_benchmark

    config = load_config(config_path)
    rows = run_duckdb_benchmark(config, queries_file=queries_file, force=force)
    click.echo(f"Wrote {len(rows)} result rows to {config.results_path}")


if __name__ == "__main__":
    cli()
