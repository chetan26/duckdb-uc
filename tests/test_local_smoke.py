"""End-to-end: generate a tiny table and run one DuckDB query against it."""
from __future__ import annotations

from pathlib import Path

import yaml

from benchmark.config import load_config
from benchmark.duckdb_runner import run_benchmark
from benchmark.generate_synthetic import run_generate


def test_generate_and_run_duckdb_smoke(tmp_path: Path, example_config_path: Path) -> None:
    raw = yaml.safe_load(example_config_path.read_text())
    raw["databricks"]["host"] = "example.cloud.databricks.com"
    raw["databricks"]["http_path"] = "/sql/1.0/warehouses/example"
    raw["s3"]["bucket"] = "example-bucket"
    raw["s3"]["local_data_dir"] = str(tmp_path / "data")
    raw["duckdb"]["temp_directory"] = str(tmp_path / "duckdb-tmp")
    raw["run"]["results_path"] = str(tmp_path / "out.csv")
    raw["run"]["cold_repetitions"] = 1
    raw["run"]["warm_repetitions"] = 1
    raw["run"]["queries_file"] = str(tmp_path / "queries.yaml")
    # Keep one narrow parquet table so the run stays fast.
    raw["tables"] = {
        "ds2": raw["tables"]["ds2"],
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(raw))

    (tmp_path / "queries.yaml").write_text(
        "- id: ds2_select_ordered_limit30\n"
        "  table: ds2\n"
        "  description: smoke\n"
        "  sql: >\n"
        "    SELECT CAST(col_0 AS BIGINT) AS col_0 FROM {ds2_table} LIMIT 5\n"
    )

    config = load_config(cfg_path)
    run_generate(config, only=["ds2"], force=True, upload=False, smoke=True, local_root=tmp_path / "data")
    assert list((tmp_path / "data" / "ds2").glob("*.parquet"))

    rows = run_benchmark(config, force=True)
    assert len(rows) == 2  # 1 cold + 1 warm
    assert all(r.latency_s >= 0 for r in rows)
    assert {r.warehouse_size for r in rows} == {"m6i.xlarge"}
    assert (tmp_path / "out.csv").exists()
