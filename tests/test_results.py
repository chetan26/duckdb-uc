from __future__ import annotations

import csv
from pathlib import Path

from benchmark.results_writer import FIELDNAMES


def test_published_results_schema(artifact_root: Path) -> None:
    path = artifact_root / "results" / "benchmark_results.csv"
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == FIELDNAMES
        rows = list(reader)
    assert rows, "published results CSV is empty"
    engines = {r["engine"] for r in rows}
    assert "duckdb" in engines
    assert "databricks_uc_serverless" in engines
    assert all(r["query_id"] for r in rows)
    assert all(r["run_type"] in {"cold", "warm"} for r in rows)
