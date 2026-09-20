"""Paper protocol pins in config.example.yaml."""
from __future__ import annotations

from pathlib import Path

import yaml


def test_example_config_matches_paper_pin(example_config_path: Path) -> None:
    raw = yaml.safe_load(example_config_path.read_text())
    duckdb = raw["duckdb"]
    run = raw["run"]
    assert duckdb["threads"] == 4
    assert duckdb["memory_limit"] == "12GB"
    assert duckdb["temp_directory"] == "/var/duckdb-tmp"
    assert duckdb["warehouse_size"] == "m6i.xlarge"
    assert run["cold_repetitions"] == 1
    assert run["warm_repetitions"] == 3
    assert run["restart_warehouse_for_cold_run"] is True


def test_duckdb_job_is_guaranteed_and_tainted(artifact_root: Path) -> None:
    raw = yaml.safe_load((artifact_root / "k8s" / "duckdb-bench-job.yaml").read_text())
    pod = raw["spec"]["template"]["spec"]
    assert pod["nodeSelector"]["node.kubernetes.io/instance-type"] == "m6i.xlarge"
    assert pod["nodeSelector"]["dedicated"] == "duckdb-bench"
    taint = pod["tolerations"][0]
    assert taint["key"] == "dedicated"
    assert taint["value"] == "duckdb-bench"
    assert taint["effect"] == "NoSchedule"
    resources = pod["containers"][0]["resources"]
    assert resources["requests"] == resources["limits"]
    assert resources["requests"]["cpu"] == "4"
    assert resources["requests"]["memory"] == "12Gi"
    assert resources["requests"]["ephemeral-storage"] == "20Gi"
    tmp = next(v for v in pod["volumes"] if v["name"] == "duckdb-tmp")
    assert tmp["emptyDir"]["sizeLimit"] == "20Gi"
    script = pod["containers"][0]["args"][0]
    assert "benchmark.cli run --config" in script
    assert "benchmark.cli run-duckdb --config" in script
    assert "rm -f /app/results/benchmark_results.csv" in script
    assert pod["serviceAccountName"] == "duckdb-uc-bench"

