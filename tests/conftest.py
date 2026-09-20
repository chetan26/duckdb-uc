from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ARTIFACT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def artifact_root() -> Path:
    return ARTIFACT_ROOT


@pytest.fixture
def example_config_path() -> Path:
    return ARTIFACT_ROOT / "config" / "config.example.yaml"


@pytest.fixture
def smoke_config(tmp_path: Path, example_config_path: Path) -> Path:
    raw = yaml.safe_load(example_config_path.read_text())
    raw["databricks"]["host"] = "example.cloud.databricks.com"
    raw["databricks"]["http_path"] = "/sql/1.0/warehouses/example"
    raw["s3"]["bucket"] = "example-bucket"
    raw["s3"]["local_data_dir"] = str(tmp_path / "data")
    raw["duckdb"]["temp_directory"] = str(tmp_path / "duckdb-tmp")
    raw["run"]["results_path"] = str(tmp_path / "results.csv")
    raw["run"]["cold_repetitions"] = 1
    raw["run"]["warm_repetitions"] = 1
    dest = tmp_path / "config.yaml"
    dest.write_text(yaml.safe_dump(raw))
    return dest
