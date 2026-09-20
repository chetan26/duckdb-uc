from __future__ import annotations

from pathlib import Path

import yaml

from benchmark.config import load_config
from benchmark.query_builder import NARROW_SHAPES, all_query_specs, measure_indexes
from benchmark.query_loader import load_queries


PUBLISHED_IDS = {
    "ds1_select_ordered_limit30",
    "ds2_select_ordered_limit30",
    "ds3_select_ordered_limit30",
    "ds4_select_ordered_limit30",
    "ds5_select_ordered_limit30",
    "ds6_select_raw_no_cast",
    "ds6_select_cast",
    "ds1_sum_aggregate",
    "ds7_select_ordered_limit1m",
    "ds3_sum_aggregate",
    "ds1_select_ordered_limit30_iceberg",
    "ds9_cold_count",
    "ds9_select_ordered_limit30",
    "ds9_select_ordered_limit30_offset450k",
    "ds9_select_ordered_limit30_wide_sort",
    "ds9_select_ordered_limit30_wide_sort_1000",
    "ds9_sum_aggregate",
    "ds9_column_pruning_select",
    "ds9_aggregate_far_columns",
    "ds9_select_ordered_limit30_iceberg_offset1k",
    "ds9_select_ordered_limit30_wide_sort_iceberg_offset1k",
    "ds10_cold_count",
    "ds10_select_ordered_limit30_wide_sort_1000_offset450k",
    "ds10_column_pruning_select",
}


def test_published_query_ids_present() -> None:
    ids = {s.id for s in all_query_specs()}
    missing = PUBLISHED_IDS - ids
    assert not missing, missing


def test_no_generic_column_leak_in_sql() -> None:
    for spec in all_query_specs():
        assert "CONVERSION_" not in spec.sql
        assert "SEGMENT_NAME" not in spec.sql
        assert "{" + spec.table + "_table}" in spec.sql or spec.sql.count("{") == 1


def test_narrow_sort_limit_uses_all_columns() -> None:
    specs = {s.id: s for s in all_query_specs()}
    for name, (_rows, cols) in NARROW_SHAPES.items():
        if name in {"ds6", "ds7"}:
            continue
        sql = specs[f"{name}_select_ordered_limit30"].sql
        assert f"col_{cols - 1}" in sql
        assert "LIMIT 30" in sql


def test_ds1_sum_uses_double_columns_only() -> None:
    specs = {s.id: s for s in all_query_specs()}
    sql = specs["ds1_sum_aggregate"].sql
    for i in measure_indexes(20):
        assert f"col_{i}" in sql
    assert "col_0" not in sql  # BIGINT, not a DOUBLE measure


def test_queries_yaml_matches_builder(artifact_root: Path) -> None:
    on_disk = yaml.safe_load((artifact_root / "benchmark" / "queries.yaml").read_text())
    built = all_query_specs()
    assert [q["id"] for q in on_disk] == [s.id for s in built]


def test_load_queries_resolves_placeholders(smoke_config: Path) -> None:
    config = load_config(smoke_config)
    duck = load_queries(config, dialect="duckdb")
    dbx = load_queries(config, dialect="databricks")
    assert duck
    assert len(duck) == len(dbx)
    parquet = next(q for q in duck if q.id == "ds1_select_ordered_limit30")
    assert "read_parquet" in parquet.sql
    assert "{ds1_table}" not in parquet.sql
    dbx_q = next(q for q in dbx if q.id == "ds1_select_ordered_limit30")
    assert "AS STRING" in dbx_q.sql
    assert "`bench_catalog`.`bench_schema`.`ds1`" in dbx_q.sql


def test_example_config_has_no_real_host(example_config_path: Path) -> None:
    text = example_config_path.read_text()
    assert "<workspace-host" in text
    assert "<your-benchmark-bucket>" in text
    assert "<account-id>" in text
