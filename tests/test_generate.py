from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from benchmark.generate_synthetic import column_type, generate_table, write_parquet_parts


def test_column_type_cycle() -> None:
    assert [column_type(i) for i in range(5)] == [
        "BIGINT",
        "DOUBLE",
        "VARCHAR",
        "BOOLEAN",
        "TIMESTAMP",
    ]
    assert column_type(5) == "BIGINT"


def test_generate_table_shape_and_names() -> None:
    table = generate_table(num_rows=50, num_cols=11, seed=0)
    assert table.num_rows == 50
    assert table.num_columns == 11
    assert table.column_names == [f"col_{i}" for i in range(11)]
    assert table.schema.field("col_0").type == pa.int64()
    assert table.schema.field("col_1").type == pa.float64()
    assert table.schema.field("col_2").type == pa.string()
    assert table.schema.field("col_3").type == pa.bool_()


def test_write_parquet_parts_splits_rows(tmp_path: Path) -> None:
    dest = tmp_path / "ds"
    write_parquet_parts(dest, num_rows=25, num_cols=5, num_files=2)
    parts = sorted(dest.glob("*.parquet"))
    assert len(parts) == 2
    rows = sum(pq.read_table(p).num_rows for p in parts)
    assert rows == 25
    first = pq.read_table(parts[0])
    assert first.column_names == [f"col_{i}" for i in range(5)]


def test_same_seed_is_deterministic() -> None:
    a = generate_table(20, 4, seed=7)
    b = generate_table(20, 4, seed=7)
    assert a.equals(b)
