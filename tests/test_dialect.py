from benchmark.dialect import to_databricks_dialect


def test_text_becomes_string() -> None:
    sql = "SELECT CAST(col_2 AS TEXT) AS col_2 FROM t"
    assert "AS STRING" in to_databricks_dialect(sql)
    assert "AS TEXT" not in to_databricks_dialect(sql)


def test_date_cast_becomes_try_cast() -> None:
    sql = "SELECT CAST(col_4 AS DATE) AS col_4 FROM t"
    assert to_databricks_dialect(sql) == "SELECT TRY_CAST(col_4 AS DATE) AS col_4 FROM t"


def test_bigint_and_double_unchanged() -> None:
    sql = "SELECT CAST(col_0 AS BIGINT), CAST(col_1 AS DOUBLE) FROM t"
    assert to_databricks_dialect(sql) == sql
