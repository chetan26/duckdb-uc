"""Build the published query set from table shapes.

Every table uses generic columns `col_0`..`col_{n-1}` whose types cycle
BIGINT / DOUBLE / VARCHAR / BOOLEAN / TIMESTAMP (see generate_synthetic).
Queries are authored DuckDB-style (`CAST(... AS TEXT)`); the Databricks
runner rewrites TEXT -> STRING and DATE CASTs to TRY_CAST.
"""
from __future__ import annotations

from dataclasses import dataclass

# CAST targets used in the shared SQL. TIMESTAMP columns are projected as
# DATE so the dialect rewrite (TRY_CAST for DATE) stays in play.
CAST_TARGETS = ("BIGINT", "DOUBLE", "TEXT", "BOOLEAN", "DATE")

# Narrow tables in the published paper (ds8 is out of scope).
NARROW_SHAPES: dict[str, tuple[int, int]] = {
    "ds1": (4_424_286, 20),
    "ds2": (679, 20),
    "ds3": (777_156, 17),
    "ds4": (1_404_840, 16),
    "ds5": (30_726, 12),
    "ds6": (7_906, 11),
    "ds7": (11_512, 11),
}

WIDE_ROWS = 500_000
WIDE_COLS = 1000


def cast_target(index: int) -> str:
    return CAST_TARGETS[index % 5]


def measure_indexes(num_cols: int) -> list[int]:
    """DOUBLE columns (index % 5 == 1), used by SUM queries."""
    return [i for i in range(num_cols) if i % 5 == 1]


def select_cast_list(num_cols: int) -> str:
    return ", ".join(
        f"CAST(col_{i} AS {cast_target(i)}) AS col_{i}" for i in range(num_cols)
    )


def select_raw_list(num_cols: int) -> str:
    return ", ".join(f"col_{i}" for i in range(num_cols))


def order_by_list(num_cols: int) -> str:
    return ", ".join(f"col_{i} ASC" for i in range(num_cols))


def table_placeholder(table: str) -> str:
    return "{" + f"{table}_table" + "}"


def sort_limit_sql(table: str, num_cols: int, limit: int = 30, offset: int = 0) -> str:
    return (
        f"SELECT {select_cast_list(num_cols)}\n"
        f"FROM {table_placeholder(table)}\n"
        f"ORDER BY {order_by_list(num_cols)}\n"
        f"LIMIT {limit} OFFSET {offset}"
    )


def raw_limit_sql(table: str, num_cols: int, limit: int = 30, offset: int = 0) -> str:
    return (
        f"SELECT {select_raw_list(num_cols)}\n"
        f"FROM {table_placeholder(table)}\n"
        f"ORDER BY {order_by_list(num_cols)}\n"
        f"LIMIT {limit} OFFSET {offset}"
    )


def sum_sql(table: str, indexes: list[int], limit: int = 1000, offset: int = 0) -> str:
    selects = ", ".join(f"SUM(CAST(col_{i} AS DOUBLE)) AS col_{i}" for i in indexes)
    order = ", ".join(f"col_{i} ASC" for i in indexes)
    return (
        f"SELECT {selects}\n"
        f"FROM {table_placeholder(table)}\n"
        f"ORDER BY {order}\n"
        f"LIMIT {limit} OFFSET {offset}"
    )


def count_sql(table: str) -> str:
    return f"SELECT count(*) AS row_count\nFROM {table_placeholder(table)}"


def prune_sql(table: str) -> str:
    return (
        f"SELECT col_3, col_500, col_887\n"
        f"FROM {table_placeholder(table)}\n"
        f"LIMIT 1000"
    )


def far_aggregate_sql(table: str) -> str:
    return (
        f"SELECT avg(col_11) AS avg_col_11, max(col_886) AS max_col_886\n"
        f"FROM {table_placeholder(table)}"
    )


def star_sort_sql(table: str, num_cols: int, limit: int = 30, offset: int = 0) -> str:
    return (
        f"SELECT *\n"
        f"FROM {table_placeholder(table)}\n"
        f"ORDER BY {order_by_list(num_cols)}\n"
        f"LIMIT {limit} OFFSET {offset}"
    )


@dataclass(frozen=True)
class QuerySpec:
    id: str
    table: str
    description: str
    sql: str


def _narrow_queries() -> list[QuerySpec]:
    specs: list[QuerySpec] = []
    for name, (rows, cols) in NARROW_SHAPES.items():
        row_label = f"{rows:,}-row"
        if name == "ds6":
            specs.append(
                QuerySpec(
                    id="ds6_select_raw_no_cast",
                    table="ds6",
                    description=(
                        f"11-col / {row_label} table: raw projection, no explicit CAST"
                    ),
                    sql=raw_limit_sql("ds6", cols, limit=30),
                )
            )
            specs.append(
                QuerySpec(
                    id="ds6_select_cast",
                    table="ds6",
                    description=(
                        "Same source as ds6_select_raw_no_cast, CAST-wrapped variant"
                    ),
                    sql=sort_limit_sql("ds6", cols, limit=30),
                )
            )
            continue
        if name == "ds7":
            specs.append(
                QuerySpec(
                    id="ds7_select_ordered_limit1m",
                    table="ds7",
                    description=(
                        f"11-col / {row_label} table: full CAST projection, "
                        "ORDER BY, LIMIT 1,000,000"
                    ),
                    sql=sort_limit_sql("ds7", cols, limit=1_000_000),
                )
            )
            continue
        specs.append(
            QuerySpec(
                id=f"{name}_select_ordered_limit30",
                table=name,
                description=(
                    f"{cols}-col / {row_label} table: full CAST projection, "
                    "ORDER BY, LIMIT 30"
                ),
                sql=sort_limit_sql(name, cols, limit=30),
            )
        )

    specs.append(
        QuerySpec(
            id="ds1_sum_aggregate",
            table="ds1",
            description="SUM over DOUBLE measure columns of ds1",
            sql=sum_sql("ds1", measure_indexes(NARROW_SHAPES["ds1"][1])),
        )
    )
    specs.append(
        QuerySpec(
            id="ds3_sum_aggregate",
            table="ds3",
            description="SUM over DOUBLE measure columns of ds3",
            sql=sum_sql("ds3", measure_indexes(NARROW_SHAPES["ds3"][1])),
        )
    )
    return specs


def _iceberg_twin(spec: QuerySpec) -> QuerySpec:
    iceberg_table = f"{spec.table}_iceberg"
    return QuerySpec(
        id=f"{spec.id}_iceberg",
        table=iceberg_table,
        description=f"{spec.description} (Iceberg)",
        sql=spec.sql.replace(table_placeholder(spec.table), table_placeholder(iceberg_table)),
    )


def _offset_label(offset: int) -> str:
    return {
        0: "",
        1_000: ", OFFSET 1000",
        100_000: ", OFFSET 100000",
        450_000: ", OFFSET 450000",
    }[offset]


def _wide_family(table: str, query_prefix: str, id_suffix: str = "") -> list[QuerySpec]:
    """Wide-table query family for ds9 (Parquet), ds9_iceberg, and ds10.

    Query ids keep the published names: ds9_select_ordered_limit30_iceberg_offset1k
    (suffix before the offset tag), ds10_select_ordered_limit30_offset1k (no extra
    suffix -- ds10 is already the sorted Iceberg table).
    """
    label = "1000-col synthetic table"
    offsets = (
        (0, ""),
        (1_000, "_offset1k"),
        (100_000, "_offset100k"),
        (450_000, "_offset450k"),
    )
    specs: list[QuerySpec] = [
        QuerySpec(
            id=f"{query_prefix}_cold_count{id_suffix}",
            table=table,
            description=f"{label}: count(*) scan baseline",
            sql=count_sql(table),
        )
    ]
    for offset, off_sfx in offsets:
        desc_off = _offset_label(offset)
        specs.append(
            QuerySpec(
                id=f"{query_prefix}_select_ordered_limit30{id_suffix}{off_sfx}",
                table=table,
                description=(
                    f"{label}: CAST projection + ORDER BY + LIMIT 30 over "
                    f"a 20-col slice (col_0..col_19){desc_off}"
                ),
                sql=sort_limit_sql(table, 20, limit=30, offset=offset),
            )
        )
    for offset, off_sfx in offsets:
        desc_off = _offset_label(offset)
        specs.append(
            QuerySpec(
                id=f"{query_prefix}_select_ordered_limit30_wide_sort{id_suffix}{off_sfx}",
                table=table,
                description=(
                    f"{label}: CAST projection + ORDER BY + LIMIT 30 over "
                    f"a 200-col slice (col_0..col_199){desc_off}"
                ),
                sql=sort_limit_sql(table, 200, limit=30, offset=offset),
            )
        )
    for offset, off_sfx in offsets:
        desc_off = _offset_label(offset)
        specs.append(
            QuerySpec(
                id=f"{query_prefix}_select_ordered_limit30_wide_sort_1000{id_suffix}{off_sfx}",
                table=table,
                description=(
                    f"{label}: SELECT * + ORDER BY all {WIDE_COLS} columns "
                    f"+ LIMIT 30{desc_off}"
                ),
                sql=star_sort_sql(table, WIDE_COLS, limit=30, offset=offset),
            )
        )
    wide_measures = [1, 6, 11, 16, 21]
    for offset, off_sfx in offsets:
        desc_off = _offset_label(offset)
        specs.append(
            QuerySpec(
                id=f"{query_prefix}_sum_aggregate{id_suffix}{off_sfx}",
                table=table,
                description=f"{label}: SUM over 5 DOUBLE columns{desc_off}",
                sql=sum_sql(table, wide_measures, offset=offset),
            )
        )
    specs.append(
        QuerySpec(
            id=f"{query_prefix}_column_pruning_select{id_suffix}",
            table=table,
            description=f"{label}: 3 of 1000 far-apart columns (prune probe)",
            sql=prune_sql(table),
        )
    )
    specs.append(
        QuerySpec(
            id=f"{query_prefix}_aggregate_far_columns{id_suffix}",
            table=table,
            description=f"{label}: avg/max over 2 far-apart DOUBLE columns",
            sql=far_aggregate_sql(table),
        )
    )
    return specs


def all_query_specs() -> list[QuerySpec]:
    narrow = _narrow_queries()
    iceberg_narrow = [_iceberg_twin(s) for s in narrow]
    return (
        narrow
        + iceberg_narrow
        + _wide_family("ds9", "ds9")
        + _wide_family("ds9_iceberg", "ds9", "_iceberg")
        + _wide_family("ds10", "ds10")
    )


def render_yaml(specs: list[QuerySpec] | None = None) -> str:
    """Render the shared queries.yaml. SQL is folded as `>` blocks."""
    specs = specs or all_query_specs()
    chunks = [
        "# Shared query set for the DuckDB vs Unity Catalog Serverless SQL",
        "# benchmark. Every table is synthetic. Columns are col_0..col_N",
        "# with types cycling BIGINT / DOUBLE / VARCHAR / BOOLEAN / TIMESTAMP.",
        "# SQL is DuckDB-style (CAST(... AS TEXT)); the Databricks runner",
        "# rewrites TEXT -> STRING (benchmark/dialect.py).",
        "",
    ]
    for spec in specs:
        indented_sql = "\n".join(f"    {line}" if line else "" for line in spec.sql.splitlines())
        chunks.append(f"- id: {spec.id}")
        chunks.append("  table: " + spec.table)
        chunks.append(f'  description: "{spec.description}"')
        chunks.append("  sql: >")
        chunks.append(indented_sql)
        chunks.append("")
    return "\n".join(chunks)
