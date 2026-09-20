"""Generate every published table as synthetic Parquet.

Column types cycle BIGINT / DOUBLE / VARCHAR / BOOLEAN / TIMESTAMP and are
named col_0..col_{n-1}. Tables are written as one or more local Parquet
files (never held fully in memory), then optionally uploaded with
`aws s3 sync`.

Only tables with `synthetic: true` in config.yaml are generated. Iceberg
twins are created later by `setup-managed-iceberg` / the sorted-Iceberg
converter, not here.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from benchmark.config import Config, TableConfig
from benchmark.query_builder import NARROW_SHAPES, WIDE_COLS, WIDE_ROWS

log = logging.getLogger(__name__)

COLUMN_TYPES = ("BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN", "TIMESTAMP")
_STRING_VOCAB = [f"SEG_{c}" for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
_BASE_EPOCH_DAY = 19723  # 2024-01-01

# Paper-scale defaults used when a table entry omits row/col/file knobs.
PAPER_SHAPES: dict[str, dict] = {
    name: {"num_rows": rows, "num_cols": cols, "num_files": 1 if rows < 50_000 else 8}
    for name, (rows, cols) in NARROW_SHAPES.items()
}
PAPER_SHAPES["ds9"] = {"num_rows": WIDE_ROWS, "num_cols": WIDE_COLS, "num_files": 16}

SMOKE_MAX_ROWS = 2_000


def column_type(index: int) -> str:
    return COLUMN_TYPES[index % 5]


def generate_column(index: int, num_rows: int, rng: np.random.Generator) -> pa.Array:
    kind = index % 5
    if kind == 0:
        return pa.array(rng.integers(0, 1_000_000, size=num_rows, dtype=np.int64))
    if kind == 1:
        return pa.array(rng.normal(loc=100.0, scale=25.0, size=num_rows))
    if kind == 2:
        idx = rng.integers(0, len(_STRING_VOCAB), size=num_rows)
        return pa.array(np.array(_STRING_VOCAB)[idx])
    if kind == 3:
        return pa.array(rng.integers(0, 2, size=num_rows).astype(bool))
    offsets = rng.integers(0, 730, size=num_rows)
    days = (_BASE_EPOCH_DAY + offsets).astype("datetime64[D]")
    return pa.array(days.astype("datetime64[s]"))


def generate_table(num_rows: int, num_cols: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    columns = {f"col_{i}": generate_column(i, num_rows, rng) for i in range(num_cols)}
    return pa.table(columns)


def write_parquet_parts(
    dest_dir: Path,
    num_rows: int,
    num_cols: int,
    num_files: int,
) -> int:
    """Write `num_files` snappy Parquet parts into dest_dir. Returns bytes written."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    rows_per_file = max(1, num_rows // num_files)
    total_bytes = 0
    for file_idx in range(num_files):
        is_last = file_idx == num_files - 1
        file_rows = num_rows - rows_per_file * (num_files - 1) if is_last else rows_per_file
        tbl = generate_table(file_rows, num_cols, seed=file_idx)
        out_path = dest_dir / f"part-{file_idx:05d}.snappy.parquet"
        pq.write_table(tbl, out_path, compression="snappy")
        size = out_path.stat().st_size
        total_bytes += size
        log.info("  wrote %s (%d rows, %.1f MB)", out_path.name, file_rows, size / 1e6)
    return total_bytes


def _run(cmd: list[str]) -> None:
    log.info("  $ %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _s3_prefix_has_data(bucket: str, prefix: str) -> bool:
    result = subprocess.run(
        ["aws", "s3", "ls", f"s3://{bucket}/{prefix}/"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and bool(result.stdout.strip())


def _table_shape(name: str, table: TableConfig, smoke: bool) -> tuple[int, int, int]:
    defaults = PAPER_SHAPES.get(name, {"num_rows": 10_000, "num_cols": 20, "num_files": 1})
    num_rows = int(table.extra.get("num_rows", defaults["num_rows"]))
    num_cols = int(table.extra.get("num_cols", defaults["num_cols"]))
    num_files = int(table.extra.get("num_files", defaults["num_files"]))
    if smoke:
        num_rows = min(num_rows, SMOKE_MAX_ROWS)
        num_files = 1
    return num_rows, num_cols, num_files


def generate_one_table(
    config: Config,
    table: TableConfig,
    dest_dir: Path,
    num_rows: int,
    num_cols: int,
    num_files: int,
    upload: bool,
    force: bool,
) -> None:
    dest_prefix = f"{config.root_prefix}/{table.path.strip('/')}" if config.root_prefix else table.path.strip("/")

    if dest_dir.exists() and any(dest_dir.glob("*.parquet")) and not force:
        log.info("Skipping %s: %s already has Parquet (pass --force to overwrite)", table.name, dest_dir)
    else:
        if dest_dir.exists() and force:
            for stale in dest_dir.glob("*.parquet"):
                stale.unlink()
        total_bytes = write_parquet_parts(dest_dir, num_rows, num_cols, num_files)
        log.info(
            "Generated %s: %d files, %d rows, %d cols, %.2f MB on disk (snappy)",
            table.name, num_files, num_rows, num_cols, total_bytes / 1e6,
        )

    if not upload:
        return
    if shutil.which("aws") is None:
        raise RuntimeError("`aws` not found on PATH -- install it or pass --no-upload.")
    if not force and _s3_prefix_has_data(config.bucket, dest_prefix):
        log.info(
            "Skipping upload of %s: s3://%s/%s/ already has data (pass --force to overwrite)",
            table.name, config.bucket, dest_prefix,
        )
        return
    _run(["aws", "s3", "sync", str(dest_dir) + "/", f"s3://{config.bucket}/{dest_prefix}/"])


def run_generate(
    config: Config,
    only: list[str] | None = None,
    force: bool = False,
    upload: bool = True,
    smoke: bool = False,
    local_root: Path | None = None,
) -> None:
    root = Path(local_root or config.local_data_dir or "data")
    targets = [
        (name, table)
        for name, table in config.tables.items()
        if table.extra.get("synthetic") and table.format == "parquet" and (not only or name in only)
    ]
    if not targets:
        raise RuntimeError(
            "No synthetic parquet tables found in config.yaml "
            f"(checked against: {sorted(only) if only else 'all tables'})."
        )

    for name, table in targets:
        num_rows, num_cols, num_files = _table_shape(name, table, smoke=smoke)
        dest_dir = root / table.path.strip("/")
        generate_one_table(
            config,
            table,
            dest_dir=dest_dir,
            num_rows=num_rows,
            num_cols=num_cols,
            num_files=num_files,
            upload=upload,
            force=force,
        )
