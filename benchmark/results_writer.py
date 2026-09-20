"""CSV result schema shared across engines:

engine, format, table, query_id, run_type, attempt, latency_s, bytes_scanned,
cost_usd, warehouse_size, timestamp
"""
from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path

FIELDNAMES = [
    "engine",
    "format",
    "table",
    "query_id",
    "run_type",
    "attempt",
    "latency_s",
    "bytes_scanned",
    "cost_usd",
    "warehouse_size",
    "timestamp",
]


@dataclass
class ResultRow:
    engine: str
    format: str
    table: str
    query_id: str
    run_type: str  # "cold" | "warm"
    attempt: int
    latency_s: float
    bytes_scanned: int | None
    cost_usd: float | None
    warehouse_size: str
    timestamp: str | None = None


class ResultsWriter:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def existing_query_ids(self, engine: str) -> set[str]:
        if not self.path.exists():
            return set()
        with self.path.open(newline="") as f:
            reader = csv.DictReader(f)
            return {row["query_id"] for row in reader if row.get("engine") == engine}

    def append_rows(self, rows: list[ResultRow]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.path.exists()

        with self.path.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if write_header:
                writer.writeheader()
            for row in rows:
                d = asdict(row)
                if d["timestamp"] is None:
                    d["timestamp"] = _now_iso()
                writer.writerow(d)


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
