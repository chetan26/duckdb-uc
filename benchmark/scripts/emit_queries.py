"""Write benchmark/queries.yaml from query_builder (source of truth)."""
from __future__ import annotations

from pathlib import Path

from benchmark.query_builder import render_yaml


def main() -> None:
    dest = Path(__file__).resolve().parents[1] / "queries.yaml"
    dest.write_text(render_yaml())
    print(f"Wrote {dest}")


if __name__ == "__main__":
    main()
