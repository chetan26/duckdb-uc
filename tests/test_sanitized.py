"""Refuse to ship credentials or a filled local config."""
from __future__ import annotations

import re
from pathlib import Path

SKIP_DIR_NAMES = {".git", ".venv", "__pycache__", ".pytest_cache", "data"}
SKIP_SUFFIXES = {".pyc", ".png", ".pdf", ".parquet"}

# Access-key and PAT shapes only. Do not list org or dataset names here.
SECRET_RES = [
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"dapi[a-f0-9]{32,}"),
    re.compile(r"arn:aws:iam::\d{12}:"),
]


def _iter_text_files(root: Path) -> list[Path]:
    files = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIR_NAMES for part in path.parts):
            continue
        if path.suffix in SKIP_SUFFIXES:
            continue
        if path.name in {".env", "config.yaml"}:
            continue
        files.append(path)
    return files


def test_no_embedded_secrets(artifact_root: Path) -> None:
    hits: list[str] = []
    for path in _iter_text_files(artifact_root):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pat in SECRET_RES:
            if pat.search(text):
                hits.append(f"{path.relative_to(artifact_root)}: {pat.pattern}")
    assert not hits, "credential-shaped strings:\n" + "\n".join(hits)


def test_gitignore_covers_secrets(artifact_root: Path) -> None:
    gi = (artifact_root / ".gitignore").read_text()
    assert ".env" in gi
    assert "config/config.yaml" in gi
