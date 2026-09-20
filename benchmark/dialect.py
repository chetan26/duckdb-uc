"""SQL dialect adaptation. Queries are authored DuckDB-style
(`CAST(x AS TEXT)`). Databricks SQL has no TEXT type, so type keywords
are rewritten before warehouse execution. Semantics are unchanged.
"""
from __future__ import annotations

import re

_DATABRICKS_TYPE_MAP = {
    "TEXT": "STRING",
}

_CAST_TYPE_RE = re.compile(
    r"\bAS\s+(" + "|".join(_DATABRICKS_TYPE_MAP) + r")\b", re.IGNORECASE
)

# Databricks CAST(... AS DATE) is stricter than DuckDB's. TRY_CAST yields
# NULL on malformed values instead of aborting the statement.
_DATE_CAST_RE = re.compile(r"\bCAST\(([^()]+?)\s+AS\s+DATE\)", re.IGNORECASE)


def to_databricks_dialect(sql: str) -> str:
    def _replace_type(match: re.Match) -> str:
        found = match.group(1).upper()
        return f"AS {_DATABRICKS_TYPE_MAP[found]}"

    sql = _CAST_TYPE_RE.sub(_replace_type, sql)
    sql = _DATE_CAST_RE.sub(r"TRY_CAST(\1 AS DATE)", sql)
    return sql
