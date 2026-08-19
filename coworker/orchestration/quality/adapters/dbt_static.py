"""Conservative static dbt/Fabric inventory and relationship adapter."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any, Callable, Iterable


_REF = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_SOURCE = re.compile(
    r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
)
_MACRO = re.compile(r"\{%\s*macro\s+([A-Za-z_][A-Za-z0-9_]*)")


def analyze_dbt_static(
    paths: Iterable[str],
    *,
    project_root: str,
    read_text: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """Return static counts/edges without claiming dbt compilation equivalence."""

    root = "" if project_root in {"", "."} else project_root.rstrip("/") + "/"
    normalized = tuple(sorted(str(path) for path in paths))
    models = [
        path
        for path in normalized
        if path.startswith(root + "models/") and path.casefold().endswith(".sql")
    ]
    macros = [
        path
        for path in normalized
        if path.startswith(root + "macros/") and path.casefold().endswith(".sql")
    ]
    tests = [
        path
        for path in normalized
        if path.startswith(root + "tests/") and path.casefold().endswith(".sql")
    ]
    seeds = [
        path
        for path in normalized
        if path.startswith(root + "seeds/")
        and PurePosixPath(path).suffix.casefold() in {".csv", ".tsv", ".json"}
    ]
    snapshots = [
        path
        for path in normalized
        if path.startswith(root + "snapshots/") and path.casefold().endswith(".sql")
    ]
    pipelines = [
        path
        for path in normalized
        if (
            path.startswith(".azure-pipelines/")
            or "/.azure-pipelines/" in path
            or path.startswith(".github/workflows/")
        )
        and PurePosixPath(path).suffix.casefold() in {".yml", ".yaml"}
    ]
    entries = [path for path in normalized if path == root + "dbt_project.yml"]
    edges: list[dict[str, str]] = []
    macro_definitions: list[dict[str, str]] = []
    if read_text is not None:
        for path in (*models, *snapshots, *macros):
            try:
                text = read_text(path)
            except (OSError, UnicodeError, ValueError):
                continue
            for target in _REF.findall(text):
                edges.append({"from": path, "kind": "ref", "to": target})
            for source_name, table_name in _SOURCE.findall(text):
                edges.append(
                    {
                        "from": path,
                        "kind": "source",
                        "to": f"{source_name}.{table_name}",
                    }
                )
            for macro in _MACRO.findall(text):
                macro_definitions.append({"name": macro, "path": path})
    return {
        "adapter": "dbt-static@1",
        "project_root": project_root,
        "project_markers": entries,
        "counts": {
            "models": len(models),
            "macro_sql": len(macros),
            "sql_tests": len(tests),
            "seeds": len(seeds),
            "snapshots": len(snapshots),
            "pipeline_yaml": len(pipelines),
        },
        "paths": {
            "models": models,
            "macros": macros,
            "tests": tests,
            "seeds": seeds,
            "snapshots": snapshots,
            "pipelines": pipelines,
        },
        "edges": edges,
        "macro_definitions": macro_definitions,
        "limitations": [
            "Static parsing is not equivalent to dbt parse/compile or a manifest generated for this exact snapshot.",
            "Dynamic Jinja, dispatch and runtime environment behavior may not be visible.",
        ],
    }
