"""Repository-analysis invariant areas, sections and strategy coverage groups."""

from __future__ import annotations


REPOSITORY_ANALYSIS_AREAS = (
    "entry",
    "models",
    "macros",
    "tests",
    "seeds",
    "snapshots",
    "deployment",
)

REPOSITORY_ANALYSIS_REQUIRED_SECTIONS = (
    "baseline_and_method",
    "architecture_overview",
    *REPOSITORY_ANALYSIS_AREAS,
    "relationships",
    "risks",
    "limitations",
)

REPOSITORY_ANALYSIS_COVERAGE_GROUPS = (
    {
        "id": "entry-models-lineage",
        "areas": ("entry", "models", "relationships"),
    },
    {
        "id": "macros-lifecycle",
        "areas": ("macros", "relationships"),
    },
    {
        "id": "quality-static-data",
        "areas": ("tests", "seeds", "snapshots"),
    },
    {
        "id": "deployment-control-plane",
        "areas": ("deployment", "entry", "relationships"),
    },
)

REPOSITORY_ANALYSIS_MARKERS = (
    "dbt_project.yml",
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
    "build.gradle",
    ".git",
)
