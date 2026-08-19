"""Optional static repository adapters for Task Quality V2."""

from .dbt_static import analyze_dbt_static

__all__ = ["analyze_dbt_static"]
