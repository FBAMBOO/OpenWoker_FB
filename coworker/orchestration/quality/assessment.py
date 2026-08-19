"""Deterministic three-axis task assessment used before model admission."""

from __future__ import annotations

import math
from typing import Mapping

from .models import Assessment, RequirementCategory, TaskContractV2


def _clamp(value: float) -> int:
    return max(0, min(100, int(round(value))))


def assess_task(
    contract: TaskContractV2,
    *,
    file_count: int = 0,
    total_bytes: int = 0,
    project_count: int = 1,
    inventory: Mapping[str, object] | None = None,
) -> Assessment:
    """Score complexity, operational risk and evidence workload independently.

    Read-only constraints lower only operational risk.  They deliberately do not
    erase reasoning depth or the amount of evidence that must be inspected.
    """

    required = tuple(item for item in contract.requirements if item.required)
    categories = {item.category for item in required}
    areas: set[str] = set()
    for requirement in required:
        raw = requirement.verification_spec.get("areas")
        if isinstance(raw, (list, tuple, set)):
            areas.update(str(item) for item in raw)

    cognitive = 12 + len(categories) * 5 + min(20, len(required) * 2)
    if RequirementCategory.RELATIONSHIP in categories:
        cognitive += 12
    if RequirementCategory.EVIDENCE in categories:
        cognitive += 8
    if len(areas) >= 7:
        cognitive += 8

    constraints = {item.type: item.value for item in contract.constraints}
    operational = 8
    if constraints.get("source_workspace_write") is True:
        operational += 35
    if constraints.get("external_write") is True:
        operational += 35
    if constraints.get("network_access") is True:
        operational += 15
    if constraints.get("source_workspace_write") is False:
        operational -= 3
    if any(item.category is RequirementCategory.SAFETY for item in required):
        operational += 5

    inventory = dict(inventory or {})
    observed_files = max(int(file_count), int(inventory.get("file_count") or 0))
    observed_bytes = max(int(total_bytes), int(inventory.get("total_bytes") or 0))
    workload = 5 + len(areas) * 5 + max(0, int(project_count) - 1) * 8
    if observed_files:
        workload += min(30, math.log2(observed_files + 1) * 3.5)
    if observed_bytes:
        workload += min(20, math.log2(observed_bytes + 1) * 0.8)
    if any(item.verification_method.value == "citation" for item in required):
        workload += 8
    if any(item.verification_method.value == "inventory_reconcile" for item in required):
        workload += 6

    rationale = (
        f"{len(required)} required requirements across {len(categories)} semantic categories.",
        f"Permissions: source_write={constraints.get('source_workspace_write')}, "
        f"external_write={constraints.get('external_write')}, network={constraints.get('network_access')}.",
        f"Evidence scope: {observed_files} files, {observed_bytes} bytes, "
        f"{max(1, int(project_count))} project(s), {len(areas)} required area(s).",
    )
    return Assessment(
        cognitive_complexity=_clamp(cognitive),
        operational_risk=_clamp(operational),
        evidence_workload=_clamp(workload),
        rationale=rationale,
    )
