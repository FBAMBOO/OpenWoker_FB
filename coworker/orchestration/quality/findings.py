"""Typed finding creation, escalation, linting, fingerprinting and dedupe."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Iterable, Sequence

from .models import (
    Finding,
    FindingCategory,
    FindingInput,
    FindingStatus,
    Severity,
    finding_fingerprint,
)


_BLOCKING_CATEGORIES = frozenset(
    {
        FindingCategory.SECURITY,
        FindingCategory.SCHEMA,
        FindingCategory.BASELINE,
        FindingCategory.CITATION,
        FindingCategory.SUPPORT,
        FindingCategory.COVERAGE,
        FindingCategory.BUDGET,
    }
)
_PROBLEM_LANGUAGE = re.compile(
    r"(?i)(?:\b(?:missing|incorrect|invalid|unresolved|unsupported|truncated|incomplete|"
    r"failed|failure|does not|cannot verify|not verified|blocking)\b|"
    r"缺少|遗漏|错误|无效|未解决|不完整|截断|无法验证|阻断)"
)


def materialize_finding(
    draft: FindingInput,
    *,
    task_id: str,
    artifact_id: str,
    artifact_hash: str,
    required_v2: bool = True,
) -> Finding:
    locator = draft.requirement_id or draft.claim_id or draft.section_id or "unknown"
    policy_blocking = (
        draft.category in _BLOCKING_CATEGORIES
        and draft.severity in {Severity.CRITICAL, Severity.HIGH}
    )
    if required_v2 and draft.category is FindingCategory.SCHEMA:
        policy_blocking = True
    return Finding(
        id=f"finding_{uuid.uuid4().hex}",
        fingerprint=finding_fingerprint(draft.category, locator, draft.message),
        task_id=task_id,
        artifact_id=artifact_id,
        artifact_hash=artifact_hash,
        category=draft.category,
        severity=draft.severity,
        blocking=bool(draft.blocking or policy_blocking),
        repairable=draft.repairable,
        requirement_id=draft.requirement_id,
        claim_id=draft.claim_id,
        section_id=draft.section_id,
        message=draft.message,
        evidence_refs=draft.evidence_refs,
        suggested_fix=draft.suggested_fix,
        status=FindingStatus.OPEN,
        created_at=datetime.now(timezone.utc),
    )


def lint_unstructured_finding(
    *,
    summary: str,
    findings: Sequence[FindingInput],
    required_v2: bool,
) -> FindingInput | None:
    """Turn problem prose with an empty finding array into an explicit finding."""

    if findings or _PROBLEM_LANGUAGE.search(str(summary)) is None:
        return None
    return FindingInput(
        category=FindingCategory.SCHEMA,
        severity=Severity.HIGH if required_v2 else Severity.MEDIUM,
        blocking=required_v2,
        repairable=True,
        section_id="review_summary",
        message=(
            "Review summary describes a problem but findings[] is empty; the issue "
            "must be submitted as a typed finding."
        ),
        suggested_fix="Resubmit the review with one typed finding per identified issue.",
    )


def deduplicate_findings(findings: Iterable[Finding]) -> tuple[Finding, ...]:
    """Keep one authoritative finding per subject fingerprint without downgrading."""

    severity_rank = {
        Severity.INFO: 0,
        Severity.LOW: 1,
        Severity.MEDIUM: 2,
        Severity.HIGH: 3,
        Severity.CRITICAL: 4,
    }
    selected: dict[tuple[str, str, str], Finding] = {}
    for finding in findings:
        key = (finding.artifact_id, finding.artifact_hash, finding.fingerprint)
        current = selected.get(key)
        if current is None:
            selected[key] = finding
            continue
        if (
            finding.blocking and not current.blocking
        ) or severity_rank[finding.severity] > severity_rank[current.severity]:
            selected[key] = finding
    return tuple(sorted(selected.values(), key=lambda item: (item.created_at or datetime.min.replace(tzinfo=timezone.utc), item.id)))


def finding_delta(
    previous: Iterable[Finding], current: Iterable[Finding]
) -> dict[str, tuple[str, ...]]:
    before = {item.fingerprint for item in previous if item.status is FindingStatus.OPEN}
    after = {item.fingerprint for item in current if item.status is FindingStatus.OPEN}
    return {
        "resolved": tuple(sorted(before - after)),
        "new": tuple(sorted(after - before)),
        "unchanged": tuple(sorted(before & after)),
    }
