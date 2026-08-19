from collections import defaultdict

from .base import Check, ValidationInputs, evidence_ids, state


def validate(inputs: ValidationInputs) -> tuple[Check, Check]:
    required: set[str] = set()
    for requirement in inputs.contract.requirements:
        raw = requirement.verification_spec.get("areas", ())
        if isinstance(raw, (list, tuple, set)):
            required.update(str(item) for item in raw)
    by_area = defaultdict(list)
    for result in inputs.coverage_results:
        by_area[result.area].append(result)
    status_pass = bool(required) and all(
        any(item.status.value == "pass" for item in by_area[area]) for area in required
    )
    evidence_pass = bool(required) and all(
        any(
            item.status.value == "pass" and item.evidence_count > 0 and item.claim_ids
            for item in by_area[area]
        )
        for area in required
    )
    claims = evidence_ids(
        claim for area in required for item in by_area[area] for claim in item.claim_ids
    )
    return (
        Check("QG-003", state(status_pass), "coverage-validator@1", claims),
        Check("QG-004", state(evidence_pass), "coverage-validator@1", claims),
    )
