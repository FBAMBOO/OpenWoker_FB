from ..models import FindingStatus
from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> Check:
    blocking = tuple(
        item.id for item in inputs.existing_findings
        if item.blocking and item.status is FindingStatus.OPEN
    )
    return Check("QG-014", state(not blocking), "finding-authority-validator@1", blocking)
