from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> tuple[Check, Check]:
    lineage = inputs.lineage_layers >= 3 and len(inputs.lineage_evidence_ids) >= 3
    control = bool(inputs.execution_control_evidence_ids)
    return (
        Check(
            "QG-005", state(lineage), "relationship-validator@1",
            inputs.lineage_evidence_ids,
        ),
        Check(
            "QG-006", state(control), "control-plane-validator@1",
            inputs.execution_control_evidence_ids,
        ),
    )
