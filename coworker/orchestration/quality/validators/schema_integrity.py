from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> Check:
    primary = next((item for item in inputs.contract.deliverables if item.primary), None)
    valid = bool(
        inputs.result_schema_valid
        and primary
        and inputs.result_schema_id == primary.result_schema_id
        and inputs.contract.schema_version == 2
    )
    return Check("QG-015", state(valid), "schema-integrity-validator@1")
