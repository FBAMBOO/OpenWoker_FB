from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> Check:
    receipt = inputs.read_receipt
    complete = bool(
        receipt
        and receipt.run_id == inputs.reviewer_run_id
        and receipt.artifact_id == inputs.artifact.id
        and receipt.artifact_hash == inputs.artifact.sha256
        and receipt.coverage_ratio == 1.0
        and receipt.covered_bytes == inputs.artifact.byte_size
        and receipt.completed_at is not None
        and receipt.completed_at > receipt.candidate_bound_at
    )
    return Check(
        "QG-013", state(complete), "complete-review-validator@1",
        (receipt.id,) if complete and receipt else (),
    )
