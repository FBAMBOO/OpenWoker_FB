from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> Check:
    snapshot = inputs.snapshot
    valid = bool(
        snapshot.task_id == inputs.contract.task_id
        and snapshot.content_hash
        and snapshot.manifest_hash
        and snapshot.resolution_reason
        and (
            snapshot.commit_oid
            or snapshot.overlay_hash
            or snapshot.directory_pack_hash
        )
    )
    return Check(
        "QG-002",
        state(valid),
        "baseline-validator@1",
        evidence_ids=(snapshot.manifest_artifact_id,),
        detail=snapshot.resolution_reason,
    )
