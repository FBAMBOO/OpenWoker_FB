from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> Check:
    return Check(
        "QG-001",
        state(not inputs.source_workspace_changes),
        "workspace-integrity@1",
        detail="source workspace changed" if inputs.source_workspace_changes else "unchanged",
    )
