from __future__ import annotations

import re

from ..artifacts import ArtifactService
from ..models import ArtifactVersionStatus
from .base import Check, ValidationInputs, state


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def validate(artifacts: ArtifactService, inputs: ValidationInputs) -> Check:
    primary = next((item for item in inputs.contract.deliverables if item.primary), None)
    artifact = inputs.artifact
    shape = bool(
        primary
        and artifact.task_id == inputs.contract.task_id
        and artifact.logical_deliverable_id == primary.id
        and artifact.filename == primary.filename
        and artifact.mime_type == primary.mime_type
        and artifact.sha256
        and artifact.byte_size is not None
        and artifact.blob_uri
        and artifact.status
        in {ArtifactVersionStatus.DRAFT, ArtifactVersionStatus.VALIDATING, ArtifactVersionStatus.VERIFIED}
    )
    headings: set[str] = set()
    if shape and artifact.blob_uri:
        try:
            text = artifacts.blobs.get(artifact.blob_uri).decode("utf-8")
            headings = {
                _slug(line.lstrip("#").strip())
                for line in text.splitlines()
                if line.startswith("#")
            }
        except (OSError, UnicodeError, ValueError):
            shape = False
    required_sections = set(primary.required_sections if primary else ())
    section_ok = all(
        section in headings
        or section.replace("_and_", "_") in headings
        or any(section in heading or heading in section for heading in headings)
        for section in required_sections
    )
    return Check(
        "QG-012",
        state(shape and section_ok),
        "artifact-contract-validator@1",
        (artifact.id,) if shape else (),
    )
