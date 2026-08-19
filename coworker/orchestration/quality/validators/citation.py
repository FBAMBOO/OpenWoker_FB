from __future__ import annotations

from ...store import OrchestrationStore
from ..repository_snapshot import RepositorySnapshotService
from .base import Check, ValidationInputs, state


def validate(
    store: OrchestrationStore,
    snapshots: RepositorySnapshotService,
    inputs: ValidationInputs,
) -> Check:
    with store._read() as connection:
        rows = connection.execute(
            """
            SELECT e.* FROM orch_evidence_refs e
            JOIN orch_claims c ON c.id=e.claim_id
            WHERE c.artifact_id=?
            """,
            (inputs.artifact.id,),
        ).fetchall()
    valid_ids: list[str] = []
    for row in rows:
        if row["snapshot_id"] != inputs.snapshot.id:
            continue
        try:
            excerpt = snapshots.read_file_lines(
                inputs.snapshot.id,
                row["path"],
                start_line=row["line_start"],
                end_line=row["line_end"],
            )
        except (LookupError, OSError, ValueError):
            continue
        if (
            excerpt["blob_hash"] == row["blob_hash"]
            and excerpt["excerpt_hash"] == row["excerpt_hash"]
        ):
            valid_ids.append(row["id"])
    return Check(
        "QG-010",
        state(bool(rows) and len(valid_ids) == len(rows)),
        "citation-validator@1",
        tuple(valid_ids),
    )
