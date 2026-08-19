from __future__ import annotations

from ...store import OrchestrationStore
from .base import Check, ValidationInputs, state


def validate(store: OrchestrationStore, inputs: ValidationInputs) -> tuple[Check, Check, Check]:
    with store._read() as connection:
        claims = connection.execute(
            "SELECT id, claim_type, severity FROM orch_claims WHERE artifact_id=?",
            (inputs.artifact.id,),
        ).fetchall()
        supporting = {
            row["claim_id"]
            for row in connection.execute(
                """
                SELECT e.claim_id FROM orch_evidence_refs e
                JOIN orch_claims c ON c.id=e.claim_id
                WHERE c.artifact_id=? AND e.support='supports'
                """,
                (inputs.artifact.id,),
            ).fetchall()
        }
        negative = {
            row["claim_id"]: row
            for row in connection.execute(
                """
                SELECT n.* FROM orch_negative_evidence n
                JOIN orch_claims c ON c.id=n.claim_id
                WHERE c.artifact_id=?
                """,
                (inputs.artifact.id,),
            ).fetchall()
        }
    high = [row for row in claims if row["severity"] in {"high", "critical"}]
    support_ok = all(row["id"] in supporting for row in high)
    absence = [row for row in claims if row["claim_type"] == "absence"]
    negative_ok = all(
        row["id"] in negative
        and bool(negative[row["id"]]["query"])
        and bool(negative[row["id"]]["scope_paths_json"])
        and bool(negative[row["id"]]["query_result_hash"])
        and negative[row["id"]]["limitations_json"] not in {"", "[]"}
        for row in absence
    )
    limitations_ok = any(row["claim_type"] == "limitation" for row in claims)
    return (
        Check("QG-007", state(support_ok), "claim-support-validator@1", tuple(supporting)),
        Check("QG-008", state(negative_ok), "negative-evidence-validator@1", tuple(negative)),
        Check("QG-009", state(limitations_ok), "limitation-validator@1"),
    )
