from __future__ import annotations

import json

from ...store import OrchestrationStore
from .base import Check, ValidationInputs, state


def validate(store: OrchestrationStore, inputs: ValidationInputs) -> Check:
    with store._read() as connection:
        rows = connection.execute(
            """
            SELECT m.* FROM orch_inventory_metrics m
            JOIN orch_repository_inventories i ON i.id=m.inventory_id
            WHERE i.snapshot_id=?
            """,
            (inputs.snapshot.id,),
        ).fetchall()
    reconciled: list[str] = []
    for row in rows:
        if row["reconciles_to"] is None:
            continue
        subtotal = sum(float(value) for value in json.loads(row["subtotals_json"]).values())
        target = float(row["reconciles_to"])
        tolerance = float(row["tolerance"])
        if abs(subtotal - target) <= tolerance and abs(float(row["value"]) - target) <= tolerance:
            reconciled.append(row["id"])
    return Check(
        "QG-011",
        state(bool(rows) and len(reconciled) == len(rows)),
        "inventory-reconciliation-validator@1",
        tuple(reconciled),
    )
