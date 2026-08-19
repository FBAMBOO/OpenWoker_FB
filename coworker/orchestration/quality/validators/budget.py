from ..models import BudgetStatus
from .base import Check, ValidationInputs, state


def validate(inputs: ValidationInputs) -> Check:
    valid = inputs.budget_integrity and inputs.budget_status not in {
        BudgetStatus.UNCONFIGURED,
        BudgetStatus.EXHAUSTED,
        BudgetStatus.OVER_BUDGET,
    }
    return Check("QG-016", state(valid), "budget-integrity-validator@1")
