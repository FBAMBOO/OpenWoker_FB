"""Domain errors raised by the orchestration persistence core."""

from __future__ import annotations


class OrchestrationError(Exception):
    """Base class for errors callers may safely handle as domain failures."""


class NotFoundError(OrchestrationError):
    """A requested orchestration entity does not exist."""


class ConflictError(OrchestrationError):
    """The requested mutation conflicts with durable state."""


class VersionConflict(ConflictError):
    """An optimistic version precondition did not match."""


class InvalidTransition(ConflictError):
    """A state-machine transition is not permitted."""


class IdempotencyConflict(ConflictError):
    """A command or idempotency key was reused with different input."""


class LeaseConflict(ConflictError):
    """A run mutation did not hold the current lease and fencing token."""


class GateConflict(ConflictError):
    """A gate was already resolved or its version was stale."""


class DAGValidationError(OrchestrationError):
    """A proposed plan is not a valid directed acyclic graph."""


class MigrationError(OrchestrationError):
    """The on-disk schema history cannot be safely migrated."""


class IntegrityError(OrchestrationError):
    """Persisted orchestration data failed an integrity check."""
