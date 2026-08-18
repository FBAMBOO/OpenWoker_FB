"""Safe, bounded payloads for the operator-visible per-run activity stream.

The activity stream is deliberately not a model chain-of-thought log.  Runtimes may
publish provider-supplied reasoning *summaries* and verifiable execution metadata, but
raw reasoning, tool results, file contents, and credentials never belong here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any


RUN_ACTIVITY_KINDS = frozenset(
    {"lifecycle", "reasoning_summary", "tool", "message", "usage", "error"}
)
RUN_ACTIVITY_STATUSES = frozenset(
    {"pending", "running", "completed", "failed", "canceled", "info"}
)
MAX_RUN_ACTIVITY_ROWS = 5_000

_SECRET_KEY_MARKERS = (
    "authorization",
    "apikey",
    "api_key",
    "auth_token",
    "access_token",
    "cookie",
    "password",
    "passwd",
    "secret",
    "credential",
)
_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s\"']+"),
    re.compile(r"(?i)\bsk-[a-z0-9_-]{12,}\b"),
    re.compile(r"(?i)((?:api[_ -]?key|access[_ -]?token|auth[_ -]?token|password|secret)\s*[:=]\s*)[^\s,;\"']+"),
)
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def bounded_activity_text(value: Any, limit: int) -> str:
    """Redact common credentials and cap UTF-8 size without splitting characters."""

    text = _CONTROL.sub("", str(value or ""))
    for pattern in _SECRET_PATTERNS:
        if pattern.groups:
            text = pattern.sub(r"\1[REDACTED]", text)
        else:
            text = pattern.sub("[REDACTED_SECRET]", text)
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return text
    suffix = "… [truncated]"
    room = max(0, limit - len(suffix.encode("utf-8")))
    return encoded[:room].decode("utf-8", errors="ignore") + suffix


def _secret_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_").strip()
    return (
        normalized == "token"
        or normalized.endswith("_token")
        or any(marker in normalized for marker in _SECRET_KEY_MARKERS)
    )


def sanitize_activity_detail(value: Any, *, depth: int = 0) -> Any:
    """Return a JSON-safe, redacted, size-bounded metadata value."""

    if depth >= 4:
        return "[TRUNCATED_DEPTH]"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for index, (raw_key, item) in enumerate(value.items()):
            if index >= 30:
                result["_truncated"] = True
                break
            key = bounded_activity_text(raw_key, 96)
            result[key] = (
                "[REDACTED]"
                if _secret_key(key)
                else sanitize_activity_detail(item, depth=depth + 1)
            )
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        sanitized = []
        for index, item in enumerate(value):
            if index >= 30:
                sanitized.append("[TRUNCATED_ITEMS]")
                break
            sanitized.append(sanitize_activity_detail(item, depth=depth + 1))
        return sanitized
    if isinstance(value, str):
        return bounded_activity_text(value, 2_048)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return bounded_activity_text(value, 512)
