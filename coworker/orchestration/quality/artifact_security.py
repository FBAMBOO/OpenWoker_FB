"""Artifact filename, MIME, size, hash and authorization hard rails."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import PurePath
from typing import Iterable

from ..handoff_models import contains_secret_like


class ArtifactSecurityError(ValueError):
    pass


MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_CHUNK_BYTES = 1024 * 1024

_WINDOWS_DEVICES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}
_MIME = re.compile(r"^[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*$", re.I)
_HASH = re.compile(r"^(?:sha256:)?([0-9a-f]{64})$")
_EXECUTABLE_MIME_PREFIXES = (
    "application/x-executable",
    "application/x-msdownload",
    "application/x-sh",
    "application/vnd.microsoft.portable-executable",
    "text/html",
    "image/svg+xml",
)


def safe_filename(value: str) -> str:
    normalized = unicodedata.normalize("NFC", str(value).strip())
    if not normalized or len(normalized.encode("utf-8")) > 255:
        raise ArtifactSecurityError("artifact filename must be 1..255 UTF-8 bytes")
    if normalized in {".", ".."} or "\x00" in normalized:
        raise ArtifactSecurityError("artifact filename is invalid")
    if any(character in normalized for character in ("/", "\\", ":")):
        raise ArtifactSecurityError("artifact filename must not contain a path")
    if normalized.endswith((".", " ")):
        raise ArtifactSecurityError("artifact filename has an unsafe Windows suffix")
    if any(ord(character) < 32 or ord(character) == 127 for character in normalized):
        raise ArtifactSecurityError("artifact filename contains control characters")
    stem = normalized.split(".", 1)[0].upper()
    if stem in _WINDOWS_DEVICES:
        raise ArtifactSecurityError("artifact filename is a reserved Windows device name")
    return normalized


def safe_mime_type(value: str) -> str:
    normalized = str(value).strip().casefold()
    if len(normalized) > 255 or not _MIME.fullmatch(normalized):
        raise ArtifactSecurityError("artifact MIME type is invalid")
    return normalized


def normalize_sha256(value: str) -> str:
    match = _HASH.fullmatch(str(value).strip().casefold())
    if match is None:
        raise ArtifactSecurityError("expected a complete SHA-256 digest")
    return f"sha256:{match.group(1)}"


def digest_bytes(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(bytes(content)).hexdigest()


def validate_size(size: int, *, maximum: int = MAX_ARTIFACT_BYTES) -> int:
    chosen = int(size)
    if chosen < 0 or chosen > int(maximum):
        raise ArtifactSecurityError(
            f"artifact size {chosen} is outside the allowed 0..{int(maximum)} bytes"
        )
    return chosen


def validate_text_secret_boundary(content: bytes, mime_type: str) -> None:
    """Fail closed on high-confidence credential material in textual artifacts."""

    mime = safe_mime_type(mime_type)
    if not (mime.startswith("text/") or mime in {"application/json", "application/xml"}):
        return
    try:
        text = bytes(content).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ArtifactSecurityError("text artifact is not valid UTF-8") from exc
    if contains_secret_like(text):
        raise ArtifactSecurityError("artifact contains high-confidence secret material")


def preview_policy(filename: str, mime_type: str) -> dict[str, bool]:
    """Return UI-safe behavior; storing an executable never means executing it."""

    safe_filename(filename)
    mime = safe_mime_type(mime_type)
    executable = mime.startswith(_EXECUTABLE_MIME_PREFIXES) or PurePath(filename).suffix.casefold() in {
        ".exe",
        ".dll",
        ".com",
        ".bat",
        ".cmd",
        ".ps1",
        ".sh",
        ".html",
        ".htm",
        ".svg",
    }
    return {
        "download_allowed": True,
        "inline_preview_allowed": not executable,
        "execute_allowed": False,
    }


def authorize_artifact(
    *,
    owner_task_id: str,
    caller_task_id: str,
    artifact_id: str,
    allowed_artifact_ids: Iterable[str] | None = None,
) -> None:
    if str(owner_task_id) != str(caller_task_id):
        raise PermissionError("artifact is outside the caller task namespace")
    if allowed_artifact_ids is not None and artifact_id not in set(allowed_artifact_ids):
        raise PermissionError("artifact is not authorized by the frozen strategy binding")
