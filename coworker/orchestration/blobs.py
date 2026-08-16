"""Small content-addressed blob store for immutable orchestration evidence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class BlobRef:
    sha256: str
    size: int
    uri: str
    mime_type: str = "application/octet-stream"

    def as_dict(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "size": self.size,
            "uri": self.uri,
            "mime_type": self.mime_type,
        }


class BlobIntegrityError(RuntimeError):
    pass


class ContentAddressedBlobStore:
    """Write-once local blobs addressed by SHA-256.

    A temporary file is fsynced and atomically renamed. Existing content is verified,
    never overwritten, so evidence references remain stable across retries and restarts.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, digest: str) -> Path:
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("invalid sha256 digest")
        return self.root / digest[:2] / digest[2:4] / digest

    def put(self, content: bytes, *, mime_type: str = "application/octet-stream") -> BlobRef:
        data = bytes(content)
        digest = hashlib.sha256(data).hexdigest()
        target = self._path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.read_bytes() != data:
                raise BlobIntegrityError(f"digest collision or corrupted blob: {digest}")
        else:
            fd, raw = tempfile.mkstemp(prefix=".blob-", dir=target.parent)
            temp = Path(raw)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp, target)
                # fsyncing the file is not enough on POSIX: the directory entry created
                # by replace can still disappear after sudden power loss.
                if os.name != "nt":
                    directory_fd = os.open(target.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
            finally:
                if temp.exists():
                    temp.unlink()
        return BlobRef(digest, len(data), f"sha256:{digest}", mime_type)

    def put_json(self, value: Any) -> BlobRef:
        data = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return self.put(data, mime_type="application/json")

    def get(self, ref: BlobRef | str) -> bytes:
        digest = ref.sha256 if isinstance(ref, BlobRef) else str(ref).removeprefix("sha256:")
        data = self._path(digest).read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise BlobIntegrityError(f"blob hash mismatch: {digest}")
        return data

    def verify(self, ref: BlobRef) -> bool:
        try:
            return len(self.get(ref)) == ref.size
        except (OSError, BlobIntegrityError, ValueError):
            return False
