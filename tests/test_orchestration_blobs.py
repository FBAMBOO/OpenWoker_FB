import json

import pytest

from coworker.orchestration.blobs import BlobIntegrityError, ContentAddressedBlobStore
from coworker.orchestration.models import EvidenceKind, TaskSpec
from coworker.orchestration.service import OrchestrationService


class FakeManager:
    default_workspace = None
    model = "gpt-5.6-sol"

    def _provider_configured(self, _provider: str) -> bool:
        return True

    def get_settings(self):
        return {
            "models": [self.model],
            "model_labels": {},
            "model_context_windows": {self.model: 400_000},
        }


def test_blob_store_is_content_addressed_and_deduplicated(tmp_path):
    store = ContentAddressedBlobStore(tmp_path / "blobs")
    first = store.put(b"evidence", mime_type="text/plain")
    second = store.put(b"evidence", mime_type="text/plain")
    assert first == second
    assert store.get(first) == b"evidence"
    assert store.verify(first)


def test_json_blob_is_canonical(tmp_path):
    store = ContentAddressedBlobStore(tmp_path / "blobs")
    a = store.put_json({"b": 2, "a": 1})
    b = store.put_json({"a": 1, "b": 2})
    assert a.sha256 == b.sha256
    assert json.loads(store.get(a)) == {"a": 1, "b": 2}


def test_corruption_is_detected(tmp_path):
    store = ContentAddressedBlobStore(tmp_path / "blobs")
    ref = store.put(b"original")
    target = store._path(ref.sha256)
    target.write_bytes(b"tampered")
    with pytest.raises(BlobIntegrityError):
        store.get(ref)
    assert not store.verify(ref)


def test_service_resolves_evidence_blob_with_indexed_store_lookup(tmp_path, monkeypatch):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task = service.store.create_task(TaskSpec("blob-lookup", "blob lookup"))
        ref = service.blobs.put(b"indexed evidence", mime_type="text/plain")
        evidence = service.store.add_evidence(
            task.id,
            kind=EvidenceKind.ARTIFACT,
            payload={"title": "Indexed artifact"},
            created_by="test",
            mime_type=ref.mime_type,
            content_hash=ref.sha256,
            blob_uri=ref.uri,
        )
        assert service.store.find_evidence_blob(ref.sha256) == evidence
        assert service.store.find_evidence_blob(f"sha256:{ref.sha256}") == evidence

        # Regression guard: blob authorization must not enumerate tasks/evidence.
        monkeypatch.setattr(
            service,
            "_all_tasks",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("task scan is forbidden")
            ),
        )
        assert service.get_blob(ref.sha256) == (b"indexed evidence", "text/plain")
    finally:
        service.store.close()
