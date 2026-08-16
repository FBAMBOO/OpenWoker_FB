from __future__ import annotations

import copy
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import coworker.orchestration.catalogs as catalogs_module
from coworker.orchestration.catalogs import CatalogConflict, ConfigurationCatalog


def test_catalog_close_releases_lifetime_lock_handle(tmp_path):
    path = tmp_path / "catalog.json"
    catalog = ConfigurationCatalog(path)
    lock_path = path.with_name(f".{path.name}.lock")
    assert lock_path.exists()

    catalog.close()
    lock_path.unlink()
    assert not lock_path.exists()


def test_profile_draft_publish_is_versioned_etagged_and_persistent(tmp_path):
    path = tmp_path / "catalog.json"
    catalog = ConfigurationCatalog(path)

    cloned = catalog.clone_profile(
        "worker",
        "safe-worker",
        overrides={"display_name": "Safe worker"},
    )
    assert cloned["current"] is None
    assert cloned["draft"]["etag"].startswith('"')
    etag = cloned["draft"]["etag"]

    published = catalog.publish_profile("safe-worker", expected_etag=etag)
    assert published["current"]["version"] == 1
    assert published["current"]["content_hash"]
    assert published["derived_from"]["profile_id"] == "worker"

    reloaded = ConfigurationCatalog(path)
    frozen = reloaded.resolve_profile("safe-worker", 1)
    assert frozen.display_name == "Safe worker"
    assert frozen.content_hash == published["current"]["content_hash"]


def test_builtin_configuration_is_immutable_and_policy_simulation_input_validates(tmp_path):
    catalog = ConfigurationCatalog(tmp_path / "catalog.json")
    with pytest.raises(CatalogConflict, match="builtin"):
        catalog.create_profile_draft("reviewer")
    report = catalog.validate_policy(
        {
            "schema_version": 1,
            "policy_id": "bounded",
            "require_verified": True,
            "allow_unknown_cost": False,
            "allowed_providers": ["openai"],
            "allowed_models": [],
            "blocked_models": [],
            "fallback_limit": 2,
            "fallback_for_explicit": False,
        }
    )
    assert report["valid"] is True


def test_cloned_policy_response_includes_immutable_provenance(tmp_path):
    catalog = ConfigurationCatalog(tmp_path / "catalog.json")

    cloned = catalog.clone_policy("quality-first", "bounded-quality")

    assert cloned["current"] is None
    assert cloned["draft"]["etag"].startswith('"')
    assert cloned["derived_from"]["policy_id"] == "quality-first"
    assert cloned["derived_from"]["version"] == 1
    assert len(cloned["derived_from"]["content_hash"]) == 64


def test_failed_profile_replace_does_not_false_commit_memory_or_etag(
    tmp_path, monkeypatch
):
    path = tmp_path / "catalog.json"
    catalog = ConfigurationCatalog(path)
    created = catalog.clone_profile(
        "worker",
        "replace-failure-worker",
        overrides={"display_name": "Before replacement"},
    )
    before = copy.deepcopy(created)
    changed_spec = {
        **before["draft"]["spec"],
        "display_name": "Must not leak into memory",
    }

    def fail_replace(_source, _destination):
        raise OSError("injected os.replace failure")

    with monkeypatch.context() as fault:
        fault.setattr(catalogs_module.os, "replace", fail_replace)
        with pytest.raises(OSError, match="replace failure"):
            catalog.save_profile_draft(
                "replace-failure-worker",
                changed_spec,
                expected_etag=before["draft"]["etag"],
            )

    in_memory = catalog.get_profile("replace-failure-worker")
    on_disk = ConfigurationCatalog(path).get_profile("replace-failure-worker")
    assert in_memory["draft"] == before["draft"]
    assert on_disk["draft"] == before["draft"]
    assert not list(tmp_path.glob(".catalog.json.*.tmp"))

    # The original ETag remains authoritative and can be retried after the fault.
    saved = catalog.save_profile_draft(
        "replace-failure-worker",
        changed_spec,
        expected_etag=before["draft"]["etag"],
    )
    assert saved["draft"]["spec"]["display_name"] == "Must not leak into memory"


def test_failed_policy_write_keeps_draft_and_versions_consistent(
    tmp_path, monkeypatch
):
    path = tmp_path / "catalog.json"
    catalog = ConfigurationCatalog(path)
    created = catalog.clone_policy("quality-first", "write-failure-policy")
    before = copy.deepcopy(created)

    real_open = catalogs_module.Path.open

    class FailingWriteStream:
        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            return False

        def write(self, _data):
            raise OSError("injected catalog write failure")

    def fail_temp_write(file_path, mode="r", *args, **kwargs):
        if mode == "wb" and file_path.name.startswith(".catalog.json."):
            return FailingWriteStream()
        return real_open(file_path, mode, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(catalogs_module.Path, "open", fail_temp_write)
        with pytest.raises(OSError, match="write failure"):
            catalog.publish_policy(
                "write-failure-policy",
                expected_etag=before["draft"]["etag"],
            )

        # Inspect both views while the failure remains injected; reads still work.
        in_memory = catalog.get_policy("write-failure-policy")
        on_disk = ConfigurationCatalog(path).get_policy("write-failure-policy")

    assert in_memory["draft"] == before["draft"]
    assert in_memory["versions"] == before["versions"] == []
    assert on_disk["draft"] == before["draft"]
    assert on_disk["versions"] == before["versions"] == []

    published = catalog.publish_policy(
        "write-failure-policy",
        expected_etag=before["draft"]["etag"],
    )
    assert published["current"]["version"] == 1
    assert published["draft"] is None


def test_independent_catalog_instances_do_not_lose_concurrent_writes(tmp_path):
    path = tmp_path / "catalog.json"
    first = ConfigurationCatalog(path)
    second = ConfigurationCatalog(path)
    barrier = threading.Barrier(2)

    def create(catalog, profile_id):
        barrier.wait(timeout=5)
        return catalog.clone_profile("worker", profile_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda item: create(*item),
                ((first, "process-a-worker"), (second, "process-b-worker")),
            )
        )

    assert {item["id"] for item in results} == {
        "process-a-worker",
        "process-b-worker",
    }
    reloaded = ConfigurationCatalog(path)
    ids = {item["id"] for item in reloaded.list_profiles()}
    assert {"process-a-worker", "process-b-worker"} <= ids


def test_etag_check_reloads_changes_committed_by_another_instance(tmp_path):
    path = tmp_path / "catalog.json"
    first = ConfigurationCatalog(path)
    second = ConfigurationCatalog(path)
    created = first.clone_profile("worker", "shared-worker")
    stale_etag = created["draft"]["etag"]

    changed = {
        **created["draft"]["spec"],
        "display_name": "Committed by first process",
    }
    first.save_profile_draft(
        "shared-worker", changed, expected_etag=stale_etag
    )

    with pytest.raises(CatalogConflict, match="stale"):
        second.save_profile_draft(
            "shared-worker",
            {**created["draft"]["spec"], "display_name": "Stale overwrite"},
            expected_etag=stale_etag,
        )


def test_file_lock_is_released_when_owner_process_crashes(tmp_path):
    path = tmp_path / "catalog.json"
    code = (
        "import os\n"
        "from coworker.orchestration.catalogs import ConfigurationCatalog\n"
        f"catalog = ConfigurationCatalog({str(path)!r})\n"
        "catalog._lock.__enter__()\n"
        "os._exit(0)\n"
    )
    crashed = subprocess.run([sys.executable, "-c", code], timeout=10)
    assert crashed.returncode == 0

    # This would hang until the test timeout if the OS byte/flock survived its owner.
    catalog = ConfigurationCatalog(path)
    created = catalog.clone_profile("worker", "after-crash-worker")
    assert created["id"] == "after-crash-worker"


@pytest.mark.skipif(os.name != "nt", reason="msvcrt error classification is Windows-only")
def test_windows_lock_fails_fast_for_non_contention_errors(tmp_path, monkeypatch):
    import msvcrt

    catalog = ConfigurationCatalog(tmp_path / "catalog.json")

    def broken_locking(_fd, _mode, _count):
        raise OSError(9, "bad file descriptor")

    monkeypatch.setattr(msvcrt, "locking", broken_locking)
    with pytest.raises(OSError, match="bad file descriptor"):
        with catalog._lock:
            pass
