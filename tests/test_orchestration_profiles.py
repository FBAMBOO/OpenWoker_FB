import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError

import pytest

from coworker.orchestration.profiles import (
    AgentProfile,
    AgentProfileDraft,
    AgentRole,
    BUILTIN_PROFILES,
    ProfileCatalog,
    ProfileValidationError,
    clone_profile,
)


def test_all_builtin_roles_are_immutable_profiles():
    assert set(BUILTIN_PROFILES) == {role.value for role in AgentRole}
    assert {"scorer", "explorer", "integrator"} <= set(BUILTIN_PROFILES)
    assert all(profile.builtin for profile in BUILTIN_PROFILES.values())

    with pytest.raises(FrozenInstanceError):
        BUILTIN_PROFILES["worker"].display_name = "changed"
    with pytest.raises(TypeError):
        BUILTIN_PROFILES["worker"] = BUILTIN_PROFILES["tester"]


def test_published_profile_json_round_trip_and_content_hash():
    metadata = {"labels": ["safe", "fast"], "nested": {"threshold": 3}}
    draft = AgentProfileDraft(
        profile_id="custom-reviewer",
        display_name="Custom reviewer",
        role=AgentRole.REVIEWER,
        instructions="Review the candidate.",
        allowed_tools=("read_file", "read_file", "grep"),
        metadata=metadata,
    )
    metadata["labels"].append("mutated")
    profile = draft.publish(7)

    payload = profile.to_dict()
    encoded = json.dumps(payload)
    restored = AgentProfile.from_dict(json.loads(encoded))

    assert restored == profile
    assert restored.to_dict() == payload
    assert restored.content_hash == profile.content_hash
    assert len(profile.content_hash) == 64
    assert profile.allowed_tools == ("read_file", "grep")
    assert profile.to_dict()["metadata"]["labels"] == ["safe", "fast"]


def test_clone_has_exact_provenance_and_cannot_mutate_builtin_identity():
    source = BUILTIN_PROFILES["worker"]
    clone = clone_profile(
        source,
        "focused-worker",
        display_name="Focused worker",
        max_iterations=4,
    )

    assert clone.base == source.ref
    assert clone.profile_id == "focused-worker"
    assert clone.publish(1).builtin is False
    with pytest.raises(ProfileValidationError):
        clone_profile(source, "worker")
    with pytest.raises(ProfileValidationError):
        clone_profile(source, " worker ")
    with pytest.raises(ProfileValidationError):
        clone_profile(source, "another-worker", builtin=True)


def test_catalog_versions_drafts_and_optimistic_publication():
    catalog = ProfileCatalog()
    draft = catalog.clone("explorer", "repo-explorer")
    assert catalog.draft("repo-explorer") is draft
    first = catalog.publish("repo-explorer", expected_previous_version=0)
    assert first.version == 1

    catalog.save_draft(first.to_draft())
    with pytest.raises(ProfileValidationError, match="stale"):
        catalog.publish("repo-explorer", expected_previous_version=0)
    second = catalog.publish("repo-explorer", expected_previous_version=1)
    assert second.version == 2
    assert catalog.versions("repo-explorer") == (first, second)


def test_catalog_publication_is_atomic_across_threads():
    catalog = ProfileCatalog(include_builtins=False)
    catalog.save_draft(
        AgentProfileDraft(
            profile_id="concurrent",
            display_name="Concurrent",
            role="worker",
            instructions="Publish once.",
        )
    )

    def publish():
        try:
            return catalog.publish("concurrent", expected_previous_version=0)
        except KeyError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: publish(), range(2)))
    assert sum(result is not None for result in results) == 1
    assert [profile.version for profile in catalog.versions("concurrent")] == [1]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"profile_id": "UPPER"},
        {"role": "unknown"},
        {"max_iterations": 0},
        {"max_children": 1},
    ],
)
def test_invalid_drafts_are_rejected(kwargs):
    values = {
        "profile_id": "valid",
        "display_name": "Valid",
        "role": "worker",
        "instructions": "Do bounded work.",
    }
    values.update(kwargs)
    with pytest.raises(ProfileValidationError):
        AgentProfileDraft(**values)


def test_metadata_must_be_json_compatible():
    with pytest.raises(ProfileValidationError, match="JSON-compatible"):
        AgentProfileDraft(
            profile_id="invalid-metadata",
            display_name="Invalid",
            role="worker",
            instructions="Work.",
            metadata={"value": object()},
        )
