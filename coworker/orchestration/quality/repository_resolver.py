"""Deterministic, offline repository/project target resolver."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import unicodedata
import uuid
from pathlib import Path
from typing import Iterable, Literal

from pydantic import Field

from .archetypes.repo_analysis import REPOSITORY_ANALYSIS_MARKERS
from .contract_rules import CURRENT_CHECKOUT_RULE, EXPLICIT_REF_RULE
from .models import QualityModel, SnapshotKind, VcsObjectFormat, VcsType


class RepositoryResolutionError(ValueError):
    pass


_PRUNE_DIRS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        ".venv",
        "venv",
        "node_modules",
        "target",
        "dist",
        "build",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
    }
)

_SAFE_GIT_SUBCOMMANDS = frozenset(
    {
        "cat-file",
        "diff",
        "ls-files",
        "ls-tree",
        "rev-list",
        "rev-parse",
        "status",
        "symbolic-ref",
        "worktree",
    }
)


def safe_git_environment() -> dict[str, str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith("GIT_"):
            env.pop(key, None)
    env.update(
        {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_PAGER": "cat",
            "GIT_EXTERNAL_DIFF": "",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_LITERAL_PATHSPECS": "1",
            "LC_ALL": "C",
        }
    )
    return env


def git_command(
    root: str | Path,
    *args: str,
    check: bool = True,
    timeout: float = 20,
    text: bool = True,
) -> subprocess.CompletedProcess:
    if not args or str(args[0]) not in _SAFE_GIT_SUBCOMMANDS:
        raise RepositoryResolutionError("git subcommand is outside the read-only allowlist")
    safe_args = [str(item) for item in args]
    if safe_args[0] == "diff":
        safe_args[1:1] = ["--no-ext-diff", "--no-textconv"]
    command = [
        "git",
        "-c",
        f"core.hooksPath={os.devnull}",
        "-c",
        "diff.external=",
        "-c",
        f"core.attributesFile={os.devnull}",
        "-c",
        "core.pager=cat",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "core.preloadIndex=false",
        "-c",
        "credential.helper=",
        "-C",
        str(Path(root).resolve()),
        "--no-pager",
        *safe_args,
    ]
    try:
        return subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text,
            encoding="utf-8" if text else None,
            errors="surrogateescape" if text else None,
            timeout=timeout,
            env=safe_git_environment(),
            shell=False,
            check=check,
        )
    except FileNotFoundError as exc:
        raise RepositoryResolutionError("git executable is unavailable") from exc
    except subprocess.TimeoutExpired as exc:
        raise RepositoryResolutionError(f"safe git command timed out: {' '.join(args)}") from exc
    except subprocess.CalledProcessError as exc:
        stderr = (
            exc.stderr.strip()
            if isinstance(exc.stderr, str)
            else bytes(exc.stderr or b"").decode("utf-8", "replace").strip()
        )
        raise RepositoryResolutionError(
            f"safe git command failed ({' '.join(args)}): {stderr[:1000]}"
        ) from exc


class RepositoryCandidate(QualityModel):
    id: str
    workspace_root: str
    repo_root: str
    project_root: str
    vcs_type: VcsType
    vcs_object_format: VcsObjectFormat | None = None
    head_oid: str | None = None
    current_branch: str | None = None
    upstream_ref: str | None = None
    default_ref: str | None = None
    default_oid: str | None = None
    ahead: int | None = Field(default=None, ge=0)
    behind: int | None = Field(default=None, ge=0)
    dirty: bool
    worktree_count: int = Field(ge=0)
    project_markers: tuple[str, ...]
    file_count: int = Field(ge=0)
    total_bytes: int = Field(ge=0)
    score: int = Field(ge=0, le=100)
    duplicate_roots: tuple[str, ...] = ()
    recommended_ref: str | None = None
    recommended_snapshot_kind: SnapshotKind
    recommendation_reason: str


class TargetResolution(QualityModel):
    schema_id: Literal["target_resolution_v2"] = "target_resolution_v2"
    schema_version: Literal[2] = 2
    id: str
    workspace_root: str
    objective_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    candidates: tuple[RepositoryCandidate, ...]
    status: Literal["resolved", "needs_target_selection", "failed"]
    recommended_candidate_id: str | None = None
    resolution_confidence: float = Field(ge=0, le=1)
    resolution_reason: str

    def recommended(self) -> RepositoryCandidate | None:
        return next(
            (
                item
                for item in self.candidates
                if item.id == self.recommended_candidate_id
            ),
            None,
        )


def _canonical_workspace(workspace: str | Path) -> Path:
    raw = Path(workspace).expanduser()
    try:
        resolved = raw.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RepositoryResolutionError(f"workspace is unreadable: {raw}") from exc
    if not resolved.is_dir():
        raise RepositoryResolutionError(f"workspace is not a directory: {resolved}")
    return resolved


def _walk_metadata(root: Path, *, max_files: int = 1_000_000) -> tuple[int, int, list[Path]]:
    count = 0
    total = 0
    markers: list[Path] = []
    for current, directories, filenames in os.walk(root, followlinks=False):
        base = Path(current)
        directories[:] = [
            name
            for name in directories
            if name not in _PRUNE_DIRS and not (base / name).is_symlink()
        ]
        for filename in filenames:
            count += 1
            if count > max_files:
                raise RepositoryResolutionError("workspace metadata scan exceeds file limit")
            path = base / filename
            try:
                total += path.lstat().st_size
            except OSError:
                continue
            if filename in REPOSITORY_ANALYSIS_MARKERS:
                markers.append(path)
    return count, total, markers


def _repo_roots(workspace: Path) -> tuple[Path, ...]:
    roots: set[Path] = set()
    result = git_command(
        workspace, "rev-parse", "--show-toplevel", check=False
    )
    if result.returncode == 0 and result.stdout.strip():
        roots.add(Path(result.stdout.strip()).resolve())
    # A workspace may intentionally contain several sibling repos/worktrees.
    for current, directories, _ in os.walk(workspace, followlinks=False):
        base = Path(current)
        relative_depth = len(base.relative_to(workspace).parts)
        directories[:] = [
            name
            for name in directories
            if name not in (_PRUNE_DIRS - {".git"}) and not (base / name).is_symlink()
        ]
        if ".git" in directories or (base / ".git").is_file():
            result = git_command(base, "rev-parse", "--show-toplevel", check=False)
            if result.returncode == 0 and result.stdout.strip():
                roots.add(Path(result.stdout.strip()).resolve())
            if ".git" in directories:
                directories.remove(".git")
        if relative_depth >= 4:
            directories[:] = []
    return tuple(sorted(roots, key=lambda item: str(item).casefold()))


def _output(root: Path, *args: str) -> str | None:
    result = git_command(root, *args, check=False)
    value = result.stdout.strip() if result.returncode == 0 else ""
    return value or None


def _default_ref(root: Path) -> str | None:
    symbolic = _output(root, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
    if symbolic:
        return symbolic
    for candidate in (
        "origin/main",
        "upstream/main",
        "main",
        "origin/master",
        "upstream/master",
        "master",
    ):
        result = git_command(root, "rev-parse", "--verify", "--quiet", f"{candidate}^{{commit}}", check=False)
        if result.returncode == 0:
            return candidate
    return None


def _object_format(root: Path, oid: str) -> VcsObjectFormat:
    configured = _output(root, "rev-parse", "--show-object-format")
    if configured in {"sha1", "sha256"}:
        return VcsObjectFormat(configured)
    return VcsObjectFormat.SHA256 if len(oid) == 64 else VcsObjectFormat.SHA1


def _explicit_ref(objective: str) -> str | None:
    match = EXPLICIT_REF_RULE.search(objective)
    if match is None:
        return None
    value = (match.group(2) or match.group(1) or "").strip()
    value = re.sub(r"(?i)^(?:ref|branch|分支)\s*[:=]?\s*", "", value)
    return value or None


def _project_root(repo: Path, markers: Iterable[Path], objective: str) -> tuple[Path, tuple[str, ...], int]:
    marker_paths = tuple(sorted(markers, key=lambda item: str(item).casefold()))
    dbt = [item for item in marker_paths if item.name == "dbt_project.yml"]
    if dbt and re.search(r"(?i)\bdbt\b|dbt项目", objective):
        selected = dbt[0].parent
        score = 95
    elif marker_paths:
        selected = marker_paths[0].parent
        score = 75
    else:
        selected = repo
        score = 50
    relative = selected.relative_to(repo)
    names = tuple(
        sorted(
            {
                path.relative_to(repo).as_posix()
                for path in marker_paths
            }
        )
    )
    return relative, names, score


def _git_candidate(workspace: Path, root: Path, objective: str) -> RepositoryCandidate:
    head = _output(root, "rev-parse", "HEAD")
    if head is None:
        raise RepositoryResolutionError(f"repository has no resolvable HEAD: {root}")
    branch = _output(root, "symbolic-ref", "--quiet", "--short", "HEAD")
    upstream = _output(root, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
    default = _default_ref(root)
    default_oid = _output(root, "rev-parse", f"{default}^{{commit}}") if default else None
    ahead = behind = None
    if default_oid:
        counts = _output(root, "rev-list", "--left-right", "--count", f"HEAD...{default_oid}")
        if counts:
            parts = counts.split()
            if len(parts) == 2:
                ahead, behind = int(parts[0]), int(parts[1])
    status = git_command(
        root, "status", "--porcelain=v1", "-z", "--untracked-files=normal", check=False, text=False
    )
    dirty = bool(status.stdout)
    worktrees = git_command(root, "worktree", "list", "--porcelain", check=False)
    worktree_paths = [
        line.removeprefix("worktree ")
        for line in worktrees.stdout.splitlines()
        if line.startswith("worktree ")
    ]
    file_count, total_bytes, markers = _walk_metadata(root)
    project_root, marker_names, score = _project_root(root, markers, objective)
    explicit = _explicit_ref(objective)
    current_checkout = CURRENT_CHECKOUT_RULE.search(objective) is not None
    if explicit:
        resolved = _output(root, "rev-parse", "--verify", f"{explicit}^{{commit}}")
        if resolved is None:
            recommendation_ref = explicit
            reason = f"Explicit ref {explicit} is missing and must be resolved by the user."
            score = max(0, score - 40)
        else:
            recommendation_ref = explicit
            reason = f"The objective explicitly selects {explicit}."
    elif current_checkout:
        recommendation_ref = "HEAD"
        reason = "The objective explicitly selects the current checkout/working tree."
    elif default_oid and default_oid != head and ahead == 0 and (behind or 0) > 0:
        recommendation_ref = default
        reason = (
            f"HEAD is behind the local default-tracking ref by {behind}; quality-first "
            "policy recommends the local ref without fetching."
        )
    elif default_oid and default_oid != head and (ahead or 0) > 0 and (behind or 0) > 0:
        recommendation_ref = None
        reason = "HEAD and the local default ref have diverged; target selection is required."
        score = min(score, 70)
    else:
        recommendation_ref = "HEAD"
        reason = "HEAD is aligned with the available local default ref or no default ref exists."
    snapshot_kind = SnapshotKind.WORKING_TREE if current_checkout and dirty else SnapshotKind.COMMIT
    return RepositoryCandidate(
        id=f"target_{uuid.uuid4().hex}",
        workspace_root=str(workspace),
        repo_root=str(root),
        project_root=project_root.as_posix() if project_root.parts else ".",
        vcs_type=VcsType.GIT,
        vcs_object_format=_object_format(root, head),
        head_oid=head,
        current_branch=branch,
        upstream_ref=upstream,
        default_ref=default,
        default_oid=default_oid,
        ahead=ahead,
        behind=behind,
        dirty=dirty,
        worktree_count=len(worktree_paths),
        project_markers=marker_names,
        file_count=file_count,
        total_bytes=total_bytes,
        score=score,
        duplicate_roots=(),
        recommended_ref=recommendation_ref,
        recommended_snapshot_kind=snapshot_kind,
        recommendation_reason=reason,
    )


class RepositoryResolver:
    def resolve(self, workspace: str | Path, *, objective: str) -> TargetResolution:
        root = _canonical_workspace(workspace)
        prompt = unicodedata.normalize("NFC", str(objective)).strip()
        objective_hash = "sha256:" + hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        repo_roots = _repo_roots(root)
        if repo_roots:
            candidates = [_git_candidate(root, repo, prompt) for repo in repo_roots]
        else:
            file_count, total_bytes, markers = _walk_metadata(root)
            project_root, marker_names, score = _project_root(root, markers, prompt)
            candidates = [
                RepositoryCandidate(
                    id=f"target_{uuid.uuid4().hex}",
                    workspace_root=str(root),
                    repo_root=str(root),
                    project_root=project_root.as_posix() if project_root.parts else ".",
                    vcs_type=VcsType.NONE,
                    dirty=False,
                    worktree_count=0,
                    project_markers=marker_names,
                    file_count=file_count,
                    total_bytes=total_bytes,
                    score=score,
                    recommended_ref=None,
                    recommended_snapshot_kind=SnapshotKind.DIRECTORY,
                    recommendation_reason="No Git repository exists; freeze a content-addressed directory pack.",
                )
            ]
        # Mark roots that point at the same frozen HEAD and project marker signature.
        updated: list[RepositoryCandidate] = []
        for candidate in candidates:
            duplicates = tuple(
                other.repo_root
                for other in candidates
                if other.id != candidate.id
                and other.head_oid is not None
                and other.head_oid == candidate.head_oid
                and other.project_markers == candidate.project_markers
            )
            updated.append(candidate.model_copy(update={"duplicate_roots": duplicates}))
        candidates = sorted(updated, key=lambda item: (-item.score, item.repo_root.casefold()))
        top = candidates[0]
        tied = [item for item in candidates if item.score == top.score]
        explicit = _explicit_ref(prompt)
        missing_explicit = bool(explicit and top.recommended_ref == explicit and _output(Path(top.repo_root), "rev-parse", "--verify", f"{explicit}^{{commit}}") is None)
        unresolved_divergence = top.vcs_type is VcsType.GIT and top.recommended_ref is None
        if len(tied) > 1 or missing_explicit or unresolved_divergence:
            status = "needs_target_selection"
            recommended_id = None
            confidence = 0.55 if tied else 0.0 if missing_explicit else 0.7
            reason = (
                "Multiple repository/project candidates have equal confidence."
                if tied
                else top.recommendation_reason
            )
        else:
            status = "resolved"
            recommended_id = top.id
            confidence = min(0.99, max(0.0, top.score / 100))
            reason = top.recommendation_reason
        return TargetResolution(
            id=f"resolution_{uuid.uuid4().hex}",
            workspace_root=str(root),
            objective_hash=objective_hash,
            candidates=tuple(candidates),
            status=status,
            recommended_candidate_id=recommended_id,
            resolution_confidence=confidence,
            resolution_reason=reason,
        )
