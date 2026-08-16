"""Local subscription-backed Agent runtimes for durable orchestration.

These adapters deliberately sit *above* OpenWorker's ordinary model providers.  Codex,
Claude Code, and Kimi Code are complete agent loops with their own tools and session
stores, not chat-completion endpoints.  Treating them as a normal ``Provider`` would
silently bypass role isolation, run fencing, and process-tree cleanup.

The runtime contract therefore has four hard properties:

* the routed logical id is separate from the vendor model and reasoning effort;
* every vendor session/thread is bound to the live run lease before a turn can act;
* stdout is a bounded, version-tolerant event protocol and stderr is never parsed as it;
* cancellation owns and drains the complete CLI/app-server process tree.

Kimi Code's managed OAuth subscription is registered for discovery but is fail-closed
for background/DAG execution.  Kimi's published community rules restrict that
subscription to interactive personal use.  The normal OpenWorker Kimi API provider is
the supported automation path until the credential is covered by a separate enterprise
automation agreement.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

from ..sessions import SessionRecord
from ..tools.shell import _ProcessTree, _create_windows_kill_job
from .blobs import ContentAddressedBlobStore
from .executor import ExecutionOutcome, RunExecutionContext
from .profiles import AgentRole
from .routing import ModelCandidate
from .store import OrchestrationStore


CODEX_GPT_5_6_SOL_MAX = "codex-subscription:gpt-5.6-sol@max"
CLAUDE_OPUS_5_HIGH = "claude-code-subscription:claude-opus-5@high"
CLAUDE_OPUS_5_MAX = "claude-code-subscription:claude-opus-5@max"
KIMI_K3_MAX = "kimi-code-subscription:kimi-code/k3@max"

SUBSCRIPTION_PROVIDER_IDS = frozenset(
    {
        "codex-subscription",
        "claude-code-subscription",
        "kimi-code-subscription",
    }
)

_READ_ONLY_ROLES = frozenset(
    {
        AgentRole.ORCHESTRATOR,
        AgentRole.PLANNER,
        AgentRole.REVIEWER,
        AgentRole.EVALUATOR,
        AgentRole.SCORER,
        AgentRole.EXPLORER,
    }
)
_VERDICT_ROLES = frozenset(
    {
        AgentRole.REVIEWER,
        AgentRole.TESTER,
        AgentRole.EVALUATOR,
        AgentRole.SCORER,
    }
)
_OUTPUT_LIMIT = 8 * 1024 * 1024
_STDERR_LIMIT = 256 * 1024
_PROBE_TIMEOUT = 10.0
_PROBE_TTL_SECONDS = 30.0
_LEGACY_RUNTIME_SCHEMA_VERSION = 1
_RUNTIME_SCHEMA_VERSION = 2
# A short-lived v1 build bound checkpoints to the first strict array contract before
# the runtime envelope was versioned to v2. Keep its immutable schema fingerprint so
# those sealed results can be verified after future output-schema changes.
_V1_STRICT_ARRAY_OUTPUT_SCHEMA_SHA256 = (
    "2286706354fced2b1943429589bf4d6cce3a447ae109b7f847b3d66f8ca7bc31"
)
_V1_DYNAMIC_MAP_OUTPUT_SCHEMA_SHA256 = (
    "e185acbd78a9aa32f3c6c63a091263162ac78374db743ce3d0082d4dc000ea98"
)
_GLOBAL_HEALTH_LOCK = threading.RLock()
_GLOBAL_HEALTH_CACHE: dict[str, tuple[float, "SubscriptionRuntimeHealth"]] = {}


@dataclass(frozen=True, slots=True)
class SubscriptionRuntimeSpec:
    runtime_id: str
    provider: str
    display_name: str
    command: str
    cli_model: str
    reasoning_effort: str
    quality: int
    context_window: int
    minimum_cli_version: tuple[int, int, int]
    protocol: str
    interactive_only: bool = False
    local_owner_only: bool = True
    capabilities: frozenset[str] = frozenset({"tools", "streaming"})

    def __post_init__(self) -> None:
        if self.provider not in SUBSCRIPTION_PROVIDER_IDS:
            raise ValueError(f"unknown subscription provider: {self.provider}")
        if not self.runtime_id.startswith(f"{self.provider}:"):
            raise ValueError("runtime id must be namespaced by its provider")
        if self.reasoning_effort not in {"low", "medium", "high", "xhigh", "max"}:
            raise ValueError("unsupported reasoning effort")
        if not 0 <= self.quality <= 100:
            raise ValueError("quality must be between 0 and 100")

    def audit_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "provider": self.provider,
            "display_name": self.display_name,
            "command": self.command,
            "model": self.cli_model,
            "reasoning_effort": self.reasoning_effort,
            "quality": self.quality,
            "context_window": self.context_window,
            "minimum_cli_version": ".".join(map(str, self.minimum_cli_version)),
            "protocol": self.protocol,
            "interactive_only": self.interactive_only,
            "local_owner_only": self.local_owner_only,
            "capabilities": sorted(self.capabilities),
        }


@dataclass(frozen=True, slots=True)
class SubscriptionRuntimeHealth:
    runtime_id: str
    provider: str
    installed: bool
    authenticated: bool
    available: bool
    policy_eligible: bool
    version: str = ""
    auth_kind: str = "unknown"
    executable: str = ""
    reason: str = ""
    checked_at: float = 0.0

    def audit_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "provider": self.provider,
            "installed": self.installed,
            "authenticated": self.authenticated,
            "available": self.available,
            "policy_eligible": self.policy_eligible,
            "version": self.version,
            "auth_kind": self.auth_kind,
            # The absolute executable path is useful locally but unnecessary in the
            # HTTP read model and can disclose a username. Expose only its filename.
            "executable": Path(self.executable).name if self.executable else "",
            "reason": self.reason,
            "checked_at": self.checked_at,
        }


def default_subscription_runtime_specs() -> tuple[SubscriptionRuntimeSpec, ...]:
    """Return the immutable, audited logical-to-vendor model mapping."""

    return (
        SubscriptionRuntimeSpec(
            runtime_id=CODEX_GPT_5_6_SOL_MAX,
            provider="codex-subscription",
            display_name="Codex Subscription · GPT-5.6 Sol · Max",
            command="codex",
            cli_model="gpt-5.6-sol",
            reasoning_effort="max",
            quality=100,
            # A conservative routable floor; the live model catalog remains the
            # authority and is checked without making a model call.
            context_window=200_000,
            minimum_cli_version=(0, 146, 0),
            protocol="codex-app-server-v2",
            capabilities=frozenset(
                {"tools", "streaming", "parallel_tool_calls"}
            ),
        ),
        SubscriptionRuntimeSpec(
            runtime_id=CLAUDE_OPUS_5_HIGH,
            provider="claude-code-subscription",
            display_name="Claude Code Subscription · Opus 5 · High",
            command="claude",
            cli_model="claude-opus-5",
            reasoning_effort="high",
            quality=98,
            context_window=200_000,
            minimum_cli_version=(2, 1, 219),
            protocol="claude-code-stream-json-v1",
        ),
        SubscriptionRuntimeSpec(
            runtime_id=CLAUDE_OPUS_5_MAX,
            provider="claude-code-subscription",
            display_name="Claude Code Subscription · Opus 5 · Max",
            command="claude",
            cli_model="claude-opus-5",
            reasoning_effort="max",
            quality=99,
            context_window=200_000,
            minimum_cli_version=(2, 1, 219),
            protocol="claude-code-stream-json-v1",
        ),
        SubscriptionRuntimeSpec(
            runtime_id=KIMI_K3_MAX,
            provider="kimi-code-subscription",
            display_name="Kimi Code Subscription · K3 · Max (interactive only)",
            command="kimi",
            cli_model="kimi-code/k3",
            reasoning_effort="max",
            quality=97,
            context_window=1_048_576,
            minimum_cli_version=(0, 29, 2),
            protocol="kimi-code-stream-json-v1",
            interactive_only=True,
        ),
    )


class SubscriptionAgentRuntime(Protocol):
    spec: SubscriptionRuntimeSpec

    def probe(self) -> SubscriptionRuntimeHealth: ...

    async def execute(self, context: RunExecutionContext) -> ExecutionOutcome: ...

    def interrupt(self, run_id: str) -> None: ...


@dataclass
class _ActiveProcess:
    run_id: str
    proc: subprocess.Popen[str]
    tree: _ProcessTree
    write_lock: threading.Lock = field(default_factory=threading.Lock)
    thread_id: Optional[str] = None
    turn_id: Optional[str] = None
    cleanup_ok: Optional[bool] = None
    finished: threading.Event = field(default_factory=threading.Event)
    interrupt_timer: Optional[threading.Timer] = None

    def send(self, value: Mapping[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n"
        with self.write_lock:
            if self.proc.stdin is None or self.proc.poll() is not None:
                raise BrokenPipeError("runtime process stdin is closed")
            self.proc.stdin.write(payload)
            self.proc.stdin.flush()


@dataclass(frozen=True, slots=True)
class _ProtocolResult:
    terminal_status: str
    final_text: str
    structured: Optional[Mapping[str, Any]]
    external_session_id: str
    external_turn_id: str
    events: tuple[Mapping[str, Any], ...]
    stderr: str
    usage: Mapping[str, int]
    runtime_version: str
    resolved_model: str
    error_kind: Optional[str] = None
    error_message: Optional[str] = None
    cleanup_ok: bool = True
    recovery_blob_sha256: str = ""


def _version_tuple(raw: str) -> tuple[int, int, int]:
    match = re.search(r"(?<!\d)(\d+)\.(\d+)(?:\.(\d+))?", str(raw))
    if not match:
        return (0, 0, 0)
    return tuple(int(value or 0) for value in match.groups())  # type: ignore[return-value]


def _bounded(value: str, limit: int) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="replace") + "\n[TRUNCATED]"


_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s\"']+"),
    re.compile(r"(?i)\b(sk-[a-z0-9_-]{12,})\b"),
    re.compile(r"(?i)(api[_-]?key\s*[:=]\s*)[^\s,;\"']+"),
    re.compile(r"(?i)(token\s*[:=]\s*)[^\s,;\"']+"),
)


def _redact_text(value: str) -> str:
    result = str(value)
    for pattern in _SECRET_PATTERNS:
        if pattern.groups >= 2:
            result = pattern.sub(r"\1[REDACTED]", result)
        elif pattern.groups == 1 and "sk-" in pattern.pattern:
            result = pattern.sub("[REDACTED_SECRET]", result)
        else:
            result = pattern.sub(r"\1[REDACTED]", result)
    return result


def _sanitize_event(value: Any, *, key: str = "") -> Any:
    lowered = key.lower().replace("_", "")
    if any(
        marker in lowered
        for marker in ("authorization", "apikey", "authtoken", "accesstoken", "cookie")
    ):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        item_type = str(value.get("type") or "")
        method = str(value.get("method") or "").lower()
        sanitized = {
            str(k): _sanitize_event(v, key=str(k)) for k, v in value.items()
        }
        # Persist readable reasoning summaries, never raw hidden reasoning/CoT.
        if item_type in {"reasoning", "thinking"} or any(
            marker in method for marker in ("reasoning", "thinking")
        ):
            for field_name in (
                "content",
                "delta",
                "encryptedContent",
                "encrypted_content",
                "reasoning",
                "signature",
                "text",
                "thinking",
            ):
                if field_name in sanitized:
                    sanitized[field_name] = "[REDACTED_REASONING]"
        return sanitized
    if isinstance(value, (list, tuple)):
        return [_sanitize_event(item, key=key) for item in value]
    if isinstance(value, str):
        return _bounded(_redact_text(value), 128 * 1024)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _redact_text(str(value))


def _safe_environment(provider: str) -> dict[str, str]:
    """Copy the local environment while removing API/provider/model overrides."""

    env = dict(os.environ)
    common = {
        "CODEX_API_KEY",
        "CODEX_ACCESS_TOKEN",
        "CODEX_MODEL",
        "OPENAI_API_KEY",
        "OPENAI_API_BASE",
        "OPENAI_BASE_URL",
        "OPENAI_MODEL",
        "OPENAI_ORG_ID",
        "OPENAI_ORGANIZATION",
        "OPENAI_PROJECT_ID",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL",
        "MOONSHOT_API_KEY",
        "KIMI_API_KEY",
        "KIMI_ACCESS_TOKEN",
        "KIMI_API_BASE",
        "KIMI_BASE_URL",
    }
    credential_exact = {
        "CLAUDE_CODE_OAUTH_TOKEN",
        "COWORKER_API_TOKEN",
        "GIT_ASKPASS",
        "GPG_AGENT_INFO",
        "SSH_ASKPASS",
        "SSH_AUTH_SOCK",
    }
    credential_markers = (
        "ACCESS_KEY",
        "ACCESS_TOKEN",
        "API_KEY",
        "AUTH_TOKEN",
        "CLIENT_SECRET",
        "COOKIE",
        "CREDENTIAL",
        "PASSWORD",
        "PASSWD",
        "PRIVATE_KEY",
        "SECRET",
    )
    claude_prefixes = (
        "CLAUDE_CODE_USE_",
        "ANTHROPIC_DEFAULT_",
        "ANTHROPIC_CUSTOM_",
    )
    claude_exact = {
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CLAUDE_CODE_EFFORT_LEVEL",
        "CLAUDE_CODE_SUBAGENT_MODEL",
        "CLAUDE_CODE_SIMPLE",
        "CLAUDE_AUTO_BACKGROUND_TASKS",
        "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS",
        "MAX_THINKING_TOKENS",
        "ANTHROPIC_MODEL",
        "ANTHROPIC_BETAS",
        "ANTHROPIC_CUSTOM_HEADERS",
        "AWS_BEARER_TOKEN_BEDROCK",
    }
    for name in tuple(env):
        upper = name.upper()
        credential_like = (
            upper in credential_exact
            or upper == "TOKEN"
            or upper.endswith("_TOKEN")
            or "_TOKEN_" in upper
            or any(marker in upper for marker in credential_markers)
        )
        if upper in common or credential_like or (
            provider == "claude-code-subscription"
            and (upper in claude_exact or upper.startswith(claude_prefixes))
        ):
            env.pop(name, None)
    env["PYTHONUNBUFFERED"] = "1"
    env["GIT_TERMINAL_PROMPT"] = "0"
    if provider == "claude-code-subscription":
        env["CLAUDE_CODE_SUBPROCESS_ENV_SCRUB"] = "1"
        env["CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS"] = "1"
        env["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"] = "1"
    if provider == "kimi-code-subscription":
        env["KIMI_MODEL_THINKING_EFFORT"] = "max"
    return env


def _run_probe(
    executable: str, args: Sequence[str], provider: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [executable, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=_PROBE_TIMEOUT,
        env=_safe_environment(provider),
        shell=False,
    )


def _result_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "status": {"type": "string", "enum": ["pass", "fail", "unknown"]},
            "criteria": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "criterion": {"type": "string"},
                        "status": {
                            "type": "string",
                            "enum": ["pass", "fail", "unknown"],
                        },
                    },
                    "required": ["criterion", "status"],
                    "additionalProperties": False,
                },
            },
            "files_touched": {"type": "array", "items": {"type": "string"}},
            "checks": {"type": "array", "items": {"type": "string"}},
            "remaining_risks": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "summary",
            "status",
            "criteria",
            "files_touched",
            "checks",
            "remaining_risks",
        ],
        "additionalProperties": False,
    }


def _prompt(context: RunExecutionContext) -> str:
    criteria = "\n".join(
        f"- {item}" for item in context.task.acceptance_criteria
    ) or "- Complete the scoped node correctly."
    configured_upstream = context.node.input.get("upstream", {})
    return (
        f"{context.profile.instructions}\n\n"
        "You are an isolated role in a durable OpenWorker multi-agent run. "
        "Do not create private subagents: OpenWorker owns all parent/child agents, "
        "budgets, DAG dependencies, review, testing, and acceptance. Do not commit or "
        "push. Work only inside the supplied workspace. End with exactly one JSON object "
        "matching the provided schema. Return criteria as an array with one object per "
        "acceptance criterion. Copy the criterion's exact text into criterion and use "
        "pass, fail, or unknown as its status.\n\n"
        f"Role: {context.profile.role.value}\n"
        f"Task: {context.task.objective}\n"
        f"Current DAG node: {context.node.title or context.node.key} "
        f"({context.node.kind.value})\n"
        f"Assignment: {context.node.instructions or context.task.objective}\n"
        f"Constraints: {list(context.task.constraints)}\n"
        f"Acceptance criteria:\n{criteria}\n"
        f"Candidate subject: {dict(context.subject)}\n"
        f"Durable upstream run evidence: {list(context.upstream_context)}\n"
        f"Configured upstream input: {configured_upstream}"
    )


def _v1_strict_array_prompt(context: RunExecutionContext) -> str:
    """Rebuild the immutable prompt used by hash-bound v1 checkpoints.

    Do not refactor this snapshot to call ``_prompt``: v2 and later prompt contracts
    may evolve while a sealed v1 result must remain independently verifiable.
    """

    criteria = "\n".join(
        f"- {item}" for item in context.task.acceptance_criteria
    ) or "- Complete the scoped node correctly."
    configured_upstream = context.node.input.get("upstream", {})
    return (
        f"{context.profile.instructions}\n\n"
        "You are an isolated role in a durable OpenWorker multi-agent run. "
        "Do not create private subagents: OpenWorker owns all parent/child agents, "
        "budgets, DAG dependencies, review, testing, and acceptance. Do not commit or "
        "push. Work only inside the supplied workspace. End with exactly one JSON object "
        "matching the provided schema. Return criteria as an array with one object per "
        "acceptance criterion. Copy the criterion's exact text into criterion and use "
        "pass, fail, or unknown as its status.\n\n"
        f"Role: {context.profile.role.value}\n"
        f"Task: {context.task.objective}\n"
        f"Current DAG node: {context.node.title or context.node.key} "
        f"({context.node.kind.value})\n"
        f"Assignment: {context.node.instructions or context.task.objective}\n"
        f"Constraints: {list(context.task.constraints)}\n"
        f"Acceptance criteria:\n{criteria}\n"
        f"Candidate subject: {dict(context.subject)}\n"
        f"Durable upstream run evidence: {list(context.upstream_context)}\n"
        f"Configured upstream input: {configured_upstream}"
    )


def _v1_dynamic_map_prompt(context: RunExecutionContext) -> str:
    """Rebuild the original v1 prompt paired with dynamic criteria-map output."""

    criteria = "\n".join(
        f"- {item}" for item in context.task.acceptance_criteria
    ) or "- Complete the scoped node correctly."
    configured_upstream = context.node.input.get("upstream", {})
    return (
        f"{context.profile.instructions}\n\n"
        "You are an isolated role in a durable OpenWorker multi-agent run. "
        "Do not create private subagents: OpenWorker owns all parent/child agents, "
        "budgets, DAG dependencies, review, testing, and acceptance. Do not commit or "
        "push. Work only inside the supplied workspace. End with exactly one JSON object "
        "matching the provided schema. Use each acceptance criterion's exact text as a "
        "criteria key; use pass, fail, or unknown.\n\n"
        f"Role: {context.profile.role.value}\n"
        f"Task: {context.task.objective}\n"
        f"Current DAG node: {context.node.title or context.node.key} "
        f"({context.node.kind.value})\n"
        f"Assignment: {context.node.instructions or context.task.objective}\n"
        f"Constraints: {list(context.task.constraints)}\n"
        f"Acceptance criteria:\n{criteria}\n"
        f"Candidate subject: {dict(context.subject)}\n"
        f"Durable upstream run evidence: {list(context.upstream_context)}\n"
        f"Configured upstream input: {configured_upstream}"
    )


def _v1_known_contract_bindings(
    context: RunExecutionContext,
) -> frozenset[tuple[str, str]]:
    return frozenset(
        {
            (
                hashlib.sha256(
                    _v1_dynamic_map_prompt(context).encode("utf-8")
                ).hexdigest(),
                _V1_DYNAMIC_MAP_OUTPUT_SCHEMA_SHA256,
            ),
            (
                hashlib.sha256(
                    _v1_strict_array_prompt(context).encode("utf-8")
                ).hexdigest(),
                _V1_STRICT_ARRAY_OUTPUT_SCHEMA_SHA256,
            ),
        }
    )


def _structured(value: Any) -> Optional[dict[str, Any]]:
    if isinstance(value, Mapping):
        return _normalize_structured(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
    return _normalize_structured(parsed) if isinstance(parsed, Mapping) else None


def _normalize_structured(value: Mapping[str, Any]) -> dict[str, Any]:
    """Convert the provider schema shape to OpenWorker's stable result shape.

    Strict structured-output schemas cannot represent an object whose property names
    are acceptance-criterion text. Providers therefore return a fixed array shape at
    the protocol boundary. OpenWorker continues to expose and persist the established
    ``criterion -> verdict`` mapping. Mapping input remains supported so sealed results
    written by older versions can still be recovered.
    """

    normalized = dict(value)
    criteria = normalized.get("criteria")
    if isinstance(criteria, Mapping):
        normalized["criteria"] = dict(criteria)
        return normalized
    if not isinstance(criteria, list):
        return normalized

    mapped: dict[str, Any] = {}
    for item in criteria:
        if not isinstance(item, Mapping) or set(item) != {"criterion", "status"}:
            return normalized
        criterion = item.get("criterion")
        status = item.get("status")
        if (
            not isinstance(criterion, str)
            or not criterion
            or not isinstance(status, str)
            or criterion in mapped
        ):
            return normalized
        mapped[criterion] = status
    normalized["criteria"] = mapped
    return normalized


def _validate_structured(value: Mapping[str, Any]) -> Optional[str]:
    required = {
        "summary": str,
        "status": str,
        "criteria": Mapping,
        "files_touched": list,
        "checks": list,
        "remaining_risks": list,
    }
    for key, expected in required.items():
        if key not in value or not isinstance(value[key], expected):
            return f"structured output field {key!r} is missing or invalid"
    if str(value["status"]).lower() not in {"pass", "fail", "unknown"}:
        return "structured output status is invalid"
    for criterion, item in dict(value["criteria"]).items():
        if not isinstance(criterion, str) or not criterion:
            return "structured output contains an invalid criterion"
        if not isinstance(item, str) or item.lower() not in {
            "pass",
            "fail",
            "unknown",
        }:
            return "structured output contains an invalid criterion verdict"
    return None


class _BaseSubscriptionRuntime:
    def __init__(
        self,
        spec: SubscriptionRuntimeSpec,
        manager: Any,
        store: OrchestrationStore,
        blob_store: ContentAddressedBlobStore,
        state_dir: str | Path,
    ) -> None:
        self.spec = spec
        self.manager = manager
        self.store = store
        self.blob_store = blob_store
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._active_lock = threading.RLock()
        self._active: dict[str, _ActiveProcess] = {}
        self._interrupt_requested: set[str] = set()

    def build_command(
        self,
        context: RunExecutionContext,
        checkpoint: Optional[Mapping[str, Any]] = None,
        schema: Optional[Mapping[str, Any]] = None,
    ) -> list[str]:
        raise NotImplementedError

    def probe(self) -> SubscriptionRuntimeHealth:
        raise NotImplementedError

    def interrupt(self, run_id: str) -> None:
        with self._active_lock:
            self._interrupt_requested.add(run_id)
            active = self._active.get(run_id)
        if active is not None:
            active.cleanup_ok = active.tree.terminate()

    def _register(self, active: _ActiveProcess) -> None:
        with self._active_lock:
            self._active[active.run_id] = active
            interrupted = active.run_id in self._interrupt_requested
        if interrupted:
            active.cleanup_ok = active.tree.terminate()

    def _unregister(self, active: _ActiveProcess) -> None:
        with self._active_lock:
            self._active.pop(active.run_id, None)
            self._interrupt_requested.discard(active.run_id)
        if active.interrupt_timer is not None:
            active.interrupt_timer.cancel()
        active.finished.set()

    @staticmethod
    def _spawn(
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
    ) -> _ActiveProcess:
        kwargs: dict[str, Any] = {
            "cwd": str(cwd),
            "env": dict(env),
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "bufsize": 1,
            "shell": False,
        }
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        proc = subprocess.Popen(list(argv), **kwargs)
        tree = _ProcessTree(
            proc,
            windows_job=_create_windows_kill_job(proc) if sys.platform == "win32" else None,
        )
        return _ActiveProcess("", proc, tree)

    def _load_checkpoint(self, context: RunExecutionContext) -> dict[str, Any]:
        checkpoint = dict(
            (context.claim.run.output or {}).get("subscription_runtime_checkpoint")
            or {}
        )
        if not checkpoint:
            return {}
        try:
            schema_version = int(checkpoint.get("schema_version", 0))
            checkpoint_attempt = int(checkpoint.get("attempt", 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "subscription runtime checkpoint identity mismatch"
            ) from exc
        if (
            schema_version
            not in {_LEGACY_RUNTIME_SCHEMA_VERSION, _RUNTIME_SCHEMA_VERSION}
            or str(checkpoint.get("runtime_id") or "") != self.spec.runtime_id
            or str(checkpoint.get("provider") or "") != self.spec.provider
            or str(checkpoint.get("model") or "") != self.spec.cli_model
            or str(checkpoint.get("reasoning_effort") or "")
            != self.spec.reasoning_effort
            or str(checkpoint.get("protocol") or "") != self.spec.protocol
            or str(checkpoint.get("run_id") or "") != context.claim.run.id
            or checkpoint_attempt != context.claim.run.attempt
        ):
            raise RuntimeError("subscription runtime checkpoint identity mismatch")
        workspace_hash = hashlib.sha256(
            str((context.workspace or Path.cwd()).resolve()).encode("utf-8")
        ).hexdigest()
        if str(checkpoint.get("workspace_sha256") or "") != workspace_hash:
            raise RuntimeError("subscription runtime checkpoint workspace mismatch")
        if schema_version == _LEGACY_RUNTIME_SCHEMA_VERSION:
            # Early version 1 checkpoints omitted prompt/schema bindings; later v1
            # builds used one of the frozen contracts above. It is safe to migrate
            # either form only for a sealed terminal result: recovery reads a
            # content-addressed blob and makes zero vendor/model calls. Never resume
            # an in-flight v1 session under a changed prompt or output contract.
            if str(checkpoint.get("state") or "") != "result_sealed":
                raise RuntimeError(
                    "legacy subscription runtime checkpoint is not safely recoverable"
                )
            legacy_prompt_bound = "prompt_sha256" in checkpoint
            legacy_schema_bound = "output_schema_sha256" in checkpoint
            if legacy_prompt_bound != legacy_schema_bound:
                raise RuntimeError(
                    "legacy subscription runtime checkpoint binding is incomplete"
                )
            if legacy_prompt_bound:
                supplied_binding = (
                    str(checkpoint.get("prompt_sha256") or ""),
                    str(checkpoint.get("output_schema_sha256") or ""),
                )
                if supplied_binding not in _v1_known_contract_bindings(context):
                    raise RuntimeError(
                        "legacy subscription runtime checkpoint binding mismatch"
                    )
            return checkpoint

        prompt_hash = hashlib.sha256(_prompt(context).encode("utf-8")).hexdigest()
        if str(checkpoint.get("prompt_sha256") or "") != prompt_hash:
            raise RuntimeError("subscription runtime checkpoint prompt mismatch")
        schema_hash = hashlib.sha256(
            json.dumps(_result_schema(), sort_keys=True).encode("utf-8")
        ).hexdigest()
        if str(checkpoint.get("output_schema_sha256") or "") != schema_hash:
            raise RuntimeError("subscription runtime checkpoint output schema mismatch")
        return checkpoint

    def _checkpoint(
        self,
        context: RunExecutionContext,
        *,
        external_session_id: str,
        external_turn_id: str = "",
        runtime_version: str = "",
        state: str,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        workspace = (context.workspace or Path.cwd()).resolve()
        value = {
            "schema_version": _RUNTIME_SCHEMA_VERSION,
            "runtime_id": self.spec.runtime_id,
            "provider": self.spec.provider,
            "model": self.spec.cli_model,
            "reasoning_effort": self.spec.reasoning_effort,
            "protocol": self.spec.protocol,
            "run_id": context.claim.run.id,
            "attempt": context.claim.run.attempt,
            "external_session_id": external_session_id,
            "external_turn_id": external_turn_id,
            "runtime_version": runtime_version,
            "state": state,
            "workspace_sha256": hashlib.sha256(
                str(workspace).encode("utf-8")
            ).hexdigest(),
            "prompt_sha256": hashlib.sha256(
                _prompt(context).encode("utf-8")
            ).hexdigest(),
            "output_schema_sha256": hashlib.sha256(
                json.dumps(_result_schema(), sort_keys=True).encode("utf-8")
            ).hexdigest(),
        }
        if extra:
            reserved = set(value)
            overlap = reserved.intersection(str(key) for key in extra)
            if overlap:
                raise ValueError(
                    "subscription checkpoint extra fields override reserved keys: "
                    + ", ".join(sorted(overlap))
                )
            value.update({str(key): item for key, item in extra.items()})
        digest = hashlib.sha256(
            json.dumps(value, sort_keys=True).encode("utf-8")
        ).hexdigest()[:24]
        self.store.checkpoint_active_run(
            context.claim.run.id,
            context.claim.lease.token,
            context.claim.lease.fencing_token,
            checkpoint=value,
            command_id=(
                f"subscription-checkpoint:{context.claim.run.id}:"
                f"{context.claim.lease.fencing_token}:{digest}"
            ),
        )
        return value

    def _seal_protocol_result(
        self,
        context: RunExecutionContext,
        result: _ProtocolResult,
    ) -> _ProtocolResult:
        """Seal a terminal CLI result before the orchestration commit boundary.

        Claude Code has a resumable conversation id but no stable, zero-model-call API
        for querying whether an earlier headless prompt committed.  Persisting the
        terminal protocol result in the content-addressed store lets a restarted owner
        finish the same run without submitting a second prompt.  Codex benefits from
        the same fast path while retaining thread-history reconciliation for an
        in-progress turn.
        """

        payload = {
            "schema_version": _RUNTIME_SCHEMA_VERSION,
            "runtime_id": self.spec.runtime_id,
            "run_id": context.claim.run.id,
            "attempt": context.claim.run.attempt,
            "terminal_status": result.terminal_status,
            "final_text": _bounded(result.final_text, _OUTPUT_LIMIT),
            "structured": dict(result.structured) if result.structured is not None else None,
            "external_session_id": result.external_session_id,
            "external_turn_id": result.external_turn_id,
            "events": [_sanitize_event(item) for item in result.events],
            "stderr": _bounded(_redact_text(result.stderr), _STDERR_LIMIT),
            "usage": {str(key): int(value) for key, value in result.usage.items()},
            "runtime_version": result.runtime_version,
            "resolved_model": result.resolved_model,
            "error_kind": result.error_kind,
            "error_message": result.error_message,
            "cleanup_ok": bool(result.cleanup_ok),
        }
        blob = self.blob_store.put_json(payload)
        self._checkpoint(
            context,
            external_session_id=result.external_session_id,
            external_turn_id=result.external_turn_id,
            runtime_version=result.runtime_version,
            state="result_sealed",
            extra={
                "recovery_blob_sha256": blob.sha256,
                "recovery_blob_size": blob.size,
                "recovery_blob_mime_type": blob.mime_type,
            },
        )
        return replace(result, recovery_blob_sha256=blob.sha256)

    def _load_sealed_protocol_result(
        self,
        context: RunExecutionContext,
        checkpoint: Mapping[str, Any],
    ) -> _ProtocolResult:
        if str(checkpoint.get("state") or "") != "result_sealed":
            raise RuntimeError("subscription runtime result is not sealed")
        digest = str(checkpoint.get("recovery_blob_sha256") or "")
        if not digest:
            raise RuntimeError("sealed subscription runtime result has no blob hash")
        try:
            raw = json.loads(self.blob_store.get(digest).decode("utf-8"))
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("sealed subscription runtime result is unreadable") from exc
        if not isinstance(raw, Mapping):
            raise RuntimeError("sealed subscription runtime result is invalid")
        try:
            checkpoint_schema_version = int(checkpoint.get("schema_version", 0))
            result_schema_version = int(raw.get("schema_version", 0))
            result_attempt = int(raw.get("attempt", 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "sealed subscription runtime result identity mismatch"
            ) from exc
        if (
            checkpoint_schema_version
            not in {_LEGACY_RUNTIME_SCHEMA_VERSION, _RUNTIME_SCHEMA_VERSION}
            or result_schema_version != checkpoint_schema_version
            or str(raw.get("runtime_id") or "") != self.spec.runtime_id
            or str(raw.get("run_id") or "") != context.claim.run.id
            or result_attempt != context.claim.run.attempt
            or str(raw.get("external_session_id") or "")
            != str(checkpoint.get("external_session_id") or "")
            or str(raw.get("external_turn_id") or "")
            != str(checkpoint.get("external_turn_id") or "")
        ):
            raise RuntimeError("sealed subscription runtime result identity mismatch")
        terminal_status = str(raw.get("terminal_status") or "")
        if terminal_status not in {"completed", "failed", "interrupted"}:
            raise RuntimeError("sealed subscription runtime result status is invalid")
        structured_raw = raw.get("structured")
        if structured_raw is not None and not isinstance(structured_raw, Mapping):
            raise RuntimeError("sealed subscription runtime structured output is invalid")
        event_values = raw.get("events")
        if event_values is None:
            event_values = []
        if not isinstance(event_values, list) or not all(
            isinstance(item, Mapping) for item in event_values
        ):
            raise RuntimeError("sealed subscription runtime events are invalid")
        usage_raw = raw.get("usage") or {}
        if not isinstance(usage_raw, Mapping):
            raise RuntimeError("sealed subscription runtime usage is invalid")
        return _ProtocolResult(
            terminal_status=terminal_status,
            final_text=str(raw.get("final_text") or ""),
            structured=(dict(structured_raw) if structured_raw is not None else None),
            external_session_id=str(raw.get("external_session_id") or ""),
            external_turn_id=str(raw.get("external_turn_id") or ""),
            events=tuple(dict(item) for item in event_values),
            stderr=str(raw.get("stderr") or ""),
            usage={str(key): int(value) for key, value in usage_raw.items()},
            runtime_version=str(raw.get("runtime_version") or ""),
            resolved_model=str(raw.get("resolved_model") or ""),
            error_kind=(str(raw["error_kind"]) if raw.get("error_kind") else None),
            error_message=(
                str(raw["error_message"]) if raw.get("error_message") else None
            ),
            cleanup_ok=bool(raw.get("cleanup_ok", False)),
            recovery_blob_sha256=digest,
        )

    def _finish_outcome(
        self,
        context: RunExecutionContext,
        result: _ProtocolResult,
    ) -> ExecutionOutcome:
        session_id = context.claim.run.session_id or f"__orch__{context.claim.run.id}"
        if not result.cleanup_ok:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="process_tree_cleanup_failed",
                error_message="subscription runtime process tree could not be reaped",
                usage=result.usage,
            )
        checkpoint = {
            "schema_version": _RUNTIME_SCHEMA_VERSION,
            "runtime_id": self.spec.runtime_id,
            "provider": self.spec.provider,
            "model": self.spec.cli_model,
            "reasoning_effort": self.spec.reasoning_effort,
            "protocol": self.spec.protocol,
            "run_id": context.claim.run.id,
            "attempt": context.claim.run.attempt,
            "external_session_id": result.external_session_id,
            "external_turn_id": result.external_turn_id,
            "runtime_version": result.runtime_version,
            "state": result.terminal_status,
            "workspace_sha256": hashlib.sha256(
                str((context.workspace or Path.cwd()).resolve()).encode("utf-8")
            ).hexdigest(),
            "prompt_sha256": hashlib.sha256(
                _prompt(context).encode("utf-8")
            ).hexdigest(),
            "output_schema_sha256": hashlib.sha256(
                json.dumps(_result_schema(), sort_keys=True).encode("utf-8")
            ).hexdigest(),
            "recovery_blob_sha256": result.recovery_blob_sha256,
        }
        audit = {
            "schema_version": _RUNTIME_SCHEMA_VERSION,
            "runtime": self.spec.audit_dict(),
            "checkpoint": checkpoint,
            "resolved_model": result.resolved_model,
            "events": [_sanitize_event(item) for item in result.events],
            "stderr": _bounded(_redact_text(result.stderr), _STDERR_LIMIT),
            "usage": dict(result.usage),
        }
        blob = self.blob_store.put_json(audit)
        evidence = (
            {
                "kind": "log",
                "title": "Subscription Agent Runtime event transcript",
                "runtime_id": self.spec.runtime_id,
                "uri": blob.uri,
                "sha256": blob.sha256,
                "size": blob.size,
                "mime_type": blob.mime_type,
            },
        )
        if result.error_kind or result.terminal_status != "completed":
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                summary=result.error_message or "subscription runtime failed",
                output={
                    "subscription_runtime": checkpoint,
                    "runtime_audit_blob": blob.as_dict(),
                },
                evidence=evidence,
                usage=result.usage,
                error_kind=result.error_kind or "subscription_runtime_failed",
                error_message=result.error_message or "subscription runtime failed",
            )
        structured = _normalize_structured(dict(result.structured or {}))
        invalid = _validate_structured(structured)
        if invalid:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                output={
                    "subscription_runtime": checkpoint,
                    "runtime_audit_blob": blob.as_dict(),
                },
                evidence=evidence,
                usage=result.usage,
                error_kind="structured_output_invalid",
                error_message=invalid,
            )
        summary = str(structured.get("summary") or "")
        output: dict[str, Any] = {
            "summary": summary,
            "structured_result": structured,
            "subscription_runtime": checkpoint,
            "subscription_runtime_checkpoint": checkpoint,
            "runtime_audit_blob": blob.as_dict(),
        }
        if context.profile.role in _VERDICT_ROLES:
            output["verdict"] = {
                "status": str(structured.get("status") or "unknown").lower(),
                "criteria": dict(structured.get("criteria") or {}),
                "summary": summary,
            }
        self._save_session(context, session_id, result.final_text, summary)
        return ExecutionOutcome(
            status="succeeded",
            session_id=session_id,
            summary=summary,
            output=output,
            evidence=evidence,
            usage=result.usage,
        )

    def _save_session(
        self,
        context: RunExecutionContext,
        session_id: str,
        final_text: str,
        summary: str,
    ) -> None:
        existing = self.manager.session_store.load(session_id)
        messages = list(existing.messages) if existing is not None else []
        marker = (
            f"subscription:{context.claim.run.id}:"
            f"{context.claim.lease.fencing_token}"
        )
        if any(str(item.get("orchestration_segment") or "") == marker for item in messages):
            return
        messages.extend(
            [
                {
                    "role": "user",
                    "content": _prompt(context),
                    "orchestration_segment": marker,
                },
                {
                    "role": "assistant",
                    "content": final_text or summary,
                    "orchestration_segment": marker,
                    "runtime": self.spec.runtime_id,
                },
            ]
        )
        self.manager.session_store.save(
            SessionRecord(
                session_id=session_id,
                workspace=str(context.workspace or ""),
                model=self.spec.runtime_id,
                mode=context.profile.role.value,
                messages=messages,
                title=context.node.title or context.node.key,
                agent=f"orchestration-{context.profile.role.value}",
            )
        )


def _stderr_reader(stream: Any, chunks: list[str]) -> None:
    size = 0
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            encoded = line.encode("utf-8", errors="replace")
            if size < _STDERR_LIMIT:
                remaining = _STDERR_LIMIT - size
                chunks.append(encoded[:remaining].decode("utf-8", errors="replace"))
                size += min(len(encoded), remaining)
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _catalog_supports(value: Any, model: str, effort: str) -> bool:
    """Search Codex model catalog shapes without coupling to one CLI release."""

    if isinstance(value, Mapping):
        identifiers = {
            str(value.get(key) or "")
            for key in ("id", "model", "slug", "name")
        }
        if model in identifiers:
            encoded = json.dumps(value, sort_keys=True).lower()
            return effort.lower() in encoded
        return any(_catalog_supports(item, model, effort) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_catalog_supports(item, model, effort) for item in value)
    return False


class CodexSubscriptionRuntime(_BaseSubscriptionRuntime):
    """GPT-5.6 Sol Max through the durable Codex app-server protocol."""

    def build_command(
        self,
        context: RunExecutionContext,
        checkpoint: Optional[Mapping[str, Any]] = None,
        schema: Optional[Mapping[str, Any]] = None,
    ) -> list[str]:
        executable = shutil.which(self.spec.command) or self.spec.command
        # Model, effort, sandbox, and prompts are sent as typed RPC parameters. They
        # do not appear in the host process list.
        return [executable, "app-server", "--stdio", "--strict-config"]

    def probe(self) -> SubscriptionRuntimeHealth:
        now = time.time()
        executable = shutil.which(self.spec.command)
        if not executable:
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                False,
                False,
                False,
                True,
                reason="Codex CLI is not installed or is not on PATH",
                checked_at=now,
            )
        try:
            version_result = _run_probe(executable, ["--version"], self.spec.provider)
            version = (version_result.stdout or version_result.stderr).strip()
            if version_result.returncode != 0 or _version_tuple(version) < self.spec.minimum_cli_version:
                return SubscriptionRuntimeHealth(
                    self.spec.runtime_id,
                    self.spec.provider,
                    True,
                    False,
                    False,
                    True,
                    version=version,
                    executable=executable,
                    reason=(
                        "Codex CLI is too old; required >= "
                        + ".".join(map(str, self.spec.minimum_cli_version))
                    ),
                    checked_at=now,
                )
            help_result = _run_probe(
                executable, ["app-server", "--help"], self.spec.provider
            )
            if help_result.returncode != 0 or "--stdio" not in help_result.stdout:
                raise RuntimeError("Codex app-server stdio protocol is unavailable")
            auth = _run_probe(executable, ["login", "status"], self.spec.provider)
            authenticated = auth.returncode == 0 and "chatgpt" in (
                auth.stdout + auth.stderr
            ).lower()
            if not authenticated:
                return SubscriptionRuntimeHealth(
                    self.spec.runtime_id,
                    self.spec.provider,
                    True,
                    False,
                    False,
                    True,
                    version=version,
                    auth_kind="not_chatgpt_subscription",
                    executable=executable,
                    reason="Codex is not logged in with a ChatGPT subscription",
                    checked_at=now,
                )
            catalog = _run_probe(executable, ["debug", "models"], self.spec.provider)
            try:
                catalog_value = json.loads(catalog.stdout)
            except json.JSONDecodeError:
                catalog_value = None
            if (
                catalog.returncode != 0
                or catalog_value is None
                or not _catalog_supports(
                    catalog_value, self.spec.cli_model, self.spec.reasoning_effort
                )
            ):
                return SubscriptionRuntimeHealth(
                    self.spec.runtime_id,
                    self.spec.provider,
                    True,
                    True,
                    False,
                    True,
                    version=version,
                    auth_kind="chatgpt_subscription",
                    executable=executable,
                    reason=(
                        f"{self.spec.cli_model} with effort "
                        f"{self.spec.reasoning_effort} is absent from the live Codex catalog"
                    ),
                    checked_at=now,
                )
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                True,
                True,
                True,
                True,
                version=version,
                auth_kind="chatgpt_subscription",
                executable=executable,
                checked_at=now,
            )
        except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                True,
                False,
                False,
                True,
                executable=executable,
                reason=_bounded(_redact_text(str(exc)), 512),
                checked_at=now,
            )

    def interrupt(self, run_id: str) -> None:
        with self._active_lock:
            self._interrupt_requested.add(run_id)
            active = self._active.get(run_id)
        if active is None:
            return
        if active.interrupt_timer is not None:
            return
        if active.thread_id and active.turn_id and active.proc.poll() is None:
            try:
                active.send(
                    {
                        "method": "turn/interrupt",
                        "id": f"openworker-interrupt-{uuid.uuid4().hex}",
                        "params": {
                            "threadId": active.thread_id,
                            "turnId": active.turn_id,
                        },
                    }
                )
                # turn/interrupt is cooperative. A bounded kill fallback owns the
                # app-server and every descendant if no terminal event arrives.
                active.interrupt_timer = threading.Timer(
                    1.5, lambda: active.tree.terminate()
                )
                active.interrupt_timer.daemon = True
                active.interrupt_timer.start()
                return
            except (OSError, BrokenPipeError):
                pass
        active.cleanup_ok = active.tree.terminate()

    async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
        session_id = context.claim.run.session_id or f"__orch__{context.claim.run.id}"
        health = await asyncio.to_thread(self.probe)
        if not health.available:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="subscription_runtime_unavailable",
                error_message=health.reason or "Codex subscription runtime is unavailable",
            )
        try:
            checkpoint = self._load_checkpoint(context)
        except RuntimeError as exc:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="recovery_checkpoint_invalid",
                error_message=str(exc),
            )
        if str(checkpoint.get("state") or "") == "result_sealed":
            try:
                return self._finish_outcome(
                    context, self._load_sealed_protocol_result(context, checkpoint)
                )
            except RuntimeError as exc:
                return ExecutionOutcome(
                    status="failed",
                    session_id=session_id,
                    error_kind="recovery_checkpoint_invalid",
                    error_message=str(exc),
                )
        operation = asyncio.create_task(
            asyncio.to_thread(self._run_app_server, context, health, checkpoint)
        )
        try:
            result = await operation
        except asyncio.CancelledError:
            self.interrupt(context.claim.run.id)
            try:
                result = await asyncio.wait_for(asyncio.shield(operation), timeout=4.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                raise
            result = self._seal_protocol_result(context, result)
            if not result.cleanup_ok:
                return self._finish_outcome(context, result)
            raise
        return self._finish_outcome(
            context, self._seal_protocol_result(context, result)
        )

    @staticmethod
    def _sandbox(context: RunExecutionContext) -> tuple[str, dict[str, Any]]:
        workspace = str((context.workspace or Path.cwd()).resolve())
        network = bool(context.task.policy.get("network", False))
        if bool(context.task.policy.get("read_only", False)) or (
            context.profile.role in _READ_ONLY_ROLES
        ):
            return "read-only", {"type": "readOnly", "networkAccess": False}
        # Testers need a writable disposable snapshot for caches/build outputs. The
        # service never publishes Tester changes to the task candidate.
        return "workspace-write", {
            "type": "workspaceWrite",
            "writableRoots": [workspace],
            "networkAccess": network,
            "excludeTmpdirEnvVar": True,
            "excludeSlashTmp": True,
        }

    def _run_app_server(
        self,
        context: RunExecutionContext,
        health: SubscriptionRuntimeHealth,
        checkpoint: Mapping[str, Any],
    ) -> _ProtocolResult:
        started = time.monotonic()
        workspace = (context.workspace or Path.cwd()).resolve()
        stderr_chunks: list[str] = []
        events: list[Mapping[str, Any]] = []
        final_messages: list[tuple[Optional[str], str]] = []
        tool_ids: set[str] = set()
        usage_tokens = 0
        external_session_id = str(checkpoint.get("external_session_id") or "")
        external_turn_id = str(checkpoint.get("external_turn_id") or "")
        resolved_model = ""
        terminal_status = "failed"
        error_kind: Optional[str] = None
        error_message: Optional[str] = None
        protocol_bytes = 0
        capability_violation: Optional[str] = None
        active: Optional[_ActiveProcess] = None
        stderr_thread: Optional[threading.Thread] = None

        def safe_error(value: Any) -> str:
            if isinstance(value, Mapping):
                return _bounded(_redact_text(str(value.get("message") or value)), 1024)
            return _bounded(_redact_text(str(value)), 1024)

        def record(message: Mapping[str, Any]) -> None:
            nonlocal usage_tokens, capability_violation
            events.append(dict(message))
            method = str(message.get("method") or "")
            params = dict(message.get("params") or {})
            if method == "model/rerouted":
                capability_violation = "Codex rerouted the explicitly pinned model"
            if method in {"item/completed", "item/started"}:
                item = dict(params.get("item") or {})
                item_type = str(item.get("type") or "")
                if item_type in {"collabAgentToolCall", "subAgentActivity"}:
                    capability_violation = (
                        "Codex attempted an internal subagent outside OpenWorker runtime control"
                    )
                if method == "item/completed":
                    if item_type == "agentMessage" and item.get("text") is not None:
                        final_messages.append(
                            (item.get("phase"), str(item.get("text") or ""))
                        )
                    if item_type in {
                        "commandExecution",
                        "fileChange",
                        "mcpToolCall",
                        "dynamicToolCall",
                        "webSearch",
                    }:
                        tool_ids.add(str(item.get("id") or f"anonymous-{len(tool_ids)}"))
            if method == "thread/tokenUsage/updated":
                total = dict((params.get("tokenUsage") or {}).get("total") or {})
                # total is cumulative for the thread and covers every model/tool loop
                # completion in this turn. Cached/reasoning tokens are subsets.
                candidate = int(total.get("inputTokens", 0) or 0) + int(
                    total.get("outputTokens", 0) or 0
                )
                usage_tokens = max(usage_tokens, candidate)

        def read_message() -> Mapping[str, Any]:
            nonlocal protocol_bytes
            if active is None or active.proc.stdout is None:
                raise RuntimeError("Codex app-server stdout is unavailable")
            line = active.proc.stdout.readline()
            if not line:
                raise RuntimeError("Codex app-server closed before a terminal event")
            protocol_bytes += len(line.encode("utf-8", errors="replace"))
            if protocol_bytes > _OUTPUT_LIMIT:
                raise RuntimeError("Codex app-server protocol output exceeded its limit")
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                events.append({"type": "protocol.invalid_json", "raw": _bounded(line, 4096)})
                raise RuntimeError("Codex app-server emitted malformed JSON") from exc
            if not isinstance(value, Mapping):
                raise RuntimeError("Codex app-server emitted a non-object message")
            message = dict(value)
            record(message)
            if "method" in message and "id" in message:
                # No durable approval bridge is advertised for this runtime version.
                # Fail closed and prevent an unattended request from hanging forever.
                method = str(message.get("method") or "")
                result: Optional[dict[str, Any]] = None
                if method in {
                    "item/commandExecution/requestApproval",
                    "item/fileChange/requestApproval",
                }:
                    result = {"decision": "cancel"}
                elif method == "item/permissions/requestApproval":
                    result = {"permissions": {}, "scope": "turn"}
                if result is not None:
                    active.send({"id": message["id"], "result": result})
                else:
                    active.send(
                        {
                            "id": message["id"],
                            "error": {
                                "code": -32601,
                                "message": "Unsupported server request denied by OpenWorker",
                            },
                        }
                    )
                raise RuntimeError(f"unexpected Codex server request: {method}")
            return message

        def request(request_id: Any, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
            assert active is not None
            active.send({"method": method, "id": request_id, "params": dict(params)})
            while True:
                message = read_message()
                if "method" in message:
                    continue
                if message.get("id") != request_id:
                    # Responses for our cooperative interrupt can arrive while the
                    # execution thread waits for another request. Preserve and ignore.
                    continue
                if "error" in message:
                    raise RuntimeError(f"{method} failed: {safe_error(message['error'])}")
                result = message.get("result")
                if not isinstance(result, Mapping):
                    raise RuntimeError(f"{method} returned an invalid response")
                return dict(result)

        try:
            active = self._spawn(
                self.build_command(context, checkpoint, _result_schema()),
                cwd=workspace,
                env=_safe_environment(self.spec.provider),
            )
            active.run_id = context.claim.run.id
            self._register(active)
            assert active.proc.stderr is not None
            stderr_thread = threading.Thread(
                target=_stderr_reader,
                args=(active.proc.stderr, stderr_chunks),
                daemon=True,
            )
            stderr_thread.start()
            initialized = request(
                1,
                "initialize",
                {
                    "clientInfo": {
                        "name": "openworker",
                        "title": "OpenWorker Subscription Runtime",
                        "version": "1.0.0",
                    }
                },
            )
            runtime_version = str(initialized.get("userAgent") or health.version)
            active.send({"method": "initialized"})
            thread_mode, sandbox_policy = self._sandbox(context)
            thread_params: dict[str, Any] = {
                "threadId": external_session_id,
                "model": self.spec.cli_model,
                "cwd": str(workspace),
                "approvalPolicy": "never",
                "approvalsReviewer": "user",
                "sandbox": thread_mode,
                "baseInstructions": "",
                "developerInstructions": _prompt(context),
                "config": {
                    "project_doc_max_bytes": 0,
                    "features": {
                        "apps": False,
                        "browser_use": False,
                        "browser_use_external": False,
                        "browser_use_full_cdp_access": False,
                        "computer_use": False,
                        "hooks": False,
                        "image_generation": False,
                        "multi_agent": False,
                        "plugins": False,
                        "skill_search": False,
                        "workspace_dependencies": False,
                    },
                    "mcp_servers": {},
                },
            }
            if external_session_id:
                thread_result = request(2, "thread/resume", thread_params)
            else:
                thread_params.pop("threadId")
                thread_params.update(
                    {
                        "ephemeral": False,
                        "serviceName": "openworker_subscription_runtime",
                    }
                )
                thread_result = request(2, "thread/start", thread_params)
            thread = dict(thread_result.get("thread") or {})
            actual_thread_id = str(thread.get("id") or "")
            if not actual_thread_id or (
                external_session_id and actual_thread_id != external_session_id
            ):
                raise RuntimeError("Codex returned a mismatched thread id")
            external_session_id = actual_thread_id
            active.thread_id = external_session_id
            resolved_model = str(thread_result.get("model") or self.spec.cli_model)
            if resolved_model != self.spec.cli_model:
                raise RuntimeError(
                    f"Codex resolved {resolved_model!r}, expected {self.spec.cli_model!r}"
                )
            if str(thread_result.get("modelProvider") or "openai") != "openai":
                raise RuntimeError("Codex thread did not resolve to the OpenAI provider")
            instruction_sources = list(thread_result.get("instructionSources") or ())
            if instruction_sources:
                raise RuntimeError(
                    "Codex loaded uncontrolled instruction sources despite the frozen profile"
                )
            self._checkpoint(
                context,
                external_session_id=external_session_id,
                runtime_version=runtime_version,
                state="thread_bound",
            )

            # A completed turn whose OpenWorker commit was interrupted is rebuilt
            # from persistent thread history without a second model call.
            if external_turn_id:
                historic = next(
                    (
                        dict(item)
                        for item in thread.get("turns") or ()
                        if str(item.get("id") or "") == external_turn_id
                    ),
                    None,
                )
                if historic is None:
                    raise RuntimeError("checkpointed Codex turn is absent after resume")
                historic_status = str(historic.get("status") or "")
                if historic_status == "completed":
                    for item in historic.get("items") or ():
                        if item.get("type") == "agentMessage":
                            final_messages.append(
                                (item.get("phase"), str(item.get("text") or ""))
                            )
                    terminal_status = "completed"
                elif historic_status == "interrupted":
                    # Controlled shutdown requeues only idempotent/read-only work;
                    # continue in the same isolated thread with a fresh turn.
                    external_turn_id = ""
                elif historic_status == "failed":
                    terminal_status = "failed"
                    error_kind = "codex_turn_failed"
                    error_message = safe_error(historic.get("error") or "Codex turn failed")
                else:
                    raise RuntimeError(
                        "checkpointed Codex turn has uncertain active state; reconciliation required"
                    )

            if terminal_status != "completed" and error_kind is None:
                turn_result = request(
                    3,
                    "turn/start",
                    {
                        "threadId": external_session_id,
                        "input": [{"type": "text", "text": _prompt(context)}],
                        "cwd": str(workspace),
                        "model": self.spec.cli_model,
                        "effort": self.spec.reasoning_effort,
                        "approvalPolicy": "never",
                        "approvalsReviewer": "user",
                        "sandboxPolicy": sandbox_policy,
                        "summary": "concise",
                        "outputSchema": _result_schema(),
                    },
                )
                turn = dict(turn_result.get("turn") or {})
                external_turn_id = str(turn.get("id") or "")
                if not external_turn_id:
                    raise RuntimeError("Codex turn/start returned no turn id")
                active.turn_id = external_turn_id
                self._checkpoint(
                    context,
                    external_session_id=external_session_id,
                    external_turn_id=external_turn_id,
                    runtime_version=runtime_version,
                    state="turn_started",
                )
                while True:
                    message = read_message()
                    method = str(message.get("method") or "")
                    params = dict(message.get("params") or {})
                    if capability_violation:
                        error_kind = "runtime_capability_violation"
                        error_message = capability_violation
                        self.interrupt(context.claim.run.id)
                    if method == "error" and not bool(params.get("willRetry", False)):
                        error_message = safe_error(params.get("error") or "Codex turn error")
                    if method != "turn/completed":
                        continue
                    terminal = dict(params.get("turn") or {})
                    if (
                        str(params.get("threadId") or "") != external_session_id
                        or str(terminal.get("id") or "") != external_turn_id
                    ):
                        continue
                    terminal_status = str(terminal.get("status") or "failed")
                    if terminal_status == "failed":
                        error_kind = error_kind or "codex_turn_failed"
                        error_message = error_message or safe_error(
                            terminal.get("error") or "Codex turn failed"
                        )
                    elif terminal_status == "interrupted":
                        error_kind = error_kind or "codex_turn_interrupted"
                        error_message = error_message or "Codex turn was interrupted"
                    break
            final_text = next(
                (text for phase, text in reversed(final_messages) if phase == "final_answer"),
                final_messages[-1][1] if final_messages else "",
            )
            structured = _structured(final_text)
            if terminal_status == "completed" and structured is None:
                error_kind = "structured_output_invalid"
                error_message = "Codex completed without the required JSON object"
            observed_cleanup_ok = active.tree.terminate() if active is not None else True
            usage = {
                "model_calls": 1 if external_turn_id else 0,
                "tool_calls": len(tool_ids),
                "tokens": usage_tokens,
                "wall_seconds": max(1, int(time.monotonic() - started)),
            }
            return _ProtocolResult(
                terminal_status=terminal_status,
                final_text=final_text,
                structured=structured,
                external_session_id=external_session_id,
                external_turn_id=external_turn_id,
                events=tuple(events),
                stderr="".join(stderr_chunks),
                usage=usage,
                runtime_version=runtime_version,
                resolved_model=resolved_model,
                error_kind=error_kind,
                error_message=error_message,
                cleanup_ok=observed_cleanup_ok,
            )
        except Exception as exc:
            observed_cleanup_ok = active.tree.terminate() if active is not None else True
            return _ProtocolResult(
                terminal_status="failed",
                final_text="",
                structured=None,
                external_session_id=external_session_id,
                external_turn_id=external_turn_id,
                events=tuple(events),
                stderr="".join(stderr_chunks),
                usage={
                    "model_calls": 1 if external_turn_id else 0,
                    "tool_calls": len(tool_ids),
                    "tokens": usage_tokens,
                    "wall_seconds": max(1, int(time.monotonic() - started)),
                },
                runtime_version=health.version,
                resolved_model=resolved_model,
                error_kind=(
                    "recovery_state_uncertain"
                    if "uncertain active state" in str(exc)
                    else "codex_protocol_error"
                ),
                error_message=_bounded(_redact_text(str(exc)), 2048),
                cleanup_ok=observed_cleanup_ok,
            )
        finally:
            if active is not None:
                if active.proc.stdin is not None:
                    try:
                        active.proc.stdin.close()
                    except OSError:
                        pass
                cleanup_ok = active.tree.terminate()
                active.cleanup_ok = (
                    cleanup_ok if active.cleanup_ok is None else active.cleanup_ok and cleanup_ok
                )
                self._unregister(active)
            if stderr_thread is not None:
                stderr_thread.join(timeout=1.0)


class ClaudeCodeSubscriptionRuntime(_BaseSubscriptionRuntime):
    """Claude Opus 5 through a local, already authenticated Claude Code CLI."""

    @staticmethod
    def _tools(context: RunExecutionContext) -> tuple[str, str]:
        visible = {"Read", "Glob", "Grep"}
        read_only = bool(context.task.policy.get("read_only", False))
        if not read_only and context.profile.role is AgentRole.TESTER:
            visible.add("Bash")
        elif not read_only and context.profile.role in {
            AgentRole.WORKER,
            AgentRole.INTEGRATOR,
        }:
            visible.update({"Edit", "Write", "Bash"})
        if bool(context.task.policy.get("network", False)):
            visible.update({"WebFetch", "WebSearch"})
        known = {
            "Read",
            "Glob",
            "Grep",
            "Edit",
            "Write",
            "Bash",
            "WebFetch",
            "WebSearch",
            "NotebookEdit",
            "Agent",
            "Task",
        }
        return ",".join(sorted(visible)), ",".join(sorted(known - visible))

    def build_command(
        self,
        context: RunExecutionContext,
        checkpoint: Optional[Mapping[str, Any]] = None,
        schema: Optional[Mapping[str, Any]] = None,
    ) -> list[str]:
        checkpoint = dict(checkpoint or {})
        executable = shutil.which(self.spec.command) or self.spec.command
        allowed, denied = self._tools(context)
        argv = [
            executable,
            "--print",
            "--model",
            self.spec.cli_model,
            "--effort",
            self.spec.reasoning_effort,
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--safe-mode",
            "--permission-mode",
            "dontAsk",
            "--tools",
            allowed,
            "--disallowedTools",
            denied,
            "--json-schema",
            json.dumps(schema or _result_schema(), separators=(",", ":")),
        ]
        external = str(checkpoint.get("external_session_id") or "")
        if external and str(checkpoint.get("state") or "") != "session_reserved":
            argv.extend(["--resume", external])
        else:
            argv.extend(
                [
                    "--session-id",
                    str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"openworker:{context.claim.run.id}:{self.spec.runtime_id}",
                        )
                    ),
                ]
            )
        return argv

    def probe(self) -> SubscriptionRuntimeHealth:
        now = time.time()
        executable = shutil.which(self.spec.command)
        if not executable:
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                False,
                False,
                False,
                True,
                reason="Claude Code CLI is not installed or is not on PATH",
                checked_at=now,
            )
        try:
            version_result = _run_probe(executable, ["--version"], self.spec.provider)
            version = (version_result.stdout or version_result.stderr).strip()
            if version_result.returncode != 0 or _version_tuple(version) < self.spec.minimum_cli_version:
                return SubscriptionRuntimeHealth(
                    self.spec.runtime_id,
                    self.spec.provider,
                    True,
                    False,
                    False,
                    True,
                    version=version,
                    executable=executable,
                    reason=(
                        "Claude Code is too old; required >= "
                        + ".".join(map(str, self.spec.minimum_cli_version))
                    ),
                    checked_at=now,
                )
            help_result = _run_probe(executable, ["--help"], self.spec.provider)
            required_flags = {
                "--effort",
                "--output-format",
                "--resume",
                "--safe-mode",
                "--json-schema",
            }
            if help_result.returncode != 0 or not required_flags.issubset(
                set(re.findall(r"--[a-zA-Z][a-zA-Z-]*", help_result.stdout))
            ):
                raise RuntimeError("Claude Code is missing required headless flags")
            auth_result = _run_probe(
                executable, ["auth", "status", "--json"], self.spec.provider
            )
            try:
                auth = json.loads(auth_result.stdout)
            except json.JSONDecodeError:
                auth = {}
            authenticated = bool(auth.get("loggedIn")) and str(
                auth.get("authMethod") or ""
            ) == "claude.ai" and str(auth.get("apiProvider") or "") == "firstParty"
            if not authenticated:
                return SubscriptionRuntimeHealth(
                    self.spec.runtime_id,
                    self.spec.provider,
                    True,
                    False,
                    False,
                    True,
                    version=version,
                    auth_kind="not_first_party_subscription",
                    executable=executable,
                    reason="Claude Code is not logged in with first-party claude.ai auth",
                    checked_at=now,
                )
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                True,
                True,
                True,
                True,
                version=version,
                auth_kind="claude_ai_subscription",
                executable=executable,
                checked_at=now,
            )
        except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                True,
                False,
                False,
                True,
                executable=executable,
                reason=_bounded(_redact_text(str(exc)), 512),
                checked_at=now,
            )

    async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
        session_id = context.claim.run.session_id or f"__orch__{context.claim.run.id}"
        health = await asyncio.to_thread(self.probe)
        if not health.available:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="subscription_runtime_unavailable",
                error_message=health.reason or "Claude Code subscription runtime is unavailable",
            )
        try:
            checkpoint = self._load_checkpoint(context)
        except RuntimeError as exc:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="recovery_checkpoint_invalid",
                error_message=str(exc),
            )
        if str(checkpoint.get("state") or "") == "result_sealed":
            try:
                return self._finish_outcome(
                    context, self._load_sealed_protocol_result(context, checkpoint)
                )
            except RuntimeError as exc:
                return ExecutionOutcome(
                    status="failed",
                    session_id=session_id,
                    error_kind="recovery_checkpoint_invalid",
                    error_message=str(exc),
                )
        if checkpoint and str(checkpoint.get("state") or "") != "session_reserved":
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="recovery_state_uncertain",
                error_message=(
                    "Claude Code has no zero-model-call headless turn-status API; "
                    "the prior prompt may have executed, so automatic replay is disabled"
                ),
            )
        if not checkpoint:
            external_session_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"openworker:{context.claim.run.id}:{self.spec.runtime_id}",
                )
            )
            checkpoint = self._checkpoint(
                context,
                external_session_id=external_session_id,
                runtime_version=health.version,
                state="session_reserved",
            )
        operation = asyncio.create_task(
            asyncio.to_thread(self._run_cli, context, health, checkpoint)
        )
        try:
            result = await operation
        except asyncio.CancelledError:
            self.interrupt(context.claim.run.id)
            try:
                result = await asyncio.wait_for(asyncio.shield(operation), timeout=4.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                raise
            result = self._seal_protocol_result(context, result)
            if not result.cleanup_ok:
                return self._finish_outcome(context, result)
            raise
        return self._finish_outcome(
            context, self._seal_protocol_result(context, result)
        )

    @staticmethod
    def _usage_from_result(event: Mapping[str, Any]) -> int:
        per_model = event.get("modelUsage") or event.get("model_usage")
        if isinstance(per_model, Mapping):
            total = 0
            for value in per_model.values():
                if isinstance(value, Mapping):
                    total += int(value.get("inputTokens", value.get("input_tokens", 0)) or 0)
                    total += int(value.get("outputTokens", value.get("output_tokens", 0)) or 0)
            if total:
                return total
        usage = dict(event.get("usage") or {})
        return int(usage.get("input_tokens", usage.get("inputTokens", 0)) or 0) + int(
            usage.get("output_tokens", usage.get("outputTokens", 0)) or 0
        )

    def _run_cli(
        self,
        context: RunExecutionContext,
        health: SubscriptionRuntimeHealth,
        checkpoint: Mapping[str, Any],
    ) -> _ProtocolResult:
        started = time.monotonic()
        workspace = (context.workspace or Path.cwd()).resolve()
        external_session_id = str(checkpoint.get("external_session_id") or "")
        events: list[Mapping[str, Any]] = []
        stderr_chunks: list[str] = []
        assistant_texts: list[str] = []
        structured_output: Optional[Mapping[str, Any]] = None
        resolved_model = ""
        usage_tokens = 0
        tool_ids: set[str] = set()
        terminal_status = "failed"
        error_kind: Optional[str] = None
        error_message: Optional[str] = None
        protocol_bytes = 0
        active: Optional[_ActiveProcess] = None
        stderr_thread: Optional[threading.Thread] = None
        cleanup_ok = True
        try:
            argv = self.build_command(context, checkpoint, _result_schema())
            active = self._spawn(
                argv,
                cwd=workspace,
                env=_safe_environment(self.spec.provider),
            )
            active.run_id = context.claim.run.id
            active.thread_id = external_session_id
            self._register(active)
            self._checkpoint(
                context,
                external_session_id=external_session_id,
                runtime_version=health.version,
                state="session_started",
            )
            assert active.proc.stderr is not None
            stderr_thread = threading.Thread(
                target=_stderr_reader,
                args=(active.proc.stderr, stderr_chunks),
                daemon=True,
            )
            stderr_thread.start()
            if active.proc.stdin is None:
                raise RuntimeError("Claude Code stdin is unavailable")
            active.proc.stdin.write(_prompt(context))
            active.proc.stdin.close()
            self._checkpoint(
                context,
                external_session_id=external_session_id,
                runtime_version=health.version,
                state="prompt_submitted",
            )
            if active.proc.stdout is None:
                raise RuntimeError("Claude Code stdout is unavailable")
            for line in iter(active.proc.stdout.readline, ""):
                if not line:
                    break
                protocol_bytes += len(line.encode("utf-8", errors="replace"))
                if protocol_bytes > _OUTPUT_LIMIT:
                    raise RuntimeError("Claude Code protocol output exceeded its limit")
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    events.append(
                        {"type": "protocol.invalid_json", "raw": _bounded(line, 4096)}
                    )
                    raise RuntimeError("Claude Code emitted malformed stream-json") from exc
                if not isinstance(raw, Mapping):
                    raise RuntimeError("Claude Code emitted a non-object event")
                event = dict(raw)
                events.append(event)
                event_type = str(event.get("type") or "")
                observed_session = str(event.get("session_id") or "")
                if observed_session and observed_session != external_session_id:
                    raise RuntimeError("Claude Code returned a mismatched session id")
                if event_type == "system" and str(event.get("subtype") or "") == "init":
                    resolved_model = str(event.get("model") or "")
                    if resolved_model and resolved_model != self.spec.cli_model:
                        raise RuntimeError(
                            f"Claude Code resolved {resolved_model!r}, expected "
                            f"{self.spec.cli_model!r}"
                        )
                    capabilities = event.get("capabilities")
                    if isinstance(capabilities, Mapping) and capabilities.get("agents"):
                        raise RuntimeError("Claude Code enabled uncontrolled built-in agents")
                elif event_type == "assistant":
                    message = dict(event.get("message") or {})
                    message_model = str(message.get("model") or "")
                    if message_model:
                        resolved_model = message_model
                    for block in message.get("content") or ():
                        if not isinstance(block, Mapping):
                            continue
                        block_type = str(block.get("type") or "")
                        if block_type == "text":
                            assistant_texts.append(str(block.get("text") or ""))
                        elif block_type == "tool_use":
                            tool_ids.add(str(block.get("id") or f"tool-{len(tool_ids)}"))
                elif event_type == "result":
                    if event.get("structured_output") is not None:
                        structured_output = _structured(event.get("structured_output"))
                    result_text = str(event.get("result") or "")
                    if result_text:
                        assistant_texts.append(result_text)
                    usage_tokens = max(usage_tokens, self._usage_from_result(event))
                    subtype = str(event.get("subtype") or "")
                    if subtype == "success" and not bool(event.get("is_error", False)):
                        terminal_status = "completed"
                    else:
                        terminal_status = "failed"
                        error_kind = (
                            "runtime_budget_exceeded"
                            if "budget" in subtype or "max_turn" in subtype
                            else "claude_code_failed"
                        )
                        error_message = _bounded(
                            _redact_text(
                                str(event.get("errors") or event.get("result") or subtype)
                            ),
                            2048,
                        )
                    denials = event.get("permission_denials") or ()
                    if denials:
                        terminal_status = "failed"
                        error_kind = "runtime_permission_denied"
                        error_message = "Claude Code required a permission outside the role ceiling"
            return_code = active.proc.wait(timeout=2)
            if return_code != 0 and terminal_status == "completed":
                terminal_status = "failed"
                error_kind = "claude_code_process_failed"
                error_message = f"Claude Code exited with status {return_code}"
            final_text = assistant_texts[-1] if assistant_texts else ""
            structured_output = structured_output or _structured(final_text)
            if terminal_status == "completed" and structured_output is None:
                error_kind = "structured_output_invalid"
                error_message = "Claude Code completed without the required JSON object"
            cleanup_ok = active.tree.terminate() if active is not None else True
            return _ProtocolResult(
                terminal_status=terminal_status,
                final_text=final_text,
                structured=structured_output,
                external_session_id=external_session_id,
                external_turn_id="",
                events=tuple(events),
                stderr="".join(stderr_chunks),
                usage={
                    "model_calls": 1,
                    "tool_calls": len(tool_ids),
                    "tokens": usage_tokens,
                    "wall_seconds": max(1, int(time.monotonic() - started)),
                },
                runtime_version=health.version,
                resolved_model=resolved_model or self.spec.cli_model,
                error_kind=error_kind,
                error_message=error_message,
                cleanup_ok=cleanup_ok,
            )
        except Exception as exc:
            cleanup_ok = active.tree.terminate() if active is not None else True
            return _ProtocolResult(
                terminal_status="failed",
                final_text="",
                structured=None,
                external_session_id=external_session_id,
                external_turn_id="",
                events=tuple(events),
                stderr="".join(stderr_chunks),
                usage={
                    "model_calls": 1,
                    "tool_calls": len(tool_ids),
                    "tokens": usage_tokens,
                    "wall_seconds": max(1, int(time.monotonic() - started)),
                },
                runtime_version=health.version,
                resolved_model=resolved_model,
                error_kind="claude_code_protocol_error",
                error_message=_bounded(_redact_text(str(exc)), 2048),
                cleanup_ok=cleanup_ok,
            )
        finally:
            if active is not None:
                cleanup_ok = active.tree.terminate()
                active.cleanup_ok = (
                    cleanup_ok if active.cleanup_ok is None else active.cleanup_ok and cleanup_ok
                )
                self._unregister(active)
            if stderr_thread is not None:
                stderr_thread.join(timeout=1.0)


class KimiCodeSubscriptionRuntime(_BaseSubscriptionRuntime):
    """Discover Kimi K3 Max while enforcing the managed-subscription use policy."""

    def build_command(
        self,
        context: RunExecutionContext,
        checkpoint: Optional[Mapping[str, Any]] = None,
        schema: Optional[Mapping[str, Any]] = None,
    ) -> list[str]:
        checkpoint = dict(checkpoint or {})
        external = str(checkpoint.get("external_session_id") or "") or (
            "session_" + uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"openworker:{context.claim.run.id}:{self.spec.runtime_id}",
            ).hex
        )
        executable = shutil.which(self.spec.command) or self.spec.command
        return [
            executable,
            "--session",
            external,
            "--model",
            self.spec.cli_model,
            "--prompt",
            _prompt(context),
            "--output-format",
            "stream-json",
        ]

    def probe(self) -> SubscriptionRuntimeHealth:
        now = time.time()
        executable = shutil.which(self.spec.command)
        if not executable:
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                False,
                False,
                False,
                False,
                reason="Kimi Code CLI is not installed or is not on PATH",
                checked_at=now,
            )
        try:
            version_result = _run_probe(executable, ["--version"], self.spec.provider)
            version = (version_result.stdout or version_result.stderr).strip()
            provider_result = _run_probe(executable, ["provider", "list"], self.spec.provider)
            provider_text = provider_result.stdout.lower()
            authenticated = (
                provider_result.returncode == 0
                and "managed:kimi-code" in provider_text
                and "source=oauth" in provider_text
                and "kimi-code/k3" in provider_text
            )
            version_ok = _version_tuple(version) >= self.spec.minimum_cli_version
            reason = (
                "Kimi Code managed OAuth subscription is interactive-only and cannot "
                "run OpenWorker background DAG/batch Agent tasks; use the Kimi Platform "
                "API provider or an enterprise automation agreement"
                if authenticated and version_ok
                else "Kimi Code OAuth K3 configuration was not detected"
            )
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                True,
                authenticated,
                False,
                False,
                version=version,
                auth_kind="kimi_managed_oauth" if authenticated else "unknown",
                executable=executable,
                reason=reason,
                checked_at=now,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return SubscriptionRuntimeHealth(
                self.spec.runtime_id,
                self.spec.provider,
                True,
                False,
                False,
                False,
                executable=executable,
                reason=_bounded(_redact_text(str(exc)), 512),
                checked_at=now,
            )

    async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
        # Deliberately reject before command construction or process launch. A task
        # field or environment variable must not become a terms-bypass switch.
        return ExecutionOutcome(
            status="failed",
            session_id=(
                context.claim.run.session_id or f"__orch__{context.claim.run.id}"
            ),
            error_kind="subscription_noninteractive_automation_forbidden",
            error_message=(
                "Kimi Code OAuth subscriptions are limited to interactive personal use; "
                "OpenWorker orchestration requires the Kimi Platform API or a separately "
                "authorized enterprise automation credential"
            ),
        )


class SubscriptionRuntimeRegistry:
    """Immutable runtime catalog plus cached, zero-model-call health probes."""

    def __init__(
        self,
        manager: Any,
        store: OrchestrationStore,
        blob_store: ContentAddressedBlobStore,
        state_dir: str | Path,
        *,
        runtimes: Optional[Iterable[SubscriptionAgentRuntime]] = None,
        local_owner_eligible: bool = True,
    ) -> None:
        if runtimes is None:
            specs = default_subscription_runtime_specs()
            built: list[SubscriptionAgentRuntime] = []
            for spec in specs:
                cls: type[_BaseSubscriptionRuntime]
                if spec.provider == "codex-subscription":
                    cls = CodexSubscriptionRuntime
                elif spec.provider == "claude-code-subscription":
                    cls = ClaudeCodeSubscriptionRuntime
                else:
                    cls = KimiCodeSubscriptionRuntime
                built.append(cls(spec, manager, store, blob_store, state_dir))
            runtimes = built
        runtime_list = list(runtimes)
        catalog = {runtime.spec.runtime_id: runtime for runtime in runtime_list}
        if len(catalog) != len(runtime_list):
            raise ValueError("duplicate subscription runtime id")
        self._runtimes = catalog
        self._local_owner_eligible = bool(local_owner_eligible)

    @property
    def specs(self) -> tuple[SubscriptionRuntimeSpec, ...]:
        return tuple(runtime.spec for runtime in self._runtimes.values())

    def resolve(self, runtime_id: str) -> Optional[SubscriptionAgentRuntime]:
        return self._runtimes.get(str(runtime_id))

    def health(
        self, runtime_id: str, *, refresh: bool = False
    ) -> SubscriptionRuntimeHealth:
        runtime = self._runtimes[str(runtime_id)]
        provider = runtime.spec.provider
        now = time.time()
        cache_key = f"{provider}:{os.environ.get('PATH', '')}"
        with _GLOBAL_HEALTH_LOCK:
            cached = _GLOBAL_HEALTH_CACHE.get(cache_key)
            if cached and not refresh and now - cached[0] < _PROBE_TTL_SECONDS:
                result = replace(cached[1], runtime_id=runtime.spec.runtime_id)
                return self._apply_owner_scope(runtime, result)
        result = runtime.probe()
        with _GLOBAL_HEALTH_LOCK:
            _GLOBAL_HEALTH_CACHE[cache_key] = (now, result)
        result = replace(result, runtime_id=runtime.spec.runtime_id)
        return self._apply_owner_scope(runtime, result)

    def _apply_owner_scope(
        self,
        runtime: SubscriptionAgentRuntime,
        health: SubscriptionRuntimeHealth,
    ) -> SubscriptionRuntimeHealth:
        if runtime.spec.local_owner_only and not self._local_owner_eligible:
            return replace(
                health,
                available=False,
                policy_eligible=False,
                reason=(
                    "Subscription Agent runtimes are restricted to a loopback-only "
                    "OpenWorker server owned by the logged-in desktop user"
                ),
            )
        return health

    def health_snapshot(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        return [
            {
                **runtime.spec.audit_dict(),
                "health": self.health(runtime.spec.runtime_id, refresh=refresh).audit_dict(),
            }
            for runtime in self._runtimes.values()
        ]

    def interactive_catalog(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        """Return subscription runtimes as interactive-session choices.

        ``SubscriptionRuntimeHealth.available`` deliberately means eligible for the
        background orchestration executor. That is not the same policy question as
        whether the logged-in desktop owner may start an interactive Agent session:
        Kimi managed OAuth, for example, is allowed for the latter and fail-closed for
        the former. Keep both decisions explicit so a UI never has to infer one from
        the other.
        """

        result: list[dict[str, Any]] = []
        for runtime in self._runtimes.values():
            spec = runtime.spec
            health = self.health(spec.runtime_id, refresh=refresh)
            interactive_eligible = False
            interactive_reason = health.reason

            if spec.local_owner_only and not self._local_owner_eligible:
                interactive_reason = (
                    "Subscription Agent runtimes are restricted to a loopback-only "
                    "OpenWorker server owned by the logged-in desktop user"
                )
            elif not health.installed:
                interactive_reason = health.reason or (
                    f"{spec.display_name} is not installed or is not on PATH"
                )
            elif not health.authenticated:
                interactive_reason = health.reason or (
                    f"{spec.display_name} is not authenticated with a subscription"
                )
            elif spec.interactive_only:
                version_ok = (
                    bool(health.version)
                    and _version_tuple(health.version) >= spec.minimum_cli_version
                )
                if version_ok:
                    interactive_eligible = True
                    interactive_reason = (
                        "Available for interactive personal sessions; background "
                        "DAG/batch execution remains blocked by subscription policy"
                    )
                else:
                    interactive_reason = (
                        f"{spec.display_name} is too old; required >= "
                        + ".".join(map(str, spec.minimum_cli_version))
                    )
            elif health.available and health.policy_eligible:
                interactive_eligible = True
                interactive_reason = (
                    "Subscription runtime is ready for interactive sessions"
                )

            background_eligible = health.available and health.policy_eligible
            result.append(
                {
                    "runtime_id": spec.runtime_id,
                    "provider": spec.provider,
                    "label": spec.display_name,
                    "model": spec.cli_model,
                    "reasoning_effort": spec.reasoning_effort,
                    "context_window": spec.context_window,
                    "interactive_only": spec.interactive_only,
                    "health": health.audit_dict(),
                    "interactive_eligible": interactive_eligible,
                    "interactive_reason": interactive_reason,
                    "background_eligible": background_eligible,
                    "background_reason": "" if background_eligible else health.reason,
                }
            )
        return result

    def model_candidates(self) -> tuple[ModelCandidate, ...]:
        result: list[ModelCandidate] = []
        for runtime in self._runtimes.values():
            health = self.health(runtime.spec.runtime_id)
            result.append(
                ModelCandidate(
                    model_id=runtime.spec.runtime_id,
                    provider=runtime.spec.provider,
                    quality=runtime.spec.quality,
                    capabilities=runtime.spec.capabilities,
                    context_window=runtime.spec.context_window,
                    latency_rank=1000,
                    configured=health.installed and health.authenticated,
                    available=health.available and health.policy_eligible,
                    verified=True,
                    catalog_revision="subscription-runtime-v1",
                )
            )
        return tuple(result)


class SubscriptionDispatchExecutor:
    """Select a native or subscription Agent runtime after deterministic routing."""

    def __init__(self, native: Any, registry: SubscriptionRuntimeRegistry) -> None:
        self.native = native
        self.registry = registry
        self._lock = threading.RLock()
        self._delegates: dict[str, Any] = {}

    async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
        selected = str(context.routing.selected_model or "")
        delegate = self.registry.resolve(selected) or self.native
        with self._lock:
            self._delegates[context.claim.run.id] = delegate
        try:
            return await delegate.execute(context)
        finally:
            with self._lock:
                self._delegates.pop(context.claim.run.id, None)

    def interrupt(self, run_id: str) -> None:
        with self._lock:
            delegate = self._delegates.get(run_id)
        if delegate is not None:
            interrupt = getattr(delegate, "interrupt", None)
            if callable(interrupt):
                interrupt(run_id)
            return
        # Cover the narrow race before execute records its delegate. Each adapter's
        # interrupt is idempotent and only touches a process registered to this run.
        native_interrupt = getattr(self.native, "interrupt", None)
        if callable(native_interrupt):
            native_interrupt(run_id)
        for runtime in self.registry._runtimes.values():
            runtime.interrupt(run_id)
