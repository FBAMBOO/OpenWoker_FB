"""P3 gate tests — persistent shell executor.

The executor drives the OS-native shell (bash on POSIX, PowerShell on Windows), so the
command strings here are parameterized per-OS. The behavior under test (cwd/env persistence,
exit codes, timeout-and-recover, truncation) is identical across both.
"""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
import sys
import threading
import time

import pytest

import coworker.tools.shell as shell_module
from coworker.permissions import PermissionEngine
from coworker.tools import ToolRegistry
from coworker.tools.shell import LocalExecutor, shell_tools

_WIN = sys.platform == "win32"

# Per-OS command snippets exercising the same behavior in the native shell.
SET_ENV = "$env:GREETING='hello_world'" if _WIN else "export GREETING=hello_world"
ECHO_ENV = "echo $env:GREETING" if _WIN else "echo $GREETING"
EXIT_OK = "cmd /c exit 0" if _WIN else "true"
EXIT_FAIL = "cmd /c exit 1" if _WIN else "false"
SLEEP_5 = "Start-Sleep -Seconds 5" if _WIN else "sleep 5"
PRINT_1000 = (
    'foreach ($i in 1..1000) { "line$i" }'
    if _WIN
    else "for i in $(seq 1 1000); do echo line$i; done"
)


@pytest.fixture
def executor(tmp_path):
    ex = LocalExecutor(cwd=tmp_path, default_timeout=10)
    yield ex
    ex.close()


def test_cwd_persists_across_calls(executor, tmp_path):
    (tmp_path / "sub").mkdir()
    executor.run("cd sub")
    result = executor.run("pwd")
    assert result["exit_code"] == 0
    assert "sub" in result["output"]
    assert executor.cwd.endswith("sub")


def test_env_persists_across_calls(executor):
    executor.run(SET_ENV)
    result = executor.run(ECHO_ENV)
    assert "hello_world" in result["output"]


def test_exit_code_captured(executor):
    assert executor.run(EXIT_OK)["exit_code"] == 0
    assert executor.run(EXIT_FAIL)["exit_code"] == 1


def test_timeout_kills_command(executor):
    start = time.monotonic()
    result = executor.run(SLEEP_5, timeout=1)
    elapsed = time.monotonic() - start
    assert result["timed_out"] is True
    assert elapsed < 4.0  # did not block for the full sleep
    # session survives the timeout — still usable (POSIX keeps the shell; Windows respawns)
    assert executor.run("echo alive")["output"].strip().endswith("alive")


def test_large_output_truncated_keeps_tail(tmp_path):
    ex = LocalExecutor(cwd=tmp_path, max_output_chars=200, default_timeout=10)
    try:
        result = ex.run(PRINT_1000)
        assert result["truncated"] is True
        assert len(result["output"]) <= 200
        # the END survives (where test/build verdicts live), the head is dropped
        assert "line1000" in result["output"]
        assert "line1\n" not in result["output"]
    finally:
        ex.close()


def test_shell_tool_integration(executor, tmp_path):
    reg = ToolRegistry()
    reg.register_all(shell_tools(executor))
    assert {"run_shell", "shell_task_output", "shell_task_kill"} <= set(reg.names())

    spec = reg.get("run_shell")
    assert spec.metadata.requires_approval is True
    # polling/killing the agent's own background tasks doesn't need approval
    assert reg.get("shell_task_output").metadata.requires_approval is False
    assert reg.get("shell_task_kill").metadata.requires_approval is False

    eng = PermissionEngine(workspace_root=tmp_path)
    decision = eng.evaluate("run_shell", {"command": "echo hi"}, spec.metadata)
    assert not decision.allowed and decision.needs_user  # high-risk → asks

    out = reg.execute("run_shell", {"command": "echo hi"})
    assert "hi" in out["output"]


def test_run_shell_accepts_description_and_clamped_timeout(executor):
    reg = ToolRegistry()
    reg.register_all(shell_tools(executor))
    # `description` rides along for approval prompts/audit; it must not break execution.
    out = reg.execute(
        "run_shell",
        {"command": "echo ok", "description": "Say ok", "timeout_seconds": 99999},
    )
    assert out["exit_code"] == 0 and "ok" in out["output"]


# -- background tasks ------------------------------------------------------------

ECHO_THEN_SLEEP = (
    "Write-Output started; Start-Sleep -Seconds 30"
    if _WIN
    else "echo started; sleep 30"
)
QUICK_ECHO = "Write-Output quick_done" if _WIN else "echo quick_done"


def _poll_output(reg, task_id, *, until_status=None, deadline=10.0):
    """Poll shell_task_output, accumulating output until a status is reached."""
    acc = ""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        res = reg.execute("shell_task_output", {"task_id": task_id})
        acc += res["output"]
        if until_status is None or res["status"] == until_status:
            if until_status is None and not acc:
                time.sleep(0.1)
                continue
            return acc, res
        time.sleep(0.1)
    return acc, res


def test_background_task_runs_and_exits(executor):
    reg = ToolRegistry()
    reg.register_all(shell_tools(executor))
    started = reg.execute(
        "run_shell", {"command": QUICK_ECHO, "run_in_background": True}
    )
    assert started["status"] == "running" and started["task_id"]

    acc, res = _poll_output(reg, started["task_id"], until_status="exited")
    assert res["status"] == "exited"
    assert res["exit_code"] == 0
    assert "quick_done" in acc

    # output reads are incremental: a second read returns nothing new
    again = reg.execute("shell_task_output", {"task_id": started["task_id"]})
    assert again["output"] == ""


def test_background_task_kill(executor):
    reg = ToolRegistry()
    reg.register_all(shell_tools(executor))
    started = reg.execute(
        "run_shell", {"command": ECHO_THEN_SLEEP, "run_in_background": True}
    )
    acc, _ = _poll_output(reg, started["task_id"])
    assert "started" in acc  # it's alive and producing output

    killed = reg.execute("shell_task_kill", {"task_id": started["task_id"]})
    assert killed["status"] == "killed"

    res = reg.execute("shell_task_output", {"task_id": started["task_id"]})
    assert res["status"] == "exited"


def test_background_unknown_task_errors(executor):
    reg = ToolRegistry()
    reg.register_all(shell_tools(executor))
    assert (
        "unknown task"
        in reg.execute("shell_task_output", {"task_id": "bg-99"})["error"]
    )
    assert (
        "unknown task" in reg.execute("shell_task_kill", {"task_id": "bg-99"})["error"]
    )


# -- constrained process-tree containment ---------------------------------------


def _python_command(script) -> str:
    if _WIN:
        # PowerShell's call operator handles interpreter/workspace paths with spaces.
        return f"& {subprocess.list2cmdline([sys.executable])} {subprocess.list2cmdline([str(script)])}"
    return f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}"


def _pid_is_running(pid: int) -> bool:
    if not _WIN:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    handle = kernel32.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION
    if not handle:
        return False
    try:
        code = wintypes.DWORD()
        return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
    finally:
        kernel32.CloseHandle(handle)


def _wait_pid_stopped(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_is_running(pid):
            return True
        time.sleep(0.05)
    return not _pid_is_running(pid)


def test_contained_normal_completion_reaps_spawned_descendant(tmp_path):
    helper = tmp_path / "spawn_then_exit.py"
    helper.write_text(
        "import subprocess, sys\n"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], "
        "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
        "print(p.pid, flush=True)\n",
        encoding="utf-8",
    )
    ex = LocalExecutor(cwd=tmp_path, contain_process_tree=True, default_timeout=10)
    try:
        result = ex.run(_python_command(helper))
        assert result["exit_code"] == 0
        child_pid = int(result["output"].strip().splitlines()[-1])
        assert _wait_pid_stopped(child_pid), "contained descendant survived command exit"
    finally:
        ex.close()


def test_contained_large_output_uses_bounded_tail(tmp_path):
    ex = LocalExecutor(
        cwd=tmp_path,
        max_output_chars=200,
        default_timeout=10,
        contain_process_tree=True,
    )
    try:
        result = ex.run(PRINT_1000)
        assert result["exit_code"] == 0
        assert result["truncated"] is True
        assert len(result["output"]) <= 200
        assert "line1000" in result["output"]
        assert "line1\n" not in result["output"]
    finally:
        ex.close()


@pytest.mark.skipif(_WIN, reason="setsid is a POSIX process-session primitive")
def test_contained_escaped_session_has_bounded_drain_and_reader_exit(tmp_path):
    helper = tmp_path / "escape_session.py"
    helper.write_text(
        "import subprocess, sys\n"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], "
        "start_new_session=True)\n"
        "print(p.pid, flush=True)\n",
        encoding="utf-8",
    )
    ex = LocalExecutor(cwd=tmp_path, contain_process_tree=True, default_timeout=10)
    child_pid = None
    readers_before = {
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("coworker-contained-output-")
    }
    try:
        started = time.monotonic()
        result = ex.run(_python_command(helper))
        elapsed = time.monotonic() - started
        child_pid = int(result["output"].strip().splitlines()[-1])
        assert elapsed < 5
        assert result["cleanup_failed"] is True
        assert ex.containment_failed is True
        assert "bounded drain deadline" in result["error"]
        readers_after = {
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("coworker-contained-output-")
        }
        assert readers_after <= readers_before
    finally:
        ex.close()
        if child_pid is not None:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass


def test_contained_termination_failure_returns_within_cleanup_budget(
    tmp_path, monkeypatch
):
    captured = []
    real_terminate = shell_module._ProcessTree.terminate

    def refuse_termination(tree, *, grace=0.5):
        captured.append(tree)
        return False

    monkeypatch.setattr(shell_module._ProcessTree, "terminate", refuse_termination)
    ex = LocalExecutor(cwd=tmp_path, contain_process_tree=True, default_timeout=10)
    try:
        started = time.monotonic()
        result = ex.run(SLEEP_5, timeout=0.1)
        elapsed = time.monotonic() - started
        assert elapsed < 4
        assert result["timed_out"] is True
        assert result["cleanup_failed"] is True
        assert ex.containment_failed is True
        assert "bounded drain deadline" in result["error"]
        assert captured
        assert not any(
            thread.name == f"coworker-contained-output-{captured[0].proc.pid}"
            for thread in threading.enumerate()
        )
    finally:
        if captured:
            real_terminate(captured[0])
        ex.close()


def test_contained_close_reaps_active_command_tree_and_disables_background(tmp_path):
    pid_file = tmp_path / "child.pid"
    helper = tmp_path / "spawn_then_wait.py"
    helper.write_text(
        "import pathlib, subprocess, sys, time\n"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], "
        "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(p.pid), encoding='utf-8')\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )
    ex = LocalExecutor(cwd=tmp_path, contain_process_tree=True, default_timeout=120)
    result: dict = {}
    runner = threading.Thread(
        target=lambda: result.update(ex.run(_python_command(helper))), daemon=True
    )
    runner.start()
    deadline = time.monotonic() + 10
    while not pid_file.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert pid_file.exists(), "helper did not start its child"
    child_pid = int(pid_file.read_text(encoding="utf-8"))

    ex.close()
    runner.join(timeout=10)
    assert not runner.is_alive()
    assert result.get("error") == "interrupted by user"
    assert _wait_pid_stopped(child_pid), "contained descendant survived executor close"
    assert "disabled" in ex.run_background("echo no")["error"]
