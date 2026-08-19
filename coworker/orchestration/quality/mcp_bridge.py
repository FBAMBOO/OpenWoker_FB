"""Stdio MCP sidecar for the run-bound Task Quality callback bridge.

The sidecar deliberately owns no orchestration state.  Claude Code starts it as a
stdio MCP server and every request is forwarded over a token-authenticated loopback
socket to the parent OpenWorker process, where the already-bound runtime callbacks
enforce task namespace, direct bindings, immutable hashes, lease and fencing tokens.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
from typing import Any, Mapping

import anyio
import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server


_HOST = "127.0.0.1"
_REQUEST_LIMIT = 1024 * 1024
_RESPONSE_LIMIT = 2 * 1024 * 1024


def _configuration() -> tuple[int, str]:
    try:
        port = int(os.environ["OPENWORKER_QUALITY_MCP_PORT"])
        token = os.environ["OPENWORKER_QUALITY_MCP_TOKEN"]
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Task Quality MCP bridge configuration is missing") from exc
    if not 1 <= port <= 65535 or len(token) < 32:
        raise RuntimeError("Task Quality MCP bridge configuration is invalid")
    return port, token


def _rpc_sync(action: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    port, token = _configuration()
    request = json.dumps(
        {"token": token, "action": action, **dict(payload)},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(request) > _REQUEST_LIMIT:
        raise RuntimeError("Task Quality MCP request exceeded its limit")
    with socket.create_connection((_HOST, port), timeout=10.0) as connection:
        connection.settimeout(60.0)
        connection.sendall(request + b"\n")
        reader = connection.makefile("rb")
        response = reader.readline(_RESPONSE_LIMIT + 1)
    if not response or len(response) > _RESPONSE_LIMIT:
        raise RuntimeError("Task Quality MCP bridge returned an invalid response")
    value = json.loads(response)
    if not isinstance(value, dict):
        raise RuntimeError("Task Quality MCP bridge returned a non-object response")
    return value


async def _rpc(action: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return await asyncio.to_thread(_rpc_sync, action, payload)


def _tool(value: Mapping[str, Any]) -> types.Tool:
    return types.Tool(
        name=str(value["name"]),
        description=str(value.get("description") or ""),
        inputSchema=dict(value.get("inputSchema") or {}),
    )


async def run() -> None:
    server = Server(
        "openworker_quality",
        version="2",
        instructions=(
            "Run-bound OpenWorker Task Quality tools. Tool arguments never include "
            "task, run, lease, fencing, profile, contract or snapshot identity."
        ),
    )

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        response = await _rpc("list", {})
        if not response.get("ok"):
            raise RuntimeError(str(response.get("error") or "tool listing failed"))
        tools = response.get("tools")
        if not isinstance(tools, list):
            raise RuntimeError("Task Quality MCP tool listing is invalid")
        return [_tool(item) for item in tools if isinstance(item, Mapping)]

    @server.call_tool(validate_input=True)
    async def call_tool(name: str, arguments: dict[str, Any]) -> Any:
        response = await _rpc(
            "call", {"name": str(name), "arguments": dict(arguments or {})}
        )
        output = str(response.get("output") or "{}")
        if not response.get("ok"):
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=output)],
                isError=True,
            )
        try:
            structured = json.loads(output)
        except json.JSONDecodeError:
            structured = {"content": output}
        if not isinstance(structured, dict):
            structured = {"items": structured}
        return structured

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main() -> None:
    anyio.run(run)


if __name__ == "__main__":
    main()
