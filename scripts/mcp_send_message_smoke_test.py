#!/usr/bin/env python3
"""End-to-end smoke test for the NetworkKit send_message MCP server."""

from __future__ import annotations

import argparse
import json
import queue
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple


class _DatabusHandler(BaseHTTPRequestHandler):
    received_messages = None

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(content_length).decode("utf-8")
        parsed = json.loads(payload)
        self.__class__.received_messages.put(parsed)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: Any) -> None:
        # Keep test output clean.
        return


def _start_fake_databus() -> Tuple[ThreadingHTTPServer, str]:
    _DatabusHandler.received_messages = queue.Queue()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _DatabusHandler)
    host, port = server.server_address
    publish_address = f"http://{host}:{port}"

    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(server.serve_forever)
    server._executor = executor  # type: ignore[attr-defined]
    return server, publish_address


def _stop_fake_databus(server: ThreadingHTTPServer) -> None:
    server.shutdown()
    server.server_close()
    server._executor.shutdown(wait=True)  # type: ignore[attr-defined]


def _write_rpc(process: subprocess.Popen, payload: Dict[str, Any]) -> None:
    data = json.dumps(payload).encode("utf-8")
    frame = f"Content-Length: {len(data)}\r\n\r\n".encode("utf-8") + data
    assert process.stdin is not None
    process.stdin.write(frame)
    process.stdin.flush()


def _read_rpc(process: subprocess.Popen, timeout_seconds: float = 5.0) -> Dict[str, Any]:
    def _read_blocking() -> Dict[str, Any]:
        if process.poll() is not None:
            stderr_output = b""
            if process.stderr is not None:
                stderr_output = process.stderr.read()
            raise RuntimeError(
                "MCP server exited before responding. "
                f"stderr: {stderr_output.decode('utf-8', errors='replace')}"
            )

        assert process.stdout is not None
        headers: Dict[str, str] = {}

        while True:
            raw = process.stdout.readline()
            if not raw:
                stderr_output = b""
                if process.stderr is not None:
                    stderr_output = process.stderr.read()
                raise RuntimeError(
                    "MCP server stdout closed while waiting for headers. "
                    f"stderr: {stderr_output.decode('utf-8', errors='replace')}"
                )
            line = raw.decode("utf-8").strip()
            if line == "":
                break
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()

        content_length = int(headers["content-length"])
        body = process.stdout.read(content_length)
        if len(body) != content_length:
            raise RuntimeError("Incomplete MCP frame body")

        return json.loads(body.decode("utf-8"))

    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(_read_blocking).result(timeout=timeout_seconds)


def _run_smoke_test(agent_name: str = "smoke-agent") -> None:
    fake_bus, publish_address = _start_fake_databus()
    process = None

    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "networkkit.mcp.send_message_server",
                "--publish-address",
                publish_address,
                "--agent-name",
                agent_name,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _write_rpc(
            process,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "networkkit-smoke", "version": "0.1.0"},
                },
            },
        )
        init_response = _read_rpc(process)
        if "error" in init_response:
            raise RuntimeError(f"initialize failed: {init_response['error']}")

        _write_rpc(
            process,
            {
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {},
            },
        )

        _write_rpc(
            process,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {},
            },
        )
        tools_response = _read_rpc(process)
        if "error" in tools_response:
            raise RuntimeError(f"tools/list failed: {tools_response['error']}")

        _write_rpc(
            process,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "send_message",
                    "arguments": {
                        "recipient": "agent-beta",
                        "content": "hello from smoke test",
                        "message_type": "CHAT",
                    },
                },
            },
        )
        call_response = _read_rpc(process)
        if "error" in call_response:
            raise RuntimeError(f"tools/call failed: {call_response['error']}")

        published = _DatabusHandler.received_messages.get(timeout=5.0)
        assert published["source"] == agent_name
        assert published["to"] == "agent-beta"
        assert published["content"] == "hello from smoke test"
        assert published["message_type"] == "CHAT"

        print("Smoke test passed.")
        print(f"initialize response id: {init_response.get('id')}")
        print(f"tools/list response id: {tools_response.get('id')}")
        print(f"tools/call response id: {call_response.get('id')}")

    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
        _stop_fake_databus(fake_bus)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an MCP send_message smoke test.")
    parser.add_argument(
        "--agent-name",
        default="smoke-agent",
        help="Agent identity expected in the published NetworkKit message.",
    )
    args = parser.parse_args()
    _run_smoke_test(agent_name=args.agent_name)


if __name__ == "__main__":
    main()
