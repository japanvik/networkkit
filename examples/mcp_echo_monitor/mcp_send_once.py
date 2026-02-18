#!/usr/bin/env python3
"""Call NetworkKit MCP send_message once using the official MCP Python client."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def run_send(
    *,
    recipient: str,
    content: str,
    message_type: str,
    publish_address: str,
    source_agent_name: str,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]

    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "networkkit.mcp.send_message_server",
            "--publish-address",
            publish_address,
            "--agent-name",
            source_agent_name,
        ],
        cwd=str(repo_root),
    )

    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools_result = await session.list_tools()
            tool_names = [tool.name for tool in tools_result.tools]
            if "send_message" not in tool_names:
                raise RuntimeError(
                    f"send_message tool not found. Available tools: {tool_names}"
                )

            result = await session.call_tool(
                "send_message",
                {
                    "recipient": recipient,
                    "content": content,
                    "message_type": message_type,
                },
            )

            print("MCP send_message call finished")
            print(f"isError={result.isError}")
            if result.structuredContent is not None:
                print(json.dumps(result.structuredContent, indent=2))
            if result.content:
                print("content:")
                for item in result.content:
                    print(item)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send one message via MCP send_message.")
    parser.add_argument("--recipient", default="echo-agent")
    parser.add_argument("--content", default="hello over mcp")
    parser.add_argument("--message-type", default="CHAT")
    parser.add_argument("--publish-address", default="http://127.0.0.1:8000")
    parser.add_argument("--source-agent-name", default="mcp-client-agent")
    args = parser.parse_args()

    asyncio.run(
        run_send(
            recipient=args.recipient,
            content=args.content,
            message_type=args.message_type,
            publish_address=args.publish_address,
            source_agent_name=args.source_agent_name,
        )
    )


if __name__ == "__main__":
    main()
