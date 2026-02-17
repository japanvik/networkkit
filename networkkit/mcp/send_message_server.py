"""Model Context Protocol server exposing the NetworkKit ``send_message`` tool."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from typing import Any, Dict, Optional

from mcp.server import Server

from networkkit.messages import Message, MessageType
from networkkit.network import HTTPMessageSender

_SERVER_NAME = "networkkit-send-message"
_DEFAULT_PUBLISH_ADDRESS = "http://127.0.0.1:8000"

_INPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "recipient": {
            "type": "string",
            "description": "Target agent or broadcast alias",
        },
        "content": {
            "type": "string",
            "description": "Message body",
        },
        "message_type": {
            "type": "string",
            "enum": [member.value for member in MessageType],
            "default": MessageType.CHAT.value,
            "description": "NetworkKit message type",
        },
    },
    "required": ["recipient", "content"],
}

_OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "message_id": {"type": "string"},
        "recipient": {"type": "string"},
        "message_type": {"type": "string"},
        "metadata": {"type": "object"},
    },
}


class _StateKeys:
    MESSAGE_SENDER = "message_sender"
    PUBLISH_ADDRESS = "publish_address"
    AGENT_NAME = "agent_name"


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def build_server(
    *,
    name: str = _SERVER_NAME,
    publish_address: Optional[str] = None,
    agent_name: Optional[str] = None,
) -> Server:
    """Create a configured MCP server exposing the ``send_message`` tool."""

    server = Server(name)
    state = server.state
    state[_StateKeys.PUBLISH_ADDRESS] = (
        publish_address
        or _get_env("NETWORKKIT_BUS_PUBLISH_ADDRESS")
        or _DEFAULT_PUBLISH_ADDRESS
    )
    state[_StateKeys.AGENT_NAME] = (
        agent_name or _get_env("NETWORKKIT_AGENT_NAME") or "networkkit"
    )
    state[_StateKeys.MESSAGE_SENDER] = None

    async def _ensure_sender() -> HTTPMessageSender:
        sender = state.get(_StateKeys.MESSAGE_SENDER)
        if sender is None:
            sender = HTTPMessageSender(
                publish_address=state[_StateKeys.PUBLISH_ADDRESS]
            )
            state[_StateKeys.MESSAGE_SENDER] = sender
        return sender

    def _resolve_message_type(value: str) -> MessageType:
        try:
            return MessageType(value)
        except ValueError as exc:
            expected = ", ".join(member.value for member in MessageType)
            raise ValueError(
                f"Unsupported message_type '{value}'. Expected one of: {expected}."
            ) from exc

    @server.tool()
    async def send_message(
        recipient: str,
        content: str,
        message_type: str = MessageType.CHAT.value,
    ) -> Dict[str, Any]:
        """Send a message using the configured NetworkKit message sender."""

        message_type_enum = _resolve_message_type(message_type)
        sender = await _ensure_sender()
        message = Message(
            source=state[_StateKeys.AGENT_NAME],
            to=recipient,
            content=content,
            message_type=message_type_enum,
        )
        await sender.send_message(message)

        return {
            "status": "sent",
            "message_id": str(uuid.uuid4()),
            "recipient": recipient,
            "message_type": message_type_enum.value,
            "metadata": {
                "publish_address": state[_StateKeys.PUBLISH_ADDRESS],
                "agent_name": state[_StateKeys.AGENT_NAME],
            },
        }

    send_message.mcp_name = "send_message"
    send_message.mcp_description = (
        "Send a message to another agent or entity on the NetworkKit bus."
    )
    send_message.input_schema = _INPUT_SCHEMA
    send_message.output_schema = _OUTPUT_SCHEMA

    return server


async def _serve(server: Server) -> None:
    try:
        await server.serve_stdio()
    finally:
        sender = server.state.get(_StateKeys.MESSAGE_SENDER)
        if sender is not None:
            await sender.close()
            server.state[_StateKeys.MESSAGE_SENDER] = None


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the NetworkKit send_message MCP server over stdio.",
    )
    parser.add_argument(
        "--publish-address",
        help="Override the bus publish address (defaults to env or http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--agent-name",
        help="Override the message source identity.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the MCP server (e.g. DEBUG, INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    _configure_logging(args.log_level)
    server = build_server(
        publish_address=args.publish_address,
        agent_name=args.agent_name,
    )
    asyncio.run(_serve(server))


if __name__ == "__main__":
    main(sys.argv[1:])
