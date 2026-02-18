"""Model Context Protocol server exposing the NetworkKit ``send_message`` tool."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from typing import Any, Callable, Dict, Optional

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
    SERVICE = "send_message_service"


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _load_server_cls() -> Any:
    try:
        from mcp.server import Server
    except ImportError as exc:  # pragma: no cover - import error path
        raise RuntimeError(
            "The 'mcp' package is required to run the MCP server. "
            "Install dependencies and retry."
        ) from exc
    return Server


class SendMessageService:
    """Core send_message logic separated from MCP wiring for easier testing."""

    def __init__(
        self,
        *,
        publish_address: str,
        agent_name: str,
        sender_factory: Callable[..., HTTPMessageSender] = HTTPMessageSender,
    ) -> None:
        self.publish_address = publish_address
        self.agent_name = agent_name
        self._sender_factory = sender_factory
        self._sender: Optional[HTTPMessageSender] = None

    async def _ensure_sender(self) -> HTTPMessageSender:
        if self._sender is None:
            self._sender = self._sender_factory(publish_address=self.publish_address)
        return self._sender

    @staticmethod
    def resolve_message_type(value: str) -> MessageType:
        try:
            return MessageType(value)
        except ValueError as exc:
            expected = ", ".join(member.value for member in MessageType)
            raise ValueError(
                f"Unsupported message_type '{value}'. Expected one of: {expected}."
            ) from exc

    async def send_message(
        self,
        *,
        recipient: str,
        content: str,
        message_type: str = MessageType.CHAT.value,
    ) -> Dict[str, Any]:
        message_type_enum = self.resolve_message_type(message_type)
        sender = await self._ensure_sender()
        outbound_message = Message(
            source=self.agent_name,
            to=recipient,
            content=content,
            message_type=message_type_enum,
        )
        await sender.send_message(outbound_message)
        return {
            "status": "sent",
            "message_id": str(uuid.uuid4()),
            "recipient": recipient,
            "message_type": message_type_enum.value,
            "metadata": {
                "publish_address": self.publish_address,
                "agent_name": self.agent_name,
            },
        }

    async def close(self) -> None:
        if self._sender is not None:
            await self._sender.close()
            self._sender = None


def build_server(
    *,
    name: str = _SERVER_NAME,
    publish_address: Optional[str] = None,
    agent_name: Optional[str] = None,
) -> Any:
    """Create a configured MCP server exposing the ``send_message`` tool."""

    Server = _load_server_cls()
    server = Server(name)
    service = SendMessageService(
        publish_address=(
            publish_address
            or _get_env("NETWORKKIT_BUS_PUBLISH_ADDRESS")
            or _DEFAULT_PUBLISH_ADDRESS
        ),
        agent_name=(agent_name or _get_env("NETWORKKIT_AGENT_NAME") or "networkkit"),
    )
    server.state[_StateKeys.SERVICE] = service

    @server.tool()
    async def send_message(
        recipient: str,
        content: str,
        message_type: str = MessageType.CHAT.value,
    ) -> Dict[str, Any]:
        """Send a message using the configured NetworkKit message sender."""

        return await service.send_message(
            recipient=recipient,
            content=content,
            message_type=message_type,
        )

    send_message.mcp_name = "send_message"
    send_message.mcp_description = (
        "Send a message to another agent or entity on the NetworkKit bus."
    )
    send_message.input_schema = _INPUT_SCHEMA
    send_message.output_schema = _OUTPUT_SCHEMA

    return server


async def _serve(server: Any) -> None:
    try:
        await server.serve_stdio()
    finally:
        service = server.state.get(_StateKeys.SERVICE)
        if service is not None:
            await service.close()


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
