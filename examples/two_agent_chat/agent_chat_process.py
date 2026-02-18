#!/usr/bin/env python3
"""Run one chat-capable agent process using NetworkKit transport primitives."""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from networkkit.messages import Message, MessageType
from networkkit.network import HTTPMessageSender, Subscriber, ZMQMessageReceiver


@dataclass
class AgentConfig:
    name: str
    peer: str
    publish_address: str
    subscribe_address: str
    startup_message: Optional[str]
    runtime_seconds: int


class ChatSubscriber(Subscriber):
    """Receives messages and sends ACK-style replies for direct CHAT messages."""

    def __init__(self, config: AgentConfig, sender: HTTPMessageSender) -> None:
        self.name = config.name
        self.peer = config.peer
        self._sender = sender

    async def handle_message(self, message: Message):
        if message.source == self.name:
            return

        print(
            f"[{self.name}] received from={message.source} to={message.to} "
            f"type={message.message_type} content={message.content}"
        )

        # Reply only to direct CHAT messages and avoid ACK loops.
        if (
            message.message_type == MessageType.CHAT.value
            and message.to == self.name
            and not message.content.startswith("ACK:")
        ):
            reply = Message(
                source=self.name,
                to=message.source,
                content=f"ACK: got '{message.content}'",
                message_type=MessageType.CHAT,
            )
            await self._sender.send_message(reply)
            print(f"[{self.name}] replied to {message.source}")

    def is_intended_for_me(self, message: Message) -> bool:
        return message.to in {self.name, "ALL"}


async def run_agent(config: AgentConfig) -> None:
    sender = HTTPMessageSender(publish_address=config.publish_address)
    receiver = ZMQMessageReceiver(subscribe_address=config.subscribe_address)
    subscriber = ChatSubscriber(config, sender)
    receiver.register_subscriber(subscriber)

    receiver_task = asyncio.create_task(receiver.start())

    try:
        # Give subscriber socket a moment to connect before first send.
        await asyncio.sleep(0.5)

        if config.startup_message:
            opening = Message(
                source=config.name,
                to=config.peer,
                content=config.startup_message,
                message_type=MessageType.CHAT,
            )
            await sender.send_message(opening)
            print(f"[{config.name}] sent startup message to {config.peer}")

        await asyncio.sleep(config.runtime_seconds)

    finally:
        await receiver.stop()
        receiver_task.cancel()
        await sender.close()


def parse_args() -> AgentConfig:
    parser = argparse.ArgumentParser(
        description="Run one NetworkKit chat agent process.",
    )
    parser.add_argument("--name", required=True, help="Agent name for this process.")
    parser.add_argument("--peer", required=True, help="Expected peer agent name.")
    parser.add_argument(
        "--publish-address",
        default="http://127.0.0.1:8000",
        help="HTTP databus address (default: http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--subscribe-address",
        default="tcp://127.0.0.1:5555",
        help="ZeroMQ subscribe address (default: tcp://127.0.0.1:5555)",
    )
    parser.add_argument(
        "--startup-message",
        help="Optional opening CHAT message sent to --peer.",
    )
    parser.add_argument(
        "--runtime-seconds",
        type=int,
        default=30,
        help="How long this process should run before exiting.",
    )

    args = parser.parse_args()
    return AgentConfig(
        name=args.name,
        peer=args.peer,
        publish_address=args.publish_address,
        subscribe_address=args.subscribe_address,
        startup_message=args.startup_message,
        runtime_seconds=args.runtime_seconds,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    config = parse_args()
    asyncio.run(run_agent(config))


if __name__ == "__main__":
    main()
