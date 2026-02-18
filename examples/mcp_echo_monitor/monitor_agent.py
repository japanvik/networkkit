#!/usr/bin/env python3
"""Monitor agent: prints every message observed on the bus."""

from __future__ import annotations

import argparse
import asyncio
import logging

from networkkit.messages import Message
from networkkit.network import Subscriber, ZMQMessageReceiver


class MonitorSubscriber(Subscriber):
    def __init__(self, *, name: str) -> None:
        self.name = name

    async def handle_message(self, message: Message):
        print(
            f"[{self.name}] source={message.source} to={message.to} "
            f"type={message.message_type} content={message.content}"
        )

    def is_intended_for_me(self, message: Message) -> bool:
        return True


async def run(name: str, subscribe_address: str) -> None:
    receiver = ZMQMessageReceiver(subscribe_address=subscribe_address)
    receiver.register_subscriber(MonitorSubscriber(name=name))

    print(f"Monitor agent '{name}' listening on {subscribe_address}")

    try:
        await receiver.start()
    finally:
        await receiver.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a monitor agent process.")
    parser.add_argument("--name", default="monitor-agent", help="Monitor agent name")
    parser.add_argument(
        "--subscribe-address",
        default="tcp://127.0.0.1:5555",
        help="ZeroMQ subscribe address",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args.name, args.subscribe_address))


if __name__ == "__main__":
    main()
