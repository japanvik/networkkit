#!/usr/bin/env python3
"""Echo agent: replies to direct CHAT messages with the same content."""

from __future__ import annotations

import argparse
import asyncio
import logging

from networkkit.messages import Message, MessageType
from networkkit.network import HTTPMessageSender, Subscriber, ZMQMessageReceiver


class EchoSubscriber(Subscriber):
    def __init__(self, *, name: str, sender: HTTPMessageSender) -> None:
        self.name = name
        self._sender = sender

    async def handle_message(self, message: Message):
        if message.source == self.name:
            return
        if message.to != self.name:
            return
        if message.message_type != MessageType.CHAT.value:
            return

        print(
            f"[{self.name}] received from={message.source} content={message.content}"
        )

        echo_reply = Message(
            source=self.name,
            to=message.source,
            content=message.content,
            message_type=MessageType.CHAT,
        )
        await self._sender.send_message(echo_reply)
        print(f"[{self.name}] echoed message back to {message.source}")

    def is_intended_for_me(self, message: Message) -> bool:
        return message.to == self.name


async def run(name: str, publish_address: str, subscribe_address: str) -> None:
    sender = HTTPMessageSender(publish_address=publish_address)
    receiver = ZMQMessageReceiver(subscribe_address=subscribe_address)
    receiver.register_subscriber(EchoSubscriber(name=name, sender=sender))

    print(
        f"Echo agent '{name}' listening on {subscribe_address} and publishing to {publish_address}"
    )

    try:
        await receiver.start()
    finally:
        await receiver.stop()
        await sender.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an echo agent process.")
    parser.add_argument("--name", default="echo-agent", help="Echo agent name")
    parser.add_argument(
        "--publish-address",
        default="http://127.0.0.1:8000",
        help="HTTP databus address",
    )
    parser.add_argument(
        "--subscribe-address",
        default="tcp://127.0.0.1:5555",
        help="ZeroMQ subscribe address",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args.name, args.publish_address, args.subscribe_address))


if __name__ == "__main__":
    main()
