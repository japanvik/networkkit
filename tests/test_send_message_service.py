import asyncio

from networkkit.messages import MessageType
from networkkit.mcp.send_message_server import SendMessageService


class DummySender:
    def __init__(self, publish_address: str):
        self.publish_address = publish_address
        self.messages = []
        self.closed = False

    async def send_message(self, message):
        self.messages.append(message)
        return "ok"

    async def close(self):
        self.closed = True


def _run(coro):
    return asyncio.run(coro)


def test_send_message_builds_expected_payload_and_response():
    holder = {}

    def factory(*, publish_address: str):
        sender = DummySender(publish_address)
        holder["sender"] = sender
        return sender

    service = SendMessageService(
        publish_address="http://bus.local:8000",
        agent_name="agent-alpha",
        sender_factory=factory,
    )

    result = _run(
        service.send_message(
            recipient="agent-beta",
            content="hello",
            message_type=MessageType.CHAT.value,
        )
    )

    sender = holder["sender"]
    assert sender.publish_address == "http://bus.local:8000"
    assert len(sender.messages) == 1
    outbound = sender.messages[0]
    assert outbound.source == "agent-alpha"
    assert outbound.to == "agent-beta"
    assert outbound.content == "hello"
    assert outbound.message_type == MessageType.CHAT.value

    assert result["status"] == "sent"
    assert result["recipient"] == "agent-beta"
    assert result["message_type"] == MessageType.CHAT.value
    assert result["metadata"]["publish_address"] == "http://bus.local:8000"
    assert result["metadata"]["agent_name"] == "agent-alpha"


def test_invalid_message_type_raises_value_error_before_sending():
    created = {"count": 0}

    def factory(*, publish_address: str):
        created["count"] += 1
        return DummySender(publish_address)

    service = SendMessageService(
        publish_address="http://bus.local:8000",
        agent_name="agent-alpha",
        sender_factory=factory,
    )

    try:
        _run(
            service.send_message(
                recipient="agent-beta",
                content="hello",
                message_type="NOT_A_TYPE",
            )
        )
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Unsupported message_type" in str(exc)

    assert created["count"] == 0


def test_sender_is_reused_and_closed_cleanly():
    created = {"count": 0}

    def factory(*, publish_address: str):
        created["count"] += 1
        return DummySender(publish_address)

    service = SendMessageService(
        publish_address="http://bus.local:8000",
        agent_name="agent-alpha",
        sender_factory=factory,
    )

    _run(service.send_message(recipient="agent-beta", content="one"))
    _run(service.send_message(recipient="agent-beta", content="two"))

    assert created["count"] == 1

    sender = service._sender
    assert sender is not None
    _run(service.close())
    assert sender.closed is True
    assert service._sender is None
