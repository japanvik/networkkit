# NetworkKit

A lightweight communication framework for distributed agents. Pub/sub messaging (ZeroMQ), HTTP ingress (FastAPI), built-in scheduling, and a single CLI to manage it all.

NetworkKit is the connective tissue for multi-agent systems. Agents don't need to know about each other — they just publish and subscribe to a shared bus. Chat bots, automation workflows, AI agents, hardware sensors — they all speak the same protocol. No broker setup, no YAML sprawl, no infrastructure overhead. One `netkit start` and you have a running message bus with scheduled tasks that everything can talk to.

## Features

- **Message bus**: ZeroMQ pub/sub + HTTP POST ingress
- **Scheduler**: interval and cron-based message scheduling (UTC)
- **Auth**: optional token-based access control
- **Config**: TOML config file with env var overrides
- **CLI**: `netkit` for daemon and schedule management
- **Persistence**: schedules survive restarts

## Install

```bash
pip install networkkit
```

## Quick Start

```bash
# Start the databus
netkit start

# Check status
netkit status

# Send a message
netkit send "hello" --to agent1 --type CHAT

# Add a scheduled message (every 5 minutes)
netkit schedule add --name heartbeat --to agent1 --type CHAT --interval 5m --content "ping"

# Add a cron job (daily at 09:00 UTC)
netkit schedule add --name daily-check --to agent1 --type CHAT --cron "0 9 * * *" --content "daily review"

# List schedules
netkit schedule list

# Trigger a schedule now
netkit schedule run heartbeat

# Remove a schedule
netkit schedule remove heartbeat

# View logs
netkit log -n 50

# Stop
netkit stop
```

## Configuration

Config file is searched in order:
1. `$NETWORKKIT_CONFIG` env var
2. `./networkkit.toml`
3. `~/.config/networkkit/networkkit.toml`

```toml
[server]
host = "0.0.0.0"
port = 8000
zmq_port = 5555
# auth_token = "your-secret-token"
data_dir = "~/.local/share/networkkit"
log_level = "INFO"
```

All settings can be overridden with env vars: `NETWORKKIT_HOST`, `NETWORKKIT_PORT`, `NETWORKKIT_ZMQ_PORT`, `NETWORKKIT_AUTH_TOKEN`, `NETWORKKIT_DATA_DIR`, `NETWORKKIT_LOG_LEVEL`.

## Message Types

| Type | Description |
|------|-------------|
| `CHAT` | Conversational message |
| `SYSTEM` | System/infrastructure message |
| `INFO` | Non-conversational notification |
| `SENSOR` | Sensor data |
| `HELO` | Availability check |
| `ACK` | Availability response |
| `ERROR` | Error notification |

## API Endpoints

All endpoints accept an optional `X-NetworkKit-Token` header when auth is enabled.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/data` | Publish a message to the bus |
| `GET` | `/schedules` | List all schedules |
| `POST` | `/schedules` | Create/update a schedule |
| `DELETE` | `/schedules/{name}` | Remove a schedule |
| `POST` | `/schedules/{name}/run` | Trigger a schedule immediately |

### Message format

```json
{
  "source": "agent1",
  "to": "agent2",
  "content": "hello",
  "message_type": "CHAT"
}
```

### Schedule format

```json
{
  "name": "heartbeat",
  "to": "agent1",
  "message_type": "CHAT",
  "content": "ping",
  "interval": "5m",
  "enabled": true
}
```

Interval supports: `30s`, `5m`, `1h`, `1d` or raw seconds. Cron uses standard 5-field UTC expressions (minute hour day month weekday, 0=Sunday).

## Python API

```python
from networkkit.messages import Message, MessageType
from networkkit.network import HTTPMessageSender, ZMQMessageReceiver

# Send a message
sender = HTTPMessageSender(publish_address="http://127.0.0.1:8000")
await sender.send_message(Message(
    source="agent1", to="agent2",
    content="hello", message_type=MessageType.CHAT,
))

# Subscribe to messages
class MySubscriber:
    name = "agent2"
    def is_intended_for_me(self, message): return message.to in (self.name, "ALL")
    async def handle_message(self, message): print(message.content)

receiver = ZMQMessageReceiver(subscribe_address="tcp://127.0.0.1:5555")
receiver.register_subscriber(MySubscriber())
await receiver.start()
```

## Runtime Files

- Config: `~/.config/networkkit/networkkit.toml`
- Schedules: `~/.local/share/networkkit/schedules.json`
- PID: `~/.local/share/networkkit/databus.pid`
- Log: `~/.local/share/networkkit/databus.log`

## License

Apache-2.0
