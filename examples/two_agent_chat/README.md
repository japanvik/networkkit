# Two-Agent Chat Example

This demo shows two independent Python processes exchanging `CHAT` messages through NetworkKit.

## Architecture

- Process 1: `agent-alpha` (subscriber + sender)
- Process 2: `agent-beta` (subscriber + sender)
- Shared transport:
  - HTTP ingest (`/data`) on the databus
  - ZeroMQ pub/sub fanout to both agents

## Run it

Open three terminals from the repository root.

1. Start the NetworkKit databus:

```bash
python -m networkkit.databus
```

2. Start `agent-beta` (waits and replies with ACK):

```bash
python examples/two_agent_chat/agent_chat_process.py \
  --name agent-beta \
  --peer agent-alpha \
  --runtime-seconds 40
```

3. Start `agent-alpha` (sends opening message):

```bash
python examples/two_agent_chat/agent_chat_process.py \
  --name agent-alpha \
  --peer agent-beta \
  --startup-message "hello from alpha" \
  --runtime-seconds 40
```

You should see output in both agent terminals similar to:

```text
[agent-beta] received from=agent-alpha to=agent-beta type=CHAT content=hello from alpha
[agent-beta] replied to agent-alpha
[agent-alpha] received from=agent-beta to=agent-alpha type=CHAT content=ACK: got 'hello from alpha'
```

## Notes

- `--runtime-seconds` keeps each process alive long enough to observe message flow.
- The script replies only to non-ACK direct chat messages to avoid reply loops.
- Use `--publish-address` and `--subscribe-address` to target remote buses.
