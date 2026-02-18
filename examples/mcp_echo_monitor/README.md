# MCP -> Echo -> Monitor Example

This example demonstrates three roles running as independent processes:

- `echo-agent`: receives direct `CHAT` messages and echoes the same content back to sender.
- `monitor-agent`: listens to all bus traffic and prints every message.
- `mcp_send_once.py`: starts the NetworkKit MCP server over stdio and calls `send_message`.

## Run the flow

Open four terminals from `/Users/vkumar/Development/networkkit`.

### 1) Start databus

```bash
uv run python -m networkkit.databus
```

### 2) Start monitor agent

```bash
uv run python examples/mcp_echo_monitor/monitor_agent.py \
  --name monitor-agent
```

### 3) Start echo agent

```bash
uv run python examples/mcp_echo_monitor/echo_agent.py \
  --name echo-agent
```

### 4) Send a message through MCP

```bash
uv run python examples/mcp_echo_monitor/mcp_send_once.py \
  --recipient echo-agent \
  --content "ping from mcp"
```

## Expected behavior

- `mcp_send_once.py` prints success and JSON-RPC tool response.
- `echo-agent` prints message receipt and sends an echo reply.
- `monitor-agent` prints two messages:
  1. `mcp-client-agent -> echo-agent` with your content
  2. `echo-agent -> mcp-client-agent` with same content

## Notes

- You can override `--publish-address` if your databus is remote.
- To change the sender identity seen by echo/monitor, pass `--source-agent-name` to `mcp_send_once.py`.
