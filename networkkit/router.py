"""NetworkKit bus router — federate two databus instances.

Subscribes to both buses via ZMQ and forwards messages between them.
Uses routing tables on each bus to determine where to forward messages.
Falls back to static agent lists if routing table is unavailable.

Usage:
    python3 -m networkkit.router
    python3 -m networkkit.router --local-agents sophia,kiro --remote-agents megu
"""

import argparse
import json
import logging
import time
from pathlib import Path

import requests
import zmq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [router] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("router")


def _is_helo_ack(msg: dict) -> bool:
    if msg.get("message_type") != "SYSTEM":
        return False
    content = msg.get("content", "").strip()
    if not content.startswith("{"):
        return False
    try:
        p = json.loads(content)
        return p.get("type") in ("HELO", "ACK")
    except (json.JSONDecodeError, KeyError):
        return False


def _fetch_peer_names(http_url: str, bus_origin_filter: str | None = None) -> set[str] | None:
    """Fetch peer names from a bus's routing table. Returns None on failure."""
    try:
        r = requests.get(f"{http_url}/peers", timeout=3)
        if r.status_code != 200:
            return None
        peers = r.json().get("peers", [])
        if bus_origin_filter:
            return {p["name"].lower() for p in peers if p.get("bus_origin") == bus_origin_filter}
        return {p["name"].lower() for p in peers}
    except Exception:
        return None


def run_bridge(local_zmq: str, remote_zmq: str,
               local_http: str, remote_http: str,
               local_agents_fallback: set[str], remote_agents_fallback: set[str]):
    ctx = zmq.Context()

    local_sub = ctx.socket(zmq.SUB)
    local_sub.connect(local_zmq)
    local_sub.setsockopt_string(zmq.SUBSCRIBE, "")
    log.info("Subscribed to local bus: %s", local_zmq)

    remote_sub = ctx.socket(zmq.SUB)
    remote_sub.setsockopt_string(zmq.SUBSCRIBE, "")
    remote_connected = False
    try:
        remote_sub.connect(remote_zmq)
        requests.get(f"{remote_http}/peers", timeout=3)
        remote_connected = True
        log.info("Subscribed to remote bus: %s", remote_zmq)
    except Exception as e:
        log.warning("Remote bus unreachable (%s), will retry", e)

    poller = zmq.Poller()
    poller.register(local_sub, zmq.POLLIN)
    if remote_connected:
        poller.register(remote_sub, zmq.POLLIN)

    last_reconnect = 0
    last_route_refresh = 0
    route_refresh_interval = 30
    _last_forwarded = {"local": ("", 0.0), "remote": ("", 0.0)}

    # Cached routing sets — refreshed periodically from bus routing tables
    local_agents = set(local_agents_fallback)
    remote_agents = set(remote_agents_fallback)

    while True:
        try:
            now = time.time()

            # Periodically refresh routing from bus routing tables
            if now - last_route_refresh > route_refresh_interval:
                last_route_refresh = now
                local_peers = _fetch_peer_names(local_http, bus_origin_filter="local")
                if local_peers:
                    local_agents = local_peers
                remote_peers = _fetch_peer_names(remote_http, bus_origin_filter="local") if remote_connected else None
                if remote_peers:
                    remote_agents = remote_peers

            # Periodically try to reconnect to remote if down
            if not remote_connected and now - last_reconnect > 30:
                last_reconnect = now
                try:
                    requests.get(f"{remote_http}/peers", timeout=3)
                    remote_sub.close()
                    remote_sub = ctx.socket(zmq.SUB)
                    remote_sub.setsockopt_string(zmq.SUBSCRIBE, "")
                    remote_sub.connect(remote_zmq)
                    poller.register(remote_sub, zmq.POLLIN)
                    remote_connected = True
                    log.info("Remote bus reconnected: %s", remote_zmq)
                except Exception:
                    pass

            socks = dict(poller.poll(timeout=5000))

            # Local → Remote
            if local_sub in socks:
                msg = local_sub.recv_json()
                to = msg.get("to", "").lower()
                source = msg.get("source", "")
                if source.startswith("router:"):
                    continue

                should_forward = (
                    _is_helo_ack(msg)
                    or to in remote_agents
                    or to.startswith("telegram:")
                    or to.startswith("voice")
                )

                if should_forward and remote_connected:
                    content = msg.get("content", "")
                    sig = f"{source}:{to}:{content[:100]}"
                    if sig == _last_forwarded["local"][0] and now - _last_forwarded["local"][1] < 2:
                        continue
                    _last_forwarded["local"] = (sig, now)
                    try:
                        msg["source"] = f"router:{msg.get('source', 'unknown')}"
                        requests.post(f"{remote_http}/data", json=msg, timeout=5)
                        log.info("LOCAL→REMOTE [%s→%s]: %s", source, to, content[:60])
                    except Exception as e:
                        log.warning("Failed to forward to remote: %s", e)

            # Remote → Local
            if remote_connected and remote_sub in socks:
                msg = remote_sub.recv_json()
                to = msg.get("to", "").lower()
                source = msg.get("source", "")
                if source.startswith("router:"):
                    continue

                clean_to = to.removeprefix("router:")
                should_forward = _is_helo_ack(msg) or clean_to in local_agents

                if should_forward:
                    try:
                        msg["source"] = f"router:{msg.get('source', 'unknown')}"
                        requests.post(f"{local_http}/data", json=msg, timeout=5)
                        log.info("REMOTE→LOCAL [%s→%s]: %s", source, to, msg.get("content", "")[:60])
                    except Exception as e:
                        log.warning("Failed to forward to local: %s", e)

        except KeyboardInterrupt:
            log.info("Bridge shutting down")
            break
        except Exception:
            log.exception("Bridge error")
            time.sleep(5)

    local_sub.close()
    remote_sub.close()
    ctx.term()


def main():
    p = argparse.ArgumentParser(description="NetworkKit bus router")
    p.add_argument("--local", default="tcp://127.0.0.1:5555", help="Local ZMQ address")
    p.add_argument("--remote", default="tcp://127.0.0.1:5556", help="Remote ZMQ address")
    p.add_argument("--local-http", default="http://127.0.0.1:8000", help="Local HTTP API")
    p.add_argument("--remote-http", default="http://127.0.0.1:8001", help="Remote HTTP API")
    p.add_argument("--local-agents", default="", help="Fallback local agents (used if routing table unavailable)")
    p.add_argument("--remote-agents", default="", help="Fallback remote agents (used if routing table unavailable)")
    args = p.parse_args()

    local_agents = {a.strip().lower() for a in args.local_agents.split(",")}
    remote_agents = {a.strip().lower() for a in args.remote_agents.split(",")}

    run_bridge(args.local, args.remote, args.local_http, args.remote_http,
               local_agents, remote_agents)


if __name__ == "__main__":
    main()
