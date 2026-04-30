"""NetworkKit bus bridge — federate two databus instances.

Subscribes to both buses via ZMQ and forwards messages between them.
Only forwards messages addressed to agents on the remote bus.

Usage:
    python3 -m networkkit.bridge \
        --local tcp://127.0.0.1:5555 \
        --remote tcp://192.168.0.103:5555 \
        --local-http http://127.0.0.1:8000 \
        --remote-http http://192.168.0.103:8000 \
        --local-agents sophia \
        --remote-agents megu
"""

import argparse
import json
import logging
import time

import requests
import zmq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [bridge] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bridge")


def run_bridge(local_zmq: str, remote_zmq: str,
               local_http: str, remote_http: str,
               local_agents: set[str], remote_agents: set[str]):
    ctx = zmq.Context()

    # Subscribe to local bus
    local_sub = ctx.socket(zmq.SUB)
    local_sub.connect(local_zmq)
    local_sub.setsockopt_string(zmq.SUBSCRIBE, "")
    log.info("Subscribed to local bus: %s", local_zmq)

    # Subscribe to remote bus
    remote_sub = ctx.socket(zmq.SUB)
    remote_sub.setsockopt_string(zmq.SUBSCRIBE, "")
    remote_connected = False
    try:
        remote_sub.connect(remote_zmq)
        # Test connectivity via HTTP
        requests.get(f"{remote_http}/schedules", timeout=3)
        remote_connected = True
        log.info("Subscribed to remote bus: %s", remote_zmq)
    except Exception as e:
        log.warning("Remote bus unreachable (%s), will retry", e)

    poller = zmq.Poller()
    poller.register(local_sub, zmq.POLLIN)
    if remote_connected:
        poller.register(remote_sub, zmq.POLLIN)

    last_reconnect = 0

    while True:
        try:
            # Periodically try to reconnect to remote if down
            if not remote_connected and time.time() - last_reconnect > 30:
                last_reconnect = time.time()
                try:
                    requests.get(f"{remote_http}/schedules", timeout=3)
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

            # Local → Remote: forward messages addressed to remote agents ONLY (not ALL)
            if local_sub in socks:
                msg = local_sub.recv_json()
                to = msg.get("to", "").lower()
                source = msg.get("source", "")
                if source.startswith("bridge:"):
                    continue  # don't loop
                content = msg.get("content", "")

                # Always forward HELO/ACK (even if to=ALL)
                is_helo_ack = False
                if msg.get("message_type") == "SYSTEM" and content.strip().startswith("{"):
                    try:
                        p = json.loads(content)
                        is_helo_ack = p.get("type") in ("HELO", "ACK")
                    except (json.JSONDecodeError, KeyError):
                        pass

                if to in remote_agents or is_helo_ack:
                    if remote_connected:
                        try:
                            msg["source"] = f"bridge:{msg.get('source', 'unknown')}"
                            requests.post(f"{remote_http}/data", json=msg, timeout=5)
                            log.info("LOCAL→REMOTE [%s→%s]: %s", source, to, msg.get("content", "")[:60])
                        except Exception as e:
                            log.warning("Failed to forward to remote: %s", e)

            # Remote → Local: forward messages addressed to local agents ONLY (not ALL)
            if remote_connected and remote_sub in socks:
                msg = remote_sub.recv_json()
                to = msg.get("to", "").lower()
                source = msg.get("source", "")
                if source.startswith("bridge:"):
                    continue
                content = msg.get("content", "")

                is_helo_ack = False
                if msg.get("message_type") == "SYSTEM" and content.strip().startswith("{"):
                    try:
                        p = json.loads(content)
                        is_helo_ack = p.get("type") in ("HELO", "ACK")
                    except (json.JSONDecodeError, KeyError):
                        pass

                if to in local_agents or is_helo_ack:
                    try:
                        msg["source"] = f"bridge:{msg.get('source', 'unknown')}"
                        # Write to local bus
                        requests.post(f"{local_http}/data", json=msg, timeout=5)
                        # Also write to mailbox for harness bridge
                        mailbox = Path.home() / ".local" / "share" / "kiro-harness" / "mailbox"
                        mailbox.mkdir(parents=True, exist_ok=True)
                        ts = time.time()
                        (mailbox / f"{ts:.6f}.json").write_text(json.dumps(msg))
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
    p = argparse.ArgumentParser(description="NetworkKit bus bridge")
    p.add_argument("--local", default="tcp://127.0.0.1:5555", help="Local ZMQ address")
    p.add_argument("--remote", default="tcp://192.168.0.103:5555", help="Remote ZMQ address")
    p.add_argument("--local-http", default="http://127.0.0.1:8000", help="Local HTTP API")
    p.add_argument("--remote-http", default="http://192.168.0.103:8000", help="Remote HTTP API")
    p.add_argument("--local-agents", default="sophia", help="Comma-separated local agent names")
    p.add_argument("--remote-agents", default="megu", help="Comma-separated remote agent names")
    args = p.parse_args()

    local_agents = {a.strip().lower() for a in args.local_agents.split(",")}
    remote_agents = {a.strip().lower() for a in args.remote_agents.split(",")}

    run_bridge(args.local, args.remote, args.local_http, args.remote_http,
               local_agents, remote_agents)


if __name__ == "__main__":
    main()
