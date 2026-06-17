#!/usr/bin/env python3
"""netkit — NetworkKit databus daemon and schedule management CLI.

Daemon:     netkit start | stop | restart | status
Schedules:  netkit schedule list | add | remove | run
Messages:   netkit send "text" --to agent --type CHAT
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests

BUS_URL = os.environ.get("NETWORKKIT_BUS_URL", "http://127.0.0.1:8000")
AUTH_TOKEN = os.environ.get("NETWORKKIT_AUTH_TOKEN", "")
PID_FILE = Path(os.environ.get("NETWORKKIT_PID_FILE", str(Path.home() / ".local" / "share" / "networkkit" / "databus.pid")))
LOG_FILE = Path(os.environ.get("NETWORKKIT_LOG_FILE", str(Path.home() / ".local" / "share" / "networkkit" / "databus.log")))


# ── Helpers ─────────────────────────────────────────────────────────

def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if AUTH_TOKEN:
        h["X-NetworkKit-Token"] = AUTH_TOKEN
    return h


def _url(path: str) -> str:
    return f"{BUS_URL.rstrip('/')}{path}"


def _read_pid() -> int | None:
    if PID_FILE.is_file():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, 0)  # check if alive
            return pid
        except (ValueError, ProcessLookupError, PermissionError):
            PID_FILE.unlink(missing_ok=True)
    return None


def _out(data) -> None:
    print(json.dumps(data, indent=2))


# ── Daemon commands ─────────────────────────────────────────────────

def cmd_start(args):
    pid = _read_pid()
    if pid:
        print(f"Already running (pid={pid})")
        return
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log_fh = open(LOG_FILE, "a")
    proc = subprocess.Popen(
        [sys.executable, "-m", "networkkit.databus"],
        stdout=log_fh, stderr=log_fh,
        start_new_session=True,
    )
    PID_FILE.write_text(str(proc.pid))
    # Wait briefly to check it didn't crash
    time.sleep(1)
    if proc.poll() is not None:
        print(f"Failed to start (exit={proc.returncode}). Check {LOG_FILE}")
        PID_FILE.unlink(missing_ok=True)
    else:
        print(f"Started (pid={proc.pid}, log={LOG_FILE})")


def cmd_stop(args):
    pid = _read_pid()
    if not pid:
        print("Not running")
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(30):
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            break
    PID_FILE.unlink(missing_ok=True)
    print(f"Stopped (pid={pid})")


def cmd_restart(args):
    cmd_stop(args)
    time.sleep(1)
    cmd_start(args)


def cmd_status(args):
    pid = _read_pid()
    if pid:
        print(f"Running (pid={pid}, log={LOG_FILE})")
    else:
        print("Not running")


def cmd_log(args):
    lines = args.lines if hasattr(args, "lines") else 20
    if not LOG_FILE.is_file():
        print(f"No log file at {LOG_FILE}")
        return
    with open(LOG_FILE) as f:
        all_lines = f.readlines()
        for line in all_lines[-lines:]:
            print(line, end="")


# ── Schedule commands ───────────────────────────────────────────────

def cmd_schedule_list(args):
    r = requests.get(_url("/schedules"), headers=_headers(), timeout=10)
    _out(r.json())


def cmd_schedule_add(args):
    body = {"name": args.name, "to": args.to, "message_type": args.type, "content": args.content, "enabled": True}
    if args.interval:
        body["interval"] = args.interval
    if args.cron:
        body["cron"] = args.cron
    r = requests.post(_url("/schedules"), json=body, headers=_headers(), timeout=10)
    _out(r.json())


def cmd_schedule_remove(args):
    r = requests.delete(_url(f"/schedules/{args.name}"), headers=_headers(), timeout=10)
    _out(r.json())


def cmd_schedule_run(args):
    r = requests.post(_url(f"/schedules/{args.name}/run"), headers=_headers(), timeout=10)
    _out(r.json())


# ── Peers command ──────────────────────────────────────────────────

def cmd_peers(args):
    params = {}
    if hasattr(args, "all") and args.all:
        params["all"] = "1"
    r = requests.get(_url("/peers"), headers=_headers(), params=params, timeout=10)
    data = r.json()
    peers = data.get("peers", [])
    if not peers:
        print("No peers registered")
        return
    print(f"{'NAME':<20} {'BUS':<8} {'AGE':<8} {'STATUS':<8} DESCRIPTION")
    print("-" * 70)
    for p in peers:
        status = "alive" if p.get("alive") else "expired"
        age = f"{int(p.get('age_seconds', 0))}s"
        print(f"{p['name']:<20} {p.get('bus_origin','?'):<8} {age:<8} {status:<8} {p.get('description','')[:30]}")


# ── Send command ────────────────────────────────────────────────────

def cmd_send(args):
    text = args.content.replace("\\n", "\n")
    # Auto-wrap plain text in JSON envelope (bus expects {"text": ...})
    if not text.strip().startswith("{"):
        content = json.dumps({"text": text}, ensure_ascii=False)
    else:
        content = text
    body = {"source": args.source, "to": args.to, "content": content, "message_type": args.type}
    r = requests.post(_url("/data"), json=body, headers=_headers(), timeout=10)
    _out(r.json())


# ── Main ────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(prog="netkit", description="NetworkKit databus CLI")
    sub = p.add_subparsers(dest="command")

    # Daemon
    sub.add_parser("start", help="Start the databus daemon")
    sub.add_parser("stop", help="Stop the databus daemon")
    sub.add_parser("restart", help="Restart the databus daemon")
    sub.add_parser("status", help="Show daemon status")
    log_p = sub.add_parser("log", help="Show recent log output")
    log_p.add_argument("-n", "--lines", type=int, default=20, help="Number of lines")

    # Schedules
    sc = sub.add_parser("schedule", help="Manage schedules")
    sc_sub = sc.add_subparsers(dest="action")
    sc_sub.add_parser("list", help="List all schedules")
    add = sc_sub.add_parser("add", help="Add/update a schedule")
    add.add_argument("--name", required=True)
    add.add_argument("--to", default="ALL")
    add.add_argument("--type", default="SYSTEM")
    add.add_argument("--content", default="")
    add.add_argument("--interval", help="e.g. 5m, 1h, 30s")
    add.add_argument("--cron", help="Cron expression (UTC)")
    rm = sc_sub.add_parser("remove", help="Remove a schedule")
    rm.add_argument("name")
    run = sc_sub.add_parser("run", help="Trigger a schedule now")
    run.add_argument("name")

    # Peers
    peers_p = sub.add_parser("peers", help="Show registered peers on the bus")
    peers_p.add_argument("--all", action="store_true", help="Include expired peers")

    # Send
    send = sub.add_parser("send", help="Send a message to the bus")
    send.add_argument("content", help="Message content")
    send.add_argument("--to", default="ALL")
    send.add_argument("--source", default="netkit-cli")
    send.add_argument("--type", default="INFO")

    args = p.parse_args()
    dispatch = {
        "start": cmd_start, "stop": cmd_stop, "restart": cmd_restart,
        "status": cmd_status, "log": cmd_log, "send": cmd_send, "peers": cmd_peers,
    }
    if args.command in dispatch:
        dispatch[args.command](args)
    elif args.command == "schedule":
        sc_dispatch = {"list": cmd_schedule_list, "add": cmd_schedule_add, "remove": cmd_schedule_remove, "run": cmd_schedule_run}
        if args.action in sc_dispatch:
            sc_dispatch[args.action](args)
        else:
            sc.print_help()
    else:
        p.print_help()


if __name__ == "__main__":
    main()
