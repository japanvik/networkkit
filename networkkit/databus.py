"""NetworkKit Databus — message bus with scheduled publishing.

Run: python -m networkkit.databus
Config: networkkit.toml (optional, searched in CWD then ~/.config/networkkit/)
"""
import asyncio
import datetime
import json
import logging
import os
import tomllib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
import zmq
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from networkkit.messages import Message, MessageType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8000,
    "zmq_port": 5555,
    "auth_token": "",
    "data_dir": str(Path.home() / ".local" / "share" / "networkkit"),
    "log_level": "INFO",
}


def _find_config_file() -> Path | None:
    env = os.environ.get("NETWORKKIT_CONFIG", "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    for candidate in [Path("networkkit.toml"), Path.home() / ".config" / "networkkit" / "networkkit.toml"]:
        if candidate.is_file():
            return candidate
    return None


def load_config() -> dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    path = _find_config_file()
    if path:
        with open(path, "rb") as f:
            file_cfg = tomllib.load(f)
        server = file_cfg.get("server", {})
        for k in ("host", "port", "zmq_port", "auth_token", "data_dir", "log_level"):
            if k in server:
                cfg[k] = server[k]
        logger.info("Config loaded from %s", path)
    # Env overrides
    for k, env_key in [("host", "NETWORKKIT_HOST"), ("port", "NETWORKKIT_PORT"),
                        ("zmq_port", "NETWORKKIT_ZMQ_PORT"), ("auth_token", "NETWORKKIT_AUTH_TOKEN"),
                        ("data_dir", "NETWORKKIT_DATA_DIR"), ("log_level", "NETWORKKIT_LOG_LEVEL")]:
        v = os.environ.get(env_key, "").strip()
        if v:
            cfg[k] = int(v) if k in ("port", "zmq_port") else v
    return cfg


CONFIG: dict[str, Any] = {}

# ── ZMQ Publisher ───────────────────────────────────────────────────

context = zmq.Context()
publisher = context.socket(zmq.PUB)


async def send_message(message: Message):
    try:
        if not message.created_at:
            message.created_at = datetime.datetime.now().isoformat()
        publisher.send_json(message.model_dump())
        return {"status": "success"}
    except Exception as e:
        logger.error("Error sending message: %s", e)
        return {"status": "error"}


# ── Auth ────────────────────────────────────────────────────────────

def _check_auth(request: Request) -> bool:
    token = CONFIG.get("auth_token", "")
    if not token:
        return True
    provided = request.headers.get("X-NetworkKit-Token", "")
    return provided == token


# ── Schedule Store ──────────────────────────────────────────────────

class ScheduleEntry(BaseModel):
    name: str
    to: str = "ALL"
    message_type: str = "SYSTEM"
    content: str = ""
    interval_seconds: int | None = None  # simple interval
    cron: str | None = None  # cron expression (future)
    enabled: bool = True


class ScheduleStore:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.schedules: dict[str, ScheduleEntry] = {}
        self._load()

    def _load(self):
        if self.file_path.is_file():
            try:
                data = json.loads(self.file_path.read_text())
                for item in data.get("schedules", []):
                    entry = ScheduleEntry(**item)
                    self.schedules[entry.name] = entry
                logger.info("Loaded %d schedules from %s", len(self.schedules), self.file_path)
            except Exception:
                logger.exception("Failed loading schedules from %s", self.file_path)

    def _save(self):
        data = {"schedules": [e.model_dump() for e in self.schedules.values()]}
        self.file_path.write_text(json.dumps(data, indent=2))

    def list(self) -> list[dict]:
        return [e.model_dump() for e in self.schedules.values()]

    def get(self, name: str) -> ScheduleEntry | None:
        return self.schedules.get(name)

    def upsert(self, entry: ScheduleEntry) -> ScheduleEntry:
        self.schedules[entry.name] = entry
        self._save()
        return entry

    def remove(self, name: str) -> bool:
        if name in self.schedules:
            del self.schedules[name]
            self._save()
            return True
        return False


def _parse_interval(raw: str) -> int | None:
    raw = raw.strip().lower()
    if not raw:
        return None
    if raw.endswith("s"):
        return int(raw[:-1])
    if raw.endswith("m"):
        return int(raw[:-1]) * 60
    if raw.endswith("h"):
        return int(raw[:-1]) * 3600
    if raw.endswith("d"):
        return int(raw[:-1]) * 86400
    try:
        return int(raw)
    except ValueError:
        return None


def _cron_field_matches(field: str, value: int) -> bool:
    """Check if a cron field matches a value. Supports *, exact, comma-separated, and ranges."""
    field = field.strip()
    if field == "*":
        return True
    for part in field.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            if int(lo) <= value <= int(hi):
                return True
        elif part.isdigit() and int(part) == value:
            return True
    return False


def _cron_matches_now(cron_expr: str, now: datetime.datetime) -> bool:
    """Check if a 5-field cron expression matches the current time."""
    fields = cron_expr.strip().split()
    if len(fields) != 5:
        return False
    minute, hour, day, month, weekday = fields
    return (
        _cron_field_matches(minute, now.minute)
        and _cron_field_matches(hour, now.hour)
        and _cron_field_matches(day, now.day)
        and _cron_field_matches(month, now.month)
        and _cron_field_matches(weekday, (now.weekday() + 1) % 7)  # cron: 0=Sunday
    )


STORE: ScheduleStore | None = None

# ── Scheduler Loop ──────────────────────────────────────────────────

async def scheduler_loop():
    """Run scheduled messages based on interval or cron."""
    last_run: dict[str, float] = {}
    last_cron_minute: dict[str, str] = {}  # track last fired minute to avoid double-fire
    while True:
        try:
            now_mono = asyncio.get_event_loop().time()
            now_dt = datetime.datetime.now(datetime.timezone.utc)
            now_minute_key = now_dt.strftime("%Y%m%d%H%M")
            if STORE:
                for entry in list(STORE.schedules.values()):
                    if not entry.enabled:
                        continue
                    should_fire = False
                    if entry.interval_seconds:
                        last = last_run.get(entry.name, 0)
                        if now_mono - last >= entry.interval_seconds:
                            should_fire = True
                    elif entry.cron:
                        matches = _cron_matches_now(entry.cron, now_dt)
                        already_fired = last_cron_minute.get(entry.name) == now_minute_key
                        if matches and not already_fired:
                            should_fire = True
                    if not should_fire:
                        continue
                    last_run[entry.name] = now_mono
                    last_cron_minute[entry.name] = now_minute_key
                    content = entry.content
                    if content == "time":
                        content = f"Current time: {now_dt.isoformat()}"
                    msg = Message(
                        source=f"scheduler:{entry.name}",
                        to=entry.to,
                        content=content,
                        message_type=MessageType(entry.message_type) if entry.message_type in MessageType.__members__ else MessageType.SYSTEM,
                    )
                    await send_message(msg)
                    logger.info("Schedule fired: %s -> %s", entry.name, entry.to)
        except Exception:
            logger.exception("Scheduler loop error")
        await asyncio.sleep(1)


# ── FastAPI App ─────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(scheduler_loop())
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/data")
async def post_message(request: Request):
    if not _check_auth(request):
        return JSONResponse({"status": "error", "detail": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
        if not isinstance(body, dict):
            return JSONResponse({"status": "error", "detail": "invalid format"}, status_code=400)
        message = Message(**body)
    except ValidationError as e:
        return JSONResponse({"status": "error", "detail": e.errors()}, status_code=400)
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=400)
    result = await send_message(message)
    return result


@app.get("/schedules")
async def list_schedules(request: Request):
    if not _check_auth(request):
        return JSONResponse({"status": "error", "detail": "unauthorized"}, status_code=401)
    return {"schedules": STORE.list() if STORE else []}


@app.post("/schedules")
async def create_schedule(request: Request):
    if not _check_auth(request):
        return JSONResponse({"status": "error", "detail": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
        # Support interval shorthand
        if "interval" in body and "interval_seconds" not in body:
            body["interval_seconds"] = _parse_interval(str(body.pop("interval", "")))
        entry = ScheduleEntry(**body)
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=400)
    if STORE:
        STORE.upsert(entry)
    return {"status": "created", "schedule": entry.model_dump()}


@app.delete("/schedules/{name}")
async def delete_schedule(name: str, request: Request):
    if not _check_auth(request):
        return JSONResponse({"status": "error", "detail": "unauthorized"}, status_code=401)
    if STORE and STORE.remove(name):
        return {"status": "deleted", "name": name}
    return JSONResponse({"status": "error", "detail": "not found"}, status_code=404)


@app.post("/schedules/{name}/run")
async def run_schedule(name: str, request: Request):
    if not _check_auth(request):
        return JSONResponse({"status": "error", "detail": "unauthorized"}, status_code=401)
    if not STORE:
        return JSONResponse({"status": "error", "detail": "no store"}, status_code=500)
    entry = STORE.get(name)
    if not entry:
        return JSONResponse({"status": "error", "detail": "not found"}, status_code=404)
    content = entry.content
    if content == "time":
        content = f"Current time: {datetime.datetime.now(datetime.timezone.utc).isoformat()}"
    msg = Message(
        source=f"scheduler:{entry.name}",
        to=entry.to,
        content=content,
        message_type=MessageType(entry.message_type) if entry.message_type in MessageType.__members__ else MessageType.SYSTEM,
    )
    await send_message(msg)
    return {"status": "sent", "name": name}


# ── Entrypoint ──────────────────────────────────────────────────────

def main():
    global CONFIG, STORE
    CONFIG = load_config()
    logger.setLevel(getattr(logging, CONFIG.get("log_level", "INFO").upper(), logging.INFO))

    zmq_port = int(CONFIG.get("zmq_port", 5555))
    publisher.bind(f"tcp://*:{zmq_port}")

    data_dir = Path(CONFIG.get("data_dir", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    STORE = ScheduleStore(data_dir / "schedules.json")

    host = CONFIG.get("host", "0.0.0.0")
    port = int(CONFIG.get("port", 8000))
    logger.info("NetworkKit databus starting host=%s port=%s zmq=%s auth=%s",
                host, port, zmq_port, "enabled" if CONFIG.get("auth_token") else "disabled")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
