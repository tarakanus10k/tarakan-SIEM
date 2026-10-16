import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

PROM_SUFFIX = ".prom"

def collect_status(
        collector,
        sender,
        started_at: Optional[float] = None,
        ) -> dict:

    def _num(fn, default: int = 0) -> int:
        try:
            value = fn()

            return default if value is None else int(value)
        except Exception:
            return default

    def _txt(fn, default: str = "") -> str:
        try:
            value = fn()

            return default if value is None else str(value)
        except Exception:
            return default

    queue_items = _num(lambda: collector.queue.size())
    queue_bytes = _num(lambda: collector.queue.total_bytes())
    spool_files = _num(lambda: sender.spool_size())

    pending_acks = _num(lambda: len(collector._state_store.pending_acks))

    file_sources = _num(lambda: len(collector._file_sources))

    def _journald_confs() -> int:
        journald = 0

        for s in collector._loader.get_valid_confs():
            fmt = getattr(s, "log_format", None)

            if getattr(fmt, "value", fmt) == "journald":
                journald += 1

        return journald

    sources = file_sources + _num(_journald_confs)

    def _dead_letter_files() -> int:
        dl_dir = Path(sender._cfg.dead_letter_dir)

        return sum(1 for _ in dl_dir.glob("*.bin"))

    dead_letter_files = _num(_dead_letter_files)

    target_url = _txt(lambda: sender._cfg.target_url)

    if started_at is not None:
        uptime = max(0.0, time.monotonic() - float(started_at))
    else:
        uptime = 0.0

    return {
        "timestamp": _txt(lambda: time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        "pid": _num(lambda: os.getpid()),
        "uptime_seconds": round(uptime, 3),
        "queue_items": queue_items,
        "queue_bytes": queue_bytes,
        "spool_files": spool_files,
        "pending_acks": pending_acks,
        "sources": sources,
        "dead_letter_files": dead_letter_files,
        "target_url": target_url,
    }

PROM_METRICS = (
    ("agent_queue_items",
     "Collector DiskQueue items awaiting send."),
    ("agent_queue_bytes",
     "Collector DiskQueue payload bytes."),
    ("agent_spool_files",
     "Sender spool files awaiting send (batch-*.bin)."),
    ("agent_pending_acks",
     "Collector pending acks (events sent, not yet acknowledged)."),
    ("agent_sources",
     "Active sources: file sources + journald configs."),
    ("agent_dead_letter_files",
     "Parked dead-letter batches (*.bin)."),
    ("agent_uptime_seconds",
     "Agent process uptime in seconds."),
)

def render_prometheus(snapshot: dict) -> str:

    lines: list[str] = []

    for name, help_text in PROM_METRICS:

        field = name[len("agent_"):]

        try:
            value = float(snapshot.get(field, 0))
        except (TypeError, ValueError):
            value = 0.0

        if value != value:
            value = 0.0

        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {_fmt_value(value)}")

    return "\n".join(lines) + "\n"


def _fmt_value(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))

    return repr(value)


def write_status_file(path, snapshot: dict) -> None:

    p = Path(path)

    tmp = p.with_name(p.name + ".tmp")

    if str(p.name).endswith(PROM_SUFFIX):
        content = render_prometheus(snapshot)
    else:
        content = json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"

    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp, p)
