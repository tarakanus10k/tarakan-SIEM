import threading
import time

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

REJECT_REASONS = (
    "invalid_json",
    "empty_body",
    "bad_schema",
    "queue_full",
    "payload_too_large",
    "decompression_error",
    "no_client_cert",
    "not_found",
)

class LogTakerMetrics:

    def __init__(self, queue_capacity: int = 0) -> None:
        self._lock = threading.Lock()
        self._started_monotonic = time.monotonic()

        self._batches_accepted = 0
        self._events_accepted = 0
        self._bytes_accepted = 0
        self._batches_deduplicated = 0
        self._rejected: dict[str, int] = {reason: 0 for reason in REJECT_REASONS}

        self._queue_depth = 0
        self._queue_capacity = int(queue_capacity)

        self._sink_rotations = 0
        self._tail_len = 0

    # ------------------------------------------------------------------
    # counters
    # ------------------------------------------------------------------

    def inc_batches(self, n: int = 1) -> None:
        with self._lock:
            self._batches_accepted += n

    def inc_events(self, n: int = 1) -> None:
        with self._lock:
            self._events_accepted += n

    def inc_bytes(self, n: int = 1) -> None:
        with self._lock:
            self._bytes_accepted += n

    def record_deduplicated(self, n: int = 1) -> None:
        with self._lock:
            self._batches_deduplicated += n

    def inc_reject(self, reason: str) -> None:
        if reason not in self._rejected:
            return

        with self._lock:
            self._rejected[reason] += 1

    # ------------------------------------------------------------------
    # gauges
    # ------------------------------------------------------------------

    def set_queue_depth(self, depth: int) -> None:
        with self._lock:
            self._queue_depth = max(0, int(depth))

    def set_queue_capacity(self, capacity: int) -> None:
        with self._lock:
            self._queue_capacity = max(0, int(capacity))

    def set_gauges(self, queue_depth: int, queue_capacity: int, tail_len: int = 0) -> None:

        with self._lock:
            self._queue_depth = max(0, int(queue_depth))
            self._queue_capacity = max(0, int(queue_capacity))
            self._tail_len = max(0, int(tail_len))

    def set_sink_rotations(self, value: int) -> None:
        with self._lock:
            self._sink_rotations = max(0, int(value))

    def uptime(self) -> float:
        return max(0.0, time.monotonic() - self._started_monotonic)

    # ------------------------------------------------------------------
    # snapshot
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "batches_accepted": self._batches_accepted,
                "events_accepted": self._events_accepted,
                "bytes_accepted": self._bytes_accepted,
                "batches_deduplicated": self._batches_deduplicated,
                "rejected": dict(self._rejected),
                "queue_depth": self._queue_depth,
                "queue_capacity": self._queue_capacity,
                "sink_rotations": self._sink_rotations,
                "tail_len": self._tail_len,
                "uptime_seconds": self.uptime(),
            }

    # ------------------------------------------------------------------
    # Prometheus text exposition (format 0.0.4)
    # ------------------------------------------------------------------

    def render_prometheus(self) -> str:
        snap = self.snapshot()
        lines: list[str] = []

        def _counter(name: str, help_text: str, value: float) -> None:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {_fmt(value)}")

        def _gauge(name: str, help_text: str, value: float) -> None:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {_fmt(value)}")

        _counter("logtaker_batches_accepted", "Accepted ingest batches (HTTP 200).", snap["batches_accepted"])
        _counter("logtaker_events_accepted", "Accepted events inside accepted batches.", snap["events_accepted"])
        _counter("logtaker_bytes_accepted", "Accepted request body bytes.", snap["bytes_accepted"])
        _counter("logtaker_batches_deduplicated_total", "Batches rejected as duplicate batch id (dedup, not re-ingested).", snap["batches_deduplicated"])
        _counter("logtaker_sink_rotations_total", "JSONL sink rotations (size and time triggers, lifetime).", snap["sink_rotations"])

        lines.append("# HELP logtaker_rejected_total Rejected batches by reason.")
        lines.append("# TYPE logtaker_rejected_total counter")
        for reason in REJECT_REASONS:
            value = snap["rejected"][reason]
            lines.append(
                f'logtaker_rejected_total{{reason="{reason}"}} {_fmt(value)}'
            )

        _gauge("logtaker_queue_depth", "Current internal events queue depth.", snap["queue_depth"])
        _gauge("logtaker_queue_capacity", "Internal events queue capacity.", snap["queue_capacity"])
        _gauge("logtaker_tail_len", "Current in-memory event tail length (GET /events).", snap["tail_len"])
        _gauge("logtaker_uptime_seconds", "LogTaker uptime in seconds.", snap["uptime_seconds"])

        return "\n".join(lines) + "\n"

def _fmt(value) -> str:

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, int):
        return str(value)

    return repr(float(value))