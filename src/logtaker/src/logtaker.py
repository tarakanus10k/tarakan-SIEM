import collections
import gzip
import io
import json
import logging
import os
import queue as _queue
import ssl
import threading
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from .metrics import (
    PROMETHEUS_CONTENT_TYPE,
    LogTakerMetrics,
)
from .dedup import BatchDedupCache

logger = logging.getLogger(__name__)

from ..include.logtaker import (
    DEFAULT_DEDUP_MAX_ENTRIES,
    DEFAULT_DEDUP_TTL,
    DEFAULT_SINK_MAX_BYTES,
    DEFAULT_SINK_MAX_FILES,
    MAX_TAIL_SIZE,

    get_default_logtaker_port,
    get_host,
    get_internal_queue_size,
    get_max_request_bytes,
    get_tail_size,

    get_server_cert,
    get_server_key,
    get_ca_cert,
    get_require_client_cert,

    get_dedup_ttl,
    get_dedup_max_entries,
    get_dedup_persist,
    get_sink_max_bytes,
    get_sink_max_files,
    _resolve_path,
)

DEFAULT_LOGTAKER_PORT = get_default_logtaker_port()

# Дедупликация пачек по batch id: окно и предел по умолчанию —
# живут в include (там же дефолты стока: DEFAULT_SINK_MAX_BYTES/FILES)


def _conf_dedup_persist_path() -> Optional[str]:
    """logtaker/dedup_persist -> путь журнала (None — выключено/не задано).

    off/none/false/0 (любой регистр) — персистенция выключена; пусто —
    не задано (CLI решает: sidecar рядом со стоком); прочее — путь,
    относительные пути резолвятся как в include (_resolve_path).
    """
    raw = get_dedup_persist().strip()

    if not raw:
        return None

    if raw.lower() in ("off", "none", "false", "0"):
        return None

    return str(_resolve_path(raw))


@dataclass
class LogTakerConfig:

    port: int = DEFAULT_LOGTAKER_PORT
    host: str = get_host()

    # TLS теперь берётся из agent.conf (<logtaker>/server_cert|server_key|
    # ca_cert|require_client_cert), а не игнорируется.
    server_cert: Optional[str] = get_server_cert() or None
    server_key: Optional[str] = get_server_key() or None
    ca_cert: Optional[str] = get_ca_cert() or None

    require_client_cert: bool = get_require_client_cert()

    internal_queue_size: int = get_internal_queue_size()

    max_request_bytes: int = get_max_request_bytes()

    # Дедупликация пачек по batch id (at-most-once в пределах окна):
    # ttl в секундах (0 — выключить), жёсткий предел числа id в кэше.
    # Значения берутся из agent.conf (<logtaker>), CLI-флаги приоритетнее.
    dedup_ttl: float = get_dedup_ttl()
    dedup_max_entries: int = get_dedup_max_entries()

    # Персистенция дедупликации: append-only JSONL-журнал, переживает
    # рестарт приёмника. None — не задано в agent.conf (CLI тогда выводит
    # sidecar <out>.dedup или выключает по --no-dedup-persist).
    dedup_persist_path: Optional[str] = _conf_dedup_persist_path()

    # Ротация JSONL-стока: порог в байтах (0 — выключить) и число копий.
    sink_max_bytes: int = get_sink_max_bytes()
    sink_max_files: int = get_sink_max_files()

    # Хвост принятых событий для GET /events: сколько ПОСЛЕДНИХ событий
    # держать в памяти (deque). Лучшие усилия по построению: In-memory,
    # теряется при рестарте, приём событий НИКОГДА не блокируется ради
    # хвоста (append идёт после того, как пачка уже принята в очередь).
    # Значение по умолчанию — из agent.conf (<logtaker>/tail_size),
    # CLI --tail-size приоритетнее. ВАЖНО: /events отдаёт СОДЕРЖИМОЕ
    # логов через того же слушателя, что и POST /ingest, — защищайте
    # его TLS/mTLS (см. man-страницу и README).
    tail_size: int = get_tail_size()

class _LogTakerServer(ThreadingHTTPServer):
    # Дочерние потоки-обработчики не должны держать процесс при выходе.
    daemon_threads = True

class LogTaker:

    def __init__(
            self,
            config: Optional[LogTakerConfig] = None
            ) -> None:

        self._cfg = config or LogTakerConfig()
        self._events_queue: _queue.Queue = _queue.Queue(maxsize=self._cfg.internal_queue_size)
        self._server: Optional[_LogTakerServer] = None
        self._thread: Optional[threading.Thread] = None
        self._ssl_ctx: Optional[ssl.SSLContext] = None

        # Хвост принятых событий (GET /events): deque(maxlen=tail_size).
        # Защитный зажим — ТРЕТИЙ слой контроля (после геттера agent.conf
        # и чека CLI --validate), работающий taker не может упасть на
        # плохом значении: отрицательное -> 0 (хвост выключен, GET
        # /events отвечает 404), большее верхнего предела -> предел.
        # Эффективное значение пишется обратно в cfg, чтобы хендлер и
        # /health видели одно и то же.
        raw_tail = int(getattr(self._cfg, "tail_size", 0) or 0)
        tail_size = max(0, min(MAX_TAIL_SIZE, raw_tail))
        self._cfg.tail_size = tail_size

        # deque.append атомарен; блокировка делает многособытийные
        # extend'ы и копии-снапшоты консистентными друг с другом
        self._tail = collections.deque(maxlen=tail_size) if tail_size > 0 else None
        self._tail_lock = threading.Lock()

        # Метрики доступны и до start() (для тестов и /health после старта)
        self.metrics = LogTakerMetrics(queue_capacity=self._cfg.internal_queue_size)

        # Дедупликация по batch id: ttl=0 выключает (кэш не создаётся,
        # /health тогда честно рапортует dedup_entries=0). persist_path —
        # журнал дедупликации: окно переживает рестарт процесса.
        self.dedup: Optional[BatchDedupCache] = None

        if self._cfg.dedup_ttl > 0:
            self.dedup = BatchDedupCache(
                ttl=self._cfg.dedup_ttl,
                max_entries=self._cfg.dedup_max_entries,
                persist_path=self._cfg.dedup_persist_path,
            )

    def start(self) -> None:

        if self._server is not None:
            return

        self._ssl_ctx = self._build_ssl_context()
        handler = _make_handler(self._cfg, self._events_queue, self.metrics,
                                dedup=self.dedup, taker=self)

        self._server = _LogTakerServer(
            (self._cfg.host, self._cfg.port), handler
        )

        if self._ssl_ctx is not None:
            self._server.socket = self._ssl_ctx.wrap_socket(
                self._server.socket, server_side=True
            )

        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True, name="logtaker-server"
        )

        self._thread.start()
        logger.info("logtaker listening on %s:%s tls=%s",
                    self._cfg.host, self._cfg.port, self._cfg.server_cert is not None)

    def stop(self) -> None:

        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

        # Журнал дедупликации: flush+close (состояние остаётся на диске)
        if self.dedup is not None:
            self.dedup.close()

    def get_events(
            self, 
            block: bool = True, 
            timeout: Optional[float] = None
            ) -> Optional[list]:

        try:
            batch = self._events_queue.get(block=block, timeout=timeout)
        except _queue.Empty:
            batch = None

        self.metrics.set_queue_depth(self._events_queue.qsize())

        return batch

    def pending_events(self) -> int:
        return self._events_queue.qsize()

    # ------------------------------------------------------------------
    # хвост принятых событий (GET /events)
    # ------------------------------------------------------------------

    def _tail_extend(self, events) -> None:
        """Принятую пачку — в хвост (только ПРИНЯТЫЕ пачки).

        Вызывается строго ПОСЛЕ успешного events_queue.put_nowait:
        дубликаты и 503 (очередь полна) выходят из do_POST раньше и в
        хвост не попадают. Best-effort по построению: хвост никогда не
        ломает приём (append в deque уже в очереди пачки не отказывает).
        """
        if self._tail is None or not events:
            return

        try:
            with self._tail_lock:
                self._tail.extend(events)
        except Exception as e:                  # pragma: no cover — страховка
            logger.warning("event tail append failed: %s", e)

    def tail_events(self, limit: Optional[int] = None) -> list:
        """Снапшот хвоста: хронологический список, новейшие последними.

        limit — сколько НОВЕЙШИХ событий вернуть (None — всё, что
        накоплено в хвосте); хвост выключен (tail_size=0) — [].
        """
        if self._tail is None:
            return []

        with self._tail_lock:
            items = list(self._tail)

        if limit is not None:
            items = items[-int(limit):] if int(limit) > 0 else []

        return items

    def tail_len(self) -> int:
        """Текущая длина хвоста (0 — хвост выключен или пуст)."""
        if self._tail is None:
            return 0

        with self._tail_lock:
            return len(self._tail)

    def _build_ssl_context(self) -> Optional[ssl.SSLContext]:

        if self._cfg.server_cert is None and self._cfg.server_key is None:
            logger.warning("TLS не настроен (server_cert/server_key не заданы) — запускаем без TLS (ТОЛЬКО тест)")
            return None

        if self._cfg.server_cert is None or self._cfg.server_key is None:
            raise RuntimeError(
                "для TLS нужны ОБА файла: server_cert и server_key "
                f"(задано: server_cert={self._cfg.server_cert!r}, "
                f"server_key={self._cfg.server_key!r}; либо задайте оба, "
                "либо запустите с --no-tls)"
            )

        for name, path in (
            ("server_cert", self._cfg.server_cert),
            ("server_key", self._cfg.server_key),
            ("ca_cert", self._cfg.ca_cert),
        ):
            if path and not os.path.isfile(path):
                raise RuntimeError(
                    f"файл сертификата не найден: {name}={path!r} "
                    "(проверьте путь в agent.conf / CLI или запустите с --no-tls)"
                )

        if self._cfg.require_client_cert and not self._cfg.ca_cert:
            raise RuntimeError(
                "require_client_cert=True требует ca_cert — без ca_cert "
                "mTLS не может быть enforcement и verify_mode останется CERT_NONE"
            )

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

        ctx.load_cert_chain(
            certfile=self._cfg.server_cert,
            keyfile=self._cfg.server_key,
        )

        if self._cfg.ca_cert:
            ctx.load_verify_locations(cafile=self._cfg.ca_cert)

            if self._cfg.require_client_cert:
                ctx.verify_mode = ssl.CERT_REQUIRED
            else:
                ctx.verify_mode = ssl.CERT_OPTIONAL

        ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        return ctx

def _make_handler(cfg: LogTakerConfig,
                  events_queue: _queue.Queue,
                  metrics: LogTakerMetrics,
                  dedup: Optional[BatchDedupCache] = None,
                  taker: Optional["LogTaker"] = None):

    class _Handler(BaseHTTPRequestHandler):

        def log_message(self, fmt, *args):
            logger.info("%s %s", self.address_string(), fmt % args)

        # ------------------------------------------------------------------
        # GET: /metrics (Prometheus), /health (JSON) и /events (хвост
        # принятых событий); прочие пути — 405
        # ------------------------------------------------------------------

        def do_GET(self):
            # ?limit=N у /events: путь и запрос разбираются urlsplit'ом
            split = urllib.parse.urlsplit(self.path)
            path = split.path
            query = split.query

            if path == "/metrics":
                # gauge хвоста — прямо перед рендером: пачки могли быть
                # приняты другими потоками с прошлого пуша
                metrics.set_gauges(
                    events_queue.qsize(),
                    cfg.internal_queue_size,
                    taker.tail_len() if taker is not None else 0,
                )

                body = metrics.render_prometheus().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", PROMETHEUS_CONTENT_TYPE)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/health":
                snap = metrics.snapshot()

                if dedup is not None and cfg.dedup_ttl > 0:
                    dedup_ttl: float = cfg.dedup_ttl

                    try:
                        dedup_entries = dedup.size()
                    except Exception:
                        dedup_entries = 0
                else:
                    dedup_ttl, dedup_entries = 0, 0

                body = json.dumps({
                    "status": "ok",
                    "uptime": round(snap["uptime_seconds"], 3),
                    "queue_depth": snap["queue_depth"],
                    "queue_capacity": snap["queue_capacity"],
                    "tls": cfg.server_cert is not None,
                    "dedup_ttl": dedup_ttl,
                    "dedup_entries": dedup_entries,
                    "sink_rotations": snap["sink_rotations"],
                    "tail_size": cfg.tail_size,
                    "tail_len": taker.tail_len() if taker is not None else 0,
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/events":
                self._serve_events(query)
                return

            self.send_error(
                405,
                "Only GET /metrics, /health, /events, POST /ingest are allowed")

        def _serve_events(self, query: str) -> None:
            """GET /events?limit=N — хвост принятых событий (новейшие последними).

            Лучшие усилия по построению: хвост in-memory (теряется при
            рестарте) и НИКОГДА не блокирует приём — append идёт после
            того, как пачка уже поставлена в очередь, попадают только
            ПРИНЯТЫЕ пачки (дубликаты и 503 — мимо).

            SECURITY: /events отдаёт СОДЕРЖИМОЕ логов через того же
            слушателя, что и POST /ingest — защищайте его TLS/mTLS
            (см. man-страницу tarakan-siem(1) и README).
            """
            if taker is None or taker._tail is None:
                # ВАЖНО: тело ошибки — ТОЛЬКО ASCII (латиница): статус-
                # строка HTTP кодируется latin-1, не-ASCII символ (например
                # длинное тире) вызвал бы UnicodeEncodeError внутри send_error.
                self.send_error(404, "Event tail is disabled (tail_size=0)")
                return

            ts = int(cfg.tail_size)

            limit = ts
            params = urllib.parse.parse_qs(query)

            if "limit" in params:
                raw = params["limit"][0]

                try:
                    limit = int(raw)
                except (TypeError, ValueError):
                    self.send_error(
                        400,
                        f"limit must be an integer between 1 and {ts}")
                    return

                if limit < 1 or limit > ts:
                    self.send_error(
                        400,
                        f"limit must be between 1 and {ts}")
                    return

            events = taker.tail_events(limit=limit)

            body = json.dumps({
                "count": len(events),
                "tail_size": ts,
                "events": events,
            }).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:

            if cfg.require_client_cert and cfg.ca_cert:
                peercert = self.connection.getpeercert()

                if not peercert:
                    metrics.inc_reject("no_client_cert")
                    self.send_error(403, "Client certificate required (mTLS)")
                    return

            if self.path not in ("/ingest", "/"):
                metrics.inc_reject("not_found")
                self.send_error(404, "Not found")
                return

            try:
                content_length = int(self.headers.get("Content-Length", 0))
            except (TypeError, ValueError):
                metrics.inc_reject("bad_schema")
                self.send_error(400, "Invalid Content-Length")
                return

            if content_length <= 0:
                metrics.inc_reject("empty_body")
                self.send_error(400, "Empty body")
                return

            if content_length > cfg.max_request_bytes:
                metrics.inc_reject("payload_too_large")
                self.send_error(413, "Payload too large")
                return

            encoding = (self.headers.get("Content-Encoding") or "").lower()

            try:
                if "gzip" in encoding:
                    decompressed = _read_gzip_stream(self.rfile, content_length,
                                                    cfg.max_request_bytes)
                else:
                    decompressed = _read_raw_stream(self.rfile, content_length,
                                                    cfg.max_request_bytes)

            except _DecompressError as e:
                logger.warning("decompression error: %s", e)
                metrics.inc_reject("decompression_error")
                self.send_error(400, f"Decompression error: {e}")
                return

            except _PayloadTooLarge as e:
                metrics.inc_reject("payload_too_large")
                self.send_error(413, str(e))
                return

            try:
                batch = json.loads(decompressed.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("invalid JSON: %s", e)
                metrics.inc_reject("invalid_json")
                self.send_error(400, f"Invalid JSON: {e}")
                return

            if not isinstance(batch, dict) or "events" not in batch:
                metrics.inc_reject("bad_schema")
                self.send_error(400, "Expected JSON object with 'events' field")
                return

            events = batch.get("events")
            if not isinstance(events, list):
                metrics.inc_reject("bad_schema")
                self.send_error(400, "'events' must be a list")
                return

            # ------------------------------------------------------------------
            # Дедупликация по batch id: ПОСЛЕ валидации схемы, ДО постановки
            # в очередь. Id берётся из заголовка X-Tarakan-Batch-Id (отправитель
            # шлёт его на каждом ретрае), fallback — batch_id из тела пачки.
            # ------------------------------------------------------------------
            batch_id = (self.headers.get("X-Tarakan-Batch-Id") or "").strip() or ""

            if not batch_id and isinstance(batch.get("batch_id"), str):
                batch_id = batch["batch_id"].strip()

            if dedup is not None and batch_id:
                if not dedup.check_and_mark(batch_id):
                    metrics.record_deduplicated()

                    body = json.dumps({"duplicate": True}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("X-Tarakan-Duplicate", "true")
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                    logger.info("duplicate batch id=%s not re-ingested", batch_id)
                    return

            try:
                events_queue.put_nowait(events)
            except _queue.Full:
                logger.warning("внутренняя очередь переполнена, отклоняем пачку")
                metrics.inc_reject("queue_full")

                # Пачка НЕ принята: снимаем отметку дедупликации, иначе
                # ретраи этой пачки навсегда проглатывались бы как дубликаты.
                if dedup is not None and batch_id:
                    dedup.unmark(batch_id)

                self.send_error(503, "Internal queue full")
                return

            # Хвост для GET /events — строго ПОСЛЕ успешной постановки в
            # очередь: только ПРИНЯТЫЕ пачки. Дубликат-ветка и 503-ветка
            # вышли выше по return, поэтому в хвост они не попадают.
            # Best-effort: хвост никогда не блокирует и не ломает приём.
            if taker is not None:
                taker._tail_extend(events)

            metrics.inc_batches()
            metrics.inc_events(len(events))
            metrics.inc_bytes(content_length)
            metrics.set_queue_depth(events_queue.qsize())

            body = json.dumps({"status": "ok", "count": len(events)}).encode("utf-8")
            self.send_response(200)
            self.send_header("X-Tarakan-Duplicate", "false")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            logger.info("accepted events=%d", len(events))

    return _Handler

class _DecompressError(Exception):
    """"""

class _PayloadTooLarge(Exception):
    """"""

def _read_gzip_stream(rfile, content_length: int, max_bytes: int) -> bytes:

    wrapper = _StreamWrapper(rfile, content_length)
    decompressed = io.BytesIO()
    total = 0

    try:
        with gzip.GzipFile(fileobj=wrapper, mode="rb") as gz:
            while True:
                chunk = gz.read(64 * 1024)

                if not chunk:
                    break

                total += len(chunk)

                if total > max_bytes:
                    raise _PayloadTooLarge(
                        f"Decompressed payload exceeds {max_bytes} bytes"
                    )
                
                decompressed.write(chunk)

    except (OSError, EOFError) as e:
        raise _DecompressError(f"gzip stream corrupted: {e}") from e
    
    return decompressed.getvalue()

def _read_raw_stream(rfile, content_length: int, max_bytes: int) -> bytes:

    if content_length > max_bytes:
        raise _PayloadTooLarge(f"Payload exceeds {max_bytes} bytes")
    
    return rfile.read(content_length)

class _StreamWrapper:

    def __init__(self, rfile, content_length: int) -> None:
        self._rfile = rfile
        self._remaining = content_length
        self._pos = 0

    def read(self, size: int = -1) -> bytes:

        if self._remaining <= 0:
            return b""
        
        if size < 0 or size > self._remaining:
            size = self._remaining

        data = self._rfile.read(size)
        self._remaining -= len(data)
        self._pos += len(data)

        return data

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 1 and offset == 0:
            return self._pos
        
        raise io.UnsupportedOperation("seek not supported on stream")