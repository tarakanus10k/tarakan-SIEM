import gzip
import json
import logging
import os
import ssl
import threading
import time
import uuid
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from ..include.logsender import (
    get_default_sender_spool_dir,
    get_default_backoff_base,
    get_default_backoff_max,
    get_default_backoff_factor,

    get_target_url,
    get_batch_size,
    get_batch_timeout,
    get_max_retries,
    get_http_timeout,

    get_dead_letter_dir,

    utc_now_iso
)

DEFAULT_SENDER_SPOOL_DIR = get_default_sender_spool_dir()
DEFAULT_BACKOFF_BASE = get_default_backoff_base()
DEFAULT_BACKOFF_MAX = get_default_backoff_max()
DEFAULT_BACKOFF_FACTOR = get_default_backoff_factor()

@dataclass
class LogSenderConfig:

    target_url: str = get_target_url()

    batch_size: int = get_batch_size()
    batch_timeout: float = get_batch_timeout()

    spool_dir: str = DEFAULT_SENDER_SPOOL_DIR

    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    ca_cert: Optional[str] = None
    insecure_skip_verify: bool = False

    backoff_base: float = DEFAULT_BACKOFF_BASE
    backoff_max: float = DEFAULT_BACKOFF_MAX
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
    max_retries: int = get_max_retries()

    http_timeout: float = get_http_timeout()

    dead_letter_dir: str = get_dead_letter_dir()

class LogSender:

    SPOOL_GLOB = "batch-*.bin"
    SPOOL_META_SUFFIX = ".meta"

    def __init__(
            self, 
            collector, 
            config: Optional[LogSenderConfig] = None
            ) -> None:

        self._collector = collector
        self._cfg = config or LogSenderConfig()
        self._spool_dir = Path(self._cfg.spool_dir)
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lock = threading.RLock()

        self._buffer: list[tuple[int, bytes]] = []
        self._buffer_first_ts: Optional[float] = None

        self._in_flight: set[int] = set()

        self._ensure_spool_dir()

    def start(self) -> None:

        if self._threads:
            return

        self._stop_event.clear()

        self._load_spool_seqs_as_in_flight()

        t_resend = threading.Thread(
            target=self._resend_spool_loop, daemon=True, name="logsender-resend"
        )
        self._threads.append(t_resend)

        t_main = threading.Thread(
            target=self._main_loop, daemon=True, name="logsender-main"
        )
        self._threads.append(t_main)

        for t in self._threads:
            t.start()

    def stop(self) -> None:

        self._stop_event.set()

        for t in self._threads:
            t.join(timeout=10.0)

        self._threads.clear()

        with self._lock:
            if self._buffer:
                self._flush_buffer_to_spool()

    def spool_size(self) -> int:

        try:
            return sum(1 for _ in self._spool_dir.glob(self.SPOOL_GLOB))
        except OSError:
            return 0

    def _load_spool_seqs_as_in_flight(self) -> int:

        loaded = 0

        try:
            meta_files = list(self._spool_dir.glob(f"*{self.SPOOL_META_SUFFIX}"))
        except OSError:
            return 0
        
        for meta_path in meta_files:
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                seqs = meta.get("seqs", [])

                with self._lock:
                    for s in seqs:
                        self._in_flight.add(s)
                        loaded += 1

            except (OSError, json.JSONDecodeError) as e:
                logger.warning("can't read spool meta %s: %s", meta_path, e)

        if loaded:
            logger.info("loaded %d in-flight seqs from spool", loaded)
        
        return loaded

    def buffer_size(self) -> int:
        with self._lock:
            return len(self._buffer)

    def _main_loop(self) -> None:

        while not self._stop_event.is_set():
            try:
                self._collect_from_queue()

                if self._is_batch_ready():
                    self._flush_buffer_to_spool()

            except Exception as e:
                logger.warning("logsender main loop error: %s", e)

            self._stop_event.wait(0.5)

    def _collect_from_queue(self) -> None:

        with self._lock:
            needed = self._cfg.batch_size - len(self._buffer)
            in_flight_snapshot = set(self._in_flight)

        if needed <= 0:
            return

        try:
            batch = self._collector.queue.peek_batch(needed, skip_seqs=in_flight_snapshot)
        except Exception as e:
            logger.warning("не удалось прочитать пачку из DiskQueue: %s", e)
            return

        if not batch:
            return

        with self._lock:
            now = time.time()

            if self._buffer_first_ts is None:
                self._buffer_first_ts = now

            self._buffer.extend(batch)

            for seq, _ in batch:
                self._in_flight.add(seq)
            logger.debug("buffer=%d batch_size=%d in_flight=%d",
                         len(self._buffer), self._cfg.batch_size, len(self._in_flight))

    def _is_batch_ready(self) -> bool:

        with self._lock:
            if len(self._buffer) >= self._cfg.batch_size:
                return True
            
            if (self._buffer_first_ts is not None
                    and (time.time() - self._buffer_first_ts) >= self._cfg.batch_timeout
                    and len(self._buffer) > 0):
                
                return True
            
            return False

    def _flush_buffer_to_spool(self) -> Optional[str]:

        with self._lock:
            if not self._buffer:
                return None
            
            items = list(self._buffer)
            self._buffer.clear()
            self._buffer_first_ts = None

        events: list[dict] = []
        seqs: list[int] = []

        for seq, compressed in items:
            try:
                single = json.loads(gzip.decompress(compressed))
                events.append(single)
                seqs.append(seq)
            except (json.JSONDecodeError, OSError) as e:
                # A13: corrupt-элемент сохраняем в dead-letter, НЕ ack'аем.
                # seq остаётся в очереди (и в _in_flight) для ручного разбора.
                logger.error("corrupt element seq=%s, moving to dead_letter: %s",
                             seq, e)
                self._save_to_dead_letter(
                    f"corrupt-{seq}-{uuid.uuid4().hex}.bin", compressed
                )

        if not events:
            return None

        batch_obj = {
            "batch_id": str(uuid.uuid4()),
            "agent_id": getattr(self._collector, "_agent_id", "unknown"),
            "created_at": utc_now_iso(),
            "count": len(events),
            "events": events,
            "seqs": seqs
        }

        payload = gzip.compress(
            json.dumps(batch_obj, ensure_ascii=False).encode("utf-8")
        )

        batch_id = batch_obj["batch_id"]
        path = self._spool_dir / f"batch-{batch_id}.bin"
        meta_path = path.with_suffix(self.SPOOL_META_SUFFIX)
        tmp = path.with_suffix(".bin.tmp")

        try:
            with tmp.open("wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp, path)

            meta_tmp = meta_path.with_suffix(".meta.tmp")

            with meta_tmp.open("w", encoding="utf-8") as f:
                json.dump({"batch_id": batch_id, "seqs": seqs}, f)

            os.replace(meta_tmp, meta_path)
        except OSError as e:
            logger.warning("can't write spool %s: %s", path, e)
            return None

        logger.info("wrote spool %s events=%d bytes=%d",
                    path.name, len(events), len(payload))
        return str(path)

    def _send_pending_spool(self) -> None:

        try:
            files = sorted(self._spool_dir.glob(self.SPOOL_GLOB))
        except OSError:
            return

        for path in files:
            if self._stop_event.is_set():
                break

            self._send_one_spool_file(path)

    def _resend_spool_loop(self) -> None:

        self._send_pending_spool()

        while not self._stop_event.wait(0.5):
            self._send_pending_spool()

    def _send_one_spool_file(self, path: Path) -> None:

        meta_path = path.with_suffix(self.SPOOL_META_SUFFIX)
        seqs: list[int] = []

        try:
            payload = path.read_bytes()
            if meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                seqs = meta.get("seqs", [])

        except OSError as e:
            logger.warning("can't read spool %s: %s", path, e)
            return

        success = False
        attempt = 0
        backoff = self._cfg.backoff_base

        while not self._stop_event.is_set():
            attempt += 1

            try:
                status = self._post(payload)

                if status == 200:
                    success = True
                    break

                if 400 <= status < 500:

                    logger.error(
                        "server returned %s for spool %s, moving to dead_letter %s",
                        status, path.name, self._cfg.dead_letter_dir
                    )

                    if self._save_to_dead_letter(path.name, payload):
                        try:
                            path.unlink(missing_ok=True)
                        except OSError as e:
                            logger.warning("can't delete spool %s: %s", path, e)

                    try:
                        meta_path.unlink(missing_ok=True)
                    except OSError as e:
                        logger.warning("can't delete meta %s: %s", meta_path, e)

                    break

                logger.warning("server returned %s for spool %s attempt=%d backoff=%.2f",
                               status, path.name, attempt, backoff)
                
            except (urllib.error.URLError, OSError, ssl.SSLError) as e:
                logger.warning("network error spool=%s attempt=%d backoff=%.2f: %s",
                               path.name, attempt, backoff, e)

            except Exception as e:
                logger.warning("unexpected error spool=%s attempt=%d: %s",
                               path.name, attempt, e)

            if self._cfg.max_retries and attempt >= self._cfg.max_retries:
                logger.warning("max_retries=%d reached for spool %s, giving up",
                               self._cfg.max_retries, path.name)
                return
            
            self._stop_event.wait(backoff)
            backoff = min(backoff * self._cfg.backoff_factor, self._cfg.backoff_max)

        if success:
            if seqs:
                try:
                    self._collector.ack_seqs(max(seqs))
                except Exception as e:
                    logger.warning("can't ack seqs=%s spool=%s: %s",
                                   seqs, path.name, e)

                with self._lock:
                    for s in seqs:
                        self._in_flight.discard(s)

            try:
                path.unlink(missing_ok=True)
                meta_path.unlink(missing_ok=True)
            except OSError as e:
                logger.warning("can't delete spool %s: %s", path, e)

            logger.info("sent spool %s seqs=%d", path.name, len(seqs))

    def _post(self, payload: bytes) -> int:

        req = urllib.request.Request(
            self._cfg.target_url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            },
        )

        ctx = ssl.create_default_context(cafile=self._cfg.ca_cert)

        if self._cfg.client_cert:
            ctx.load_cert_chain(
                certfile=self._cfg.client_cert,
                keyfile=self._cfg.client_key,
            )

        if self._cfg.insecure_skip_verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        try:
            with urllib.request.urlopen(
                req, 
                timeout=self._cfg.http_timeout, 
                context=ctx
                ) as resp:

                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def _ensure_spool_dir(self) -> None:
        try:
            self._spool_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.warning("can't create spool dir %s: %s", self._spool_dir, e)

    def _save_to_dead_letter(self, name: str, data: bytes) -> Optional[Path]:

        try:

            dl_dir = Path(self._cfg.dead_letter_dir)
            dl_dir.mkdir(parents=True, exist_ok=True)
            target = dl_dir / name
            tmp = target.with_suffix(".bin.tmp")

            with tmp.open("wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp, target)
            return target
        
        except OSError as e:
            logger.error("can't save to dead_letter %s/%s: %s",
                         self._cfg.dead_letter_dir, name, e)
            return None