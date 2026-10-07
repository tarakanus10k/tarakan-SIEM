import gzip
import io
import json
import queue as _queue
import ssl
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from ..include.logtaker import (
    get_default_logtaker_port,
    get_host,
    get_internal_queue_size,
    get_max_request_bytes
)

DEFAULT_LOGTAKER_PORT = get_default_logtaker_port()

@dataclass
class LogTakerConfig:

    port: int = DEFAULT_LOGTAKER_PORT
    host: str = get_host()

    server_cert: Optional[str] = None
    server_key: Optional[str] = None
    ca_cert: Optional[str] = None

    require_client_cert: bool = True

    internal_queue_size: int = get_internal_queue_size()

    max_request_bytes: int = get_max_request_bytes()

class LogTaker:

    def __init__(
            self, 
            config: Optional[LogTakerConfig] = None
            ) -> None:

        self._cfg = config or LogTakerConfig()
        self._events_queue: _queue.Queue = _queue.Queue(maxsize=self._cfg.internal_queue_size)
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._ssl_ctx: Optional[ssl.SSLContext] = None

    def start(self) -> None:

        if self._server is not None:
            return
        
        self._ssl_ctx = self._build_ssl_context()
        handler = _make_handler(self._cfg, self._events_queue)

        self._server = ThreadingHTTPServer(
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
        print(self._cfg.host, self._cfg.port, self._cfg.server_cert is not None)

    def stop(self) -> None:

        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def get_events(
            self, 
            block: bool = True, 
            timeout: Optional[float] = None
            ) -> Optional[list]:

        try:
            return self._events_queue.get(block=block, timeout=timeout)
        except _queue.Empty:
            return None

    def pending_events(self) -> int:
        return self._events_queue.qsize()

    def _build_ssl_context(self) -> Optional[ssl.SSLContext]:

        if self._cfg.server_cert is None and self._cfg.server_key is None:
            print("сертификат сервера не задан — запускаем без TLS (ТОЛЬКО тест)")
            return None
        
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

        if self._cfg.server_cert:
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

def _make_handler(cfg: LogTakerConfig, events_queue: _queue.Queue):

    class _Handler(BaseHTTPRequestHandler):

        def log_message(self, fmt, *args):
            print(self.address_string(), fmt % args)

        def do_GET(self):
            self.send_error(405, "Only POST is allowed")

        def do_POST(self) -> None:

            if cfg.require_client_cert and cfg.ca_cert:
                peercert = self.connection.getpeercert()

                if not peercert:
                    self.send_error(403, "Client certificate required (mTLS)")
                    return

            if self.path not in ("/ingest", "/"):
                self.send_error(404, "Not found")
                return

            content_length = int(self.headers.get("Content-Length", 0))
                
            if content_length <= 0:
                self.send_error(400, "Empty body")
                return
                
            if content_length > cfg.max_request_bytes:
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
                print(e)
                self.send_error(400, f"Decompression error: {e}")
                return
                
            except _PayloadTooLarge as e:
                self.send_error(413, str(e))
                return

            try:
                batch = json.loads(decompressed.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(e)
                self.send_error(400, f"Invalid JSON: {e}")
                return

            if not isinstance(batch, dict) or "events" not in batch:
                self.send_error(400, "Expected JSON object with 'events' field")
                return

            events = batch.get("events")
            if not isinstance(events, list):
                self.send_error(400, "'events' must be a list")
                return

            try:
                events_queue.put_nowait(events)
            except _queue.Full:
                print("внутренняя очередь переполнена, отклоняем пачку")
                self.send_error(503, "Internal queue full")
                return

            body = json.dumps({"status": "ok", "count": len(events)}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            print(len(events))

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