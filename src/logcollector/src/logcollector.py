import gzip
import json
import hashlib
import threading
import uuid
import socket
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .base_reader import BaseReader
from .read_multiline_regex import MultilineRegexReader
from .read_multiline_unit import MultilineUnitReader
from .read_json import JsonReader
from .read_syslog import SyslogReader
from .read_journald import (
    JournaldReader,
    format_journald_entry,
    _HAS_SYSTEMD
)
from .state import (
    DEFAULT_STATE_DIR,
    SourceState, 
    StateStore,
    PendingAck,
    get_file_inode,
    get_file_size,
    find_rotated_file,
    detect_rotation,
    detect_truncate
    )
from .queue import (
    DEFAULT_QUEUE_DIR,
    DiskQueue,
    QueueFullError,
    QueueLimits
)

from . import config as _config
from ..include.logcollector import (
    get_default_state_flush_interval,
    get_default_selector_timeout,
    get_queue_max_items,
    get_queue_max_bytes,

    get_host_ip,
    get_os_info,
    utc_now_iso
)

@dataclass
class _FileSource:

    source: _config.LocalFile
    file: "object"
    location: str
    reader: BaseReader
    state: Optional[SourceState] = None

@dataclass
class LogCollectorConfig:

    state_dir: str = DEFAULT_STATE_DIR
    queue_dir: str = DEFAULT_QUEUE_DIR

    default_state_flush_interval: float = get_default_state_flush_interval()
    default_selector_timeout: float = get_default_selector_timeout()
    queue_max_items: Optional[int] = get_queue_max_items()
    queue_max_bytes: Optional[int] = get_queue_max_bytes()

    encoding: str = "utf-8"

    first_read_tail_bytes: int = 0

class LogCollector:

    def __init__(
            self, 
            config_loader: _config.AgentLogConfig, 
            collector_config: Optional[LogCollectorConfig] = None, 
            agent_id: Optional[str] = None
            ) -> None:

        self._loader = config_loader
        self._cfg = collector_config or LogCollectorConfig()
        self._agent_id = agent_id or str(uuid.uuid4())

        self._hostname = socket.gethostname()
        self._host_ip = get_host_ip()
        self._os_info = get_os_info()

        self._file_sources: dict[str, _FileSource] = {}
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lock = threading.RLock()

        self._state_store = StateStore(
            state_dir=self._cfg.state_dir,
            flush_interval=self._cfg.default_state_flush_interval
        )

        self._queue = DiskQueue(
            queue_dir=self._cfg.queue_dir,
            limits=QueueLimits(
                max_items=self._cfg.queue_max_items,
                max_bytes=self._cfg.queue_max_bytes
            )
        )

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def start(self) -> None:

        if self._threads:
            return

        self._stop_event.clear()

        self._state_store.start_periodic_flush()

        t_file = threading.Thread(
            target=self._file_loop, daemon=True, name="logcollector-file"
        )
        self._threads.append(t_file)

        t_sync = threading.Thread(
            target=self._sync_loop, daemon=True, name="logcollector-sync"
        )
        self._threads.append(t_sync)

        t_journal = threading.Thread(
            target=self._journald_loop, daemon=True, name="logcollector-journald"
        )
        self._threads.append(t_journal)

        for t in self._threads:
            t.start()

    def stop(self) -> None:

        self._stop_event.set()

        for t in self._threads:
            t.join(timeout=5.0)

        self._threads.clear()

        with self._lock:
            for fs in list(self._file_sources.values()):
                try:
                    if fs.state is not None:
                        for msg in fs.reader.flush():
                            self._process_message(fs.source, msg, fs.state, new_offset=fs.state.offset)
                except Exception:
                    print("error: flush reader")

        with self._lock:
            for fs in list(self._file_sources.values()):
                self._unregister_file_sources(fs)
            self._file_sources.clear()

        self._state_store.stop_periodic_flush()

    # ------------------------------------------------------------------
    # 
    # ------------------------------------------------------------------

    def _sync_loop(self) -> None:

        self._sync_sources()
        while not self._stop_event.wait(2.0):
            self._sync_sources()

    def _sync_sources(self) -> None:

        with self._lock:
            try:
                sources = self._loader.get_valid_confs()
            except Exception:
                return

            wanted: dict[str, _config.LocalFile] = {}

            for s in sources:
                if s.log_format == _config.LogFormat.JOURNALD:
                    continue

                wanted[s.location] = s

            for location, fs in list(self._file_sources.items()):
                if location not in wanted:
                    self._unregister_file_source(fs)

            for location, src in wanted.items():
                if location in self._file_sources:
                    continue

                self._register_file_source(src)

    # ------------------------------------------------------------------
    # 
    # ------------------------------------------------------------------

    def _register_file_source(self, source: _config.LocalFile) -> None:

        location = source.location

        try:
            f = open(location, "rb", buffering=0)
        except OSError as e:
            return

        reader = self._make_reader(source)

        fs = _FileSource(
            source=source,
            file=f,
            location=location,
            reader=reader
        )

        with self._lock:
            self._file_sources[location] = fs

        self._init_file_position(fs)

    def _unregister_file_source(self, fs: _FileSource) -> None:

        with self._lock:
            self._file_sources.pop(fs.location, None)

        try:
            fs.file.close()
        except OSError:
            pass

    def _make_reader(self, source: _config.LocalFile) -> BaseReader:

        if source.multiline is not None:
            ml = source.multiline

            if ml.type == _config.MultilineType.REGEX:
                return MultilineRegexReader(
                    pattern=ml.pattern or "", encoding=self._cfg.encoding
                )

            if ml.type == _config.MultilineType.UNIT:
                return MultilineUnitReader(
                    lines=ml.lines or 1, encoding=self._cfg.encoding
                )

        if source.log_format == _config.LogFormat.JSON:
            return JsonReader(encoding=self._cfg.encoding)

        return SyslogReader(encoding=self._cfg.encoding)

    # ------------------------------------------------------------------
    # Positioning
    # ------------------------------------------------------------------

    def _init_file_position(self, fs: _FileSource) -> None:

        source = fs.source
        state = self._state_store.get_or_create(
            source_key=self._source_key(source),
            log_format=source.log_format.value,
            location=source.location
        )
        fs.state = state

        source_key = self._source_key(source)
        max_pending_offset = state.offset

        for ack in self._state_store.pending_acks.all():
            if ack.source_key == source_key and ack.new_offset is not None:
                if ack.new_offset > max_pending_offset:
                    max_pending_offset = ack.new_offset

        fs.read_offset = max_pending_offset

        inode = get_file_inode(fs.location)
        size = get_file_size(fs.location)

        if inode is None or size is None:
            return

        if state.first_seen:

            if self._cfg.first_read_tail_bytes > 0:
                start = max(0, size - self._cfg.first_read_tail_bytes)

            else:
                start = size

            fs.file.seek(start)
            state.offset = start
            state.inode = inode
            state.size = size
            state.first_seen = False
            fs.read_offset = start
            self._state_store.update(state)

            return

        if detect_rotation(state, fs.location):
            self._handle_rotation(fs, state, old_inode=state.inode or 0)

            return

        if detect_truncate(state, fs.location):

            fs.file.seek(0)
            state.offset = 0
            state.size = size
            fs.read_offset = 0
            self._state_store.update(state)

            return

        start = max(state.offset, fs.read_offset)

        if start > size:
            start = size

        fs.file.seek(start)
        fs.read_offset = start
        state.inode = inode
        state.size = size
        self._state_store.update(state)

    def _handle_rotation(
            self, 
            fs: _FileSource, 
            state: SourceState, 
            old_inode: int
            ) -> None:

        hint_dir = str(Path(fs.location).parent)
        hint_name = Path(fs.location).name
        old_path = find_rotated_file(old_inode, hint_dir, hint_name)

        if old_path:
            try:
                with open(old_path, "rb") as old_f:
                    old_f.seek(state.offset)

                    while True:
                        chunk = old_f.read(64 * 1024)

                        if not chunk:
                            break

                        messages = fs.reader.read(chunk)

                        for msg in messages:
                            self._process_message(fs.source, msg, state, new_state=state.offset)

            except OSError as e:
                print("can't read old file")

            for msg in fs.reader.flush():
                self._process_message(fs.source, msg, state, new_state=state.offset)

        else:
            for msg in fs.reader.flush():
                self._process_message(fs.source, msg, state, new_state=state.offset)

        try:
            fs.file.close()
        except OSError:
            pass

        try:
            fs.file = open(fs.location, "rb", buffering=0)
        except OSError as e:
            return

        new_inode = get_file_inode(fs.location)
        new_size = get_file_size(fs.location)
        state.offset = 0
        state.inode = new_inode
        state.size = new_size
        fs.read_offset = 0
        self._state_store.update(state)

    # ------------------------------------------------------------------
    # file stream
    # ------------------------------------------------------------------

    def _file_loop(self) -> None:
        
        while not self._stop_event.is_set():
            with self._lock:
                sources_snapshot = list(self._file_sources.values())

            for fs in sources_snapshot:
                if self._stop_event.is_set():
                    break

                self._poll_sources(fs)
            self._stop_event.wait(self._cfg.default_selector_timeout)

    def _poll_sources(self, fs: _FileSource) -> None:

        state = fs.state
        if state is None:
            return

        if detect_rotation(state, fs.location):
            self._handle_rotation(fs, state, old_inode=state.inode or 0)
            return

        if detect_truncate(state, fs.location):

            for msg in fs.reader.flush():
                self._process_message(fs.source, msg, state, new_offset=0)

            fs.file.seek(0)
            state.offset = 0
            fs.read_offset = 0
            state.size = get_file_size(fs.location) or 0
            self._state_store.update(state)

        current_size = get_file_size(fs.location)
        if current_size is None:
            return

        start = max(state.offset, fs.read_offset)

        if current_size <= start:
            return

        try:
            fs.file.seek(start)
            chunk = fs.file.read(current_size - start)

        except OSError as e:
            return

        if not chunk:
            return

        messages = fs.reader.read(chunk)
        new_offset = start + len(chunk)
        fs.read_offset = new_offset

        for msg in messages:
            self._process_message(fs.source, msg, state, new_offset=new_offset, new_size=current_size)

    # ------------------------------------------------------------------
    # json stream
    # ------------------------------------------------------------------

    def _journald_loop(self) -> None:

        while not self._stop_event.is_set():
            try:
                sources = [
                    s for s in self._loader.get_valid_confs() if s.log_format == _config.LogFormat.JOURNALD
                    ]
            except Exception:
                self._stop_event.wait(5.0)
                continue

            if not sources:
                self._stop_event.wait(2.0)
                continue

            if not _HAS_SYSTEMD:
                self._stop_event.wait(10.0)
                continue

            for source in sources:
                if self._stop_event.is_set():
                    break
                self._read_journald_source(source)

            self._stop_event.wait(1.0)

    def _read_journald_source(self, source: _config.LocalFile) -> None:

        source_key = self._source_key(source)
        state = self._state_store.get_or_create(
            source_key=source_key,
            log_format=source.log_format.value,
            location=source.location
        )

        field = source.filter.field if source.filter else None
        pattern = source.filter.pattern if source.filter else None

        try:
            reader = JournaldReader(
                cursor=state.cursor, field=field, pattern=pattern
            )
        except RuntimeError as e:
            return

        try:
            gen = reader.iter_entries(wait=True)
            entry = next(gen, None)

            if entry is not None:
                raw_message = format_journald_entry(entry)
                self._process_message(source, raw_message, state, new_cursor=reader.cursor)

        except StopIteration:
            pass
        except Exception:
            print("error: read journald")

    # ------------------------------------------------------------------
    # work with 1 object
    # ------------------------------------------------------------------

    def _process_message(
            self, 
            source: _config.LocalFile, 
            raw_message: str, 
            state: SourceState,
            new_offset: Optional[int] = None,
            new_size: Optional[int] = None,
            new_cursor: Optional[int] = None
            ) -> None:

        if not raw_message:
            return

        obj = self._build_log_object(source, raw_message)

        try:
            obj_json = json.dumps(obj, ensure_ascii=False).encode(self._cfg.encoding)
        except (TypeError, ValueError) as e:
            return

        compressed = gzip.compress(obj_json)

        try:
            seq = self._queue.put(compressed)
        except QueueFullError as e:
            return
        except Exception:
            return

        source_key = self._source_key(source)

        ack = PendingAck(
            seq=seq,
            source_key=source_key,
            new_offset=new_offset if new_offset is not None else state.offset,
            new_cursor=new_cursor,
            new_size=new_size
        )

        self._state_store.pending_acks.add(ack)

    def ack_seqs(self, up_to_seq: int) -> int:

        with self._lock:
            acks = self._state_store.pending_acks.get_up_to(up_to_seq)

            if not acks:
                return self._queue.ack(up_to_seq)

            by_source: dict[str, PendingAck] = {}
            for ack in acks:
                cur = by_source.get(ack.source_key)
                
                if cur is None or ack.seq > cur.seq:
                    by_source[ack.source_key] = ack

            for source_key, ack in by_source.items():
                state = self._state_store.get(source_key)

                if state is None:
                    continue

                if ack.new_offset is not None:
                    state.offset = ack.new_offset

                if ack.new_cursor is not None:
                    state.cursor = ack.new_cursor

                if ack.new_size is not None:
                    state.size = ack.new_size

                self._state_store.update(state)

            self._state_store.pending_acks.remove_up_to(up_to_seq)

            return self._queue.ack(up_to_seq)

    @property
    def queue(self) -> DiskQueue:
        return self._queue

    def _build_log_object(self, source: _config.LocalFile, raw_message: str) -> dict:

        hash_value = hashlib.sha256(raw_message.encode(self._cfg.encoding)).hexdigest()

        service_name = self._extract_service_name(source)

        return {
            "raw_message": raw_message,
            "hash": hash_value,
            "log_file_path": source.location,
            "source_type": self._source_type(source),
            "service_name": service_name,
            "agent_id": self._agent_id,
            "hostname": self._hostname,
            "host_ip": self._host_ip,
            "os": self._os_info,
            "timestamp_read": utc_now_iso()
        }

    @staticmethod
    def _source_type(source: _config.LocalFile) -> str:

        if source.log_format == _config.LogFormat.JOURNALD:
            return "journald"

        return "file"

    @staticmethod
    def _extract_service_name(source: _config.LocalFile) -> str:

        if source.log_format == _config.LogFormat.JOURNALD:
            if source.filter and source.filter.pattern:
                name = re.sub(r"[\^\$\(\)\[\]\?]", "", source.filter.pattern)
                return name or "journald"

            return "journald"

        try:
            return Path(source.location).stem
        except (ValueError, OSError):
            return source.location

    @staticmethod
    def _source_key(source: _config.LocalFile) -> str:

        f = ""
        if source.filter:
            f = f"|{source.filter.field}={source.filter.pattern}"

        m = ""
        if source.multiline:
            m = f"|ml={source.multiline.type.value}"

        return f"{source.log_format.value}::{source.location}{f}{m}"