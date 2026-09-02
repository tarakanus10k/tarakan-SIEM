import gzip
import json
import hashlib
import threading
import uuid
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .base_reader import BaseReader
from .state import (
    DEFAULT_STATE_DIR,
    SourceState, 
    StateStore
    )

from . import config as _config
from ..include.logcollector import (
    get_default_state_flush_interval,
    get_default_selector_timeout,
    get_queue_max_items,
    get_queue_max_bytes,

    get_host_ip,
    get_os_info
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

    default_state_flush_interval: float = get_default_state_flush_interval()
    default_selector_timeout: float = get_default_selector_timeout()
    queue_max_items: Optional[int] = get_queue_max_items()
    queue_max_bytes: Optional[int] = get_queue_max_bytes()

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
                            self._process_message(fs.source, msg, fs.state)
                except Exception:
                    print("error: flush reader")

        with self._lock:
            for fs in list(self._file_sources.values()):
                self._unregister_file_sources(fs)
            self._file_sources.clear()

        self._state_store.stop_periodic_flush()