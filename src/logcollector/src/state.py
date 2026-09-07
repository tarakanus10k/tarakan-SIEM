import json
import os
import threading
from dataclasses import dataclass, asdict
from typing import Optional
from pathlib import Path

from ..include.state import (
    get_default_state_dir,
    get_file_inode,
    get_file_size,
    find_rotated_file
    )

DEFAULT_STATE_DIR = get_default_state_dir()

@dataclass
class SourceState:

    source_key: str
    log_format: str
    location: str

    offset: int = 0
    inode: Optional[int] = None
    size: Optional[int] = None

    cursor: Optional[str] = None

    first_seen: bool = True

    def to_dict(self) -> dict:

        d = asdict(self)

        return d

    @staticmethod
    def from_dict(data: dict) -> "SourceState":
        return SourceState(**{k: v for k, v in data.items() if k in SourceState.__dataclass_fields__})

class StateStore:

    def __init__(
            self, 
            state_dir: Path = DEFAULT_STATE_DIR, 
            flush_interval: float = 5.0
            ) -> None:

        self._state_dir = Path(state_dir)
        self._flush_interval = flush_interval
        self._states: dict[str, SourceState] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None
        self._ensure_dir()

    def _ensure_dir(self) -> None:

        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print("can't create state dirs")

    def get(self, source_key: str) -> Optional[SourceState]:

        with self._lock:

            if source_key in self._states:
                return self._states[source_key]

            loaded = self._load_from_disk(source_key)

            if loaded is not None:
                loaded.first_seen = False
                self._states[source_key] = loaded

                return loaded

            return None

    def get_or_create(
            self, 
            source_key: str, 
            log_format: str, 
            location: str
            ) -> SourceState:

        with self._lock:
            st = self.get(source_key)

            if st is not None:
                return st

            st = SourceState(
                source_key=source_key, 
                log_format=log_format, 
                location=location
                )

            self._states[source_key] = st

            return st

    def update(self, state: SourceState) -> None:
        with self._lock:
            self._states[state.source_key] = state

    def remove(self, source_key: str) -> None:
        with self._lock:
            self._states.pop(source_key, None)
            self._delete_from_disk(source_key)

    def _state_file(self, source_key: str) -> Path:

        safe = "".join(c if c.isalnum() or c in "-._" else "_" for c in source_key)
        safe = safe[:200]

        return self._state_dir / f"{safe}.json"

    def _load_from_disk(self, source_key: str) -> Optional[SourceState]:

        path = self._state_dir(source_key)

        if not path.exists():
            return None

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            return SourceState.from_dict(data)

        except (OSError, json.JSONDecodeError, TypeError) as e:
            return None

    def _safe_to_disk(self, state: SourceState) -> None:

        path = self._state_file(state.source_key)
        tmp = path.with_suffix("json.tmp")

        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, ensure_ascii=False)

            os.replace(tmp, path)

        except OSError as e:
            print("can't safe state")

    def _delete_from_disk(self, source_key: str) -> None:

        path = self._state_file(source_key)

        try:
            path.unlink(missing_ok=True)
        except OSError as e:
            print("can't delete state")

    def flush(self) -> None:

        with self._lock:
            states = list(self._states.values())

        for st in states:
            self._safe_to_disk(st)

    def start_periodic_flush(self) -> None:

        if self._flush_thread is not None:
            return

        self._stop_event.clear()

        self._flush_thread = threading.Thread(
            target=self._flush_loop, daemon=True, name="logcollector-state_flush"
        )

        self._flush_thread.start()

    def stop_periodic_flush(self) -> None:

        self._stop_event.set()

        if self._flush_thread is not None:
            self._flush_thread.join(timeout=5.0)
            self._flush_thread = None

        self.flush()

    def _flush_loop(self) -> None:
        while not self._stop_event.wait(self._flush_interval):
            try:
                self.flush()
            except Exception:
                print("error: periodical state flush")

def detect_rotation(state: SourceState, path: str) -> bool:

    current_inode = get_file_inode(path)

    if current_inode is None or state.inode is None:
        return False

    return current_inode != state.inode

def detect_truncate(state: SourceState, path: str) -> bool:

    current_size = get_file_size(path)

    if current_size is None or state.size is None:
        return False

    return current_size < state.size