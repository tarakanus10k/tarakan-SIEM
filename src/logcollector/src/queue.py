from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import threading
import os
import json

from ..include.queue import (
    get_default_queue_dir,
    get_queue_max_items,
    get_queue_max_bytes
    )

DEFAULT_QUEUE_DIR = get_default_queue_dir()

@dataclass
class QueueLimits:

    max_items: Optional[int] = get_queue_max_items()
    max_bytes: Optional[int] = get_queue_max_bytes()

class QueueFullError(Exception):
    """"""

class DiskQueue:

    META_FILE = "meta.json"
    DATA_GLOB = "item-*.bin"

    def __init__(
            self, 
            queue_dir: Path = DEFAULT_QUEUE_DIR, 
            limits: Optional[QueueLimits] = None
            ) -> None:

        self._dir = Path(queue_dir)
        self._limits = limits or QueueLimits()
        self._lock = threading.RLock()
        self._head: int = 0
        self._tail: int = 0
        self._total_bytes: int = 0
        self._count: int = 0
        self._ensure_dir()
        self._load_meta()

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def put(self, payload: bytes) -> int:

        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError

        with self._lock:
            if self._limits.max_items is not None:
                if self._count >= self._limits.max_items:
                    raise QueueFullError

            if self._limits.max_items is not None:
                if self._total_bytes + len(payload) > self._limits.max_bytes:
                    raise QueueFullError

            seq = self._tail
            path = self._item_path(seq)
            self._atomic_write(path, payload)

            self._tail += 1
            self._count += 1
            self._total_bytes += len(payload)
            self._save_meta()

            return seq

    def get(self) -> Optional[bytes]:

        with self._lock:
            if self._count == 0:
                return None

            while self._head < self._tail:
                path = self._item_path(self._head)
                if path.exists():
                    break

                self._head += 1

            if self._head >= self._tail:
                self._count = 0
                self._total_bytes = 0
                self._save_meta()

                return None

            path = self._item_path(self._head)

            try:
                payload = path.read_bytes()
            except OSError as e:
                return None

            try:
                path.unlink()
            except OSError as e:
                print("can't delete the queue-element")

            self._head += 1
            self._count -= 1
            self._total_bytes = max(0, self._total_bytes - len(payload))
            self._save_meta()

            return payload

    def peek(self) -> Optional[bytes]:

        with self._lock:
            if self._count == 0:
                return None

            path = self._item_path(self._head)
            if not path.exists():
                return None

            try:
                return path.read_bytes()
            except OSError:
                return None

    def size(self) -> int:
        with self._lock:
            return self._count

    def total_bytes(self) -> int:
        with self._lock:
            return self._total_bytes

    def is_full(self) -> bool:
        with self._lock:
            if self._limits.max_items is not None and self._count >= self._limits.max_items:
                return True

            if self._limits.max_bytes is not None and self._total_bytes >= self._limits.max_bytes:
                return True

            return False

    def clear(self) -> None:
        with self._lock:
            for p in self._dir.glob("item-*.bin"):
                try:
                    p.unlink()
                except OSError:
                    pass

            self._head = 0
            self._tail = 0
            self._count = 0
            self._total_bytes = 0
            self._save_meta()

    # ------------------------------------------------------------------
    # 
    # ------------------------------------------------------------------

    def _ensure_dir(self) -> None:
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise RuntimeError

    def _item_path(self, seq: int) -> Path:
        return self._dir / f"item-{seq:020d}.bin"

    def _meta_path(self) -> Path:
        return self._dir / self.META_FILE

    def _atomic_write(self, path: Path, data: bytes) -> None:

        tmp = path.with_suffix(".bin.tmp")
        try:
            with tmp.open("wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp, path)

        except OSError as e:
            tmp.unlink(missing_ok=True)
            raise RuntimeError

    def _load_meta(self) -> None:

        path = self._meta_path()
        if not path.exists():
            self._rebuild_from_disk()
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                meta = json.load(f)

            self._head = int(meta.get("head", 0))
            self._tail = int(meta.get("tail", 0))
            self._count = int(meta.get("count", 0))
            self._total_bytes = int(meta.get("total_bytes", 0))

        except (OSError, ValueError, json.JSONDecodeError) as e:
            self._rebuild_from_disk()

    def _rebuild_from_disk(self) -> None:

        seqs: list[int] = []
        total = 0

        for p in self._dir.glob("item-*.bin"):
            try:

                name = p.stem
                seq = int(name.split("-", 1)[1])
                seqs.append(seq)
                total += p.stat().st_size

            except (ValueError, OSError):
                continue

        if not seqs:

            self._head = 0
            self._tail = 0
            self._count = 0
            self._total_bytes = 0

            return

        seqs.sort()
        self._head = seqs[0]
        self._tail = seqs[-1] + 1
        self._count = len(seqs)
        self._total_bytes = total
        self._save_meta()

    def _save_meta(self) -> None:

        path = self._meta_path()
        tmp = path.with_suffix(".json.tmp")

        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump({
                    "head": self._head,
                    "tail": self._tail,
                    "count": self._count,
                    "total_bytes": self._total_bytes
                }, f)

            os.replace(tmp, path)

        except OSError as e:
            print("can't save queue meta.json")