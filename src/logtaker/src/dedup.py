import json
import logging
import os
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

PERSIST_MAX_CONSECUTIVE_ERRORS = 3


class BatchDedupCache:

    def __init__(
            self,
            ttl: float = 300.0,
            max_entries: int = 100_000,
            clock: Optional[callable] = None,
            persist_path=None
            ) -> None:

        self._ttl = max(0.0, float(ttl))
        self._max_entries = max(1, int(max_entries))

        self._clock = clock or time.monotonic

        self._lock = threading.Lock()

        self._entries: "OrderedDict[str, float]" = OrderedDict()
        self._last_sweep = self._clock()

        self._persist_path = Path(persist_path) if persist_path else None
        self._persist_enabled = self._persist_path is not None
        self._persist_errors = 0
        self._persist_fh = None

        if self._persist_path is not None:
            self._load_journal()

    # ------------------------------------------------------------------
    # публичный API
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._ttl > 0

    @property
    def ttl(self) -> float:
        return self._ttl

    @property
    def max_entries(self) -> int:
        return self._max_entries

    @property
    def persist_path(self):
        return self._persist_path

    @property
    def persistence_enabled(self) -> bool:
        return self._persist_enabled

    def check_and_mark(self, batch_id: str) -> bool:

        if not batch_id or self._ttl <= 0:
            return True

        now = self._clock()

        with self._lock:
            self._sweep_expired(now)

            expiry = self._entries.get(batch_id)

            if expiry is not None:
                if expiry > now:
                    new_expiry = now + self._ttl
                    self._entries.move_to_end(batch_id)
                    self._entries[batch_id] = new_expiry
                    self._journal_mark(batch_id, new_expiry, now)
                    return False

                self._entries.pop(batch_id, None)

            new_expiry = now + self._ttl
            self._entries[batch_id] = new_expiry
            self._enforce_bound()
            self._journal_mark(batch_id, new_expiry, now)

            return True

    def unmark(self, batch_id: str) -> None:

        if not batch_id:
            return

        with self._lock:
            self._entries.pop(batch_id, None)

            if self._persist_enabled:
                self._journal_write({"id": batch_id, "del": True})

    def size(self) -> int:
        with self._lock:
            self._sweep_expired(self._clock())
            return len(self._entries)

    def close(self) -> None:

        with self._lock:
            if self._persist_fh is not None:
                try:
                    self._persist_fh.flush()
                except Exception:
                    pass
                try:
                    self._persist_fh.close()
                except Exception:
                    pass
                self._persist_fh = None

    # ------------------------------------------------------------------
    # внутреннее: окно TTL
    # ------------------------------------------------------------------

    def _sweep_expired(self, now: float) -> None:

        if self._ttl <= 0:
            return

        if now - self._last_sweep < self._ttl / 2.0:
            return

        self._last_sweep = now

        while self._entries:
            batch_id, expiry = next(iter(self._entries.items()))

            if expiry > now:
                break

            self._entries.pop(batch_id, None)

        if self._persist_enabled:
            self._compact_journal(now)

    def _enforce_bound(self) -> None:
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    # ------------------------------------------------------------------
    # внутреннее: персистенция (append-only JSONL-журнал)
    # ------------------------------------------------------------------

    def _journal_mark(self, batch_id, expiry_mono: float, now_mono: float) -> None:

        exp_wall = time.time() + (expiry_mono - now_mono)
        self._journal_write({"id": batch_id, "exp": exp_wall})

    def _journal_write(self, record: dict) -> None:

        if not self._persist_enabled:
            return

        try:
            if self._persist_fh is None:
                parent = self._persist_path.parent

                if str(parent) and not parent.exists():
                    parent.mkdir(parents=True, exist_ok=True)

                self._persist_fh = open(
                    self._persist_path, "a", encoding="utf-8")

            self._persist_fh.write(
                json.dumps(record, ensure_ascii=False) + "\n")
            self._persist_fh.flush()

            self._persist_errors = 0

        except Exception as e:
            self._persist_fail(e)

    def _persist_fail(self, e: Exception) -> None:
        self._persist_errors += 1

        if self._persist_fh is not None:
            try:
                self._persist_fh.close()
            except Exception:
                pass
            self._persist_fh = None

        if self._persist_enabled and (
                self._persist_errors >= PERSIST_MAX_CONSECUTIVE_ERRORS):
            self._persist_enabled = False
            logger.warning(
                "журнал дедупликации %s отключён после %d ошибок записи "
                "подряд (%s) — дедупликация продолжается в памяти",
                self._persist_path, self._persist_errors, e)
        else:
            logger.warning("журнал дедупликации %s: ошибка записи: %s",
                           self._persist_path, e)

    def _load_journal(self) -> None:

        try:
            with open(self._persist_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except FileNotFoundError:
            return
        except OSError as e:
            logger.warning(
                "журнал дедупликации %s не читается (%s) — "
                "стартую с пустым окном", self._persist_path, e)
            return

        now_wall = time.time()
        now_mono = self._clock()

        restored: "OrderedDict[str, float]" = OrderedDict()

        for line in lines:
            line = line.strip()

            if not line:
                continue

            try:
                rec = json.loads(line)
            except ValueError:
                continue

            if not isinstance(rec, dict):
                continue

            batch_id = rec.get("id")

            if not isinstance(batch_id, str) or not batch_id:
                continue

            if rec.get("del") is True:
                restored.pop(batch_id, None)
                continue

            try:
                exp_wall = float(rec.get("exp"))
            except (TypeError, ValueError):
                continue

            if batch_id in restored:
                restored.move_to_end(batch_id)

            restored[batch_id] = exp_wall

        for batch_id, exp_wall in restored.items():
            remaining = exp_wall - now_wall

            if remaining <= 0:
                continue

            self._entries[batch_id] = now_mono + remaining

        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def _compact_journal(self, now_mono: float) -> None:

        if self._persist_fh is not None:
            try:
                self._persist_fh.close()
            except Exception:
                pass
            self._persist_fh = None

        tmp = self._persist_path.with_name(self._persist_path.name + ".tmp")
        now_wall = time.time()

        try:
            with open(tmp, "w", encoding="utf-8") as f:
                for batch_id, expiry in self._entries.items():
                    rec = {"id": batch_id,
                           "exp": now_wall + (expiry - now_mono)}
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")

                f.flush()

            os.replace(tmp, self._persist_path)
            self._persist_errors = 0

        except Exception as e:
            self._persist_fail(e)