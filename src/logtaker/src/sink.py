import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class RotatingJsonlSink:

    def __init__(
            self,
            path,
            max_bytes: int = 10 * 1024 * 1024,
            max_files: int = 5,
            rotate_interval: float = 0.0,
            ) -> None:

        self._path = Path(path)

        self._max_bytes = max(0, int(max_bytes))
        self._max_files = max(1, int(max_files))

        self._rotate_interval = float(rotate_interval)

        if self._rotate_interval < 0:
            raise ValueError(
                f"{rotate_interval!r}")

        parent = self._path.parent
        if str(parent) and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)

        self._fh = open(self._path, "a", encoding="utf-8")

        try:
            self._size = self._path.stat().st_size
        except OSError:
            self._size = 0

        self._opened_at = time.monotonic()

        self._rotations = 0

    # ------------------------------------------------------------------
    # публичный API
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def max_files(self) -> int:
        return self._max_files

    @property
    def rotate_interval(self) -> float:
        return self._rotate_interval

    @property
    def rotations(self) -> int:
        return self._rotations

    @property
    def seconds_until_rotation(self) -> Optional[float]:
        if self._rotate_interval <= 0:
            return None

        return max(0.0, self._rotate_interval
                   - (time.monotonic() - self._opened_at))

    def write(self, event) -> None:

        line = json.dumps(event, ensure_ascii=False) + "\n"
        line_bytes = len(line.encode("utf-8"))

        if self._should_rotate(line_bytes) or self._interval_elapsed():
            self.rotate()

        self._fh.write(line)
        self._fh.flush()
        self._size += line_bytes

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.flush()
        except OSError:
            pass

        self._fh.close()

    def rotate(self) -> None:

        self._rotations += 1

        self._fh.flush()
        self._fh.close()

        oldest = Path(f"{self._path}.{self._max_files}")

        try:
            oldest.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("can't remove oldest sink %s: %s", oldest, e)

        for i in range(self._max_files - 1, 0, -1):
            src = Path(f"{self._path}.{i}")
            dst = Path(f"{self._path}.{i + 1}")

            if src.exists():
                try:
                    os.replace(src, dst)
                except OSError as e:
                    logger.warning("can't shift sink %s -> %s: %s", src, dst, e)

        try:
            os.replace(self._path, Path(f"{self._path}.1"))
        except OSError as e:

            logger.warning("can't rotate sink %s: %s — продолжаю append", self._path, e)

        self._fh = open(self._path, "a", encoding="utf-8")
        self._size = self._path.stat().st_size

        self._opened_at = time.monotonic()

    # ------------------------------------------------------------------
    # внутреннее
    # ------------------------------------------------------------------

    def _should_rotate(self, incoming_bytes: int) -> bool:

        if self._max_bytes <= 0:
            return False

        if self._size == 0:
            return False

        return self._size + incoming_bytes > self._max_bytes

    def _interval_elapsed(self) -> bool:

        if self._rotate_interval <= 0:
            return False

        if self._size == 0:
            return False

        return (time.monotonic() - self._opened_at) >= self._rotate_interval
