import logging
from typing import Iterator, Optional

logger = logging.getLogger(__name__)

try:
    from systemd import journal as _journal
    _HAS_SYSTEMD = True
except ImportError:
    _journal = None
    _HAS_SYSTEMD = False

class JournaldReader:

    DRAIN_LIMIT = 1000
    WAIT_TIMEOUT = 0.5

    def __init__(
            self,
            cursor: Optional[str] = None,
            field: Optional[str] = None,
            pattern: Optional[str] = None
            ) -> None:

        self._cursor = cursor
        self._field = field
        self._pattern = pattern

        if not _HAS_SYSTEMD:
            raise RuntimeError(
                "journald недоступен: модуль systemd.journal (python-systemd) "
                "не установлен или systemd не запущен. Установите пакет "
                "python3-systemd (apt install python3-systemd) или "
                "python-systemd (pip), либо уберите journald-источники из conf.d "
                "— файловые источники продолжат работать"
            )

        self._reader = _journal.Reader()

        if self._field and self._pattern:
            self._reader.add_match(**{self._field: self._pattern})

        if self._cursor:
            try:
                self._reader.seek_cursor(self._cursor)
                next(self._reader, None)

            except Exception:
                logger.warning("cursor %r не найден, чтение с конца", self._cursor)
                self._reader.seek_tail()

        else:
            self._reader.seek_tail()

        try:
            self._reader.log_level(_journal.LOG_DEBUG)
        except Exception as e:
            logger.debug("reader.log_level failed: %s", e)

    @staticmethod
    def is_available() -> bool:
        return _HAS_SYSTEMD

    def iter_entries(self, wait: bool = True) -> Iterator[dict]:
        drained = 0

        while drained < self.DRAIN_LIMIT:
            try:
                entry = next(self._reader, None)
            except Exception as e:
                logger.warning("error: read journald entry: %s", e)
                return

            if entry is None:
                if wait:
                    try:
                        self._reader.wait(self.WAIT_TIMEOUT)
                    except Exception as e:
                        logger.warning("error: journald wait: %s", e)
                return

            self._cursor = entry.get("__CURSOR")
            drained += 1
            yield entry

    @property
    def cursor(self) -> Optional[str]:
        return self._cursor

def format_journald_entry(entry: dict) -> str:

    message = entry.get("MESSAGE", "")
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="replace")

    unit = entry.get("_SYSTEMD_UNIT") or entry.get("SYSLOG_IDENTIFIER") or "unknown"
    if isinstance(unit, bytes):
        unit = unit.decode("utf-8", errors="replace")

    ts = entry.get("__REALTIME_TIMESTAMP")
    return f"[{unit}] {message}" if ts is None else f"[{unit}] {ts} {message}"
