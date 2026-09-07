from typing import Iterator, Optional

try:
    from systemd import journal as _journal
    _HAS_SYSTEMD = True
except ImportError:
    _journal = None
    _HAS_SYSTEMD = False

class JournaldReader:

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
            raise RuntimeError

    @staticmethod
    def is_available() -> bool:
        return _HAS_SYSTEMD

    def iter_entries(self, wait: bool = True) -> Iterator[dict]:

        reader = _journal.Reader()

        if self._field and self._pattern:
            reader.add_match(**{self._field: self._pattern})

        if self._cursor:
            try:
                reader.seek_cursor(self._cursor)
                next(reader, None)
            except Exception:
                reader.seek_tail()
                next(reader, None)

        else:
            reader.seek_tail()
            next(reader, None)

        if wait:
            reader.log_level(_journal.LOG_DEBUG)

        while True:
            entry = reader.__next__()
            if entry is None:
                reader.wait()
                continue

            self._cursor = entry.get("__CURSOR")
            yield entry

    @property
    def cursor(self) -> Optional[str]:
        return self._cursor

def format_journald_entry(entry: dict) -> str:

    message = entry.get("MESSAGE", "")
    if isinstance(message, bytes):
        message = message.decode("utf-8", errors="replace")

    unit = entry.get("_SYSTEM_UNIT") or entry.get("SYSLOG_IDENTIFIER") or "unknow"
    if isinstance(unit, bytes):
        unit = unit.decode("utf-8", errors="replace")

    ts = entry.get("__REALTIME_TIMESTAMP")
    return f"[{unit}] {message}" if ts is None else f"[{unit}] {ts} {message}"