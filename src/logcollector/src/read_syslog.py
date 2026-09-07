from typing import Union

from .base_reader import (
    BaseReader,
    decode_chunk,
    split_lines
)

class SyslogReader(BaseReader):

    def __init__(self, encoding: str = "utf-8") -> None:
        self._encoding = encoding
        self._buffer: str = ""

    def read(self, data: Union[bytes, str]) -> list[str]:

        text = decode_chunk(data, self._encoding)

        if not text:
            return []

        self._buffer += text
        lines, remainder = split_lines(self._buffer)
        self._buffer = remainder

        return [ln.rstrip("\r") for ln in lines if ln is not None]

    def flush(self) -> list[str]:

        if not self._buffer:
            return []

        last, self._buffer = self._buffer, ""
        return [last.rstrip("\r")]