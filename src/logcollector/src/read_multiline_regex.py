import re
from typing import Union

from .base_reader import (
    BaseReader,
    decode_chunk,
    split_lines
    )

class MultilineRegexReader(BaseReader):

    def __init__(self, pattern: str, encoding: str = "utf-8") -> None:

        self._encoding = encoding
        self._pattern = re.compile(pattern)
        self._buffer: str = ""
        self._current: list[str] = []

    def read(self, data: Union[bytes, str]) -> list[str]:

        text = decode_chunk(data, self._encoding)

        if not text:
            return []

        self._buffer += text
        lines, remainder = split_lines(self._buffer)
        self._buffer = remainder

        messages: list[str] = []
        for line in lines:
            line = line.rstrip("\r")

            if self._pattern.match(line):
                if self._current:
                    messages.append("\n".join(self._current))
                    self._current = []

            self._current.append(line)
        return messages

    def flush(self) -> list[str]:

        out: list[str] = []
        if self._current:
            out.append("\n".join(self._current))
            self._current = []

        if self._buffer:
            out.append(self._buffer.rstrip("\r"))
            self._buffer = ""

        return out