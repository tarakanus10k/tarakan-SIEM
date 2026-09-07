from typing import Union

from .base_reader import (
    BaseReader, 
    decode_chunk
    )

from ..include.read_json import (
    find_object_end,
    is_valid_json
)

class JsonReader(BaseReader):

    def __init__(self, encoding: str = "utf-8") -> None:

        self._encoding = encoding
        self._buffer: str = ""

    def read(self, data: Union[bytes, str]) -> list[str]:

        text = decode_chunk(data, self._encoding)

        if not text:
            return []

        self._buffer += text
        messages, remainder = self._extract_objects(self._buffer)
        self._buffer = remainder

        return messages

    def flush(self) -> list[str]:

        if not self._buffer:
            return []

        candidate, self._buffer = self._buffer, ""

        if is_valid_json(candidate):
            return [candidate]

        return []

    @staticmethod
    def _extract_objects(text: str) -> tuple[list[str], str]:

        messages: list[str] = []
        i = 0
        n = len(text)

        while i < n:
            if text[i] != "{":
                i += 1
                continue

            end = find_object_end(text, 1)
            if end == -1:
                return messages, text[i:]

            messages.append(text[i:end + 1])
            i = end + 1

        return messages, ""