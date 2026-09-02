import abc
from typing import Iterable, Union

class BaseReader(abc.ABC):

    @abc.abstractmethod
    def read(self, data: Union[bytes, str]) -> list[str]:
        raise NotImplementedError

    def flush(self) -> list[str]:
        return []

    def feed_all(self, chunks: Iterable[Union[bytes, str]]) -> list[str]:

        out: list[str] = []

        for chunk in chunks:
            out.extend(self.read(chunk))

        return out

def decode_chunk(
        data: Union[bytes, str], 
        encoding: str = "utf-8", 
        errors: str = "replace"
        ) -> str:

    if isinstance(data, bytes):
        return data.decode(encoding, errors=errors)

    return data

def split_lines(text: str) -> tuple[list[str], str]:

    if not text:
        return [], ""

    lines = text.split("\n")
    remainder = lines.pop()

    return lines, remainder