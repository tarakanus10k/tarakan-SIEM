import glob
import os
from typing import List

def masks_cheker(pattern: str) -> List[str]:
    if any(ch in pattern for ch in ("*", "?", "[")):
        return sorted(glob.glob(pattern))
    return pattern

def exists_cheker(path: str) -> bool:
    return os.path.isfile(path)

def readeble_cheker(path: str) -> bool:
    """make check: can be the file read or not"""