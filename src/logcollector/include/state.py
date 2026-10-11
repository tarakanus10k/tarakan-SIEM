import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _agent_conf_path() -> Path:

    return Path(__file__).resolve().parent.parent / "config" / "agent.conf"


@lru_cache(maxsize=1)
def _conf_root() -> ET.Element:
    tree = ET.parse(_agent_conf_path())
    return tree.getroot()


def _get_text(path: str, default: str = "") -> str:
    try:
        v = _conf_root().findtext(path)
        return (v.strip() if v else default)
    except (OSError, ET.ParseError) as e:
        logger.error("не удалось прочитать agent.conf: %s", e)
        return default


def get_default_state_dir() -> Path:
    return Path(_get_text("logcollector/default_state_dir",
                          "/var/tsec/queue/logcollector"))


def get_ack_file() -> str:
    return _get_text("logcollector/default_ack_file", "pending_acks.json")


def get_file_inode(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_ino
    except OSError:
        return None

def find_rotated_file(
        old_inode: int, 
        hint_dir: str, 
        hint_name: str
        ) -> Optional[str]:

    try:
        entries = os.listdir(hint_dir)
    except OSError:
        return None

    for entry in entries:
        full = os.path.join(hint_dir, entry)

        try:
            st = os.stat(full)
        except OSError:
            continue

        if st.st_ino == old_inode:
            return full

    return None

def get_file_size(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_size
    except OSError:
        return None
