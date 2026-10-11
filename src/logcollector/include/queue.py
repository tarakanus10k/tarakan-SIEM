import xml.etree.ElementTree as ET
from pathlib import Path
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


def get_default_queue_dir() -> Path:
    return Path(_get_text("logcollector/default_queue_dir",
                          "/var/tsec/queue/logcollector/outbox"))


def get_queue_max_items() -> int:
    return int(_get_text("logcollector/queue_max_items", "100000") or "100000")


def get_queue_max_bytes() -> int:
    return int(_get_text("logcollector/queue_max_bytes", "1073741824") or "1073741824")
