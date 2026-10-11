import socket
import platform
import time
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


def get_default_state_flush_interval() -> float:
    return float(_get_text("logcollector/default_state_flush_interval", "5.0") or "5.0")


def get_default_selector_timeout() -> float:
    return float(_get_text("logcollector/default_selector_timeout", "1.0") or "1.0")


def get_queue_max_items() -> int:
    return int(_get_text("logcollector/queue_max_items", "100000") or "100000")


def get_queue_max_bytes() -> int:
    return int(_get_text("logcollector/queue_max_bytes", "1073741824") or "1073741824")


def get_host_ip() -> str:

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()

    return str(ip)

def get_os_info():
    return str(f"{platform.system()} {platform.release()}".strip())

def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
