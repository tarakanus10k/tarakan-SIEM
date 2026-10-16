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
def _conf_root() -> "ET.Element | None":
    try:
        tree = ET.parse(_agent_conf_path())
        return tree.getroot()
    except (OSError, ET.ParseError) as e:
        logger.error("не удалось прочитать agent.conf: %s", e)
        return None

def _get_text(path: str, default: str = "") -> str:
    root = _conf_root()
    if root is None:
        return default

    try:
        v = root.findtext(path)
    except (OSError, ET.ParseError) as e:
        logger.error("не удалось прочитать agent.conf: %s", e)
        return default

    return (v.strip() if v else default)

def _get_int(path: str, default: int) -> int:
    raw = _get_text(path, "")
    if not raw:
        return default

    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("неверное целое в agent.conf %s=%r — использую default %s",
                       path, raw, default)
        return default

def _get_float(path: str, default: float) -> float:
    raw = _get_text(path, "")
    if not raw:
        return default

    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("неверное число в agent.conf %s=%r — использую default %s",
                       path, raw, default)
        return default

def get_default_state_flush_interval() -> float:
    return _get_float("logcollector/default_state_flush_interval", 5.0)

def get_default_selector_timeout() -> float:
    return _get_float("logcollector/default_selector_timeout", 1.0)

def get_queue_max_items() -> int:
    return _get_int("logcollector/queue_max_items", 100000)

def get_queue_max_bytes() -> int:
    return _get_int("logcollector/queue_max_bytes", 1073741824)

def get_host_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.settimeout(1.0)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        s.close()

    return str(ip)

def get_os_info():
    return str(f"{platform.system()} {platform.release()}".strip())

def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
