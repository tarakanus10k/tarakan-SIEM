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


def get_default_logtaker_port() -> int:
    return int(_get_text("logtaker/default_logtaker_port", "8443") or "8443")


def get_host() -> str:
    return _get_text("logtaker/host", "0.0.0.0")


def get_internal_queue_size() -> int:
    return int(_get_text("logtaker/internal_queue_size", "10000") or "10000")


def get_max_request_bytes() -> int:
    return int(_get_text("logtaker/max_request_bytes", "67108864") or "67108864")
