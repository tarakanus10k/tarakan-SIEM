import os
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


def get_default_sender_spool_dir() -> Path:
    return Path(_get_text("logsender/default_sender_spool_dir",
                          "/var/tsec/queue/logsender"))


def get_default_backoff_base() -> float:
    return float(_get_text("logsender/default_backoff_base", "1.0") or "1.0")


def get_default_backoff_max() -> float:
    return float(_get_text("logsender/default_backoff_max", "300.0") or "300.0")


def get_default_backoff_factor() -> float:
    return float(_get_text("logsender/default_backoff_factor", "2.0") or "2.0")


def get_target_url() -> str:
    return str(_get_text("logsender/target_url", ""))


def get_batch_size() -> int:
    return int(_get_text("logsender/batch_size", "100") or "100")


def get_batch_timeout() -> float:
    return float(_get_text("logsender/batch_timeout", "5.0") or "5.0")


def get_max_retries() -> int:
    return int(_get_text("logsender/max_retries", "0") or "0")


def get_http_timeout() -> float:
    return float(_get_text("logsender/http_timeout", "30.0") or "30.0")


def get_client_cert() -> str:
    return _get_text("logsender/client_cert", "")


def get_client_key() -> str:
    return _get_text("logsender/client_key", "")


def get_ca_cert() -> str:
    return _get_text("logsender/ca_cert", "")


def get_dead_letter_dir() -> str:
    return _get_text("logsender/dead_letter_dir",
                     "/var/tsec/queue/logsender/dead_letter")


def utc_now_iso() -> str:
    import time as _t
    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())
