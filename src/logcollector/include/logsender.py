import xml.etree.ElementTree as ET
from pathlib import Path
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]

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
        logger.warning("неверное целое в agent.conf %s=%r — использую default %s", path, raw, default)
        return default

def _get_float(path: str, default: float) -> float:
    raw = _get_text(path, "")
    if not raw:
        return default

    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("неверное число в agent.conf %s=%r — использую default %s", path, raw, default)
        return default

def _resolve_path(raw: str) -> Path:
    p = Path(raw)

    if p.is_absolute():
        return p

    cwd_candidate = Path.cwd() / p
    if cwd_candidate.exists():
        return cwd_candidate

    repo_candidate = _REPO_ROOT / p
    if repo_candidate.exists():
        return repo_candidate

    return cwd_candidate

def get_default_sender_spool_dir() -> Path:
    return _resolve_path(_get_text("logsender/default_sender_spool_dir", "/var/tsec/queue/logsender"))

def get_default_backoff_base() -> float:
    return _get_float("logsender/default_backoff_base", 1.0)

def get_default_backoff_max() -> float:
    return _get_float("logsender/default_backoff_max", 300.0)

def get_default_backoff_factor() -> float:
    return _get_float("logsender/default_backoff_factor", 2.0)

def get_target_url() -> str:
    return str(_get_text("logsender/target_url", ""))

def get_batch_size() -> int:
    return _get_int("logsender/batch_size", 100)

def get_batch_timeout() -> float:
    return _get_float("logsender/batch_timeout", 5.0)

def get_max_retries() -> int:
    return _get_int("logsender/max_retries", 0)

def get_http_timeout() -> float:
    return _get_float("logsender/http_timeout", 30.0)

def get_client_cert() -> str:
    return _get_text("logsender/client_cert", "")

def get_client_key() -> str:
    return _get_text("logsender/client_key", "")

def get_ca_cert() -> str:
    return _get_text("logsender/ca_cert", "")

def get_dead_letter_dir() -> str:
    return str(_resolve_path(_get_text("logsender/dead_letter_dir", "/var/tsec/queue/logsender/dead_letter")))

def utc_now_iso() -> str:
    import time as _t
    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())