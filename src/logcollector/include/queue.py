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

def get_default_queue_dir() -> Path:
    return _resolve_path(_get_text("logcollector/default_queue_dir", "/var/tsec/queue/logcollector/outbox"))

def get_queue_max_items() -> int:
    return _get_int("logcollector/queue_max_items", 100000)

def get_queue_max_bytes() -> int:
    return _get_int("logcollector/queue_max_bytes", 1073741824)