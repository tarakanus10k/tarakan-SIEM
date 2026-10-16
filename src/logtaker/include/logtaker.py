import xml.etree.ElementTree as ET
from pathlib import Path
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)

DEFAULT_DEDUP_TTL = 300.0
DEFAULT_DEDUP_MAX_ENTRIES = 100_000

DEFAULT_SINK_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_SINK_MAX_FILES = 5

DEFAULT_SINK_ROTATE_INTERVAL = 0.0

DEFAULT_TAIL_SIZE = 1000
MAX_TAIL_SIZE = 100_000

DEDUP_PERSIST_OFF_VALUES = frozenset(("off", "none", "false", "0"))

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

def get_default_logtaker_port() -> int:
    return _get_int("logtaker/default_logtaker_port", 8443)

def get_host() -> str:
    return _get_text("logtaker/host", "0.0.0.0")

def get_internal_queue_size() -> int:
    return _get_int("logtaker/internal_queue_size", 10000)

def get_max_request_bytes() -> int:
    return _get_int("logtaker/max_request_bytes", 67108864)

def get_server_cert() -> str:
    return str(_resolve_path(_get_text("logtaker/server_cert", "")))

def get_server_key() -> str:
    return str(_resolve_path(_get_text("logtaker/server_key", "")))

def get_ca_cert() -> str:
    return str(_resolve_path(_get_text("logtaker/ca_cert", "")))


def get_require_client_cert() -> bool:
    return _get_text("logtaker/require_client_cert", "true").lower() in (
        "true", "1", "yes", "on"
    )

def get_dedup_ttl() -> float:
    return _get_float("logtaker/dedup_ttl", DEFAULT_DEDUP_TTL)

def get_dedup_max_entries() -> int:
    return _get_int("logtaker/dedup_max_entries", DEFAULT_DEDUP_MAX_ENTRIES)

def get_dedup_persist() -> str:
    return _get_text("logtaker/dedup_persist", "")

def get_sink_max_bytes() -> int:
    return _get_int("logtaker/sink_max_bytes", DEFAULT_SINK_MAX_BYTES)

def get_sink_max_files() -> int:
    return _get_int("logtaker/sink_max_files", DEFAULT_SINK_MAX_FILES)

def get_sink_rotate_interval() -> float:
    return _get_float("logtaker/sink_rotate_interval", DEFAULT_SINK_ROTATE_INTERVAL)

def get_tail_size() -> int:
    raw = _get_int("logtaker/tail_size", DEFAULT_TAIL_SIZE)

    return max(0, min(MAX_TAIL_SIZE, raw))
