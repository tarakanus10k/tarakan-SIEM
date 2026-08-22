import os
import glob
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from enum import Enum

def get_dir_path():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("agent/default_log_conf")

    return Path(value.strip())

def get_retry_interval():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("agent/default_retry_interval")
    
    return float(value.strip())

def get_scan_interval():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()
    
    value = root.findtext("agent/default_scan_interval")
        
    return float(value.strip())

class LogFormat(str, Enum):

    SYSLOG = "syslog"
    JOURNALD = "journald"
    JSON = "json"

@dataclass
class JournaldFilter:

    field: str
    pattern: str

class MultilineType(str, Enum):

    REGEX = "regex"
    UNIT = "unit"

@dataclass
class Multiline:

    type: Optional[MultilineType] = None
    pattern: Optional[str] = None
    lines: Optional[int] = None

@dataclass
class LocalFileRaw:

    log_format: LogFormat
    location: str
    filter: Optional[JournaldFilter] = None
    multiline: Optional[Multiline] = None
    conf_location: Optional[str] = None

    def dedup_key(self) -> tuple:

        f = (self.filter.field, self.filter.pattern) if self.filter else None

        m = None
        if self.multiline:
            m = (self.multiline.type.value, self.multiline.pattern, self.multiline.lines)

        return (self.log_format.value, self.location, f, m)

@dataclass
class LocalFile:

    log_format: LogFormat
    location: str
    filter: Optional[JournaldFilter] = None
    multiline: Optional[Multiline] = None
    conf_location: Optional[str] = None

# ------------------------------------------------------------------
# parse filter helper
# ------------------------------------------------------------------

def parse_filter(
        localfile_node: ET.Element, 
        log_format: LogFormat, 
        conf_location: str
        ) -> Optional[JournaldFilter]:

    filter_node = localfile_node.find("filter")

    if filter_node is None:
        return None

    if log_format != LogFormat.JOURNALD:
        return None

    field_type_attr = (filter_node.get("field") or "").strip()
    field_type_pattern = (filter_node.text or "").strip()

    if not field_type_attr or not field_type_pattern:
        return None

    return JournaldFilter(field=field_type_attr, pattern=field_type_pattern)

# ------------------------------------------------------------------
# parse multiline helper
# ------------------------------------------------------------------

def parse_multiline(
        localfile_node: ET.Element, 
        log_format: LogFormat, 
        conf_location: str
        ) -> Optional[Multiline]:

    multiline_node = localfile_node.find("multiline")

    if multiline_node is None:
        return None

    if log_format == LogFormat.JSON:
        return None

    multiline_type_raw = (multiline_node.get("type") or "").strip()
    multiline_type_value = (multiline_node.text or "").strip()

    try:
        multiline_type = MultilineType(multiline_type_raw)
    except ValueError:
        return None

    if multiline_type == MultilineType.REGEX:
        if not multiline_type_value:
            return None

        return Multiline(type=multiline_type, pattern=multiline_type_value)

    try:
        n = int(multiline_type_value)
    except ValueError:
        return None

    if n < 1:
        return None

    return Multiline(type=multiline_type, lines=n)

# ------------------------------------------------------------------
# extraction helper
# ------------------------------------------------------------------

def is_text(localfile_node: Optional[ET.Element]) -> Optional[str]:

    if localfile_node is not None and localfile_node.text:
        return localfile_node.text
    
    return None

# ------------------------------------------------------------------
# open glob mask helper
# ------------------------------------------------------------------

def open_glob_mask(location: str) -> list[str]:

    if any(ch in location for ch in "*[?"):
        return sorted(glob.glob(location))

    return [location]

# ------------------------------------------------------------------
# unique valid source key
# ------------------------------------------------------------------

def source_key_from_obj(s: LocalFile) -> tuple:

    f = (s.filter.field, s.filter.pattern) if s.filter else None

    m = None
    if s.multiline:
        m = (s.multiline.type.value, s.multiline.pattern, s.multiline.lines)

    return (s.log_format.value, s.location, f, m)