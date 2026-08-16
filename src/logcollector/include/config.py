from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import xml.etree.ElementTree as ET

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

@dataclass
class LogFormat:

    SYSLOG: str = "syslog"
    JOURNALD: str = "journald"
    JSON: str = "json"

@dataclass
class JournaldFilter:

    field: str
    pattern: str

@dataclass
class MultilineType:

    REGEX: str = "regex"
    UNIT: str = "unit"

@dataclass
class Multiline:

    type: Optional[MultilineType] = None
    pattern: Optional[str] = None
    lines: Optional[int] = None

@dataclass
class LocalFile:

    log_format: LogFormat
    location: str
    filter: Optional[JournaldFilter] = None
    multiline: Optional[Multiline] = None

def parse_filter(localfile_node: ET.Element, log_format: LogFormat) -> Optional[JournaldFilter]:

    filter_node = localfile_node.find("filter")
    
    if filter_node is None:
        return None
    
    if log_format != LogFormat.JOURNALD:
        return None
    
    filter_field_attr = (filter_node.get("field")).strip()
    filter_field_value = (filter_node.text).strip()
    
    if not filter_field_attr or not filter_field_value:
        return None
    
    return JournaldFilter(field=filter_field_attr, pattern=filter_field_value)

def parse_multiline(localfile_node: ET.Element, log_format: LogFormat) -> Optional[Multiline]:

    multiline_node = localfile_node.find("multiline")
    
    if multiline_node is None:
        return None
    
    if log_format == LogFormat.JSON:
        return None
    
    multiline_type_attr = (multiline_node.get("type")).strip()
    multiline_type_value = (multiline_node.text).strip()
    
    if multiline_type_attr == MultilineType.REGEX:
        if not multiline_type_value:
            return None
        return Multiline(type=multiline_type_attr, pattern=multiline_type_value)

    try:
        n = int(multiline_type_value)
    except ValueError:
        return None
    
    if multiline_type_attr == MultilineType.UNIT:
        if n < 1:
            return None
        return Multiline(type=multiline_type_attr, lines=n)