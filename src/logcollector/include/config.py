import os
import glob
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

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

def _is_glob(location: str) -> bool:
    return any(char in location for char in "*?")

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

    def dedup_key(self) -> tuple:

        f = (self.filter.field, self.filter.pattern) if self.filter else None

        m = None
        if self.multiline:
            m = (self.multiline.type.value, self.multiline.pattern, self.multiline.lines)

        return (self.log_format.value, self.location, f, m)

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

def delete_dupl(unvalid_conf: list[LocalFile]) -> list[LocalFile]:

    unique_item_list: list[LocalFile] = []
    unique_item_key: set[tuple] = set()

    for list_item in unvalid_conf:
        key = list_item.dedup_key()

        if key in unique_item_key:
            continue

        unique_item_key.add(key)
        unique_item_list.append(list_item)

    return unique_item_list

def open_glob_mask(unique_item_list: list[LocalFile]) -> list[LocalFile]:

    list_with_glob: list[LocalFile] = []

    for list_item in unique_item_list:

        list_item.location = os.path.expanduser(list_item.location)
        list_item.location = os.path.expandvars(list_item.location)

        if not _is_glob(list_item.location):
            list_with_glob.append(list_item)
            continue

        matches = glob.glob(list_item.location, recursive=True)

        for m in sorted(matches):
            m_path = Path(m)

            if m_path.is_file():
                list_with_glob.append(
                    LocalFile(
                        log_format=list_item.log_format,
                        location=m_path,
                        filter=list_item.filter,
                        multiline=list_item.multiline
                        )
                    )

    return list_with_glob
    
def validate_journald_log(
        unique_item_list: list[LocalFile], 
        valid_conf: list[LocalFile]
        ) -> list[LocalFile]:

    for list_item in unique_item_list:

        if list_item.log_format == LogFormat.JOURNALD:
            if list_item.location is None:
                continue

            if hasattr(os, "geteuid") and os.geteuid():
                valid_conf.append(list_item)

            try:
                from systemd import journal
            except ImportError:
                pass

            try:
                with journal.Reader() as reader:
                    reader.seek_head()
                valid_conf.append(list_item)
            except Exception:
                pass

    return valid_conf

def validate_syslog_json_log(
        unique_item_list: list[LocalFile], 
        valid_conf: list[LocalFile]
        ) -> list[LocalFile]:

    for list_item in unique_item_list:

        if list_item.log_format == LogFormat.SYSLOG or list_item.log_format == LogFormat.JSON:
            if list_item.location is None:
                continue

            if hasattr(os, "geteuid") and os.geteuid():
                valid_conf.append(list_item)

            location = Path(list_item.location)

            if not os.path.exists(location):
                continue

            if os.access(location, os.R_OK):
                valid_conf.append(list_item)

    return valid_conf