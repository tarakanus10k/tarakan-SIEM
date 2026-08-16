from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import xml.etree.ElementTree as ET

from ..include.config import parse_filter, parse_multiline
from ..include.config import get_dir_path, get_retry_interval
from ..include.config import LogFormat, JournaldFilter, MultilineType, Multiline, LocalFile

@dataclass
class AgentConfig:

    conf_dir_path: Path = get_dir_path()
    conf_retry_interval: float = get_retry_interval()

class AgentLogConfig:

    def __init__(
              self, 
              conf_dir_path: Path = AgentConfig.conf_dir_path,
              conf_retry_interval: float = AgentConfig.conf_retry_interval
              ) -> None:
        
        self.conf_dir_path = Path(conf_dir_path)
        self.conf_retry_interval = float(conf_retry_interval)

    def load_conf(self) -> list[LocalFile]:

        localfiles: list[LocalFile] = []

        for conf_file in sorted(self.conf_dir_path.glob("*.conf")):
            tree = ET.parse(conf_file)
            root = tree.getroot()

            for localfile_node in root.findall("localfile"):

                log_format_node = localfile_node.find("log_format")
                if log_format_node is None:
                    return None

                log_format = (log_format_node.text).strip()

                location_node = localfile_node.find("location")
                if location_node is None:
                    return None

                location = (location_node.text).strip()

                journal_filter = parse_filter(localfile_node, log_format)
                multiline = parse_multiline(localfile_node, log_format)

                localfiles.append(
                    LocalFile(
                        log_format=log_format,
                        location=location,
                        filter=journal_filter,
                        multiline=multiline
                    )
                )

        return localfiles

    def validate_localfiles(self, lf: LocalFile) -> None:
        """"""