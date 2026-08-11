import os
from dataclasses import dataclass
import glob

@dataclass
class LogCollectorConfig:
    """"""

@dataclass
class PathCollectorConfig:
    """"""

@dataclass
class MetadataCollectorConfig:

    log_file_path: str
    source_type: str
    service_name: str

    agent_id: int
    hostname: str
    host_ip: str
    os: str

    timestamp_read: float