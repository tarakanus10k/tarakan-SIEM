from dataclasses import dataclass

@dataclass
class LOGCONFIG:

    name: str
    path: str

@dataclass
class FILESTATE:

    path: str
    offset: int
    inode: int
    size: int

@dataclass
class LOGDATA:

    name: str
    path: str
    line: str

    hostname: str
    ip_address: str
    agent_id: str
    timestamp: float

@dataclass
class LOGEVENT:

    event_type: str
    path: str
    name: str
    info: str
    extra: str
    timestamp: float

@dataclass
class AGENTCONFIG:

    conf_dir: str = "config/conf.d"