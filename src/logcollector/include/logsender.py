import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

def get_default_sender_spool_dir():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/default_state_dir")
    
    return Path(value.strip())

def get_default_backoff_base():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/default_backoff_base")
    
    return float(value.strip())

def get_default_backoff_max():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/default_backoff_max")
    
    return float(value.strip())

def get_default_backoff_factor():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/default_backoff_factor")
    
    return float(value.strip())

def get_target_url():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/target_url")
    
    return str(value.strip())

def get_batch_size():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/batch_size")
    
    return int(value.strip())

def get_batch_timeout():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/batch_timeout")
    
    return float(value.strip())

def get_max_retries():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/max_retries")
    
    return int(value.strip())

def get_http_timeout():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logsender/http_timeout")
    
    return float(value.strip())

def utc_now_iso() -> str:
    import time as _t
    return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())