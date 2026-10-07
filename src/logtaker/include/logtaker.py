import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

def get_default_logtaker_port():
    tree = ET.parse("src/logtaker/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logtaker/default_logtaker_port")
    
    return int(value.strip())

def get_host():
    tree = ET.parse("src/logtaker/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logtaker/host")
    
    return str(value.strip())

def get_internal_queue_size():
    tree = ET.parse("src/logtaker/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logtaker/internal_queue_size")
    
    return int(value.strip())

def get_max_request_bytes():
    tree = ET.parse("src/logtaker/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logtaker/max_request_bytes")
    
    return int(value.strip())