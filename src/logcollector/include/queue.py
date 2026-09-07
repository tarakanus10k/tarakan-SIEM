import xml.etree.ElementTree as ET
from pathlib import Path

def get_default_queue_dir():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logcollector/default_queue_dir")
    
    return Path(value.strip())

def get_queue_max_items():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logcollector/queue_max_items")
    
    return int(value.strip())

def get_queue_max_bytes():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logcollector/queue_max_bytes")
    
    return int(value.strip())