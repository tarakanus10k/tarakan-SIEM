import socket
import platform
import time
import xml.etree.ElementTree as ET

def get_default_state_flush_interval():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logcollector/default_state_flush_interval")
    
    return float(value.strip())

def get_default_selector_timeout():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logcollector/default_selector_timeout")
    
    return float(value.strip())

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

def get_host_ip():

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()

    return str(ip)

def get_os_info():
    return str(f"{platform.system()} {platform.release()}".strip())

def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())