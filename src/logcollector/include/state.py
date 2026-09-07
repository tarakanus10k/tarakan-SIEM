import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

def get_default_state_dir():
    tree = ET.parse("src/logcollector/config/agent.conf")
    root = tree.getroot()

    value = root.findtext("logcollector/default_state_dir")
    
    return Path(value.strip())

def get_file_inode(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_ino
    except OSError:
        return None

def find_rotated_file(
        old_inode: int, 
        hint_dir: str, 
        hint_name: str
        ) -> Optional[str]:

    try:
        entries = os.listdir(hint_dir)
    except OSError:
        return None

    for entry in entries:
        full = os.path.join(hint_dir, entry)

        try:
            st = os.stat(full)
        except OSError:
            continue

        if st.st_ino == old_inode:
            return full

    return None

def get_file_size(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_size
    except OSError:
        return None
