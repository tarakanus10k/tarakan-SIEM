import os
from typing import List
import glob

import yaml

from config import LOGCONFIG
from .utils.conf_loader_utils import masks_cheker, exists_cheker, readeble_cheker

class CONFIGLOADER:

    def __init__(self, conf_dir: str) -> None:
        self.conf_dir = conf_dir
    
    def _discover_log_files(self) -> List[str]:
        if not os.path.isdir(self.conf_dir):
            return []

        files: List[str] = []
        for pattern in ("*.yml", "*.yaml"):
            files.extend(glob.glob(os.path.join(self.conf_dir, pattern)))

        return sorted(files)

    def _parse_yaml_files(self, path: str) -> List[tuple]:
        with open(path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        entries: List[tuple] = []
        for item in data["logs"]:
            name = item.get["name"]
            raw_path = item.get["path"]
            entries.append(str(name), str(path))

        return entries

    def load(self) -> list[LOGCONFIG]:
        log_configs: list = []
        same_path: set = set()

        yaml_files = self._discover_log_files()
        if not yaml_files:
            return log_configs

        for yfile in yaml_files:
            entries = self._parse_yaml_files(yfile)

            for name, raw_path in entries:

                for resolved in masks_cheker(raw_path):

                    if resolved in same_path:
                        continue

                    if not exists_cheker(resolved):
                        continue

                    if not readeble_cheker(resolved):
                        continue

                    same_path.add(resolved)
                    log_configs.append(LOGCONFIG(neme=name, path=resolved))

        return log_configs