# FILE FOR GET DATA FROM CONFIG FILES

import os
import yaml
import glob
from dataclasses import dataclass
# from dotenv import load_dotenv

# load_dotenv()

@dataclass
class LOGCONFIG:

    name: str
    path: str

def load_config(conf_dir: str = "conf.d") -> list[LOGCONFIG]:
    log_configs = []
    for file_path in glob.glob(os.path.join(conf_dir, "*.y*ml")):
        with open(file_path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            for log_def in data["logs"]:
                log_configs.append(LOGCONFIG(
                    name=log_def.get("name", os.path.basename(file_path)),
                    path=log_def["path"]
                ))

        return log_configs