from pathlib import Path
from typing import Any, Dict

import yaml


def load_config(config_path: str = "configs/dev.yaml") -> Dict[str, Any]:
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with path.open("r") as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError(f"Config file is empty: {config_path}")

    return config