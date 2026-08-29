from __future__ import annotations

import json
from pathlib import Path


def race_bucket(names_path: Path) -> str:
    data = json.loads(names_path.read_text(encoding="utf-8"))
    return data["race_bucket"]
