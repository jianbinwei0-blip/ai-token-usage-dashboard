from __future__ import annotations

import shutil
from pathlib import Path


def seed_runtime_html(source_html: Path, runtime_html: Path) -> None:
    runtime_html.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_html, runtime_html)
