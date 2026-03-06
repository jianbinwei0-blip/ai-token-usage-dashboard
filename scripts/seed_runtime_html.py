#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dashboard_core.runtime_html import seed_runtime_html


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: seed_runtime_html.py <source_html> <runtime_html>")
    seed_runtime_html(Path(sys.argv[1]), Path(sys.argv[2]))


if __name__ == "__main__":
    main()
