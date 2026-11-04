#!/usr/bin/env python3
"""Seed disposable HTML using the same saved settings as every other surface."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from dashboard_core.config import DashboardConfig
from dashboard_core.runtime_html import seed_runtime_html


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    config = DashboardConfig.from_env(repo)
    if not os.environ.get("AI_USAGE_DASHBOARD_HTML"):
        seed_runtime_html((config.runtime_dir or repo) / "dashboard/index.html", config.dashboard_html)
    os.execv(sys.executable, [sys.executable, str(repo / "scripts/ai_usage_recalc_server.py")])


if __name__ == "__main__":
    main()
