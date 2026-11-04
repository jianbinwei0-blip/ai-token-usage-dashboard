from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .settings import SharedSettings, load_settings


@dataclass(frozen=True)
class DashboardConfig:
    host: str
    port: int
    dashboard_html: Path
    sessions_root: Path
    claude_projects_root: Path
    pi_agent_root: Path
    dsh_home: Path
    pricing_file: Path | None = None
    parse_cache_file: Path | None = None
    recalc_log_file: Path | None = None
    runtime_dir: Path | None = None
    dashboard_url: str = "http://127.0.0.1:8765"
    settings_file: Path | None = None
    legacy_parse_cache_file: Path | None = None

    @classmethod
    def from_env(cls, repo_root: Path, *, settings: SharedSettings | None = None) -> "DashboardConfig":
        settings = settings if settings is not None else load_settings(repo_root)
        runtime = settings.runtime_dir
        parse_cache_file_value = os.environ.get("AI_USAGE_PARSE_CACHE_FILE")
        recalc_log_file_value = os.environ.get("AI_USAGE_RECALC_LOG_FILE")
        return cls(
            host=settings.server_host,
            port=settings.server_port,
            dashboard_html=Path(os.environ.get("AI_USAGE_DASHBOARD_HTML", str(runtime / "tmp" / "index.runtime.html"))).expanduser(),
            sessions_root=settings.codex_sessions_root,
            claude_projects_root=settings.claude_projects_root,
            pi_agent_root=settings.pi_agent_root,
            dsh_home=settings.dsh_home,
            pricing_file=settings.pricing_file,
            parse_cache_file=(Path(parse_cache_file_value).expanduser() if parse_cache_file_value and "AI_USAGE_HISTORY_FILE" not in os.environ else settings.history_file),
            legacy_parse_cache_file=(runtime / "tmp" / "recalc_parse_cache.json") if not parse_cache_file_value else None,
            recalc_log_file=Path(recalc_log_file_value).expanduser() if recalc_log_file_value else runtime / "tmp" / "recalc_timings.jsonl",
            runtime_dir=runtime,
            dashboard_url=settings.dashboard_url,
            settings_file=settings.settings_file,
        )
