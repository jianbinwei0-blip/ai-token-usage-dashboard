from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DashboardConfig:
    host: str
    port: int
    dashboard_html: Path
    sessions_root: Path
    claude_projects_root: Path
    pi_agent_root: Path

    @classmethod
    def from_env(cls, repo_root: Path) -> "DashboardConfig":
        return cls(
            host=os.environ.get("AI_USAGE_SERVER_HOST", "127.0.0.1"),
            port=int(os.environ.get("AI_USAGE_SERVER_PORT", "8765")),
            dashboard_html=Path(
                os.environ.get(
                    "AI_USAGE_DASHBOARD_HTML",
                    str(repo_root / "dashboard" / "index.html"),
                )
            ),
            sessions_root=Path(
                os.environ.get(
                    "AI_USAGE_CODEX_SESSIONS_ROOT",
                    str(Path.home() / ".codex" / "sessions"),
                )
            ),
            claude_projects_root=Path(
                os.environ.get(
                    "AI_USAGE_CLAUDE_PROJECTS_ROOT",
                    str(Path.home() / ".claude" / "projects"),
                )
            ),
            pi_agent_root=Path(
                os.environ.get(
                    "AI_USAGE_PI_AGENT_ROOT",
                    str(Path.home() / ".pi" / "agent"),
                )
            ),
        )
