import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.config import DashboardConfig


class DashboardConfigEnvTests(unittest.TestCase):
    def test_reads_ai_usage_env_names(self) -> None:
        repo_root = Path("/tmp/repo")
        with patch.dict(
            os.environ,
            {
                "AI_USAGE_SERVER_HOST": "0.0.0.0",
                "AI_USAGE_SERVER_PORT": "9001",
                "AI_USAGE_DASHBOARD_HTML": "/tmp/ai-dashboard/index.html",
                "AI_USAGE_CODEX_SESSIONS_ROOT": "/tmp/ai-dashboard/codex",
                "AI_USAGE_CLAUDE_PROJECTS_ROOT": "/tmp/ai-dashboard/claude",
                "AI_USAGE_PI_AGENT_ROOT": "/tmp/ai-dashboard/pi-agent",
                "AI_USAGE_PRICING_FILE": "/tmp/ai-dashboard/pricing.json",
                "AI_USAGE_RECALC_LOG_FILE": "/tmp/ai-dashboard/recalc.jsonl",
            },
            clear=True,
        ):
            cfg = DashboardConfig.from_env(repo_root)

        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertEqual(cfg.port, 9001)
        self.assertEqual(cfg.dashboard_html, Path("/tmp/ai-dashboard/index.html"))
        self.assertEqual(cfg.sessions_root, Path("/tmp/ai-dashboard/codex"))
        self.assertEqual(cfg.claude_projects_root, Path("/tmp/ai-dashboard/claude"))
        self.assertEqual(cfg.pi_agent_root, Path("/tmp/ai-dashboard/pi-agent"))
        self.assertEqual(cfg.pricing_file, Path("/tmp/ai-dashboard/pricing.json"))
        self.assertEqual(cfg.recalc_log_file, Path("/tmp/ai-dashboard/recalc.jsonl"))

    def test_uses_defaults_when_env_is_unset(self) -> None:
        repo_root = Path("/tmp/repo")
        with patch.dict(os.environ, {}, clear=True):
            cfg = DashboardConfig.from_env(repo_root)

        self.assertEqual(cfg.host, "127.0.0.1")
        self.assertEqual(cfg.port, 8765)
        self.assertEqual(cfg.dashboard_html, repo_root / "dashboard" / "index.html")
        self.assertEqual(cfg.sessions_root, Path.home() / ".codex" / "sessions")
        self.assertEqual(cfg.claude_projects_root, Path.home() / ".claude" / "projects")
        self.assertEqual(cfg.pi_agent_root, Path.home() / ".pi" / "agent")
        self.assertIsNone(cfg.pricing_file)
        self.assertEqual(cfg.recalc_log_file, repo_root / "tmp" / "recalc_timings.jsonl")


if __name__ == "__main__":
    unittest.main()
