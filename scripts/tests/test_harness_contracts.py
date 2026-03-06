import json
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.config import DashboardConfig
from dashboard_core.pipeline import recalc_dashboard


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
<head><meta charset=\"UTF-8\" /><title>Fixture</title></head>
<body>
  <main>
    <section class=\"stats\">
      <article class=\"stat\"><div class=\"label\">placeholder</div><div class=\"value\">0</div></article>
    </section>
    <section class=\"table-wrap\">
      <table>
        <tbody>
          <tr><td><span class=\"rank\">1</span></td><td>2026-01-01</td><td class=\"num\">0</td><td class=\"num total-col\">0</td></tr>
        </tbody>
      </table>
    </section>
  </main>
  <script id=\"usageDataset\" type=\"application/json\">{}</script>
</body>
</html>
"""


class HarnessContractsTests(unittest.TestCase):
    def _write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

    def _read_dataset_from_html(self, html: str) -> dict:
        match = re.search(
            r'<script id="usageDataset" type="application/json">(.*?)</script>',
            html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        return json.loads(match.group(1))

    def test_recalc_pipeline_is_deterministic_with_fixed_clock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dashboard_path = root / "dashboard" / "index.html"
            dashboard_path.parent.mkdir(parents=True, exist_ok=True)
            dashboard_path.write_text(HTML_TEMPLATE, encoding="utf-8")

            codex_root = root / "codex"
            claude_root = root / "claude"

            self._write_jsonl(
                codex_root / "2026" / "03" / "03" / "session-a.jsonl",
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 120}},
                        },
                    }
                ],
            )

            self._write_jsonl(
                claude_root / "project-a" / "session.jsonl",
                [
                    {
                        "requestId": "r-1",
                        "sessionId": "s-1",
                        "timestamp": "2026-03-03T05:00:00Z",
                        "message": {
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 3,
                                "cache_read_input_tokens": 2,
                                "output_tokens": 5,
                            }
                        },
                    }
                ],
            )

            config = DashboardConfig(
                host="127.0.0.1",
                port=8765,
                dashboard_html=dashboard_path,
                sessions_root=codex_root,
                claude_projects_root=claude_root,
            )
            fixed_now = datetime(2026, 3, 4, 15, 0, tzinfo=timezone.utc)

            payload_first = recalc_dashboard(config, now=fixed_now)
            html_first = dashboard_path.read_text(encoding="utf-8")

            payload_second = recalc_dashboard(config, now=fixed_now)
            html_second = dashboard_path.read_text(encoding="utf-8")

            self.assertEqual(payload_first, payload_second)
            self.assertEqual(html_first, html_second)
            self.assertEqual(html_first.count('id="usageDataset"'), 1)

            dataset = self._read_dataset_from_html(html_first)
            self.assertEqual(dataset["generated_at"], fixed_now.isoformat())
            self.assertEqual(dataset["providers_available"], {"codex": True, "claude": True, "combined": True})

    def test_recalc_pipeline_clamps_current_week_end_on_monday(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dashboard_path = root / "dashboard" / "index.html"
            dashboard_path.parent.mkdir(parents=True, exist_ok=True)
            dashboard_path.write_text(HTML_TEMPLATE, encoding="utf-8")

            codex_root = root / "codex"
            claude_root = root / "claude"

            self._write_jsonl(
                codex_root / "2026" / "03" / "02" / "session-monday.jsonl",
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 777}},
                        },
                    }
                ],
            )

            config = DashboardConfig(
                host="127.0.0.1",
                port=8765,
                dashboard_html=dashboard_path,
                sessions_root=codex_root,
                claude_projects_root=claude_root,
            )
            monday_now = datetime(2026, 3, 2, 18, 0, tzinfo=timezone.utc)

            recalc_dashboard(config, now=monday_now)
            html = dashboard_path.read_text(encoding="utf-8")

            self.assertIn("Current Week (2026-03-02 to 2026-03-02", html)
            self.assertEqual(html.count('id="usageDataset"'), 1)

    def test_recalc_pipeline_includes_today_in_current_week_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dashboard_path = root / "dashboard" / "index.html"
            dashboard_path.parent.mkdir(parents=True, exist_ok=True)
            dashboard_path.write_text(HTML_TEMPLATE, encoding="utf-8")

            codex_root = root / "codex"
            claude_root = root / "claude"

            self._write_jsonl(
                codex_root / "2026" / "03" / "03" / "session-tuesday.jsonl",
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 500}},
                        },
                    }
                ],
            )
            self._write_jsonl(
                codex_root / "2026" / "03" / "04" / "session-wednesday.jsonl",
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 250}},
                        },
                    }
                ],
            )

            config = DashboardConfig(
                host="127.0.0.1",
                port=8765,
                dashboard_html=dashboard_path,
                sessions_root=codex_root,
                claude_projects_root=claude_root,
            )
            wednesday_now = datetime(2026, 3, 4, 18, 0, tzinfo=timezone.utc)

            recalc_dashboard(config, now=wednesday_now)
            html = dashboard_path.read_text(encoding="utf-8")

            self.assertIn("Today (2026-03-04, 1 sessions)", html)
            self.assertIn("Current Week (2026-03-02 to 2026-03-04, 2 sessions)", html)
            self.assertEqual(html.count('id="usageDataset"'), 1)


if __name__ == "__main__":
    unittest.main()
