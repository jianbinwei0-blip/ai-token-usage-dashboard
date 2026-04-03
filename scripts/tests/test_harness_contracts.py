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
    <section class=\"panel range-panel\">
      <div class=\"usage-chart-control-group\">
        <label for=\"usageProvider\">Provider</label>
        <select id=\"usageProvider\" class=\"usage-chart-sort\" aria-label=\"Usage provider\">
          <option value=\"combined\">Combined</option>
          <option value=\"codex\">Codex</option>
          <option value=\"claude\">Claude</option>
        </select>
      </div>
    </section>
    <section class=\"stats\">
      <article class=\"stat\"><div class=\"label\">placeholder</div><div class=\"value\">0</div></article>
    </section>
    <section class=\"table-wrap\">
      <table>
        <tbody id=\"dailyUsageTableBody\">
          <tr><td><span class=\"rank\">1</span></td><td>2026-01-01</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num total-col\">0</td></tr>
        </tbody>
      </table>
    </section>
    <section class=\"table-wrap\">
      <table>
        <tbody id=\"usageBreakdownTableBody\">
          <tr><td><span class=\"rank\">1</span></td><td>cli</td><td>model</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num\">0</td><td class=\"num total-col\">0</td></tr>
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
                        "type": "session_meta",
                        "payload": {
                            "id": "codex-session-a",
                            "originator": "codex_cli_rs",
                        },
                    },
                    {
                        "type": "turn_context",
                        "payload": {
                            "model": "gpt-5.2",
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 100,
                                    "cached_input_tokens": 20,
                                    "output_tokens": 20,
                                    "total_tokens": 120,
                                }
                            },
                        },
                    },
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
                            "model": "claude-sonnet-4-6",
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 3,
                                "cache_read_input_tokens": 2,
                                "output_tokens": 5,
                            },
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
                pi_agent_root=root / "pi-agent",
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
            self.assertEqual(
                dataset["providers_available"],
                {"codex": True, "claude": True, "pi": False, "combined": True},
            )
            self.assertEqual(payload_first["input_tokens"], 110)
            self.assertEqual(payload_first["output_tokens"], 25)
            self.assertEqual(payload_first["cached_tokens"], 25)
            self.assertEqual(payload_first["ytd_total_tokens"], 140)
            self.assertAlmostEqual(payload_first["input_cost_usd"], 0.00028)
            self.assertAlmostEqual(payload_first["output_cost_usd"], 0.000375)
            self.assertAlmostEqual(payload_first["cached_cost_usd"], 0.00001685)
            self.assertAlmostEqual(payload_first["total_cost_usd"], 0.00067185)
            self.assertTrue(payload_first["cost_complete"])
            self.assertEqual(payload_first["pricing"]["warning_count"], 0)
            self.assertEqual(payload_first["providers"]["combined"]["input_tokens"], 110)
            self.assertEqual(payload_first["providers"]["combined"]["output_tokens"], 25)
            self.assertEqual(payload_first["providers"]["combined"]["cached_tokens"], 25)
            self.assertAlmostEqual(payload_first["providers"]["combined"]["total_cost_usd"], 0.00067185)

            combined_rows = dataset["providers"]["combined"]["rows"]
            claude_day = datetime.fromisoformat("2026-03-03T05:00:00+00:00").astimezone().date().isoformat()
            self.assertEqual(
                combined_rows,
                [
                    {
                        "date": "2026-03-03",
                        "sessions": 1,
                        "input_tokens": 100,
                        "output_tokens": 20,
                        "cached_tokens": 20,
                        "total_tokens": 120,
                        "input_cost_usd": 0.00025,
                        "output_cost_usd": 0.0003,
                        "cached_cost_usd": 0.000005,
                        "total_cost_usd": 0.000555,
                        "cost_complete": True,
                        "cost_status": "complete",
                        "breakdown_rows": [
                            {
                                "agent_cli": "codex_cli_rs",
                                "model": "gpt-5.2",
                                "sessions": 1,
                                "input_tokens": 100,
                                "output_tokens": 20,
                                "cached_tokens": 20,
                                "total_tokens": 120,
                                "input_cost_usd": 0.00025,
                                "output_cost_usd": 0.0003,
                                "cached_cost_usd": 0.000005,
                                "total_cost_usd": 0.000555,
                                "cost_complete": True,
                                "cost_status": "complete",
                            }
                        ],
                    },
                    {
                        "date": claude_day,
                        "sessions": 1,
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cached_tokens": 5,
                        "total_tokens": 20,
                        "input_cost_usd": 0.00003,
                        "output_cost_usd": 0.000075,
                        "cached_cost_usd": 0.00001185,
                        "total_cost_usd": 0.00011685,
                        "cost_complete": True,
                        "cost_status": "complete",
                        "breakdown_rows": [
                            {
                                "agent_cli": "claude-code",
                                "model": "claude-sonnet-4-6",
                                "sessions": 1,
                                "input_tokens": 10,
                                "output_tokens": 5,
                                "cached_tokens": 5,
                                "total_tokens": 20,
                                "input_cost_usd": 0.00003,
                                "output_cost_usd": 0.000075,
                                "cached_cost_usd": 0.00001185,
                                "total_cost_usd": 0.00011685,
                                "cost_complete": True,
                                "cost_status": "complete",
                            }
                        ],
                    },
                ],
            )
            self.assertEqual(
                dataset["providers"]["combined"]["activity_rows"],
                [
                    {
                        "date": claude_day,
                        "hour": datetime.fromisoformat("2026-03-03T05:00:00+00:00").astimezone().hour,
                        "sessions": 1,
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cached_tokens": 5,
                        "total_tokens": 20,
                        "input_cost_usd": 0.00003,
                        "output_cost_usd": 0.000075,
                        "cached_cost_usd": 0.00001185,
                        "total_cost_usd": 0.00011685,
                        "cost_complete": True,
                        "cost_status": "complete",
                    },
                    {
                        "date": "2026-03-03",
                        "hour": 0,
                        "sessions": 1,
                        "input_tokens": 100,
                        "output_tokens": 20,
                        "cached_tokens": 20,
                        "total_tokens": 120,
                        "input_cost_usd": 0.00025,
                        "output_cost_usd": 0.0003,
                        "cached_cost_usd": 0.000005,
                        "total_cost_usd": 0.000555,
                        "cost_complete": True,
                        "cost_status": "complete",
                    },
                ],
            )

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
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 700,
                                    "cached_input_tokens": 0,
                                    "output_tokens": 77,
                                    "total_tokens": 777,
                                }
                            },
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
                pi_agent_root=root / "pi-agent",
            )
            monday_now = datetime(2026, 3, 2, 18, 0, tzinfo=timezone.utc)

            recalc_dashboard(config, now=monday_now)
            html = dashboard_path.read_text(encoding="utf-8")

            self.assertIn('<div class="label">Current Week</div>', html)
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
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 450,
                                    "cached_input_tokens": 0,
                                    "output_tokens": 50,
                                    "total_tokens": 500,
                                }
                            },
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
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 200,
                                    "cached_input_tokens": 0,
                                    "output_tokens": 50,
                                    "total_tokens": 250,
                                }
                            },
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
                pi_agent_root=root / "pi-agent",
            )
            wednesday_now = datetime(2026, 3, 4, 18, 0, tzinfo=timezone.utc)

            recalc_dashboard(config, now=wednesday_now)
            html = dashboard_path.read_text(encoding="utf-8")

            self.assertIn('<div class="label">Today</div>', html)
            self.assertIn('<div class="label">Current Week</div>', html)
            self.assertIn("Monthly Avg", html)
            self.assertIn("Weekly Avg", html)
            self.assertIn("Daily Avg", html)
            self.assertIn("Monthly Avg Cost", html)
            self.assertIn("Weekly Avg Cost", html)
            self.assertIn("Daily Avg Cost", html)
            self.assertIn("Input Tokens", html)
            self.assertIn("Total Cost", html)
            self.assertLess(html.index('<div class="label">Today</div>'), html.index("Total Tokens"))
            self.assertEqual(html.count('id="usageDataset"'), 1)

    def test_recalc_pipeline_marks_partial_cost_when_model_pricing_is_unmapped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dashboard_path = root / "dashboard" / "index.html"
            dashboard_path.parent.mkdir(parents=True, exist_ok=True)
            dashboard_path.write_text(HTML_TEMPLATE, encoding="utf-8")

            codex_root = root / "codex"
            self._write_jsonl(
                codex_root / "2026" / "03" / "03" / "session-unknown.jsonl",
                [
                    {
                        "type": "turn_context",
                        "payload": {
                            "model": "unknown-model",
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 10,
                                    "cached_input_tokens": 0,
                                    "output_tokens": 5,
                                    "total_tokens": 15,
                                }
                            },
                        },
                    },
                ],
            )

            config = DashboardConfig(
                host="127.0.0.1",
                port=8765,
                dashboard_html=dashboard_path,
                sessions_root=codex_root,
                claude_projects_root=root / "claude",
                pi_agent_root=root / "pi-agent",
            )

            payload = recalc_dashboard(config, now=datetime(2026, 3, 4, 15, 0, tzinfo=timezone.utc))
            dataset = self._read_dataset_from_html(dashboard_path.read_text(encoding="utf-8"))

            self.assertFalse(payload["cost_complete"])
            self.assertEqual(payload["cost_status"], "partial")
            self.assertEqual(payload["pricing"]["warning_count"], 1)
            self.assertEqual(payload["pricing"]["warnings"], [{"provider": "codex", "model": "unknown-model"}])
            self.assertFalse(dataset["providers"]["combined"]["rows"][0]["cost_complete"])
            self.assertEqual(dataset["providers"]["combined"]["rows"][0]["cost_status"], "partial")

    def test_recalc_pipeline_adds_pi_provider_and_filters_provider_selector(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dashboard_path = root / "dashboard" / "index.html"
            dashboard_path.parent.mkdir(parents=True, exist_ok=True)
            dashboard_path.write_text(HTML_TEMPLATE, encoding="utf-8")

            codex_root = root / "codex"
            claude_root = root / "claude"
            pi_agent_root = root / "pi-agent"

            self._write_jsonl(
                codex_root / "2026" / "03" / "03" / "session-a.jsonl",
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 100,
                                    "cached_input_tokens": 17,
                                    "output_tokens": 20,
                                    "total_tokens": 120,
                                }
                            },
                        },
                    }
                ],
            )

            self._write_jsonl(
                pi_agent_root / "sessions" / "--Users-jwei--" / "2026-03-03T21-46-05-286Z_session-a.jsonl",
                [
                    {
                        "type": "session",
                        "id": "pi-session-a",
                        "timestamp": "2026-03-03T21:46:05.286Z",
                        "cwd": "/Users/jwei",
                    },
                    {
                        "type": "model_change",
                        "id": "model-a",
                        "timestamp": "2026-03-03T21:46:10.000Z",
                        "provider": "openai-codex",
                        "modelId": "gpt-5.4",
                    },
                    {
                        "type": "message",
                        "id": "assistant-a1",
                        "timestamp": "2026-03-03T21:46:54.632Z",
                        "message": {
                            "role": "assistant",
                            "usage": {
                                "input": 30,
                                "output": 3,
                                "cacheRead": 0,
                                "cacheWrite": 0,
                                "totalTokens": 33,
                            },
                        },
                    },
                ],
            )

            config = DashboardConfig(
                host="127.0.0.1",
                port=8765,
                dashboard_html=dashboard_path,
                sessions_root=codex_root,
                claude_projects_root=claude_root,
                pi_agent_root=pi_agent_root,
            )
            fixed_now = datetime(2026, 3, 4, 15, 0, tzinfo=timezone.utc)

            recalc_dashboard(config, now=fixed_now)
            html = dashboard_path.read_text(encoding="utf-8")
            dataset = self._read_dataset_from_html(html)

            self.assertEqual(
                dataset["providers_available"],
                {"codex": True, "claude": False, "pi": True, "combined": True},
            )
            self.assertEqual(
                dataset["providers"]["pi"]["rows"],
                [
                    {
                        "date": "2026-03-03",
                        "sessions": 1,
                        "input_tokens": 30,
                        "output_tokens": 3,
                        "cached_tokens": 0,
                        "total_tokens": 33,
                        "input_cost_usd": 0.000075,
                        "output_cost_usd": 0.000045,
                        "cached_cost_usd": 0.0,
                        "total_cost_usd": 0.00012,
                        "cost_complete": True,
                        "cost_status": "complete",
                        "breakdown_rows": [
                            {
                                "agent_cli": "pi",
                                "model": "gpt-5.4",
                                "sessions": 1,
                                "input_tokens": 30,
                                "output_tokens": 3,
                                "cached_tokens": 0,
                                "total_tokens": 33,
                                "input_cost_usd": 0.000075,
                                "output_cost_usd": 0.000045,
                                "cached_cost_usd": 0.0,
                                "total_cost_usd": 0.00012,
                                "cost_complete": True,
                                "cost_status": "complete",
                            }
                        ],
                    }
                ],
            )
            self.assertIn('<option value="combined">Combined</option>', html)
            self.assertIn('<option value="codex">Codex</option>', html)
            self.assertIn('<option value="pi">PI</option>', html)
            self.assertNotIn('<option value="claude">Claude</option>', html)


if __name__ == "__main__":
    unittest.main()
