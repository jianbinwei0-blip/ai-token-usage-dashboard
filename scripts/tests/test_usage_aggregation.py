import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import codex_usage_recalc_server as server


class UsageAggregationTests(unittest.TestCase):
    def _write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

    def test_collect_codex_daily_totals_uses_latest_total_token_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            session_file = root / "2026" / "02" / "27" / "session-a.jsonl"
            self._write_jsonl(
                session_file,
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 100}},
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 250}},
                        },
                    },
                ],
            )

            session_file_b = root / "2026" / "02" / "27" / "session-b.jsonl"
            self._write_jsonl(
                session_file_b,
                [
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {"total_token_usage": {"total_tokens": 75}},
                        },
                    }
                ],
            )

            totals = server._collect_codex_daily_totals(root)
            usage_day = datetime(2026, 2, 27).date()

            self.assertIn(usage_day, totals)
            self.assertEqual(totals[usage_day].sessions, 2)
            self.assertEqual(totals[usage_day].total_tokens, 325)

    def test_collect_claude_daily_totals_dedupes_by_request_and_uses_max_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            project_file = root / "-Users-jwei" / "session-1.jsonl"
            self._write_jsonl(
                project_file,
                [
                    {
                        "requestId": "req-1",
                        "sessionId": "session-1",
                        "timestamp": "2026-02-27T03:00:00Z",
                        "message": {
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 20,
                                "cache_read_input_tokens": 30,
                                "output_tokens": 8,
                            }
                        },
                    },
                    {
                        "requestId": "req-1",
                        "sessionId": "session-1",
                        "timestamp": "2026-02-27T03:00:02Z",
                        "message": {
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 20,
                                "cache_read_input_tokens": 30,
                                "output_tokens": 177,
                            }
                        },
                    },
                    {
                        "requestId": "req-2",
                        "sessionId": "session-2",
                        "timestamp": "2026-02-27T04:00:00Z",
                        "message": {
                            "usage": {
                                "input_tokens": 1,
                                "cache_creation_input_tokens": 2,
                                "cache_read_input_tokens": 3,
                                "output_tokens": 4,
                            }
                        },
                    },
                ],
            )

            totals = server._collect_claude_daily_totals(root)
            expected_day = datetime.fromisoformat("2026-02-27T03:00:00+00:00").astimezone().date()

            self.assertIn(expected_day, totals)
            # req-1 => 10+20+30+177, req-2 => 1+2+3+4
            self.assertEqual(totals[expected_day].total_tokens, 247)
            self.assertEqual(totals[expected_day].sessions, 2)

    def test_collect_claude_daily_totals_uses_local_date_bucketing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            project_file = root / "-Users-jwei" / "session-utc.jsonl"
            ts = "2026-01-02T00:30:00Z"
            self._write_jsonl(
                project_file,
                [
                    {
                        "requestId": "req-local-date",
                        "sessionId": "session-local-date",
                        "timestamp": ts,
                        "message": {
                            "usage": {
                                "input_tokens": 2,
                                "cache_creation_input_tokens": 0,
                                "cache_read_input_tokens": 0,
                                "output_tokens": 3,
                            }
                        },
                    }
                ],
            )

            totals = server._collect_claude_daily_totals(root)
            expected_day = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone().date()

            self.assertIn(expected_day, totals)
            self.assertEqual(totals[expected_day].total_tokens, 5)
            self.assertEqual(totals[expected_day].sessions, 1)

    def test_combine_daily_totals_merges_overlapping_dates(self) -> None:
        day_a = datetime(2026, 2, 27).date()
        day_b = datetime(2026, 2, 26).date()

        codex_daily = {
            day_a: server.DailyTotals(date=day_a, sessions=2, total_tokens=300),
            day_b: server.DailyTotals(date=day_b, sessions=1, total_tokens=50),
        }
        claude_daily = {
            day_a: server.DailyTotals(date=day_a, sessions=3, total_tokens=700),
        }

        combined = server._combine_daily_totals(codex_daily, claude_daily)

        self.assertEqual(combined[day_a].sessions, 5)
        self.assertEqual(combined[day_a].total_tokens, 1000)
        self.assertEqual(combined[day_b].sessions, 1)
        self.assertEqual(combined[day_b].total_tokens, 50)

    def test_collect_claude_daily_totals_missing_root_returns_empty(self) -> None:
        missing = Path("/tmp/definitely-not-a-real-claude-path-for-tests")
        totals = server._collect_claude_daily_totals(missing)
        self.assertEqual(totals, {})


if __name__ == "__main__":
    unittest.main()
