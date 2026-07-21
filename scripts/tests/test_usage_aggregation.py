import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ai_usage_recalc_server as server
from dashboard_core.collectors import (
    collect_claude_usage_data,
    collect_codex_usage_data,
    collect_pi_usage_data,
    load_persistent_parse_caches,
    save_persistent_parse_caches,
)


class UsageAggregationTests(unittest.TestCase):
    def _write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

    def test_collect_codex_daily_totals_uses_latest_total_snapshot_and_extracts_cli_model_breakdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            session_file = root / "2026" / "02" / "27" / "session-a.jsonl"
            self._write_jsonl(
                session_file,
                [
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "codex-session-a",
                            "originator": "codex_cli_rs",
                            "source": "cli",
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
                                    "input_tokens": 60,
                                    "cached_input_tokens": 10,
                                    "output_tokens": 40,
                                    "total_tokens": 100,
                                }
                            },
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 150,
                                    "cached_input_tokens": 25,
                                    "output_tokens": 100,
                                    "total_tokens": 250,
                                }
                            },
                        },
                    },
                ],
            )

            session_file_b = root / "2026" / "02" / "27" / "session-b.jsonl"
            self._write_jsonl(
                session_file_b,
                [
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "codex-session-b",
                            "source": "cli",
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 50,
                                    "cached_input_tokens": 5,
                                    "output_tokens": 25,
                                    "total_tokens": 75,
                                }
                            },
                        },
                    },
                ],
            )

            totals = server._collect_codex_daily_totals(root)
            usage_day = datetime(2026, 2, 27).date()

            self.assertIn(usage_day, totals)
            self.assertEqual(totals[usage_day].sessions, 2)
            self.assertEqual(totals[usage_day].input_tokens, 200)
            self.assertEqual(totals[usage_day].output_tokens, 125)
            self.assertEqual(totals[usage_day].cached_tokens, 30)
            self.assertEqual(totals[usage_day].total_tokens, 325)
            self.assertFalse(totals[usage_day].cost_complete)
            self.assertAlmostEqual(totals[usage_day].input_cost_usd, 0.000375)
            self.assertAlmostEqual(totals[usage_day].output_cost_usd, 0.0015)
            self.assertAlmostEqual(totals[usage_day].cached_cost_usd, 0.00000625)
            self.assertAlmostEqual(totals[usage_day].total_cost_usd, 0.00188125)

            breakdown = totals[usage_day].breakdowns[("codex_cli_rs", "gpt-5.2")]
            self.assertEqual(breakdown.sessions, 1)
            self.assertEqual(breakdown.input_tokens, 150)
            self.assertEqual(breakdown.cached_tokens, 25)
            self.assertEqual(breakdown.output_tokens, 100)
            self.assertEqual(breakdown.total_tokens, 250)
            self.assertAlmostEqual(breakdown.total_cost_usd, 0.00188125)

            fallback_breakdown = totals[usage_day].breakdowns[("cli", "unknown")]
            self.assertEqual(fallback_breakdown.sessions, 1)
            self.assertEqual(fallback_breakdown.total_tokens, 75)
            self.assertAlmostEqual(fallback_breakdown.total_cost_usd, 0.0)
            self.assertFalse(fallback_breakdown.cost_complete)

    def test_collect_codex_usage_data_tracks_activity_hour_from_session_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            session_file = root / "2026" / "02" / "28" / "session-hour.jsonl"
            self._write_jsonl(
                session_file,
                [
                    {
                        "timestamp": "2026-02-28T06:45:00Z",
                        "type": "session_meta",
                        "payload": {
                            "id": "codex-session-hour",
                            "timestamp": "2026-02-28T06:45:00Z",
                            "originator": "codex_cli_rs",
                        },
                    },
                    {
                        "timestamp": "2026-02-28T06:45:02Z",
                        "type": "turn_context",
                        "payload": {
                            "model": "gpt-5.2",
                        },
                    },
                    {
                        "timestamp": "2026-02-28T06:50:00Z",
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 12,
                                    "cached_input_tokens": 4,
                                    "output_tokens": 6,
                                    "total_tokens": 22,
                                }
                            },
                        },
                    },
                ],
            )

            totals, activity_totals = collect_codex_usage_data(root)
            local_dt = datetime.fromisoformat("2026-02-28T06:45:00+00:00").astimezone()
            usage_day = local_dt.date()
            self.assertIn(usage_day, totals)
            self.assertIn((usage_day, local_dt.hour), activity_totals)
            self.assertEqual(activity_totals[(usage_day, local_dt.hour)].sessions, 1)
            self.assertEqual(activity_totals[(usage_day, local_dt.hour)].total_tokens, 22)

    def test_collect_claude_daily_totals_dedupes_by_request_and_keeps_model_cached_breakdown(self) -> None:
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
                            "model": "claude-sonnet-4-6",
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 20,
                                "cache_read_input_tokens": 30,
                                "output_tokens": 8,
                            },
                        },
                    },
                    {
                        "requestId": "req-1",
                        "sessionId": "session-1",
                        "timestamp": "2026-02-27T03:00:02Z",
                        "message": {
                            "model": "claude-sonnet-4-6",
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 20,
                                "cache_read_input_tokens": 30,
                                "output_tokens": 177,
                            },
                        },
                    },
                    {
                        "requestId": "req-2",
                        "sessionId": "session-2",
                        "timestamp": "2026-02-27T04:00:00Z",
                        "message": {
                            "model": "claude-haiku-4-5-20251001",
                            "usage": {
                                "input_tokens": 1,
                                "cache_creation_input_tokens": 2,
                                "cache_read_input_tokens": 3,
                                "output_tokens": 4,
                            },
                        },
                    },
                ],
            )

            totals = server._collect_claude_daily_totals(root)
            expected_day = datetime.fromisoformat("2026-02-27T03:00:00+00:00").astimezone().date()

            self.assertIn(expected_day, totals)
            self.assertEqual(totals[expected_day].input_tokens, 11)
            self.assertEqual(totals[expected_day].cached_tokens, 55)
            self.assertEqual(totals[expected_day].output_tokens, 181)
            self.assertEqual(totals[expected_day].total_tokens, 247)
            self.assertEqual(totals[expected_day].sessions, 2)
            self.assertTrue(totals[expected_day].cost_complete)
            self.assertAlmostEqual(totals[expected_day].input_cost_usd, 0.0000308)
            self.assertAlmostEqual(totals[expected_day].output_cost_usd, 0.002671)
            self.assertAlmostEqual(totals[expected_day].cached_cost_usd, 0.00008624)
            self.assertAlmostEqual(totals[expected_day].total_cost_usd, 0.00278804)

            sonnet = totals[expected_day].breakdowns[("claude-code", "claude-sonnet-4-6")]
            self.assertEqual(sonnet.sessions, 1)
            self.assertEqual(sonnet.cached_tokens, 50)
            self.assertEqual(sonnet.total_tokens, 237)
            self.assertAlmostEqual(sonnet.total_cost_usd, 0.002769)

            haiku = totals[expected_day].breakdowns[("claude-code", "claude-haiku-4-5-20251001")]
            self.assertEqual(haiku.sessions, 1)
            self.assertEqual(haiku.total_tokens, 10)
            self.assertAlmostEqual(haiku.total_cost_usd, 0.00001904)

    def test_collect_claude_daily_totals_extracts_usage_attribution_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            project_file = root / "-Users-jwei" / "session-attribution.jsonl"
            self._write_jsonl(
                project_file,
                [
                    {
                        "type": "user",
                        "sessionId": "session-attribution",
                        "timestamp": "2026-02-27T12:00:00Z",
                        "message": {
                            "content": "<command-message>commit</command-message><command-message>myplugin:status</command-message>",
                        },
                    },
                    {
                        "type": "assistant",
                        "sessionId": "session-attribution",
                        "timestamp": "2026-02-27T12:00:01Z",
                        "message": {
                            "content": [
                                {"type": "tool_use", "id": "tool-1", "name": "mcp__testmcp__query", "input": {}},
                                {"type": "tool_use", "id": "tool-2", "name": "Agent", "input": {"subagent_type": "Explore"}},
                                {"type": "tool_use", "id": "tool-3", "name": "Read", "input": {"file_path": "README.md"}},
                                {"type": "tool_use", "id": "tool-4", "name": "extension__extpack__search", "input": {"query": "usage"}},
                            ]
                        },
                    },
                    {
                        "requestId": "req-attribution",
                        "sessionId": "session-attribution",
                        "timestamp": "2026-02-27T12:00:02Z",
                        "message": {
                            "model": "claude-sonnet-4-6",
                            "usage": {
                                "input_tokens": 10,
                                "cache_creation_input_tokens": 20,
                                "cache_read_input_tokens": 30,
                                "output_tokens": 8,
                            },
                        },
                    },
                ],
            )

            totals = server._collect_claude_daily_totals(root)
            expected_day = datetime.fromisoformat("2026-02-27T12:00:02+00:00").astimezone().date()

            self.assertIn(expected_day, totals)
            attribution = totals[expected_day].attributions
            self.assertEqual(attribution[("mcp_server", "testmcp")].total_tokens, 68)
            self.assertEqual(attribution[("agent", "Explore")].total_tokens, 68)
            self.assertEqual(attribution[("tool", "mcp:testmcp/query")].events, 1)
            self.assertEqual(attribution[("tool", "mcp:testmcp/query")].total_tokens, 17)
            self.assertEqual(attribution[("tool", "Read")].total_tokens, 17)
            self.assertEqual(attribution[("tool", "extension:extpack/search")].total_tokens, 17)
            self.assertEqual(attribution[("plugin", "myplugin")].events, 1)
            self.assertEqual(attribution[("plugin", "myplugin")].total_tokens, 34)
            self.assertEqual(attribution[("plugin", "extpack")].events, 1)
            self.assertEqual(attribution[("plugin", "extpack")].total_tokens, 34)
            self.assertAlmostEqual(attribution[("plugin", "myplugin")].total_cost_usd, 0.000117)
            self.assertEqual(attribution[("skill", "/commit")].events, 1)
            self.assertEqual(attribution[("skill", "/commit")].total_tokens, 34)
            self.assertEqual(attribution[("skill", "/myplugin:status")].total_tokens, 34)

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
                            "model": "claude-sonnet-4-6",
                            "usage": {
                                "input_tokens": 2,
                                "cache_creation_input_tokens": 0,
                                "cache_read_input_tokens": 0,
                                "output_tokens": 3,
                            },
                        },
                    }
                ],
            )

            totals = server._collect_claude_daily_totals(root)
            expected_day = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone().date()

            self.assertIn(expected_day, totals)
            self.assertEqual(totals[expected_day].total_tokens, 5)
            self.assertEqual(totals[expected_day].sessions, 1)

    def test_collect_pi_daily_totals_sums_assistant_usage_counts_unique_sessions_and_tracks_model_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            first_session = root / "sessions" / "--Users-jwei--" / "2026-03-05T03-26-26-798Z_session-a.jsonl"
            self._write_jsonl(
                first_session,
                [
                    {
                        "type": "session",
                        "id": "pi-session-a",
                        "timestamp": "2026-03-05T03:26:26.798Z",
                        "cwd": "/Users/jwei",
                    },
                    {
                        "type": "model_change",
                        "id": "model-1",
                        "timestamp": "2026-03-05T03:26:27.000Z",
                        "provider": "openai-codex",
                        "modelId": "gpt-5.4",
                    },
                    {
                        "type": "message",
                        "id": "assistant-a1",
                        "timestamp": "2026-03-05T03:27:00.000Z",
                        "message": {
                            "role": "assistant",
                            "usage": {
                                "input": 90,
                                "output": 10,
                                "cacheRead": 4,
                                "cacheWrite": 6,
                                "totalTokens": 110,
                            },
                        },
                    },
                    {
                        "type": "model_change",
                        "id": "model-2",
                        "timestamp": "2026-03-05T03:27:30.000Z",
                        "provider": "openai-codex",
                        "modelId": "gpt-5.5",
                    },
                    {
                        "type": "message",
                        "id": "assistant-a2",
                        "timestamp": "2026-03-05T03:28:00.000Z",
                        "message": {
                            "role": "assistant",
                            "usage": {
                                "input": 180,
                                "output": 20,
                                "cacheRead": 8,
                                "cacheWrite": 12,
                                "totalTokens": 220,
                            },
                        },
                    },
                    {
                        "type": "message",
                        "id": "tool-result-a",
                        "timestamp": "2026-03-05T03:28:01.000Z",
                        "message": {
                            "role": "toolResult",
                            "content": [{"type": "text", "text": "ignored"}],
                        },
                    },
                ],
            )

            second_session = root / "sessions" / "--Users-jwei--" / "2026-03-05T04-10-00-000Z_session-b.jsonl"
            self._write_jsonl(
                second_session,
                [
                    {
                        "type": "session",
                        "id": "pi-session-b",
                        "timestamp": "2026-03-05T04:10:00.000Z",
                        "cwd": "/Users/jwei",
                    },
                    {
                        "type": "model_change",
                        "id": "model-3",
                        "timestamp": "2026-03-05T04:10:05.000Z",
                        "provider": "openai-codex",
                        "modelId": "gpt-5.4",
                    },
                    {
                        "type": "message",
                        "id": "assistant-b1",
                        "timestamp": "2026-03-05T04:11:00.000Z",
                        "message": {
                            "role": "assistant",
                            "usage": {
                                "input": 45,
                                "output": 5,
                                "cacheRead": 0,
                                "cacheWrite": 0,
                                "totalTokens": 50,
                            },
                        },
                    },
                ],
            )

            totals = server._collect_pi_daily_totals(root)
            expected_day = datetime.fromisoformat("2026-03-05T03:27:00+00:00").astimezone().date()

            self.assertIn(expected_day, totals)
            self.assertEqual(totals[expected_day].input_tokens, 315)
            self.assertEqual(totals[expected_day].output_tokens, 35)
            self.assertEqual(totals[expected_day].cached_tokens, 30)
            self.assertEqual(totals[expected_day].total_tokens, 380)
            self.assertEqual(totals[expected_day].sessions, 2)
            self.assertTrue(totals[expected_day].cost_complete)
            self.assertAlmostEqual(totals[expected_day].input_cost_usd, 0.0007875)
            self.assertAlmostEqual(totals[expected_day].output_cost_usd, 0.000525)
            self.assertAlmostEqual(totals[expected_day].cached_cost_usd, 0.000048)
            self.assertAlmostEqual(totals[expected_day].total_cost_usd, 0.0013605)

            gpt54 = totals[expected_day].breakdowns[("pi", "gpt-5.4")]
            self.assertEqual(gpt54.sessions, 2)
            self.assertEqual(gpt54.total_tokens, 160)
            self.assertAlmostEqual(gpt54.total_cost_usd, 0.0005785)

            gpt55 = totals[expected_day].breakdowns[("pi", "gpt-5.5")]
            self.assertEqual(gpt55.sessions, 1)
            self.assertEqual(gpt55.total_tokens, 220)
            self.assertAlmostEqual(gpt55.total_cost_usd, 0.000782)

    def test_collect_pi_daily_totals_prefers_native_cost_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            session_file = root / "sessions" / "--Users-jwei--" / "2026-03-06T00-00-00-000Z_session-native.jsonl"
            self._write_jsonl(
                session_file,
                [
                    {
                        "type": "session",
                        "id": "pi-native-cost-session",
                        "timestamp": "2026-03-06T00:00:00.000Z",
                    },
                    {
                        "type": "model_change",
                        "id": "model-native",
                        "timestamp": "2026-03-06T00:00:01.000Z",
                        "provider": "openai-codex",
                        "modelId": "gpt-5.4",
                    },
                    {
                        "type": "message",
                        "id": "assistant-native",
                        "timestamp": "2026-03-06T00:01:00.000Z",
                        "message": {
                            "role": "assistant",
                            "usage": {
                                "input": 6444,
                                "output": 81,
                                "cacheRead": 0,
                                "cacheWrite": 0,
                                "totalTokens": 6525,
                                "cost": {
                                    "input": 0.01611,
                                    "output": 0.001215,
                                    "cacheRead": 0.0,
                                    "cacheWrite": 0.0,
                                    "total": 0.017325,
                                },
                            },
                        },
                    },
                ],
            )

            totals = server._collect_pi_daily_totals(root)
            expected_day = datetime.fromisoformat("2026-03-06T00:01:00+00:00").astimezone().date()

            self.assertIn(expected_day, totals)
            self.assertAlmostEqual(totals[expected_day].input_cost_usd, 0.01611)
            self.assertAlmostEqual(totals[expected_day].output_cost_usd, 0.001215)
            self.assertAlmostEqual(totals[expected_day].cached_cost_usd, 0.0)
            self.assertAlmostEqual(totals[expected_day].total_cost_usd, 0.017325)
            self.assertTrue(totals[expected_day].cost_complete)

    def test_deleted_session_logs_remain_in_persistent_usage_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cache_file = root / "usage-history.json"
            codex_root = root / "codex"
            claude_root = root / "claude"
            pi_root = root / "pi"

            codex_session = codex_root / "2026" / "03" / "08" / "session.jsonl"
            self._write_jsonl(
                codex_session,
                [
                    {
                        "timestamp": "2026-03-08T12:00:00Z",
                        "type": "session_meta",
                        "payload": {"id": "codex-history", "originator": "codex_cli_rs"},
                    },
                    {"type": "turn_context", "payload": {"model": "gpt-5.2"}},
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 100,
                                    "cached_input_tokens": 20,
                                    "output_tokens": 30,
                                    "total_tokens": 130,
                                }
                            },
                        },
                    },
                ],
            )
            codex_duplicate = codex_root / "2026" / "03" / "08" / "session-copy.jsonl"
            codex_duplicate.write_text(codex_session.read_text(encoding="utf-8"), encoding="utf-8")

            claude_session = claude_root / "project" / "session.jsonl"
            self._write_jsonl(
                claude_session,
                [
                    {
                        "timestamp": "2026-03-08T12:05:00Z",
                        "sessionId": "claude-history",
                        "requestId": "request-1",
                        "message": {
                            "model": "claude-sonnet-4-6",
                            "usage": {
                                "input_tokens": 50,
                                "cache_creation_input_tokens": 10,
                                "cache_read_input_tokens": 20,
                                "output_tokens": 15,
                            },
                        },
                    }
                ],
            )

            pi_session = pi_root / "sessions" / "project" / "session.jsonl"
            self._write_jsonl(
                pi_session,
                [
                    {"type": "session", "id": "pi-history", "timestamp": "2026-03-08T12:10:00Z"},
                    {
                        "type": "model_change",
                        "timestamp": "2026-03-08T12:10:01Z",
                        "modelId": "gpt-5.4",
                    },
                    {
                        "type": "message",
                        "timestamp": "2026-03-08T12:11:00Z",
                        "message": {
                            "role": "assistant",
                            "usage": {
                                "input": 80,
                                "output": 20,
                                "cacheRead": 5,
                                "cacheWrite": 5,
                                "totalTokens": 110,
                            },
                        },
                    },
                ],
            )

            load_persistent_parse_caches(cache_file)
            before = (
                collect_codex_usage_data(codex_root),
                collect_claude_usage_data(claude_root),
                collect_pi_usage_data(pi_root),
            )
            self.assertEqual(sum(day.sessions for day in before[0][0].values()), 2)
            self.assertEqual(sum(day.total_tokens for day in before[0][0].values()), 260)
            save_persistent_parse_caches(cache_file)

            codex_session.unlink()
            codex_duplicate.unlink()
            claude_session.unlink()
            pi_session.unlink()
            after_deletion = (
                collect_codex_usage_data(codex_root),
                collect_claude_usage_data(claude_root),
                collect_pi_usage_data(pi_root),
            )
            self.assertEqual(after_deletion, before)
            save_persistent_parse_caches(cache_file)

            load_persistent_parse_caches(root / "empty-cache.json")
            load_persistent_parse_caches(cache_file)
            after_restart = (
                collect_codex_usage_data(codex_root),
                collect_claude_usage_data(claude_root),
                collect_pi_usage_data(pi_root),
            )
            self.assertEqual(after_restart, before)

    def test_combine_daily_totals_merges_overlapping_dates_and_breakdowns(self) -> None:
        day_a = datetime(2026, 2, 27).date()
        day_b = datetime(2026, 2, 26).date()

        codex_daily = {
            day_a: server.DailyTotals(
                date=day_a,
                sessions=2,
                input_tokens=200,
                output_tokens=60,
                cached_tokens=20,
                total_tokens=260,
                input_cost_usd=0.001,
                output_cost_usd=0.002,
                cached_cost_usd=0.0001,
                total_cost_usd=0.0031,
            ),
            day_b: server.DailyTotals(
                date=day_b,
                sessions=1,
                input_tokens=40,
                output_tokens=10,
                cached_tokens=5,
                total_tokens=50,
                input_cost_usd=0.0002,
                output_cost_usd=0.0003,
                cached_cost_usd=0.00004,
                total_cost_usd=0.00054,
                cost_complete=False,
            ),
        }
        codex_daily[day_a].add_breakdown(
            agent_cli="codex_cli_rs",
            model="gpt-5.2",
            sessions=2,
            input_tokens=200,
            output_tokens=60,
            cached_tokens=20,
            total_tokens=260,
            input_cost_usd=0.001,
            output_cost_usd=0.002,
            cached_cost_usd=0.0001,
            total_cost_usd=0.0031,
        )
        codex_daily[day_b].add_breakdown(
            agent_cli="codex_cli_rs",
            model="gpt-5.2",
            sessions=1,
            input_tokens=40,
            output_tokens=10,
            cached_tokens=5,
            total_tokens=50,
            input_cost_usd=0.0002,
            output_cost_usd=0.0003,
            cached_cost_usd=0.00004,
            total_cost_usd=0.00054,
            cost_complete=False,
        )

        claude_daily = {
            day_a: server.DailyTotals(
                date=day_a,
                sessions=3,
                input_tokens=70,
                output_tokens=30,
                cached_tokens=40,
                total_tokens=140,
                input_cost_usd=0.004,
                output_cost_usd=0.005,
                cached_cost_usd=0.0002,
                total_cost_usd=0.0092,
            ),
        }
        claude_daily[day_a].add_breakdown(
            agent_cli="claude-code",
            model="claude-sonnet-4-6",
            sessions=3,
            input_tokens=70,
            output_tokens=30,
            cached_tokens=40,
            total_tokens=140,
            input_cost_usd=0.004,
            output_cost_usd=0.005,
            cached_cost_usd=0.0002,
            total_cost_usd=0.0092,
        )

        combined = server._combine_daily_totals(codex_daily, claude_daily)

        self.assertEqual(combined[day_a].sessions, 5)
        self.assertEqual(combined[day_a].input_tokens, 270)
        self.assertEqual(combined[day_a].output_tokens, 90)
        self.assertEqual(combined[day_a].cached_tokens, 60)
        self.assertEqual(combined[day_a].total_tokens, 400)
        self.assertAlmostEqual(combined[day_a].input_cost_usd, 0.005)
        self.assertAlmostEqual(combined[day_a].output_cost_usd, 0.007)
        self.assertAlmostEqual(combined[day_a].cached_cost_usd, 0.0003)
        self.assertAlmostEqual(combined[day_a].total_cost_usd, 0.0123)
        self.assertTrue(combined[day_a].cost_complete)
        self.assertEqual(combined[day_a].breakdowns[("codex_cli_rs", "gpt-5.2")].total_tokens, 260)
        self.assertEqual(combined[day_a].breakdowns[("claude-code", "claude-sonnet-4-6")].total_tokens, 140)
        self.assertEqual(combined[day_b].sessions, 1)
        self.assertEqual(combined[day_b].total_tokens, 50)
        self.assertAlmostEqual(combined[day_b].total_cost_usd, 0.00054)
        self.assertFalse(combined[day_b].cost_complete)

    def test_collect_claude_daily_totals_missing_root_returns_empty(self) -> None:
        missing = Path("/tmp/definitely-not-a-real-claude-path-for-tests")
        totals = server._collect_claude_daily_totals(missing)
        self.assertEqual(totals, {})

    def test_current_week_end_includes_today(self) -> None:
        monday = date(2026, 3, 2)
        tuesday = date(2026, 3, 3)

        self.assertEqual(server._current_week_end(monday), monday)
        self.assertEqual(server._current_week_end(tuesday), tuesday)


if __name__ == "__main__":
    unittest.main()
