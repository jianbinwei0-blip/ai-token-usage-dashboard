"""Synthetic Codex event accounting regressions; never read real usage history."""

import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core import collectors


class CodexEventTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = tempfile.TemporaryDirectory(prefix="codex-events-")
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name) / "sessions"
        collectors.load_persistent_parse_caches(Path(self.scratch.name) / "history.json")
        self.addCleanup(collectors.load_persistent_parse_caches, None)

    @staticmethod
    def _timestamp(day: date, hour: int = 12, minute: int = 0) -> str:
        return datetime(day.year, day.month, day.day, hour, minute).astimezone().isoformat()

    @staticmethod
    def _usage(input_tokens: int, output_tokens: int, cached_tokens: int = 0) -> dict:
        return {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        }

    def _token_event(self, timestamp: str, input_tokens: int, output_tokens: int, cached_tokens: int = 0) -> dict:
        return {
            "type": "event_msg",
            "timestamp": timestamp,
            "payload": {
                "type": "token_count",
                "info": {"total_token_usage": self._usage(input_tokens, output_tokens, cached_tokens)},
            },
        }

    def _write_session(self, rows: list, name: str = "session.jsonl", day: date = date(2026, 10, 20)) -> Path:
        path = self.root / day.strftime("%Y/%m/%d") / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    def test_cumulative_deltas_follow_event_date_and_active_model(self) -> None:
        first_day = date(2026, 10, 20)
        second_day = date(2026, 10, 21)
        first = self._timestamp(first_day, 23, 59)
        second = self._timestamp(second_day, 0, 1)
        self._write_session([
            {"type": "session_meta", "timestamp": first, "payload": {
                "id": "synthetic-session", "originator": "codex_cli_rs", "timestamp": first,
            }},
            {"type": "turn_context", "timestamp": first, "payload": {"model": "gpt-6-astra"}},
            self._token_event(first, 100, 10),
            {"type": "turn_context", "timestamp": second, "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(second, 200, 20),
        ])

        daily, activity = collectors.collect_codex_usage_data(self.root)

        self.assertEqual({day: row.total_tokens for day, row in daily.items()}, {
            first_day: 110, second_day: 110,
        })
        self.assertEqual(set(daily[first_day].breakdowns), {("codex_cli_rs", "gpt-6-astra")})
        self.assertEqual(set(daily[second_day].breakdowns), {("codex_cli_rs", "gpt-6.1-sol")})
        self.assertAlmostEqual(daily[first_day].total_cost_usd, 0.0015)
        self.assertAlmostEqual(daily[second_day].total_cost_usd, 0.0003)
        self.assertAlmostEqual(sum(row.total_cost_usd for row in daily.values()), 0.0018)
        self.assertEqual({key: row.total_tokens for key, row in activity.items()}, {
            (first_day, 23): 110, (second_day, 0): 110,
        })
        self.assertTrue(all(row.cost_complete for row in daily.values()))

    def test_untimestamped_repeated_metadata_does_not_rewind_a_newer_context(self) -> None:
        started = self._timestamp(date(2026, 10, 20), 9)
        resumed_day = date(2026, 10, 21)
        resumed = self._timestamp(resumed_day, 14)
        self._write_session([
            {"type": "session_meta", "payload": {"timestamp": started}},
            {"type": "turn_context", "timestamp": resumed, "payload": {"model": "gpt-6-astra"}},
            {"type": "session_meta", "payload": {"timestamp": started}},
            self._token_event("", 100, 10),
        ])
        daily, activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(set(daily), {resumed_day})
        self.assertEqual(set(activity), {(resumed_day, 14)})

    def test_repeated_snapshots_do_not_add_tokens_cost_or_sessions(self) -> None:
        day = date(2026, 10, 20)
        first = self._timestamp(day, 10)
        later = self._timestamp(day, 11)
        first_event = self._token_event(first, 100, 10, 40)
        first_event["payload"]["info"]["last_token_usage"] = self._usage(100, 10, 40)
        first_event["payload"]["info"]["total_token_usage"]["reasoning_output_tokens"] = 8
        self._write_session([
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
            first_event, first_event,
            self._token_event(later, 200, 20, 80),
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(self._timestamp(date(2026, 10, 21)), 200, 20, 80),
        ])

        daily, activity = collectors.collect_codex_usage_data(self.root)

        self.assertEqual(set(daily), {day})
        self.assertEqual(daily[day].total_tokens, 220)
        self.assertEqual(daily[day].cached_tokens, 80)
        self.assertEqual(daily[day].sessions, 1)
        self.assertEqual(daily[day].breakdowns[("codex", "gpt-6-astra")].sessions, 1)
        self.assertEqual(set(daily[day].breakdowns), {("codex", "gpt-6-astra")})
        self.assertAlmostEqual(daily[day].total_cost_usd, 0.00228)
        self.assertEqual({key: row.total_tokens for key, row in activity.items()}, {
            (day, 10): 110, (day, 11): 110,
        })
        self.assertTrue(all(row.sessions == 1 for row in activity.values()))

    def test_counter_resets_preserve_pre_reset_usage(self) -> None:
        day = date(2026, 10, 20)
        for reset_input, reset_output in ((20, 2), (0, 0)):
            with self.subTest(reset=(reset_input, reset_output)):
                path = self._write_session([
                    {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
                    self._token_event(self._timestamp(day, 10), 100, 10),
                    self._token_event(self._timestamp(day, 11), reset_input, reset_output),
                    self._token_event(self._timestamp(day, 11, 1), 30, 3),
                    self._token_event(self._timestamp(day, 11, 2), 30, 3),
                ], name=f"reset-{reset_input}.jsonl")
                usage = collectors.parse_codex_session_usage(path)
                self.assertEqual(usage["input_tokens"], 130)
                self.assertEqual(usage["output_tokens"], 13)
                self.assertEqual(usage["total_tokens"], 143)
                rows = collectors.build_codex_session_contributions(
                    path, self.root, usage, collectors.PricingCatalog.from_file(None),
                )[1]
                self.assertEqual(sum(row[9] for row in rows), 143)
                self.assertEqual(sum(row[13] for row in rows), 390_000)

    def test_session_counts_are_additive_across_source_logs_not_events(self) -> None:
        day = date(2026, 10, 20)
        rows = [
            {"type": "session_meta", "payload": {"id": "same-synthetic-id"}},
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(self._timestamp(day, 10), 100, 10),
            self._token_event(self._timestamp(day, 10, 1), 200, 20),
        ]
        self._write_session(rows)
        self._write_session(rows, name="session-copy.jsonl")

        daily, activity = collectors.collect_codex_usage_data(self.root)

        self.assertEqual(daily[day].total_tokens, 440)
        self.assertEqual(daily[day].sessions, 2)
        self.assertEqual(daily[day].breakdowns[("codex", "gpt-6.1-sol")].sessions, 2)
        self.assertEqual(activity[(day, 10)].sessions, 2)

    def test_last_usage_fallback_counts_distinct_events_but_not_duplicate_event(self) -> None:
        first_day = date(2026, 10, 20)
        second_day = date(2026, 10, 21)
        first = self._token_event(self._timestamp(first_day), 0, 0)
        first["payload"]["info"] = {"total_token_usage": None, "last_token_usage": self._usage(100, 10)}
        second = self._token_event(self._timestamp(second_day), 0, 0)
        second["payload"]["info"] = {"last_token_usage": self._usage(100, 10)}
        self._write_session([
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
            first, first,
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            second, second,
        ])

        daily, _activity = collectors.collect_codex_usage_data(self.root)

        self.assertEqual({day: row.total_tokens for day, row in daily.items()}, {
            first_day: 110, second_day: 110,
        })
        self.assertAlmostEqual(sum(row.total_cost_usd for row in daily.values()), 0.0018)

    def test_cumulative_snapshots_confirm_fallbacks_without_counting_them_twice(self) -> None:
        day = date(2026, 10, 20)
        first = self._token_event(self._timestamp(day, 10), 0, 0)
        first["payload"]["info"] = {"last_token_usage": self._usage(100, 10, 40)}
        middle = self._token_event(self._timestamp(day, 13), 0, 0)
        middle["payload"]["info"] = {"total_token_usage": {}, "last_token_usage": self._usage(50, 5, 20)}
        self._write_session([
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
            first,
            self._token_event(self._timestamp(day, 11), 100, 10, 40),
            self._token_event(self._timestamp(day, 12), 200, 20, 80),
            middle,
            self._token_event(self._timestamp(day, 14), 250, 25, 100),
            self._token_event(self._timestamp(day, 15), 300, 30, 120),
        ])

        daily, activity = collectors.collect_codex_usage_data(self.root)

        self.assertEqual(daily[day].input_tokens, 300)
        self.assertEqual(daily[day].output_tokens, 30)
        self.assertEqual(daily[day].cached_tokens, 120)
        self.assertEqual(daily[day].total_tokens, 330)
        self.assertAlmostEqual(daily[day].total_cost_usd, 0.00342)
        self.assertEqual({hour: row.total_tokens for (_day, hour), row in activity.items()}, {
            10: 110, 12: 110, 13: 55, 15: 55,
        })

    def test_usable_cumulative_snapshot_has_precedence_over_last_usage(self) -> None:
        day = date(2026, 10, 20)
        event = self._token_event(self._timestamp(day), 100, 10)
        event["payload"]["info"]["last_token_usage"] = self._usage(1_000, 100)
        self._write_session([event, event])
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(daily[day].total_tokens, 110)

    def test_local_calendar_month_leap_day_and_year_boundaries(self) -> None:
        for first_day, second_day in (
            (date(2026, 1, 31), date(2026, 2, 1)),
            (date(2028, 2, 29), date(2028, 3, 1)),
            (date(2026, 12, 31), date(2027, 1, 1)),
        ):
            with self.subTest(first_day=first_day):
                path = self._write_session([
                    {"type": "session_meta", "payload": {"timestamp": self._timestamp(first_day, 20)}},
                    {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
                    self._token_event(self._timestamp(first_day, 23, 59), 100, 10),
                    self._token_event(self._timestamp(second_day, 0, 1), 200, 20),
                ], name=f"boundary-{first_day}.jsonl")
                usage = collectors.parse_codex_session_usage(path)
                rows = collectors.build_codex_session_contributions(
                    path, self.root, usage, collectors.PricingCatalog.from_file(None),
                )[1]
                self.assertEqual([(row[0], row[2], row[9]) for row in rows], [
                    (first_day, 23, 110), (second_day, 0, 110),
                ])

    def test_event_offsets_are_converted_to_local_calendar_time(self) -> None:
        timestamp = "2026-11-01T00:05:00+14:00"
        local = datetime.fromisoformat(timestamp).astimezone()
        self._write_session([self._token_event(timestamp, 100, 10)])
        daily, activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(set(daily), {local.date()})
        self.assertEqual(set(activity), {(local.date(), local.hour)})

    def test_timestamp_fallback_prefers_latest_context_then_metadata_then_rollout_then_path(self) -> None:
        day = date(2026, 10, 21)
        token = self._token_event("invalid", 100, 10)
        cases = (
            ([{"type": "session_meta", "payload": {"timestamp": self._timestamp(day, 9)}},
              {"type": "turn_context", "timestamp": self._timestamp(day, 14), "payload": {}}, token],
             "context.jsonl", day, 14),
            ([{"type": "session_meta", "payload": {"timestamp": self._timestamp(day, 9)}}, token],
             "metadata.jsonl", day, 9),
            ([token], "rollout-2026-10-21T08-10-00-synthetic.jsonl", day, 8),
            ([token], "no-timestamp.jsonl", date(2026, 10, 20), 0),
        )
        for events, name, expected_day, expected_hour in cases:
            with self.subTest(name=name):
                path = self._write_session(events, name=name)
                usage = collectors.parse_codex_session_usage(path)
                row = collectors.build_codex_session_contributions(
                    path, self.root, usage, collectors.PricingCatalog.from_file(None),
                )[1][0]
                self.assertEqual((row[0], row[1], row[2]), (expected_day, expected_day, expected_hour))

    def test_valid_event_timestamp_does_not_require_dated_directory(self) -> None:
        day = date(2026, 10, 21)
        self.root.mkdir(parents=True)
        path = self.root / "flat-session.jsonl"
        path.write_text(json.dumps(self._token_event(self._timestamp(day), 100, 10)) + "\n", encoding="utf-8")
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual({day: row.total_tokens for day, row in daily.items()}, {day: 110})

    def test_malformed_rollout_timestamp_does_not_block_directory_fallback(self) -> None:
        path = self._write_session([
            self._token_event("invalid", 100, 10),
        ], name="rollout-2026-99-99T25-61-61-synthetic.jsonl")
        usage = collectors.parse_codex_session_usage(path)
        row = collectors.build_codex_session_contributions(
            path, self.root, usage, collectors.PricingCatalog.from_file(None),
        )[1][0]
        self.assertEqual((row[0], row[2]), (date(2026, 10, 20), 0))

    def test_unknown_model_remains_partial_instead_of_borrowing_a_later_model(self) -> None:
        day = date(2026, 10, 20)
        self._write_session([
            self._token_event(self._timestamp(day, 10), 100, 10),
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(self._timestamp(day, 11), 200, 20),
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
        ])
        daily, activity = collectors.collect_codex_usage_data(self.root)
        self.assertFalse(daily[day].cost_complete)
        self.assertEqual(set(daily[day].breakdowns), {("codex", "unknown"), ("codex", "gpt-6.1-sol")})
        self.assertEqual(daily[day].breakdowns[("codex", "unknown")].total_tokens, 110)
        self.assertAlmostEqual(daily[day].total_cost_usd, 0.0003)
        self.assertFalse(activity[(day, 10)].cost_complete)
        self.assertTrue(activity[(day, 11)].cost_complete)

    def test_explicit_token_event_model_is_used_without_turn_context(self) -> None:
        day = date(2026, 10, 20)
        first = self._token_event(self._timestamp(day, 10), 100, 10)
        first["payload"]["model"] = "gpt-6-astra"
        second = self._token_event(self._timestamp(day, 11), 200, 20)
        second["payload"]["info"]["model"] = "gpt-6.1-sol"
        self._write_session([first, second])
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual({model: row.total_tokens for (_cli, model), row in daily[day].breakdowns.items()}, {
            "gpt-6-astra": 110, "gpt-6.1-sol": 110,
        })
        self.assertAlmostEqual(daily[day].total_cost_usd, 0.0018)

    def test_malformed_events_and_usage_are_ignored_and_missing_total_is_derived(self) -> None:
        day = date(2026, 10, 20)
        valid = self._token_event(self._timestamp(day), 100, 10, 40)
        valid["payload"]["info"]["total_token_usage"].pop("total_tokens")
        valid["payload"]["info"]["total_token_usage"]["reasoning_output_tokens"] = 9
        invalid = self._token_event(self._timestamp(day), 0, 0)
        invalid["payload"]["info"]["total_token_usage"] = {
            "input_tokens": -3, "output_tokens": "100", "cached_input_tokens": True, "total_tokens": 5.5,
        }
        path = self._write_session([
            [], None, 7, {"type": "session_meta", "payload": []},
            {"type": "event_msg", "payload": {"type": "token_count", "info": None}},
            invalid, valid,
        ])
        with path.open("a", encoding="utf-8") as handle:
            handle.write("{malformed-json\n\n")
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(daily[day].total_tokens, 110)
        self.assertEqual(daily[day].cached_tokens, 40)
        self.assertFalse(daily[day].cost_complete)

    def test_total_only_usage_is_not_reported_as_complete_zero_cost(self) -> None:
        day = date(2026, 10, 20)
        event = self._token_event(self._timestamp(day), 0, 0)
        event["payload"]["info"]["total_token_usage"] = {"total_tokens": 110}
        self._write_session([event])
        daily, activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(daily[day].total_tokens, 110)
        self.assertFalse(daily[day].cost_complete)
        self.assertFalse(activity[(day, 12)].cost_complete)

    def test_nonadjacent_duplicate_snapshot_is_not_a_counter_reset(self) -> None:
        day = date(2026, 10, 20)
        first = self._token_event(self._timestamp(day, 10), 100, 10)
        second = self._token_event(self._timestamp(day, 11), 200, 20)
        self._write_session([
            first, second, first, second,
            self._token_event(self._timestamp(day, 12), 300, 30),
        ])
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(daily[day].total_tokens, 330)

    def test_last_only_duplicate_of_cumulative_event_is_not_counted_again(self) -> None:
        day = date(2026, 10, 20)
        cumulative = self._token_event(self._timestamp(day), 100, 10)
        cumulative["payload"]["info"]["last_token_usage"] = self._usage(100, 10)
        fallback = self._token_event(self._timestamp(day), 0, 0)
        fallback["payload"]["info"] = {"last_token_usage": self._usage(100, 10)}
        self._write_session([cumulative, fallback])
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(daily[day].total_tokens, 110)

    def test_live_legacy_cache_is_reparsed_and_deleted_legacy_usage_is_retained(self) -> None:
        first_day = date(2026, 10, 20)
        second_day = date(2026, 10, 21)
        retained_day = date(2026, 10, 19)
        live = self._write_session([
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
            self._token_event(self._timestamp(first_day), 100, 10),
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(self._timestamp(second_day), 200, 20),
        ])
        deleted = live.parent / "deleted-legacy.jsonl"
        stat = live.stat()
        pricing_key = collectors.pricing_cache_key(collectors.PricingCatalog.from_file(None))
        live_row = ["t", first_day.toordinal(), first_day.toordinal(), 12,
                    "live-legacy", "codex", "gpt-6.1-sol", 200, 20, 0, 220,
                    400_000, 200_000, 0, 600_000, True]
        deleted_row = ["t", retained_day.toordinal(), retained_day.toordinal(), 9,
                       "deleted-legacy", "codex", "gpt-6-astra", 100, 10, 0, 110,
                       0, 0, 0, 0, False]
        for version in (4, 5):
            with self.subTest(cache_version=version):
                cache = Path(self.scratch.name) / f"legacy-{version}.json"
                cache.write_text(json.dumps({"version": version, "codex": {
                    str(live): [stat.st_size, stat.st_mtime_ns, pricing_key, live_row],
                    str(deleted): {"size": 1, "mtime_ns": 1, "pricing_key": pricing_key, "contribution": deleted_row},
                }}), encoding="utf-8")
                collectors.load_persistent_parse_caches(cache)
                daily, _activity = collectors.collect_codex_usage_data(self.root)
                self.assertEqual({day: row.total_tokens for day, row in daily.items()}, {
                    retained_day: 110, first_day: 110, second_day: 110,
                })
                self.assertAlmostEqual(sum(row.total_cost_usd for row in daily.values()), 0.0033)
                self.assertTrue(all(row.cost_complete for row in daily.values()))
                self.assertEqual(collectors._CODEX_SESSION_USAGE_CACHE[str(live)][3][0], "codex-events-v1")
                # A retained legacy aggregate is preserved, not fabricated into
                # an event history whose original log no longer exists.
                self.assertEqual(len(collectors._CODEX_SESSION_USAGE_CACHE[str(deleted)][3]), 15)

    def test_legacy_cached_null_is_reparsed_for_newly_supported_last_usage(self) -> None:
        day = date(2026, 10, 20)
        event = self._token_event(self._timestamp(day), 0, 0)
        event["payload"]["info"] = {"last_token_usage": self._usage(100, 10)}
        path = self._write_session([event])
        catalog = collectors.PricingCatalog.from_file(None)
        stat = path.stat()
        collectors._CODEX_SESSION_USAGE_CACHE[str(path)] = (
            stat.st_size, stat.st_mtime_ns, collectors.pricing_cache_key(catalog), None,
        )
        daily, _activity = collectors.collect_codex_usage_data(self.root, catalog)
        self.assertEqual(daily[day].total_tokens, 110)

    def test_unchanged_event_cache_avoids_reparsing(self) -> None:
        self._write_session([self._token_event(self._timestamp(date(2026, 10, 20)), 100, 10)])
        first = collectors.collect_codex_usage_data(self.root)
        with patch.object(collectors, "parse_codex_session_usage", side_effect=AssertionError("unexpected reparse")):
            self.assertEqual(collectors.collect_codex_usage_data(self.root), first)

    def test_deleted_event_history_revalues_each_model_when_rate_fingerprint_changes(self) -> None:
        first_day = date(2026, 10, 20)
        second_day = date(2026, 10, 21)
        path = self._write_session([
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
            self._token_event(self._timestamp(first_day), 100, 10),
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(self._timestamp(second_day), 200, 20),
        ])
        catalog = collectors.PricingCatalog.from_file(None)
        old_card = json.loads(json.dumps(catalog._rate_card))
        old_card["providers"]["codex"].pop("gpt-6-astra")
        old_catalog = collectors.PricingCatalog(old_card, source=catalog.source)
        self.assertEqual(old_catalog.version, catalog.version)
        before, _activity = collectors.collect_codex_usage_data(self.root, old_catalog)
        self.assertFalse(before[first_day].cost_complete)
        self.assertAlmostEqual(sum(row.total_cost_usd for row in before.values()), 0.0003)
        path.unlink()
        after, activity = collectors.collect_codex_usage_data(self.root, catalog)
        self.assertEqual({day: row.total_tokens for day, row in after.items()}, {first_day: 110, second_day: 110})
        self.assertTrue(all(row.cost_complete for row in after.values()))
        self.assertAlmostEqual(sum(row.total_cost_usd for row in after.values()), 0.0018)
        self.assertAlmostEqual(sum(row.total_cost_usd for row in activity.values()), 0.0018)

    def test_event_bundle_survives_persistent_history_roundtrip_after_source_deletion(self) -> None:
        first_day = date(2026, 10, 20)
        second_day = date(2026, 10, 21)
        path = self._write_session([
            {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
            self._token_event(self._timestamp(first_day), 100, 10),
            {"type": "turn_context", "payload": {"model": "gpt-6.1-sol"}},
            self._token_event(self._timestamp(second_day), 200, 20),
        ])
        before = collectors.collect_codex_usage_data(self.root)
        cache = Path(self.scratch.name) / "saved-history.json"
        collectors.save_persistent_parse_caches(cache)
        path.unlink()
        collectors.load_persistent_parse_caches(Path(self.scratch.name) / "empty.json")
        collectors.load_persistent_parse_caches(cache)
        self.assertEqual(collectors.collect_codex_usage_data(self.root), before)

    def test_missing_token_time_uses_resume_event_not_original_session_start(self) -> None:
        started = self._timestamp(date(2026, 10, 20), 10)
        resumed_day = date(2026, 10, 21)
        resumed = self._timestamp(resumed_day, 14)
        self._write_session([
            {"type": "session_meta", "timestamp": started, "payload": {"timestamp": started}},
            {"type": "session_meta", "timestamp": resumed, "payload": {"timestamp": started}},
            self._token_event("invalid", 100, 10),
        ])
        daily, activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual(set(daily), {resumed_day})
        self.assertEqual(set(activity), {(resumed_day, 14)})

    def test_append_and_truncated_json_tail_do_not_double_count_earlier_events(self) -> None:
        first_day = date(2026, 10, 20)
        second_day = date(2026, 10, 21)
        first = self._token_event(self._timestamp(first_day), 100, 10)
        path = self._write_session([first])
        collectors.collect_codex_usage_data(self.root)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(self._token_event(self._timestamp(second_day), 200, 20)) + "\n")
            handle.write('{"type":"event_msg","payload":')
        daily, _activity = collectors.collect_codex_usage_data(self.root)
        self.assertEqual({day: row.total_tokens for day, row in daily.items()}, {first_day: 110, second_day: 110})
        self.assertEqual(sum(row.sessions for row in daily.values()), 2)


if __name__ == "__main__":
    unittest.main()
