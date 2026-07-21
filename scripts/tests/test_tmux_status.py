import datetime as dt
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.tmux_status import (  # noqa: E402
    build_tmux_status_snapshot,
    effective_health,
    format_age_short,
    format_chatgpt_plan,
    format_next_refresh_time,
    format_quota_reset_time,
    format_recalc_short,
    format_refresh_time,
    format_tokens_short,
    format_usd_short,
    render_tmux_status,
    subscription_effective_state,
)
from render_tmux_status import (  # noqa: E402
    parse_args as parse_tmux_status_args,
    refresh_subscription_snapshot,
)


class TmuxStatusTests(unittest.TestCase):
    def test_compact_formatters(self) -> None:
        self.assertEqual(format_tokens_short(90_189_559), "90.2M")
        self.assertEqual(format_tokens_short(562_382_364), "562.4M")
        self.assertEqual(format_tokens_short(5_371_446_165), "5.37B")
        self.assertEqual(format_usd_short(13_425.03), "$13.4k")
        self.assertEqual(format_usd_short(47.0), "$47")
        self.assertEqual(format_recalc_short(68.4), "68ms")
        self.assertEqual(format_recalc_short(1_240), "1.2s")

    def test_default_status_range_is_month_to_date(self) -> None:
        with mock.patch.object(sys, "argv", ["render_tmux_status.py"]):
            args = parse_tmux_status_args()
            self.assertEqual(args.range_preset, "mtd")
            self.assertEqual(args.chatgpt_usage, "auto")
            self.assertEqual(args.codex_bin, "codex")

        dataset_payload = {
            "generated_at": "2026-04-21T15:04:00+00:00",
            "providers_available": {"codex": True, "claude": False, "pi": False, "combined": True},
            "pricing": {"source": "built-in", "version": "2026-03-08", "warnings": []},
            "providers": {
                "combined": {
                    "rows": [
                        {"date": "2026-04-21", "total_tokens": 100, "total_cost_usd": 1.0, "cost_complete": True},
                        {"date": "2026-04-01", "total_tokens": 50, "total_cost_usd": 0.5, "cost_complete": True},
                        {"date": "2026-03-31", "total_tokens": 1_000, "total_cost_usd": 10.0, "cost_complete": True},
                    ]
                }
            },
        }

        snapshot = build_tmux_status_snapshot(
            dataset_payload,
            now=dt.datetime(2026, 4, 21, 15, 4, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(snapshot["range"]["preset"], "mtd")
        self.assertEqual(snapshot["range"]["label"], "Month to Date")
        self.assertEqual(snapshot["range"]["from"], "2026-04-01")
        self.assertEqual(snapshot["range"]["to"], "2026-04-21")
        self.assertEqual(snapshot["metrics"]["today_tokens"], 100)
        self.assertEqual(snapshot["metrics"]["range_tokens"], 150)
        self.assertAlmostEqual(snapshot["metrics"]["range_cost_usd"], 1.5)

    def test_build_snapshot_for_combined_wtd(self) -> None:
        dataset_payload = {
            "generated_at": "2026-04-21T15:04:00+00:00",
            "providers_available": {"codex": True, "claude": True, "pi": True, "combined": True},
            "pricing": {
                "source": "built-in",
                "version": "2026-03-08",
                "warnings": [{"provider": "claude", "model": "sonnet"}],
            },
            "providers": {
                "combined": {
                    "rows": [
                        {
                            "date": "2026-04-21",
                            "input_tokens": 40_000_000,
                            "output_tokens": 10_000_000,
                            "cached_tokens": 40_189_559,
                            "total_tokens": 90_189_559,
                            "total_cost_usd": 120.0,
                            "cost_complete": True,
                        },
                        {
                            "date": "2026-04-20",
                            "input_tokens": 100_000_000,
                            "output_tokens": 20_000_000,
                            "cached_tokens": 80_000_000,
                            "total_tokens": 200_000_000,
                            "total_cost_usd": 300.0,
                            "cost_complete": True,
                        },
                        {
                            "date": "2026-04-19",
                            "input_tokens": 150_000_000,
                            "output_tokens": 22_192_805,
                            "cached_tokens": 100_000_000,
                            "total_tokens": 272_192_805,
                            "total_cost_usd": 412.0,
                            "cost_complete": True,
                        },
                        {
                            "date": "2026-04-12",
                            "input_tokens": 333,
                            "output_tokens": 333,
                            "cached_tokens": 333,
                            "total_tokens": 999,
                            "total_cost_usd": 1.0,
                            "cost_complete": True,
                        },
                    ]
                },
                "claude": {
                    "rows": [
                        {"date": "2026-04-21", "total_tokens": 5_000, "total_cost_usd": 10.0, "cost_complete": False}
                    ]
                },
            },
        }

        snapshot = build_tmux_status_snapshot(
            dataset_payload,
            {"total": 68.4},
            scope="combined",
            range_preset="wtd",
            now=dt.datetime(2026, 4, 21, 15, 4, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(snapshot["health"], "partial")
        self.assertEqual(snapshot["providers"], ["codex", "claude", "pi"])
        self.assertEqual(snapshot["range"]["preset"], "wtd")
        self.assertEqual(snapshot["version"], 3)
        self.assertEqual(snapshot["metrics"]["today_tokens"], 90_189_559)
        self.assertEqual(snapshot["metrics"]["today_input_tokens"], 40_000_000)
        self.assertEqual(snapshot["metrics"]["today_output_tokens"], 10_000_000)
        self.assertEqual(snapshot["metrics"]["today_cached_tokens"], 40_189_559)
        self.assertEqual(snapshot["metrics"]["range_tokens"], 290_189_559)
        self.assertEqual(snapshot["metrics"]["range_input_tokens"], 140_000_000)
        self.assertEqual(snapshot["metrics"]["range_output_tokens"], 30_000_000)
        self.assertEqual(snapshot["metrics"]["range_cached_tokens"], 120_189_559)
        self.assertAlmostEqual(snapshot["metrics"]["range_cost_usd"], 420.0)
        self.assertEqual(snapshot["quality"]["warning_count"], 1)
        self.assertFalse(snapshot["quality"]["pricing_complete"])

    def test_build_snapshot_uses_local_day_not_utc_day(self) -> None:
        pacific = dt.timezone(dt.timedelta(hours=-7))
        dataset_payload = {
            "generated_at": "2026-04-23T02:55:00+00:00",
            "providers_available": {"codex": True, "claude": False, "pi": False, "combined": True},
            "pricing": {"source": "built-in", "version": "2026-03-08", "warnings": []},
            "providers": {
                "combined": {
                    "rows": [
                        {
                            "date": "2026-04-22",
                            "input_tokens": 100_000,
                            "output_tokens": 20_000,
                            "cached_tokens": 3_456,
                            "total_tokens": 123_456,
                            "total_cost_usd": 7.5,
                            "cost_complete": True,
                        },
                    ]
                }
            },
        }

        snapshot = build_tmux_status_snapshot(
            dataset_payload,
            {"total": 12.0},
            scope="combined",
            range_preset="wtd",
            now=dt.datetime(2026, 4, 22, 20, 0, tzinfo=pacific),
        )

        self.assertEqual(snapshot["metrics"]["today_tokens"], 123_456)
        self.assertEqual(snapshot["range"]["to"], "2026-04-22")

    def test_render_variants(self) -> None:
        now = dt.datetime(2026, 4, 21, 15, 5, tzinfo=dt.timezone.utc)
        base_snapshot = {
            "generated_at": "2026-04-21T15:04:00+00:00",
            "health": "ok",
            "scope": "combined",
            "providers": ["codex", "claude", "pi"],
            "range": {"preset": "mtd"},
            "metrics": {
                "today_tokens": 90_189_559,
                "today_input_tokens": 40_000_000,
                "today_output_tokens": 10_000_000,
                "today_cached_tokens": 40_189_559,
                "range_tokens": 562_382_364,
                "range_input_tokens": 250_000_000,
                "range_output_tokens": 62_382_364,
                "range_cached_tokens": 250_000_000,
                "range_cost_usd": 13_425.03,
                "recalc_ms": 68.4,
            },
            "quality": {"pricing_complete": True, "warning_count": 0},
        }

        self.assertEqual(
            render_tmux_status(base_snapshot, now=now),
            "AI · Today 90.2M · MTD 562.4M · $13.4k · 15:04 → 15:05",
        )

        partial_snapshot = {
            **base_snapshot,
            "health": "partial",
            "quality": {"pricing_complete": False, "warning_count": 2},
        }
        self.assertEqual(
            render_tmux_status(partial_snapshot, now=now),
            "AI partial · Today 90.2M · MTD 562.4M · $13.4k* · 15:04 → 15:05",
        )

        borderline_snapshot = {**base_snapshot, "generated_at": "2026-04-21T15:00:00+00:00"}
        self.assertEqual(effective_health(borderline_snapshot, now=now), "ok")

        stale_now = dt.datetime(2026, 4, 21, 15, 11, tzinfo=dt.timezone.utc)
        stale_snapshot = {**base_snapshot, "generated_at": "2026-04-21T15:00:00+00:00"}
        self.assertEqual(effective_health(stale_snapshot, now=stale_now), "stale")
        self.assertEqual(
            render_tmux_status(stale_snapshot, now=stale_now),
            "AI stale · Today 90.2M · MTD 562.4M · $13.4k · 15:00 → 15:05",
        )

        error_snapshot = {**base_snapshot, "health": "error"}
        self.assertEqual(render_tmux_status(error_snapshot, now=now), "AI error · 15:04 → 15:05")
        styled = render_tmux_status(base_snapshot, now=now, use_tmux_style=True)
        self.assertIn("#[fg=#58A6FF,bold]AI#[default]", styled)
        self.assertNotIn("#[fg=#7EE787,bold]ok#[default]", styled)
        self.assertIn("#[fg=#E6EDF3,bold]90.2M#[default]", styled)
        self.assertIn("#[fg=#79C0FF,bold]562.4M#[default]", styled)
        self.assertNotIn(" I ", render_tmux_status(base_snapshot, now=now))
        self.assertNotIn(" O ", render_tmux_status(base_snapshot, now=now))
        self.assertIn("#[fg=#8B949E]MTD#[default] #[fg=#79C0FF,bold]562.4M#[default]", styled)
        self.assertIn("#[fg=#7EE787,bold]$13.4k#[default]", styled)
        self.assertIn("#[fg=#E6EDF3]15:04#[default]", styled)
        self.assertIn("#[fg=#79C0FF]15:05#[default]", styled)
        unavailable = render_tmux_status({"providers": []}, use_tmux_style=True)
        self.assertIn("unavailable", unavailable)

    def test_render_chatgpt_quota_with_total_tokens_and_mtd_cost(self) -> None:
        now = dt.datetime(2026, 4, 21, 15, 5, tzinfo=dt.timezone.utc)
        primary_reset = int(dt.datetime(2026, 4, 21, 18, 0, tzinfo=dt.timezone.utc).timestamp())
        weekly_reset = int(dt.datetime(2026, 4, 24, 9, 0, tzinfo=dt.timezone.utc).timestamp())
        subscription = {
            "version": 1,
            "state": "ok",
            "fetched_at": "2026-04-21T15:04:00+00:00",
            "account_type": "chatgpt",
            "plan": "prolite",
            "limits": [
                {
                    "id": "codex",
                    "name": None,
                    "primary": {
                        "remaining_percent": 88,
                        "window_duration_minutes": 300,
                        "resets_at": primary_reset,
                    },
                    "secondary": {
                        "remaining_percent": 83,
                        "window_duration_minutes": 10_080,
                        "resets_at": weekly_reset,
                    },
                    "credits": None,
                    "rate_limit_reached_type": None,
                }
            ],
        }
        snapshot = {
            "generated_at": "2026-04-21T15:04:00+00:00",
            "health": "ok",
            "scope": "combined",
            "providers": ["codex"],
            "range": {"preset": "mtd"},
            "metrics": {
                "today_tokens": 90_189_559,
                "range_tokens": 562_382_364,
                "range_cost_usd": 13_425.03,
            },
            "quality": {"pricing_complete": True},
            "subscription": subscription,
        }

        self.assertEqual(format_chatgpt_plan("prolite"), "Pro Lite")
        self.assertEqual(format_quota_reset_time(primary_reset, now=now), "18:00")
        self.assertEqual(format_quota_reset_time(weekly_reset, now=now), "Fri 09:00")
        self.assertEqual(format_quota_reset_time(weekly_reset, now=now, compact=True), "Fri")
        self.assertEqual(subscription_effective_state(subscription, now=now), "ok")
        self.assertEqual(
            render_tmux_status(snapshot, now=now),
            "GPT Pro Lite · 5h 88% left ↻18:00 · Weekly 83% left ↻Fri 09:00 · Today 90.2M · MTD 562.4M · $13.4k · 15:04 → 15:05",
        )

        compact = render_tmux_status(snapshot, now=now, max_width=96)
        self.assertEqual(
            compact,
            "GPT Pro Lite · 5h 88% ↻18:00 · 7d 83% ↻Fri 09:00 · Today 90.2M · MTD 562.4M · $13.4k",
        )
        self.assertLessEqual(len(compact), 96)
        self.assertIn("5h 88% ↻18:00", compact)
        self.assertIn("7d 83% ↻Fri 09:00", compact)
        self.assertEqual(render_tmux_status(snapshot, now=now, max_width=16), "5h 88% ↻18:00")
        self.assertNotIn(" I ", compact)
        self.assertNotIn(" O ", compact)
        styled = render_tmux_status(snapshot, now=now, max_width=96, use_tmux_style=True)
        self.assertIn("#[fg=#7EE787,bold]GPT#[default]", styled)
        self.assertIn("#[fg=#7EE787,bold]88%#[default]", styled)
        self.assertIn("#[fg=#7EE787,bold]$13.4k#[default]", styled)

        low_limit = {
            **subscription["limits"][0],
            "primary": {**subscription["limits"][0]["primary"], "remaining_percent": 40},
        }
        low_subscription = {**subscription, "limits": [low_limit]}
        low_output = render_tmux_status({**snapshot, "subscription": low_subscription}, now=now, max_width=96)
        self.assertIn("5h 40% ↻18:00", low_output)

        credited_limit = {
            **subscription["limits"][0],
            "credits": {"has_credits": True, "unlimited": False, "balance": "25.4"},
        }
        credited_subscription = {**subscription, "limits": [credited_limit]}
        credited = render_tmux_status({**snapshot, "subscription": credited_subscription}, now=now, max_width=96)
        self.assertIn("Cr 25", credited)
        self.assertIn("MTD 562.4M · $13.4k", credited)

        stale_subscription = {**subscription, "fetched_at": "2026-04-21T14:40:00+00:00"}
        self.assertEqual(subscription_effective_state(stale_subscription, now=now), "stale")
        stale_output = render_tmux_status({**snapshot, "subscription": stale_subscription}, now=now, max_width=96)
        self.assertIn("GPT Pro Lite stale", stale_output)

    def test_render_always_shows_inactive_five_hour_quota_and_reset_state(self) -> None:
        now = dt.datetime(2026, 4, 21, 15, 5, tzinfo=dt.timezone.utc)
        snapshot = {
            "generated_at": "2026-04-21T15:04:00+00:00",
            "health": "ok",
            "scope": "combined",
            "providers": ["codex"],
            "range": {"preset": "mtd"},
            "metrics": {"today_tokens": 100, "range_tokens": 200, "range_cost_usd": 3},
            "quality": {"pricing_complete": True},
            "subscription": {
                "state": "ok",
                "fetched_at": "2026-04-21T15:04:00+00:00",
                "account_type": "chatgpt",
                "plan": "prolite",
                "limits": [
                    {
                        "id": "codex",
                        "primary": {
                            "remaining_percent": 100,
                            "window_duration_minutes": 300,
                            "resets_at": None,
                            "inferred": True,
                        },
                        "secondary": {
                            "remaining_percent": 83,
                            "window_duration_minutes": 10_080,
                            "resets_at": int(
                                dt.datetime(2026, 4, 24, 9, 0, tzinfo=dt.timezone.utc).timestamp()
                            ),
                        },
                        "rate_limit_reached_type": None,
                    }
                ],
            },
        }

        for width in (96, 48, 16):
            with self.subTest(width=width):
                output = render_tmux_status(snapshot, now=now, max_width=width)
                self.assertIn("5h 100% ↻now", output)
                self.assertLessEqual(len(output), width)

    def test_render_includes_only_a_more_constrained_named_quota(self) -> None:
        now = dt.datetime(2026, 4, 21, 15, 5, tzinfo=dt.timezone.utc)
        reset = int(dt.datetime(2026, 4, 21, 19, 0, tzinfo=dt.timezone.utc).timestamp())
        canonical = {
            "id": "codex",
            "primary": {"remaining_percent": 88, "window_duration_minutes": 300, "resets_at": reset},
            "secondary": {"remaining_percent": 83, "window_duration_minutes": 10_080, "resets_at": reset},
            "rate_limit_reached_type": None,
        }
        named = {
            "id": "codex_spark",
            "name": "GPT-5.3-Codex-Spark",
            "primary": {"remaining_percent": 60, "window_duration_minutes": 300, "resets_at": reset},
            "secondary": None,
            "rate_limit_reached_type": None,
        }
        snapshot = {
            "generated_at": "2026-04-21T15:04:00+00:00",
            "health": "ok",
            "scope": "combined",
            "providers": ["codex"],
            "range": {"preset": "mtd"},
            "metrics": {"today_tokens": 100, "range_tokens": 200, "range_cost_usd": 3},
            "quality": {"pricing_complete": True},
            "subscription": {
                "state": "ok",
                "fetched_at": "2026-04-21T15:04:00+00:00",
                "account_type": "chatgpt",
                "plan": "plus",
                "limits": [canonical, named],
            },
        }

        output = render_tmux_status(snapshot, now=now, max_width=96)
        self.assertIn("Spark 5h 60%", output)
        self.assertIn("MTD 200 · $3", output)
        self.assertLessEqual(len(output), 96)

        unconstrained_named = {
            **named,
            "primary": {"remaining_percent": 100, "window_duration_minutes": 300, "resets_at": reset},
        }
        unconstrained = {
            **snapshot,
            "subscription": {**snapshot["subscription"], "limits": [canonical, unconstrained_named]},
        }
        self.assertNotIn("Spark", render_tmux_status(unconstrained, now=now, max_width=96))

    def test_subscription_refresh_uses_stale_last_good_data_after_failure(self) -> None:
        previous_subscription = {
            "version": 1,
            "state": "ok",
            "fetched_at": "2026-04-21T15:00:00+00:00",
            "account_type": "chatgpt",
            "plan": "plus",
            "limits": [{"id": "codex", "primary": {"remaining_percent": 75}}],
        }
        attempted_at = dt.datetime(2026, 4, 21, 15, 10, tzinfo=dt.timezone.utc)
        with mock.patch("render_tmux_status.fetch_chatgpt_subscription_usage", side_effect=RuntimeError("offline")):
            fallback = refresh_subscription_snapshot(
                {"subscription": previous_subscription},
                codex_binary="codex",
                timeout_seconds=1,
                now=attempted_at,
            )

        self.assertEqual(fallback["state"], "stale")
        self.assertEqual(fallback["fetched_at"], "2026-04-21T15:00:00+00:00")
        self.assertEqual(fallback["last_attempted_at"], "2026-04-21T15:10:00+00:00")
        self.assertNotIn("offline", str(fallback))

    def test_age_formatter(self) -> None:
        now = dt.datetime(2026, 4, 21, 15, 5, tzinfo=dt.timezone.utc)
        self.assertEqual(format_age_short("2026-04-21T15:04:50+00:00", now=now), "now")
        self.assertEqual(format_age_short("2026-04-21T14:05:00+00:00", now=now), "1h")
        self.assertEqual(format_age_short("2026-04-19T15:05:00+00:00", now=now), "2d")
        self.assertEqual(format_refresh_time("2026-04-21T15:04:50+00:00", now=now), "15:04")
        self.assertEqual(format_next_refresh_time("2026-04-21T15:04:50+00:00", now=now), "15:05")
        self.assertEqual(format_next_refresh_time("2026-04-21T15:05:00+00:00", now=now), "15:10")


if __name__ == "__main__":
    unittest.main()
