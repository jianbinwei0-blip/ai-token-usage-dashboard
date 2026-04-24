import datetime as dt
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.tmux_status import (  # noqa: E402
    build_tmux_status_snapshot,
    effective_health,
    format_age_short,
    format_next_refresh_time,
    format_recalc_short,
    format_refresh_time,
    format_tokens_short,
    format_usd_short,
    render_tmux_status,
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
                        {"date": "2026-04-21", "total_tokens": 90_189_559, "total_cost_usd": 120.0, "cost_complete": True},
                        {"date": "2026-04-20", "total_tokens": 200_000_000, "total_cost_usd": 300.0, "cost_complete": True},
                        {"date": "2026-04-19", "total_tokens": 272_192_805, "total_cost_usd": 412.0, "cost_complete": True},
                        {"date": "2026-04-12", "total_tokens": 999, "total_cost_usd": 1.0, "cost_complete": True},
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
        self.assertEqual(snapshot["metrics"]["today_tokens"], 90_189_559)
        self.assertEqual(snapshot["metrics"]["range_tokens"], 290_189_559)
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
                        {"date": "2026-04-22", "total_tokens": 123_456, "total_cost_usd": 7.5, "cost_complete": True},
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
            "range": {"preset": "wtd"},
            "metrics": {
                "today_tokens": 90_189_559,
                "range_tokens": 562_382_364,
                "range_cost_usd": 13_425.03,
                "recalc_ms": 68.4,
            },
            "quality": {"pricing_complete": True, "warning_count": 0},
        }

        self.assertEqual(
            render_tmux_status(base_snapshot, now=now),
            "AI ok · T 90.2M · WTD 562.4M · WTD $13.4k · 15:04 → 15:05",
        )

        partial_snapshot = {
            **base_snapshot,
            "health": "partial",
            "quality": {"pricing_complete": False, "warning_count": 2},
        }
        self.assertEqual(
            render_tmux_status(partial_snapshot, now=now),
            "AI partial · T 90.2M · WTD 562.4M · WTD $13.4k* · 15:04 → 15:05",
        )

        borderline_snapshot = {**base_snapshot, "generated_at": "2026-04-21T15:00:00+00:00"}
        self.assertEqual(effective_health(borderline_snapshot, now=now), "ok")

        stale_now = dt.datetime(2026, 4, 21, 15, 11, tzinfo=dt.timezone.utc)
        stale_snapshot = {**base_snapshot, "generated_at": "2026-04-21T15:00:00+00:00"}
        self.assertEqual(effective_health(stale_snapshot, now=stale_now), "stale")
        self.assertEqual(
            render_tmux_status(stale_snapshot, now=stale_now),
            "AI stale · T 90.2M · WTD 562.4M · WTD $13.4k · 15:00 → 15:05",
        )

        error_snapshot = {**base_snapshot, "health": "error"}
        self.assertEqual(render_tmux_status(error_snapshot, now=now), "AI error · 15:04 → 15:05")
        styled = render_tmux_status(base_snapshot, now=now, use_tmux_style=True)
        self.assertIn("#[fg=#58A6FF,bold]AI#[default]", styled)
        self.assertIn("#[fg=#7EE787,bold]ok#[default]", styled)
        self.assertIn("#[fg=#E6EDF3,bold]90.2M#[default]", styled)
        self.assertIn("#[fg=#79C0FF,bold]562.4M#[default]", styled)
        self.assertIn("#[fg=#8B949E]WTD#[default] #[fg=#7EE787,bold]$13.4k#[default]", styled)
        self.assertIn("#[fg=#E6EDF3]15:04#[default]", styled)
        self.assertIn("#[fg=#79C0FF]15:05#[default]", styled)
        unavailable = render_tmux_status({"providers": []}, use_tmux_style=True)
        self.assertIn("unavailable", unavailable)

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
