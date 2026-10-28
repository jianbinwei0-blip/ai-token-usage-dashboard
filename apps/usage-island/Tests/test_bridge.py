from __future__ import annotations
import copy
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

BRIDGE_DIR = Path(__file__).resolve().parents[1] / "Sources/UsageIslandApp/Resources/Bridge"
sys.path.insert(0, str(BRIDGE_DIR))
import usage_bridge as bridge


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime(2026, 10, 26, 22, 30, tzinfo=dt.timezone.utc)
        self.snapshot = {
            "version": 3, "generated_at": self.now.isoformat(), "health": "ok",
            "scope": "combined", "providers": ["codex", "claude", "pi", "dsh"],
            "range": {"preset": "mtd"},
            "metrics": {"today_tokens": 8600000, "range_tokens": 3540000000, "range_cost_usd": 5800},
            "quality": {"pricing_complete": True},
            "subscription": {
                "version": 2, "state": "ok", "fetched_at": self.now.isoformat(),
                "account_type": "chatgpt", "plan": "pro",
                "limits": [{
                    "id": "codex",
                    "primary": {"window_duration_minutes": 300, "remaining_percent": 100, "inferred": True},
                    "secondary": {"window_duration_minutes": 10080, "remaining_percent": 99,
                                  "resets_at": int((self.now + dt.timedelta(days=6, hours=8, minutes=16)).timestamp())},
                    "credits": {"has_credits": True, "unlimited": False, "balance": "12345.67"},
                }],
            },
        }

    def result(self, **kwargs):
        return bridge.payload(self.snapshot, now=self.now, **kwargs)

    def test_values_match_current_display_and_retired_quota_is_absent(self):
        result = self.result()
        fields = {f["key"]: f["value"] for f in result["fields"]}
        self.assertEqual(fields, {
            "plan": "GPT Pro", "weekly": "99% left", "reset": "6d 8h 16m",
            "credits": "12,346", "today": "8.6M", "month": "3.54B",
            "cost": "$5.8k", "refresh": "22:30 → 22:35",
        })
        self.assertNotIn("5h", result["text"])
        self.assertEqual(result["providers"], ["codex", "claude", "pi", "dsh"])

    def test_bundled_formatter_matches_repository_source(self):
        canonical = Path(__file__).resolve().parents[3] / "scripts/dashboard_core/tmux_status.py"
        self.assertEqual(Path(bridge.formatter.__file__).read_bytes(), canonical.read_bytes())

    def test_canonical_formatter_owns_all_display_values(self):
        result = self.result()
        self.assertEqual(result["text"], bridge.formatter.render_tmux_status(self.snapshot, now=self.now))
        self.assertEqual(result["schema_version"], 1)

    def test_stale_usage_and_quota_are_separate(self):
        self.snapshot["generated_at"] = (self.now - dt.timedelta(minutes=10)).isoformat()
        result = self.result()
        self.assertEqual(result["health"], "stale")
        self.assertEqual(result["subscription_state"], "ok")
        self.snapshot["subscription"]["fetched_at"] = (self.now - dt.timedelta(minutes=16)).isoformat()
        self.assertEqual(self.result()["subscription_state"], "stale")

    def test_partial_pricing_is_never_presented_as_complete(self):
        self.snapshot["health"] = "partial"
        self.snapshot["quality"]["pricing_complete"] = False
        result = self.result()
        self.assertFalse(result["pricing_complete"])
        self.assertIn("$5.8k*", result["text"])
        self.assertEqual(next(f for f in result["fields"] if f["key"] == "cost")["tone"], "warning")

    def test_failed_collection_does_not_mutate_or_relabel_cache_as_fresh(self):
        before = copy.deepcopy(self.snapshot)
        result = self.result(failed=True)
        self.assertEqual(self.snapshot, before)
        self.assertEqual(result["health"], "error")
        self.assertIn("Collection failed", result["message"])
        self.assertNotIn("today", [f["key"] for f in result["fields"]])

    def test_missing_cache_does_not_become_zero_tokens(self):
        result = bridge.payload(None, now=self.now)
        self.assertEqual(result["health"], "unavailable")
        self.assertEqual(result["text"], "AI unavailable")
        self.assertNotIn("today", [f["key"] for f in result["fields"]])

    def test_signed_out_account_omits_quota_instead_of_inventing_it(self):
        self.snapshot["subscription"] = {"state": "not_applicable", "account_type": None}
        fields = {f["key"]: f["value"] for f in self.result()["fields"]}
        self.assertNotIn("weekly", fields)
        self.assertNotIn("credits", fields)
        self.assertEqual(fields["today"], "8.6M")

    def test_zero_and_low_quota_keep_numeric_meaning(self):
        for value, tone in [(0, "error"), (12, "warning"), (99, "positive")]:
            self.snapshot["subscription"]["limits"][0]["secondary"]["remaining_percent"] = value
            field = next(f for f in self.result()["fields"] if f["key"] == "weekly")
            self.assertEqual(field["tone"], tone)
            self.assertEqual(field["value"], f"{value}% left")

    def test_named_model_quota_and_unlimited_credits_survive(self):
        self.snapshot["subscription"]["limits"][0]["credits"]["unlimited"] = True
        self.snapshot["subscription"]["limits"].append({
            "id": "gpt-6", "name": "GPT-6",
            "secondary": {"window_duration_minutes": 10080, "remaining_percent": 12},
        })
        result = self.result()
        self.assertIn("12%", result["text"])
        self.assertIn("Credits ∞", result["text"])
        self.assertEqual(sum(f["key"] == "plan" for f in result["fields"]), 1)

    def test_malformed_and_nonfinite_cache_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cache.json"
            for content in ['{', '[]', '{}', '{"metrics":[]}', '{"providers":{}}',
                            '{"metrics":{"today_tokens":NaN}}',
                            '{"metrics":{"today_tokens":-1}}']:
                path.write_text(content)
                self.assertIsNone(bridge.read_cache(path), content)
            missing = copy.deepcopy(self.snapshot)
            del missing["metrics"]["today_tokens"]
            path.write_text(json.dumps(missing))
            self.assertIsNone(bridge.read_cache(path))
            missing["metrics"]["today_tokens"] = None
            path.write_text(json.dumps(missing))
            self.assertIsNone(bridge.read_cache(path))
            path.write_text(json.dumps(self.snapshot))
            self.assertEqual(bridge.read_cache(path), self.snapshot)

    def test_bad_nested_account_payload_is_a_visible_error_not_a_crash(self):
        self.snapshot["subscription"]["limits"] = [42, None]
        result = self.result()
        self.assertIn("limits", result["text"])
        json.dumps(result, allow_nan=False)

    def test_reset_detail_payload_preserves_counts_and_old_metrics(self):
        before = self.result()["fields"]
        self.snapshot["subscription"].update(
            reset_credits_available=3,
            reset_credit_expirations=[
                {"count": 2, "expires_at": 1_900_000_000, "does_not_expire": False},
                {"count": 1, "expires_at": None, "does_not_expire": True},
            ],
        )
        result = self.result()
        self.assertEqual(result["fields"], before)
        self.assertEqual(result["reset_credits"], {
            "available_count": 3, "details_complete": True,
            "expirations": [
                {"count": 2, "expires_at": 1_900_000_000, "does_not_expire": False},
                {"count": 1, "expires_at": None, "does_not_expire": True},
            ],
        })

    def test_legacy_capped_and_zero_reset_metadata_are_distinct(self):
        subscription = self.snapshot["subscription"]
        subscription["reset_credits_available"] = 5
        legacy = self.result()["reset_credits"]
        self.assertEqual(legacy["available_count"], 5)
        self.assertIsNone(legacy["expirations"])
        self.assertFalse(legacy["details_complete"])
        subscription["reset_credit_expirations"] = [{"count": 1, "expires_at": 1_900_000_000}]
        capped = self.result()["reset_credits"]
        self.assertEqual(capped["available_count"], 5)
        self.assertFalse(capped["details_complete"])
        subscription.update(reset_credits_available=0, reset_credit_expirations=[])
        self.assertEqual(self.result()["reset_credits"], {
            "available_count": 0, "expirations": [], "details_complete": True,
        })

    def test_reset_details_follow_account_and_scope_availability(self):
        self.snapshot["subscription"]["reset_credits_available"] = 2
        self.snapshot["scope"] = "claude"
        self.assertIsNone(self.result()["reset_credits"])
        self.snapshot["scope"] = "combined"
        self.snapshot["subscription"]["account_type"] = "apiKey"
        self.assertIsNone(self.result()["reset_credits"])

    def test_reset_metadata_is_sanitized_without_breaking_token_totals(self):
        self.snapshot["subscription"].update(
            reset_credits_available=True,
            reset_credit_expirations=[
                None, {"count": -1}, {"count": True},
                {"count": 1, "expires_at": -5, "does_not_expire": True, "id": "private-id"},
                {"count": 1, "expires_at": None, "does_not_expire": True},
            ],
        )
        result = self.result()
        info = result["reset_credits"]
        self.assertIsNone(info["available_count"])
        self.assertEqual(info["expirations"], [
            {"count": 1, "expires_at": None, "does_not_expire": False},
            {"count": 1, "expires_at": None, "does_not_expire": True},
        ])
        self.assertNotIn("private-id", json.dumps(result))
        self.assertTrue(any(field["key"] == "today" for field in result["fields"]))

    def test_stale_reset_metadata_keeps_its_original_dates_and_timestamp(self):
        subscription = self.snapshot["subscription"]
        subscription.update(
            state="stale", reset_credits_available=2,
            reset_credit_expirations=[{"count": 2, "expires_at": 1_900_000_000, "does_not_expire": False}],
        )
        result = self.result()
        self.assertEqual(result["subscription_state"], "stale")
        self.assertEqual(result["subscription_fetched_at"], subscription["fetched_at"])
        self.assertEqual(result["reset_credits"]["available_count"], 2)
        self.assertEqual(result["reset_credits"]["expirations"][0]["expires_at"], 1_900_000_000)

    def test_collector_reuses_same_cache_scope_and_five_minute_schedule(self):
        with patch.object(bridge, "run_command", return_value=(0, "GPT Pro")) as run:
            self.assertTrue(bridge.refresh_cache())
        args, kwargs = run.call_args
        command = args[0]
        self.assertEqual(command[command.index("--cache-file") + 1], str(bridge.CACHE))
        self.assertEqual(command[command.index("--scope") + 1], "combined")
        self.assertEqual(command[command.index("--range") + 1], "mtd")
        self.assertEqual(command[command.index("--refresh-interval-minutes") + 1], "5")
        self.assertEqual(kwargs["timeout"], 40)

    def test_zero_exit_code_error_and_timeout_are_failures(self):
        for response in [(0, "AI error · GPT Pro"), (1, "")]:
            with patch.object(bridge, "run_command", return_value=response):
                self.assertFalse(bridge.refresh_cache())
        with patch.object(bridge, "run_command", side_effect=subprocess.TimeoutExpired("collector", 40)):
            self.assertFalse(bridge.refresh_cache())

    def test_resources_are_validated_and_isolated_from_tmux_cache(self):
        with patch.object(bridge, "run_command", return_value=(0, "RAM 72% DSK 61%")) as run:
            self.assertEqual(bridge.resource_payload(), {"ram": "72%", "disk": "61%", "available": True})
        self.assertIn("AIUsageIsland", run.call_args.kwargs["env"]["XDG_CACHE_HOME"])
        with patch.object(bridge, "run_command", return_value=(0, "bad output")):
            self.assertEqual(bridge.resource_payload(), {"ram": "n/a", "disk": "n/a", "available": False})

    def test_timeout_cleans_descendants_after_group_leader_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "child.pid"
            code = (
                "import subprocess,sys,pathlib; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)']); "
                "pathlib.Path(sys.argv[1]).write_text(str(p.pid))"
            )
            with self.assertRaises(subprocess.TimeoutExpired):
                bridge.run_command([sys.executable, "-c", code, str(marker)], timeout=0.2)
            child_pid = int(marker.read_text())
            for _ in range(50):
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.02)
            else:
                self.fail("Collector descendant survived its process-group timeout")
            self.assertFalse(bridge.ACTIVE_CHILDREN)

    def test_process_timeout_reaps_child(self):
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            bridge.run_command([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.05)
        self.assertLess(time.monotonic() - started, 3)
        self.assertFalse(bridge.ACTIVE_CHILDREN)


if __name__ == "__main__":
    unittest.main()
