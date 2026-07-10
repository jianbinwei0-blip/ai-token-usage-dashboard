import datetime as dt
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.chatgpt_subscription import (  # noqa: E402
    CodexAppServerError,
    fetch_chatgpt_subscription_usage,
    normalize_chatgpt_subscription_usage,
)


class ChatgptSubscriptionTests(unittest.TestCase):
    def _fake_codex(
        self,
        directory: Path,
        *,
        account_result: dict,
        rate_limits_result: dict,
        delay_seconds: float = 0.0,
    ) -> tuple[Path, Path]:
        executable = directory / "fake-codex"
        log_path = directory / "methods.jsonl"
        script = textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import json
            import sys
            import time

            account_result = json.loads({json.dumps(json.dumps(account_result))})
            rate_limits_result = json.loads({json.dumps(json.dumps(rate_limits_result))})
            log_path = {str(log_path)!r}
            delay_seconds = {delay_seconds!r}
            initialized = False

            for line in sys.stdin:
                message = json.loads(line)
                method = message.get("method")
                with open(log_path, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(method) + "\\n")
                if delay_seconds:
                    time.sleep(delay_seconds)
                if method == "initialize":
                    print(json.dumps({{"id": message["id"], "result": {{"codexHome": "/tmp", "platformFamily": "unix", "platformOs": "macos", "userAgent": "fake"}}}}), flush=True)
                elif method == "initialized":
                    initialized = True
                elif not initialized:
                    print(json.dumps({{"id": message["id"], "error": {{"message": "Not initialized"}}}}), flush=True)
                elif method == "account/read":
                    print(json.dumps({{"id": message["id"], "result": account_result}}), flush=True)
                elif method == "account/rateLimits/read":
                    print(json.dumps({{"id": message["id"], "result": rate_limits_result}}), flush=True)
            """
        )
        executable.write_text(script, encoding="utf-8")
        executable.chmod(0o700)
        return executable, log_path

    def test_normalizes_chatgpt_plan_windows_and_named_limits(self) -> None:
        now = dt.datetime(2026, 7, 10, 16, 0, tzinfo=dt.timezone.utc)
        account = {
            "account": {
                "type": "chatgpt",
                "email": "private@example.com",
                "planType": "prolite",
            },
            "requiresOpenaiAuth": True,
        }
        rate_limits = {
            "rateLimits": {
                "limitId": "codex",
                "planType": "prolite",
                "primary": {"usedPercent": 12, "windowDurationMins": 300, "resetsAt": 1_800_000_000},
                "secondary": {"usedPercent": 17, "windowDurationMins": 10_080, "resetsAt": 1_800_500_000},
                "credits": {"hasCredits": True, "unlimited": False, "balance": "25.4"},
            },
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "primary": {"usedPercent": 12, "windowDurationMins": 300, "resetsAt": 1_800_000_000},
                    "secondary": {"usedPercent": 17, "windowDurationMins": 10_080, "resetsAt": 1_800_500_000},
                },
                "codex_spark": {
                    "limitId": "codex_spark",
                    "limitName": "GPT-5.3-Codex-Spark",
                    "primary": {"usedPercent": 40, "windowDurationMins": 300, "resetsAt": 1_800_100_000},
                },
            },
            "rateLimitResetCredits": {"availableCount": 3},
        }

        result = normalize_chatgpt_subscription_usage(account, rate_limits, now=now)

        self.assertEqual(result["state"], "ok")
        self.assertEqual(result["account_type"], "chatgpt")
        self.assertEqual(result["plan"], "prolite")
        self.assertEqual(result["fetched_at"], "2026-07-10T16:00:00+00:00")
        self.assertEqual(result["reset_credits_available"], 3)
        self.assertEqual(len(result["limits"]), 2)
        self.assertEqual(result["limits"][0]["id"], "codex")
        self.assertEqual(result["limits"][0]["primary"]["remaining_percent"], 88)
        self.assertEqual(result["limits"][0]["secondary"]["remaining_percent"], 83)
        self.assertEqual(result["limits"][0]["credits"]["balance"], "25.4")
        self.assertEqual(result["limits"][1]["name"], "GPT-5.3-Codex-Spark")
        self.assertNotIn("private@example.com", json.dumps(result))

    def test_clamps_percentages_and_rejects_invalid_windows(self) -> None:
        account = {"account": {"type": "chatgpt", "planType": "plus"}}
        rate_limits = {
            "rateLimits": {
                "primary": {"usedPercent": 140, "windowDurationMins": -1, "resetsAt": 0},
                "secondary": {"usedPercent": -10, "windowDurationMins": 10_080, "resetsAt": 1_900_000_000},
            }
        }

        result = normalize_chatgpt_subscription_usage(account, rate_limits)

        primary = result["limits"][0]["primary"]
        secondary = result["limits"][0]["secondary"]
        self.assertEqual(primary["remaining_percent"], 0)
        self.assertIsNone(primary["window_duration_minutes"])
        self.assertIsNone(primary["resets_at"])
        self.assertEqual(secondary["remaining_percent"], 100)

    def test_non_chatgpt_account_is_not_applicable(self) -> None:
        result = normalize_chatgpt_subscription_usage(
            {"account": {"type": "apiKey"}},
            {},
            now=dt.datetime(2026, 7, 10, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result["state"], "not_applicable")
        self.assertEqual(result["account_type"], "apiKey")
        self.assertNotIn("limits", result)

    def test_fetches_via_app_server_handshake_without_persisting_email(self) -> None:
        account = {
            "account": {"type": "chatgpt", "email": "private@example.com", "planType": "plus"},
            "requiresOpenaiAuth": True,
        }
        rate_limits = {
            "rateLimits": {
                "limitId": "codex",
                "primary": {"usedPercent": 25, "windowDurationMins": 300, "resetsAt": 1_900_000_000},
                "secondary": {"usedPercent": 50, "windowDurationMins": 10_080, "resetsAt": 1_900_500_000},
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            executable, log_path = self._fake_codex(
                Path(tmp),
                account_result=account,
                rate_limits_result=rate_limits,
            )

            result = fetch_chatgpt_subscription_usage(
                codex_binary=executable,
                timeout_seconds=2,
                now=dt.datetime(2026, 7, 10, 16, 0, tzinfo=dt.timezone.utc),
            )

            methods = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                methods,
                ["initialize", "initialized", "account/read", "account/rateLimits/read"],
            )
            self.assertEqual(result["plan"], "plus")
            self.assertEqual(result["limits"][0]["primary"]["remaining_percent"], 75)
            self.assertNotIn("private@example.com", json.dumps(result))

    def test_fetch_skips_rate_limit_request_for_api_key_account(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            executable, log_path = self._fake_codex(
                Path(tmp),
                account_result={"account": {"type": "apiKey"}, "requiresOpenaiAuth": True},
                rate_limits_result={},
            )

            result = fetch_chatgpt_subscription_usage(codex_binary=executable, timeout_seconds=2)

            methods = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(methods, ["initialize", "initialized", "account/read"])
            self.assertEqual(result["state"], "not_applicable")

    def test_fetch_enforces_an_overall_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            executable, _log_path = self._fake_codex(
                Path(tmp),
                account_result={"account": {"type": "chatgpt", "planType": "plus"}},
                rate_limits_result={},
                delay_seconds=1.0,
            )

            with self.assertRaisesRegex(CodexAppServerError, "timed out"):
                fetch_chatgpt_subscription_usage(codex_binary=executable, timeout_seconds=0.1)


if __name__ == "__main__":
    unittest.main()
