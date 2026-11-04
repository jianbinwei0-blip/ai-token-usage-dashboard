from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.config import DashboardConfig
from dashboard_core.settings import SettingsError, default_history_path, default_settings_path, load_settings, validate_dashboard_url
import usage_doctor as doctor


class SettingsTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.home = Path(self.scratch.name)
        self.file = self.home / "settings.json"
        self.repo = self.home / "checkout"
        self.env = {"HOME": str(self.home), "AI_USAGE_SETTINGS_FILE": str(self.file)}

    def save(self, **values):
        self.file.write_text(json.dumps(dict(version=1, **values)), encoding="utf-8")

    def load(self, **kwargs):
        return load_settings(self.repo, environ=self.env, home=self.home, **kwargs)

    def config(self):
        with patch.dict(os.environ, self.env, clear=True):
            return DashboardConfig.from_env(self.repo)

    def test_platform_default_paths(self):
        self.assertEqual(default_settings_path(environ={}, home=self.home, platform="darwin"),
                         self.home / "Library/Application Support/ai-token-usage-dashboard/settings.json")
        self.assertEqual(default_settings_path(environ={}, home=self.home, platform="linux"),
                         self.home / ".config/ai-token-usage-dashboard/settings.json")
        self.assertEqual(default_settings_path(environ={"XDG_CONFIG_HOME": str(self.home / "xdg")}, home=self.home, platform="linux"),
                         self.home / "xdg/ai-token-usage-dashboard/settings.json")
        self.assertEqual(default_settings_path(environ={"APPDATA": str(self.home / "roaming")}, home=self.home, platform="win32"),
                         self.home / "roaming/ai-token-usage-dashboard/settings.json")

    def test_absent_default_is_read_only_and_html_is_disposable(self):
        self.env.pop("AI_USAGE_SETTINGS_FILE")
        result = self.load()
        self.assertFalse(result.settings_loaded)
        cfg = self.config()
        self.assertEqual(cfg.dashboard_html, self.repo / "tmp/index.runtime.html")
        self.assertEqual(cfg.parse_cache_file, default_history_path(environ=self.env, home=self.home))
        self.assertEqual(cfg.legacy_parse_cache_file, self.repo / "tmp/recalc_parse_cache.json")
        self.assertEqual(cfg.runtime_dir, self.repo)
        self.assertEqual(list(self.home.iterdir()), [])

    def test_history_path_precedence_preserves_the_legacy_explicit_alias(self):
        self.save(history_file="~/saved-history.json")
        self.assertEqual(self.config().parse_cache_file, self.home / "saved-history.json")
        self.env["AI_USAGE_PARSE_CACHE_FILE"] = str(self.home / "explicit-legacy.json")
        self.assertEqual(self.config().parse_cache_file, self.home / "explicit-legacy.json")
        self.env["AI_USAGE_HISTORY_FILE"] = str(self.home / "explicit-history.json")
        self.assertEqual(self.config().parse_cache_file, self.home / "explicit-history.json")
        self.assertFalse((self.home / "explicit-history.json").exists())

    def test_explicit_missing_settings_fail_closed(self):
        with self.assertRaisesRegex(SettingsError, "does not exist"):
            self.load()
        self.assertFalse(self.file.exists())

    def test_saved_runtime_roots_pricing_and_url_are_shared_with_core(self):
        self.save(runtime_dir="~/runtime", dashboard_url="http://localhost:9321/dashboard/",
                  pricing_file="~/prices.json", codex_sessions_root="~/sources/codex",
                  claude_projects_root="~/sources/claude", pi_agent_root="~/sources/pi", dsh_home="~/sources/dsh")
        cfg = self.config()
        self.assertEqual(cfg.runtime_dir, self.home / "runtime")
        self.assertEqual(cfg.dashboard_html, self.home / "runtime/tmp/index.runtime.html")
        self.assertEqual(cfg.recalc_log_file, self.home / "runtime/tmp/recalc_timings.jsonl")
        self.assertEqual(cfg.dashboard_url, "http://localhost:9321/dashboard/")
        self.assertEqual((cfg.host, cfg.port), ("localhost", 9321))
        self.assertEqual(cfg.pricing_file, self.home / "prices.json")
        self.assertEqual(cfg.sessions_root, self.home / "sources/codex")
        self.assertEqual(cfg.claude_projects_root, self.home / "sources/claude")
        self.assertEqual(cfg.pi_agent_root, self.home / "sources/pi")
        self.assertEqual(cfg.dsh_home, self.home / "sources/dsh")
        self.assertEqual(cfg.settings_file, self.file)
        self.assertTrue(self.load().settings_loaded)
        self.assertEqual(self.load().sources["runtime_dir"], "saved")

    def test_every_explicit_override_survives_saved_settings(self):
        self.save(runtime_dir="~/saved", dashboard_url="http://localhost:9321/path", pricing_file="~/saved-prices.json",
                  codex_sessions_root="~/saved-codex", claude_projects_root="~/saved-claude",
                  pi_agent_root="~/saved-pi", dsh_home="~/saved-dsh")
        names = {
            "AI_USAGE_DASHBOARD_DIR": "runtime", "AI_USAGE_DASHBOARD_HTML": "html",
            "AI_USAGE_CODEX_SESSIONS_ROOT": "codex", "AI_USAGE_CLAUDE_PROJECTS_ROOT": "claude",
            "AI_USAGE_PI_AGENT_ROOT": "pi", "AI_USAGE_DSH_HOME": "dsh", "AI_USAGE_PRICING_FILE": "prices",
            "AI_USAGE_PARSE_CACHE_FILE": "parse", "AI_USAGE_RECALC_LOG_FILE": "log",
        }
        self.env.update({key: str(self.home / value) for key, value in names.items()})
        self.env.update(AI_USAGE_SERVER_HOST="0.0.0.0", AI_USAGE_SERVER_PORT="9001",
                        AI_USAGE_DASHBOARD_URL="https://127.0.0.1:9443/custom")
        cfg = self.config()
        self.assertEqual((cfg.host, cfg.port), ("0.0.0.0", 9001))
        self.assertEqual(cfg.dashboard_url, "https://127.0.0.1:9443/custom")
        for attribute, value in {
            "runtime_dir": "runtime", "dashboard_html": "html", "sessions_root": "codex",
            "claude_projects_root": "claude", "pi_agent_root": "pi", "dsh_home": "dsh",
            "pricing_file": "prices", "parse_cache_file": "parse", "recalc_log_file": "log",
        }.items():
            self.assertEqual(getattr(cfg, attribute), self.home / value)

    def test_port_override_updates_saved_browser_url_without_wildcard_address(self):
        self.save(dashboard_url="http://localhost:9321/path")
        self.env.update(AI_USAGE_SERVER_HOST="0.0.0.0", AI_USAGE_SERVER_PORT="9001")
        self.assertEqual(self.load().dashboard_url, "http://127.0.0.1:9001/path")
        self.env["AI_USAGE_SERVER_HOST"] = "::"
        self.assertEqual(self.load().dashboard_url, "http://[::1]:9001/path")
        self.env["AI_USAGE_SERVER_HOST"] = "example.invalid"
        self.assertEqual(self.load().dashboard_url, "http://localhost:9001/path")

    def test_saved_null_or_empty_environment_explicitly_select_builtin_pricing(self):
        conventional = self.home / ".config/ai-token-usage-dashboard/pricing.json"
        conventional.parent.mkdir(parents=True)
        conventional.write_text("not inspected")
        self.save()
        self.assertEqual(self.load().pricing_file, conventional)
        self.save(pricing_file=None)
        self.assertIsNone(self.load().pricing_file)
        self.save(pricing_file="~/saved-prices.json")
        self.env["AI_USAGE_PRICING_FILE"] = ""
        self.assertIsNone(self.load().pricing_file)
        self.env["AI_USAGE_PRICING_FILE"] = str(self.home / "explicit.json")
        self.assertEqual(self.load().pricing_file, self.home / "explicit.json")

    def test_saved_schema_is_strict_even_when_masked(self):
        self.env["AI_USAGE_DASHBOARD_DIR"] = str(self.home / "explicit")
        bad_files = ["[1]", "{}", '{"version":true}', '{"version":2}',
                     '{"version":1,"unknown":"hidden-settings-value"}',
                     '{"version":1,"version":1}', '{"version":1,"runtime_dir":"relative"}',
                     '{"version":1,"runtime_dir":null}', '{"version":1,"dsh_home":12}',
                     '{"version":1,"dashboard_url":"https://remote.invalid"}',
                     '{"version":1,"pricing_file":"$HOME/pricing.json"}', "{", "x" * 65537]
        for content in bad_files:
            with self.subTest(content=content[:64]):
                self.file.write_text(content)
                with self.assertRaises(SettingsError) as error:
                    self.load()
                self.assertNotIn("hidden-settings-value", str(error.exception))

    def test_url_validation_rejects_unsafe_browser_destinations(self):
        for value in ["http://localhost", "https://127.0.0.1:9443/", "http://[::1]:8765", "http://127.1.2.3:80/path"]:
            self.assertEqual(validate_dashboard_url(value), value)
        for value in ["file:///etc/passwd", "javascript:alert(1)", "https://example.com", "//localhost",
                      "http://127.0.0.1.evil", "http://localhost.evil", "http://localhost@evil.test",
                      "http://user:secret@localhost", "http://0.0.0.0:8765", "http://[::]:8765",
                      "http://localhost:0", "http://localhost:65536", "http://localhost:",
                      "http://localhost?token=secret", "http://localhost#secret", "http://localhost\\@evil.test",
                      "http://localhost\n", "http://[::1%25lo0]:80", None, 123]:
            with self.subTest(value=value), self.assertRaises(SettingsError):
                validate_dashboard_url(value)

    def test_invalid_environment_is_not_silently_replaced_with_defaults(self):
        self.save()
        for key, value in [("AI_USAGE_SETTINGS_FILE", "relative"), ("AI_USAGE_DASHBOARD_DIR", ""),
                           ("AI_USAGE_SERVER_PORT", "0"), ("AI_USAGE_SERVER_PORT", "bad"),
                           ("AI_USAGE_DASHBOARD_URL", "https://remote.invalid")]:
            with self.subTest(key=key, value=value):
                env = dict(self.env, **{key: value})
                with self.assertRaises(SettingsError):
                    load_settings(self.repo, environ=env, home=self.home)


class DoctorTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.home = Path(self.scratch.name)
        self.runtime = self.home / "runtime"
        self.now = dt.datetime(2026, 11, 4, 12, tzinfo=dt.timezone.utc)
        self.env_patch = patch.dict(os.environ, {
            "HOME": str(self.home), "AI_USAGE_SETTINGS_FILE": str(self.home / "settings.json"),
            "AI_USAGE_PRICING_FILE": "",
        }, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.sources = {name: str(self.home / "sources" / name) for name in (
            "codex_sessions_root", "claude_projects_root", "pi_agent_root", "dsh_home")}
        for path in self.sources.values():
            Path(path).mkdir(parents=True)
            (Path(path) / "transcript.jsonl").write_text("private-transcript-never-print")
        self.settings = self.home / "settings.json"
        self.settings.write_text(json.dumps(dict(version=1, runtime_dir=str(self.runtime), history_file=str(self.runtime / "tmp/recalc_parse_cache.json"), **self.sources)))
        for name in doctor.RUNTIME_FILES:
            dest = self.runtime / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(doctor.REPO_ROOT / name, dest)
        self.cache = self.runtime / "tmp/tmux_status.json"
        self.cache.parent.mkdir()
        self.snapshot = {
            "version": doctor._snapshot_version(doctor.REPO_ROOT / "scripts/dashboard_core/tmux_status.py"),
            "generated_at": self.now.isoformat(), "health": "ok", "providers": ["codex"],
            "metrics": {"today_tokens": 987654321, "range_tokens": 987654321, "range_cost_usd": 123456789},
            "subscription": {"plan": "private-account", "token": "private-token"},
            "extra": "private-cache-marker",
        }
        self.write_cache()
        self.parse_cache = self.runtime / "tmp/recalc_parse_cache.json"
        self.parse_cache.write_text(json.dumps({
            "version": doctor._source_version(doctor.REPO_ROOT / "scripts/dashboard_core/collectors.py", "_PERSISTENT_CACHE_VERSION"),
            "codex": {"private-record-path": "private-retained-content"},
        }))
        (self.runtime / "tmp/index.runtime.html").write_text("private-generated-html")

    def write_cache(self):
        self.cache.write_text(json.dumps(self.snapshot))

    def report(self):
        return doctor.build_report(now=self.now)

    def test_metadata_only_report_is_read_only_and_never_starts_collection(self):
        before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.home.rglob("*") if p.is_file()}
        with patch("subprocess.Popen", side_effect=AssertionError("No subprocess allowed")):
            report = self.report()
        self.assertTrue(report["ok"], report)
        self.assertTrue(report["read_only"])
        self.assertEqual(report["cache"]["freshness"], "fresh")
        self.assertEqual(report["runtime"]["status"], "ok")
        text = json.dumps(report)
        for secret in ["987654321", "123456789", "private-account", "private-token", "private-cache-marker",
                       "private-transcript-never-print", "private-record-path", "private-retained-content", "private-generated-html"]:
            self.assertNotIn(secret, text)
        self.assertEqual(before, {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.home.rglob("*") if p.is_file()})

    def test_runtime_missing_drift_and_missing_cache_are_distinct(self):
        self.cache.unlink()
        report = self.report()
        self.assertEqual(report["cache"]["status"], "missing")
        self.assertEqual(report["runtime"]["status"], "ok")
        settings_source = self.runtime / "scripts/dashboard_core/settings.py"
        settings_source.write_text("SETTINGS_VERSION = 99\n")
        report = self.report()
        self.assertEqual(report["runtime"]["status"], "drift")
        self.assertEqual(report["runtime"]["settings_version"], 99)
        shutil.rmtree(self.runtime)
        self.assertEqual(self.report()["runtime"]["status"], "missing")

    def test_missing_source_is_not_mistaken_for_runtime_failure(self):
        shutil.rmtree(Path(self.sources["dsh_home"]))
        report = self.report()
        self.assertEqual(report["sources"]["dsh"]["status"], "missing")
        self.assertEqual(report["runtime"]["status"], "ok")
        self.assertTrue(any("provider you do not use" in step for step in report["recovery"]))

    def test_cache_schema_freshness_and_collection_failure_have_recovery(self):
        for changes, expected in [
            ({"version": 999}, ("incompatible", "fresh")),
            ({"generated_at": (self.now - dt.timedelta(minutes=6)).isoformat()}, ("ok", "stale")),
            ({"generated_at": (self.now + dt.timedelta(minutes=6)).isoformat()}, ("ok", "clock_skew")),
            ({"generated_at": "private-invalid-timestamp"}, ("ok", "unknown")),
            ({"health": "error"}, ("ok", "fresh")),
            ({"health": {"private-secret": 1}}, ("ok", "fresh")),
        ]:
            with self.subTest(changes=changes):
                original = dict(self.snapshot)
                self.snapshot.update(changes)
                self.write_cache()
                report = self.report()
                self.assertEqual((report["cache"]["status"], report["cache"]["freshness"]), expected)
                self.assertFalse(report["ok"])
                self.assertTrue(any("--chatgpt-usage off" in step for step in report["recovery"]))
                self.snapshot = original

    def test_malformed_or_nonfinite_cache_is_not_reported_as_compatible(self):
        for metrics in [[], {"today_tokens": True, "range_tokens": 1},
                        {"today_tokens": float("nan"), "range_tokens": 1},
                        {"today_tokens": 1, "range_tokens": -1}]:
            with self.subTest(metrics=metrics):
                self.snapshot["metrics"] = metrics
                self.write_cache()
                report = self.report()
                self.assertEqual(report["cache"]["status"], "invalid")
                self.assertFalse(report["ok"])

    def test_corrupt_or_incompatible_history_is_preserved_with_backup_guidance(self):
        for raw in ["{", '{"version":999}']:
            self.parse_cache.write_text(raw)
            report = self.report()
            self.assertIn(report["parse_cache"]["status"], {"invalid", "incompatible"})
            self.assertTrue(any("deleted logs" in step and "preserve" in step for step in report["recovery"]))
            self.assertEqual(self.parse_cache.read_text(), raw)

    def test_invalid_settings_cli_never_echoes_contents(self):
        self.settings.write_text('{"version":1,"secret":"must-not-print-this"}')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = doctor.main(["--json"])
        self.assertEqual(code, 2)
        report = json.loads(output.getvalue())
        self.assertTrue(report["read_only"])
        self.assertEqual(report["settings"]["status"], "invalid")
        self.assertNotIn("must-not-print-this", output.getvalue())


if __name__ == "__main__":
    unittest.main()
