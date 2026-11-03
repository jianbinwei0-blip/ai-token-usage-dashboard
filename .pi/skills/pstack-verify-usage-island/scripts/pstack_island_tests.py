#!/usr/bin/env python3
"""Safety/lifecycle checks; native behavioral coverage lives in the real app suite."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("pstack_island", Path(__file__).with_name("pstack_island.py"))
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class SafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pstack-island-safety-")
        self.root = Path(self.temp.name).resolve()
        self.runs = self.root / "runs"
        self.runs.mkdir()
        self.patch = patch.object(verify, "RUNS", self.runs)
        self.patch.start()
        self.run = self.runs / "20261103T120000-1234abcd"
        self.run.mkdir()
        (self.run / "pstack-evidence").mkdir()
        self.state = {"owner": verify.OWNER, "repo": str(verify.REPO),
                      "bundleID": "local.pstack.UsageIsland." + self.run.name.replace("-", ""),
                      "pid": None, "protected": {"installedExecutable": str(self.root / "absent-app"),
                      "installedHash": None, "preferences": str(self.root / "absent-prefs"),
                      "preferencesHash": None, "processes": {}}}
        verify.save(self.run / verify.STATE, self.state)

    def tearDown(self):
        self.patch.stop()
        self.temp.cleanup()

    def test_accepts_owned_record_but_not_neighbor_or_wrong_owner(self):
        self.assertEqual(verify.load(self.run)[0], self.run)
        outside = self.root / self.run.name
        outside.mkdir()
        verify.save(outside / verify.STATE, self.state)
        with self.assertRaises(ValueError):
            verify.load(outside)
        self.state["owner"] = "someone-else"
        verify.save(self.run / verify.STATE, self.state)
        with self.assertRaises(ValueError):
            verify.load(self.run)

    def test_rejects_symlink_run_before_resolution(self):
        alias = self.runs / "20261103T120001-1234abcd"
        alias.symlink_to(self.run, target_is_directory=True)
        with self.assertRaises(ValueError):
            verify.load(alias)

    def test_rejects_symlinked_state_scratch_and_evidence(self):
        for name in [verify.STATE, "pstack-scratch", "pstack-evidence"]:
            with self.subTest(name=name):
                target = self.run / name
                moved = self.root / (name + "-outside")
                if target.exists():
                    target.rename(moved)
                else:
                    moved.mkdir()
                target.symlink_to(moved)
                with self.assertRaises(ValueError):
                    verify.load(self.run)
                target.unlink()
                moved.rename(target)

    def test_rejects_production_bundle_identifier(self):
        self.state["bundleID"] = "local.jwei.AIUsageIsland"
        verify.save(self.run / verify.STATE, self.state)
        with self.assertRaises(ValueError):
            verify.load(self.run)

    def test_private_environment_does_not_inherit_live_overrides_or_bridge(self):
        with patch.dict("os.environ", {"AI_USAGE_BRIDGE_DIR": "/private/live", "AI_USAGE_CACHE_FILE": "/live/cache",
                                      "AI_USAGE_CHATGPT_USAGE": "auto", "SECRET_TOKEN": "do-not-copy"}):
            env = verify.environment(self.run)
        self.assertEqual(env["HOME"], env["CFFIXED_USER_HOME"])
        self.assertTrue(Path(env["HOME"]).is_relative_to(verify.scratch(self.run)))
        self.assertNotIn("SECRET_TOKEN", env)
        self.assertNotIn("AI_USAGE_BRIDGE_DIR", env)
        self.assertEqual(env["AI_USAGE_CHATGPT_USAGE"], "off")
        for key in ["AI_USAGE_CACHE_FILE", "AI_USAGE_DASHBOARD_DIR", "AI_USAGE_CODEX_SESSIONS_ROOT",
                    "AI_USAGE_CLAUDE_PROJECTS_ROOT", "AI_USAGE_PI_AGENT_ROOT", "AI_USAGE_DSH_HOME"]:
            self.assertTrue(Path(env[key]).is_relative_to(verify.scratch(self.run)), key)

    def test_refuses_pid_reuse_without_signalling(self):
        self.state.update(pid=321, identity="original birth and command")
        with patch.object(verify, "identity", return_value="different process"), patch.object(verify.os, "kill") as kill:
            with self.assertRaises(RuntimeError):
                verify.stop_owned(self.run, self.state)
            kill.assert_not_called()

    def test_refuses_matching_birth_with_wrong_executable(self):
        self.state.update(pid=321, identity="matching identity")
        with patch.object(verify, "identity", return_value="matching identity"), \
             patch.object(verify, "comm", return_value="/Applications/Live.app/Contents/MacOS/UsageIsland"), \
             patch.object(verify.os, "kill") as kill:
            with self.assertRaises(RuntimeError):
                verify.stop_owned(self.run, self.state)
            kill.assert_not_called()

    def test_cleanup_refusal_retains_scratch_on_identity_drift(self):
        verify.scratch(self.run).mkdir()
        (verify.scratch(self.run) / "retain").write_text("owned scratch")
        with patch.object(verify, "stop_owned", side_effect=RuntimeError("identity drift")):
            with self.assertRaises(RuntimeError):
                verify.cleanup(self.run, self.state)
        self.assertTrue((verify.scratch(self.run) / "retain").exists())

    def test_cleanup_is_idempotent_and_preserves_nested_evidence(self):
        verify.scratch(self.run).mkdir()
        (verify.scratch(self.run) / "remove").write_text("scratch")
        screenshot = self.run / "pstack-evidence/native/expanded.png"
        screenshot.parent.mkdir()
        screenshot.write_bytes(b"retained fake image")
        first = verify.cleanup(self.run, self.state)
        second = verify.cleanup(self.run, self.state)
        self.assertTrue(first["ok"] and second["ok"])
        self.assertFalse(verify.scratch(self.run).exists())
        self.assertEqual(screenshot.read_bytes(), b"retained fake image")
        self.assertTrue(json.loads((self.run / verify.STATE).read_text())["stopped"])
        self.assertEqual(len(list((self.run / "pstack-evidence").glob("pstack-cleanup*.json"))), 2)

    def test_evidence_is_append_only(self):
        a = verify.evidence(self.run, "pstack-result.json", {"ok": False})
        b = verify.evidence(self.run, "pstack-result.json", {"ok": True})
        self.assertNotEqual(a, b)
        self.assertFalse(json.loads(a.read_text())["ok"])
        self.assertTrue(json.loads(b.read_text())["ok"])

    def test_cleaned_run_cannot_launch(self):
        self.state["stopped"] = True
        with self.assertRaises(RuntimeError), patch.object(verify.subprocess, "Popen") as popen:
            verify.launch(self.run, self.state)
        popen.assert_not_called()

    def test_exclusive_driver_refuses_another_lock_holder(self):
        with verify.exclusive():
            with self.assertRaises(RuntimeError):
                with verify.exclusive():
                    self.fail("Must not acquire a second driver lock")


if __name__ == "__main__":
    unittest.main(verbosity=2)
