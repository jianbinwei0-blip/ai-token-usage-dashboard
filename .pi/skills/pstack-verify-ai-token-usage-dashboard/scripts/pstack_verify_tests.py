#!/usr/bin/env python3
"""Safety contracts for the local verification helpers, without live processes."""
import datetime as dt
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("pstack_verifier", Path(__file__).with_name("pstack_verify.py"))
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class PstackVerificationSafety(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pstack-verify-safety-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run = self.root / "pstack-run"
        (self.run / "pstack-evidence").mkdir(parents=True)
        (self.run / "pstack-scratch").mkdir()
        self.state = {"owner": verify.OWNER, "repo": str(verify.REPO), "port": 65001,
                      "url": "http://127.0.0.1:65001", "status": "stopped", "sourceSignatures": {}}

    def test_refuse_unowned_or_production_target(self):
        with patch.object(verify, "RUNS", self.root):
            verify.save(self.run / verify.STATE, {**self.state, "owner": "someone-else"})
            with self.assertRaisesRegex(ValueError, "Not an owned"):
                verify.load(self.run)
            verify.save(self.run / verify.STATE, {**self.state, "port": 8765, "url": "http://127.0.0.1:8765"})
            with self.assertRaisesRegex(ValueError, "non-production loopback"):
                verify.load(self.run)
            verify.save(self.run / verify.STATE, {**self.state, "url": "https://example.invalid:65001"})
            with self.assertRaisesRegex(ValueError, "non-production loopback"):
                verify.load(self.run)

    def test_fixture_is_deterministic_and_only_synthetic(self):
        state = {**self.state, "today": "2026-11-03"}
        verify.seed(self.run, state)
        self.assertEqual(verify.oracle(state)["tokens"], 2410000)
        self.assertAlmostEqual(verify.oracle(state)["cost"], 2.269)
        self.assertEqual(verify.oracle(state, "pi")["tokens"], 350000)
        env = verify.environment(self.run, state)
        self.assertEqual(env["AI_USAGE_CHATGPT_USAGE"], "off")
        for key in ["AI_USAGE_CODEX_SESSIONS_ROOT", "AI_USAGE_CLAUDE_PROJECTS_ROOT", "AI_USAGE_PI_AGENT_ROOT", "AI_USAGE_DSH_HOME", "HOME"]:
            self.assertTrue(Path(env[key]).is_relative_to(self.run))
        self.assertNotIn("OPENAI_API_KEY", env)
        self.assertNotIn("ANTHROPIC_API_KEY", env)

    def test_repeated_evidence_never_overwrites_the_first_observation(self):
        first = verify.evidence(self.run, "pstack-api.json", {"tokens": 1})
        second = verify.evidence(self.run, "pstack-api.json", {"tokens": 2})
        self.assertNotEqual(first, second)
        self.assertEqual(json.loads(first.read_text()), {"tokens": 1})
        self.assertEqual(json.loads(second.read_text()), {"tokens": 2})

    def test_reused_pid_is_never_killed_or_cleaned(self):
        state = {**self.state, "pid": 99999999, "status": "running", "processIdentity": "owned creation identity"}
        with patch.object(verify, "identity", return_value="different identity"), patch.object(verify.os, "kill") as kill:
            with self.assertRaisesRegex(RuntimeError, "PID identity changed"):
                verify.stop(self.run, state)
            kill.assert_not_called()
        self.assertTrue((self.run / "pstack-scratch").is_dir())

    def test_cleanup_preserves_nested_evidence(self):
        nested = self.run / "pstack-evidence/pstack-browser-1"
        nested.mkdir()
        screenshot = nested / "pstack-screen.png"
        screenshot.write_bytes(b"evidence")
        with patch.object(verify, "signatures", return_value={}), patch.object(verify.socket, "socket") as socket:
            socket.return_value.__enter__.return_value.connect_ex.return_value = 1
            result = verify.stop(self.run, self.state)
        self.assertTrue(result["scratchRemoved"])
        self.assertTrue(screenshot.exists())
        self.assertIn("pstack-browser-1/pstack-screen.png", result["retainedEvidence"])
        self.assertTrue((self.run / "pstack-evidence/pstack-cleanup.json").exists())

    def test_cleanup_refuses_a_scratch_symlink(self):
        (self.run / "pstack-scratch").rmdir()
        other = self.root / "unowned"
        other.mkdir()
        (other / "keep.txt").write_text("keep")
        (self.run / "pstack-scratch").symlink_to(other, target_is_directory=True)
        with patch.object(verify.socket, "socket") as socket:
            socket.return_value.__enter__.return_value.connect_ex.return_value = 1
            with self.assertRaisesRegex(RuntimeError, "Scratch is a symlink"):
                verify.stop(self.run, self.state)
        self.assertEqual((other / "keep.txt").read_text(), "keep")

    def test_source_drift_blocks_doctor_before_any_http_drive(self):
        state = {**self.state, "status": "running", "pid": 99999999, "processIdentity": "owned",
                 "today": dt.date.today().isoformat()}
        with patch.object(verify, "identity", return_value="owned"), patch.object(verify, "signatures", return_value={"changed": True}), patch.object(verify, "request") as request:
            with self.assertRaisesRegex(AssertionError, "Product source changed"):
                verify.doctor(self.run, state)
            request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
