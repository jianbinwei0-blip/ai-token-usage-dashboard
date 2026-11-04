"""Installer failure injection against synthetic bundles and a private HOME only."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

INSTALLER = Path(__file__).resolve().parents[1] / "scripts/install.sh"

STUB = r'''
import json, os
from pathlib import Path
import shutil
import subprocess
import sys
name = Path(sys.argv[0]).name
args = sys.argv[1:]
log = Path(os.environ["INSTALL_LOG_FILE"])
with log.open("a") as handle:
    handle.write(json.dumps(dict(tool=name, args=args)) + "\n")
mode = os.environ.get("INSTALL_FAILURE", "")
destination = Path(os.environ["HOME"]) / "Applications/AI Usage Island.app"
if name == "pgrep":
    count = sum(json.loads(line)["tool"] == "pgrep" for line in log.read_text().splitlines())
    sys.exit(2 if mode == "probe-error" else 0 if mode == "running" or (mode == "late-running" and count >= 2) else 1)
if name == "ditto":
    if mode == "copy":
        Path(args[1]).mkdir(parents=True)
        (Path(args[1]) / "partial").write_text("partial candidate")
        sys.exit(41)
    shutil.copytree(args[0], args[1])
    if mode == "incomplete":
        (Path(args[1]) / "Contents/Info.plist").unlink()
    sys.exit(0)
if name == "codesign":
    staged = ".AIUsageIsland-install." in args[-1]
    if (mode == "verify-stage" and staged) or (mode in {"verify-final", "rollback"} and not staged):
        sys.exit(42)
    sys.exit(0)
if name == "open":
    sys.exit(43 if mode == "launch" else 0)
if name == "mv":
    source, target = map(Path, args)
    promotion = ".AIUsageIsland-install." in str(source) and target == destination
    backup = source == destination and "/Backups/" in str(target)
    restore = "/Backups/" in str(source) and target == destination
    if ((promotion and mode == "promote") or (backup and mode == "backup")
            or (restore and mode == "rollback")):
        sys.exit(44)
    if promotion and mode == "partial-promote":
        shutil.move(str(source), str(target))
        sys.exit(45)
    sys.exit(subprocess.run(["/bin/mv", *args]).returncode)
raise AssertionError("Unexpected stub tool")
'''


@unittest.skipUnless(os.name == "posix", "bash installer requires a POSIX host")
class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.app_root = self.root / "app-source"
        self.script = self.app_root / "scripts/install.sh"
        self.script.parent.mkdir(parents=True)
        shutil.copyfile(INSTALLER, self.script)
        self.source = self.app_root / "dist/AI Usage Island.app"
        self.bundle(self.source, "new app")
        self.destination = self.home / "Applications/AI Usage Island.app"
        self.bundle(self.destination, "old app")
        self.source_before = self.tree(self.source)
        self.previous = self.tree(self.destination)
        self.backups = self.home / "Library/Application Support/AIUsageIsland/Backups"
        self.log = self.root / "tools.jsonl"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for tool in ["ditto", "codesign", "pgrep", "open", "mv"]:
            path = self.bin / tool
            path.write_text(f"#!{sys.executable}\n" + STUB)
            path.chmod(0o755)
        self.retained = {}
        for relative in ["runtime/tmp/recalc_parse_cache.json", "config/settings.json", "config/pricing.json"]:
            path = self.home / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"retained fixture, never installer-owned")
            self.retained[path] = path.read_bytes()

    def bundle(self, path, label):
        executable = path / "Contents/MacOS/UsageIsland"
        executable.parent.mkdir(parents=True)
        executable.write_text(label)
        executable.chmod(0o755)
        (path / "Contents/Info.plist").write_text("synthetic plist: " + label)
        (path / "retained-resource.txt").write_text("synthetic resource: " + label)

    def tree(self, directory):
        return {str(p.relative_to(directory)): p.read_bytes() for p in directory.rglob("*") if p.is_file()}

    def run_install(self, mode="", args=None):
        env = {
            "HOME": str(self.home), "PATH": f"{self.bin}:/usr/bin:/bin",
            "INSTALL_LOG_FILE": str(self.log), "INSTALL_FAILURE": mode,
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        return subprocess.run(["/bin/bash", str(self.script), *(args if args is not None else ["--no-open"])],
                              env=env, capture_output=True, text=True, timeout=15)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def assert_untouched_data_and_clean_staging(self):
        self.assertEqual({p: p.read_bytes() for p in self.retained}, self.retained)
        self.assertEqual(self.tree(self.source), self.source_before)
        self.assertEqual(list((self.home / "Applications").glob(".AIUsageIsland-install.*")), [])
        self.assertFalse((self.home / "Applications/.AIUsageIsland-install.lock").exists())

    def test_all_unknown_extra_or_empty_arguments_are_rejected_before_mutation(self):
        before = self.tree(self.home)
        for args in [["--unknown"], ["--no-open", "extra"], ["--no-open", "--no-open"], [""], ["--help", "--no-open"]]:
            with self.subTest(args=args):
                result = self.run_install(args=args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(self.tree(self.home), before)
                self.assertEqual(self.calls(), [])
                self.assertFalse(self.backups.exists())

    def test_help_is_read_only(self):
        before = self.tree(self.home)
        result = self.run_install(args=["--help"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.tree(self.home), before)
        self.assertEqual(self.calls(), [])

    def test_success_verifies_before_replacement_and_retains_previous_bundle(self):
        result = self.run_install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.tree(self.destination), self.source_before)
        backups = list(self.backups.glob("*/AI Usage Island.app"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(self.tree(backups[0]), self.previous)
        calls = self.calls()
        tools = [call["tool"] for call in calls]
        self.assertLess(tools.index("codesign"), tools.index("mv"))
        self.assertEqual(tools.count("codesign"), 2)
        self.assertNotIn("open", tools)
        self.assert_untouched_data_and_clean_staging()

    def test_default_launch_is_stubbed_and_only_happens_after_final_verification(self):
        result = self.run_install(args=[])
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual(calls[-1], {"tool": "open", "args": [str(self.destination)]})
        self.assertEqual(calls[-2]["tool"], "codesign")
        self.assert_untouched_data_and_clean_staging()

    def test_copy_incomplete_stage_and_stage_verification_failures_never_move_previous(self):
        for mode in ["copy", "incomplete", "verify-stage"]:
            with self.subTest(mode=mode):
                self.log.unlink(missing_ok=True)
                result = self.run_install(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.tree(self.destination), self.previous)
                self.assertNotIn("mv", [call["tool"] for call in self.calls()])
                self.assertNotIn("Installed:", result.stdout)
                self.assert_untouched_data_and_clean_staging()

    def test_promotion_final_verification_backup_and_launch_failures_restore_previous(self):
        for mode in ["promote", "partial-promote", "verify-final", "backup", "launch"]:
            with self.subTest(mode=mode):
                result = self.run_install(mode, args=[] if mode == "launch" else None)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.tree(self.destination), self.previous)
                self.assertNotIn("Installed:", result.stdout)
                self.assert_untouched_data_and_clean_staging()

    def test_failed_first_install_removes_only_failed_candidate(self):
        shutil.rmtree(self.destination)
        result = self.run_install("verify-final")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.destination.exists())
        self.assert_untouched_data_and_clean_staging()

    def test_failed_rollback_preserves_previous_at_named_recovery_location(self):
        result = self.run_install("rollback")
        self.assertNotEqual(result.returncode, 0)
        backups = list(self.backups.glob("*/AI Usage Island.app"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(self.tree(backups[0]), self.previous)
        self.assertIn(str(backups[0]), result.stderr)
        self.assertIn("Automatic rollback failed", result.stderr)
        self.assertFalse(self.destination.exists())
        self.assert_untouched_data_and_clean_staging()

    def test_running_or_late_started_app_prevents_replacement(self):
        for mode in ["running", "late-running"]:
            with self.subTest(mode=mode):
                self.log.unlink(missing_ok=True)
                result = self.run_install(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.tree(self.destination), self.previous)
                self.assertNotIn("mv", [call["tool"] for call in self.calls()])
                self.assert_untouched_data_and_clean_staging()

    def test_process_probe_error_fails_closed(self):
        result = self.run_install("probe-error")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.tree(self.destination), self.previous)
        self.assertEqual([call["tool"] for call in self.calls()], ["pgrep"])
        self.assert_untouched_data_and_clean_staging()

    def test_symlink_destination_is_refused_without_touching_target(self):
        target = self.root / "other-app"
        shutil.move(self.destination, target)
        self.destination.symlink_to(target, target_is_directory=True)
        result = self.run_install()
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(self.destination.is_symlink())
        self.assertEqual(self.tree(target), self.previous)
        self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
