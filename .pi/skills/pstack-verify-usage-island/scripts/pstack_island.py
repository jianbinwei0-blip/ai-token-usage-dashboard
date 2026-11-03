#!/usr/bin/env python3
"""Owned, synthetic, real-app verification for the native macOS Usage Island."""
from __future__ import annotations

import argparse
import contextlib
import copy
import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import plistlib
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[4]
APP_ROOT = REPO / "apps/usage-island"
RUNS = REPO / "tmp/pstack-usage-island"
OWNER = "pstack-verify-usage-island-v1"
STATE = "pstack-island-run.json"
FIXTURES = REPO / ".pi/skills/pstack-verify-ai-token-usage-dashboard/scripts/pstack_verify.py"
CHILDREN: dict[int, subprocess.Popen] = {}


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    temporary.replace(path)


def evidence(run, name, value):
    path = run / "pstack-evidence" / name
    if path.exists():
        path = path.with_name(f"{path.stem}-{time.time_ns()}{path.suffix}")
    save(path, value)
    return path


def digest(path):
    path = Path(path)
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def signatures():
    paths = {APP_ROOT / "Package.swift", FIXTURES, Path(__file__).resolve(), REPO / "dashboard/index.html"}
    for directory in [APP_ROOT / "Sources", APP_ROOT / "Tests", APP_ROOT / "scripts",
                      APP_ROOT / "packaging", APP_ROOT / "Vendor", REPO / "scripts"]:
        paths.update(p for p in directory.rglob("*") if p.suffix in {".swift", ".py", ".sh", ".plist"})
    return {str(p.relative_to(REPO)): digest(p) for p in sorted(paths)}


def scratch(run):
    return run / "pstack-scratch"


def app(run):
    return scratch(run) / "Pstack Usage Island.app"


def executable(run):
    return app(run) / "Contents/MacOS/UsageIsland"


def home(run):
    return scratch(run) / "home"


def diagnostic(run):
    return home(run) / "Library/Caches/AIUsageIsland/presentation.json"


def load(run):
    raw = Path(run).absolute()
    if raw.is_symlink() or raw.resolve() != raw or raw.parent != RUNS.resolve():
        raise ValueError("Expected a non-symlink owned run directly under tmp/pstack-usage-island")
    if not re.fullmatch(r"\d{8}T\d{6}-[a-f0-9]{8}", raw.name):
        raise ValueError("Invalid native run name")
    if (raw / STATE).is_symlink() or scratch(raw).is_symlink() or (raw / "pstack-evidence").is_symlink():
        raise ValueError("Refusing symlinked run state, scratch, or evidence")
    state = json.loads((raw / STATE).read_text())
    if state.get("owner") != OWNER or state.get("repo") != str(REPO):
        raise ValueError("Not an owned native verification run for this repository")
    expected_id = "local.pstack.UsageIsland." + raw.name.replace("-", "")
    if state.get("bundleID") != expected_id:
        raise ValueError("Native verification requires its unique non-production bundle ID")
    return raw, state


def identity(pid):
    if type(pid) is not int or pid < 2:
        return ""
    child = CHILDREN.get(pid)
    if child is not None and child.poll() is not None:
        return ""
    result = subprocess.run(["ps", "-p", str(pid), "-o", "lstart=", "-o", "comm="],
                            capture_output=True, text=True)
    return " ".join(result.stdout.split()) if result.returncode == 0 else ""


def comm(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "comm="], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def protected_state():
    live_app = Path.home() / "Applications/AI Usage Island.app/Contents/MacOS/UsageIsland"
    rows = subprocess.check_output(["ps", "-axo", "pid=,comm="], text=True).splitlines()
    processes = {}
    for row in rows:
        parts = row.strip().split(None, 1)
        if len(parts) == 2 and parts[1] == str(live_app):
            pid = int(parts[0])
            processes[str(pid)] = identity(pid)
    preference = Path.home() / "Library/Preferences/local.jwei.AIUsageIsland.plist"
    return {"installedExecutable": str(live_app), "installedHash": digest(live_app),
            "processes": processes, "preferences": str(preference), "preferencesHash": digest(preference)}


def protected_unchanged(state):
    before = state["protected"]
    return (digest(before["installedExecutable"]) == before["installedHash"]
            and digest(before["preferences"]) == before["preferencesHash"]
            and all(identity(int(pid)) == value for pid, value in before["processes"].items()))


def environment(run):
    s = scratch(run)
    env = {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
           "HOME": str(home(run)), "CFFIXED_USER_HOME": str(home(run)),
           "TMPDIR": str(s / "tmp") + "/", "XDG_CONFIG_HOME": str(home(run) / ".config"),
           "XDG_CACHE_HOME": str(home(run) / ".cache"), "LANG": "en_US.UTF-8",
           "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1",
           "AI_USAGE_PYTHON": sys.executable,
           "AI_USAGE_DASHBOARD_DIR": str(s / "fixture-runtime"),
           "AI_USAGE_CACHE_FILE": str(s / "native-cache.json"),
           "AI_USAGE_CODEX_SESSIONS_ROOT": str(s / "codex"),
           "AI_USAGE_CLAUDE_PROJECTS_ROOT": str(s / "claude"),
           "AI_USAGE_PI_AGENT_ROOT": str(s / "pi"), "AI_USAGE_DSH_HOME": str(s / "dsh"),
           "AI_USAGE_DASHBOARD_HTML": str(s / "index.runtime.html"),
           "AI_USAGE_PARSE_CACHE_FILE": str(s / "pstack-parse-cache.json"),
           "AI_USAGE_PRICING_FILE": str(s / "pstack-pricing.json"),
           "AI_USAGE_RECALC_LOG_FILE": str(run / "pstack-evidence/pstack-recalc.jsonl"),
           "AI_USAGE_CHATGPT_USAGE": "off", "AI_USAGE_CODEX_BIN": str(s / "pstack-forbidden-codex"),
           "PSTACK_SCRATCH": str(s),
           "PSTACK_COLLECTOR_CALLS": str(run / "pstack-evidence/pstack-fixture-collector.jsonl")}
    for key in ["TZ", "AI_USAGE_SDK"]:
        if os.environ.get(key):
            env[key] = os.environ[key]
    return env


@contextlib.contextmanager
def exclusive():
    RUNS.mkdir(parents=True, exist_ok=True)
    with (RUNS / ".drive.lock").open("a") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another native verification command is driving the desktop; do not double-drive")
        yield


def checked(run, name, arguments, *, env=None, cwd=REPO, timeout=600):
    log = run / "pstack-evidence" / f"{name}-{time.time_ns()}.log"
    started = dt.datetime.now().astimezone().isoformat()
    with log.open("w") as output:
        child = subprocess.Popen([str(a) for a in arguments], cwd=cwd, env=env or environment(run),
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            raise RuntimeError(f"{name} timed out; retained {log}")
    evidence(run, f"{name}.json", {"command": [str(a) for a in arguments], "code": code,
             "startedAt": started, "endedAt": dt.datetime.now().astimezone().isoformat(),
             "log": str(log.relative_to(run))})
    if code:
        raise RuntimeError(f"{name} exited {code}; retained {log}")
    return log.read_text()


def bridge_directory(run):
    return app(run) / "Contents/Resources/AIUsageIsland_UsageIslandApp.bundle/Bridge"


def bridge(run, *, cache=None, refresh=False, fail=False, mode="ai"):
    env = environment(run)
    if cache is not None:
        env["AI_USAGE_CACHE_FILE"] = str(cache)
    if fail:
        env["PSTACK_REFRESH_FAILURE"] = "1"
    args = [sys.executable, str(bridge_directory(run) / "usage_bridge.py"), mode]
    if refresh:
        args.append("--refresh")
    result = subprocess.run(args, env=env, cwd=scratch(run), capture_output=True, text=True, timeout=55)
    if result.returncode:
        raise RuntimeError(f"Bundled adapter failed: {result.stderr[:800]}")
    return json.loads(result.stdout)


def field_values(payload):
    return {field["key"]: field["value"] for field in payload["fields"]}


def seed(run, state):
    spec = importlib.util.spec_from_file_location("pstack_dashboard_fixtures", FIXTURES)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.seed(run, state)
    for path in [scratch(run) / "tmp", home(run) / "Library/Preferences"]:
        path.mkdir(parents=True, exist_ok=True)
    # Replay only the already-normalized cache at the production collector
    # subprocess boundary. No UI state setter or replacement adapter is used.
    runtime = scratch(run) / "fixture-runtime/scripts"
    runtime.mkdir(parents=True)
    (runtime / "render_tmux_status.py").write_text('''#!/usr/bin/env python3
import argparse, json, os
from pathlib import Path
import sys, time
p=argparse.ArgumentParser()
p.add_argument("--cache-file", required=True)
p.add_argument("--scope", choices=["combined"], required=True)
p.add_argument("--range", choices=["mtd"], required=True)
p.add_argument("--refresh-interval-minutes", choices=["5"], required=True)
p.add_argument("--max-width", choices=["96"], required=True)
a=p.parse_args()
cache=Path(a.cache_file).resolve()
assert cache.is_relative_to(Path(os.environ["PSTACK_SCRATCH"]).resolve())
with open(os.environ["PSTACK_COLLECTOR_CALLS"], "a") as f:
 f.write(json.dumps({"at":time.time(), "cache":str(cache), "args":sys.argv[1:],
                     "fixtureReplay":True, "failed":os.environ.get("PSTACK_REFRESH_FAILURE")=="1"})+"\\n")
if os.environ.get("PSTACK_REFRESH_FAILURE")=="1":
 sys.exit(97)
print("AI fixture cache replay; no collection or account access")
''')


def collect(run, state):
    cache = scratch(run) / "collected-cache.json"
    checked(run, "pstack-real-collector", [sys.executable, REPO / "scripts/render_tmux_status.py",
            "--cache-file", cache, "--scope", "combined", "--range", "mtd",
            "--refresh-interval-minutes", "5", "--max-width", "96"])
    snapshot = json.loads(cache.read_text())
    records = state["records"]
    expected_today = sum(r["tokens"] for r in records if r["date"] == state["today"])
    month = [r for r in records if r["date"][:7] == state["today"][:7]]
    assert snapshot["metrics"]["today_tokens"] == expected_today
    assert snapshot["metrics"]["range_tokens"] == sum(r["tokens"] for r in month)
    assert math.isclose(snapshot["metrics"]["range_cost_usd"], sum(r["cost"] for r in month), abs_tol=1e-9)
    assert snapshot["quality"]["pricing_complete"] is True
    assert not (scratch(run) / "pstack-account-called").exists()
    state["collectorSnapshot"] = snapshot
    state["expectedFields"] = {"today": "1.9M" if dt.date.fromisoformat(state["today"]).day == 1 else "1.4M",
                               "month": "1.9M", "cost": "$2", "plan": "GPT Pro",
                               "weekly": "73% left", "credits": "1,234"}
    evidence(run, "pstack-collector-proof.json", {"ok": True, "realCollector": True,
             "snapshot": snapshot, "expectedRecords": records, "accountAccess": False})


def scenario(state, name):
    now = dt.datetime.now().astimezone()
    stamp = lambda minutes=0: (now - dt.timedelta(minutes=minutes)).isoformat()
    data = copy.deepcopy(state["collectorSnapshot"])
    data["generated_at"] = stamp()
    data["subscription"] = {
        "version": 3, "state": "ok", "fetched_at": stamp(), "account_type": "chatgpt", "plan": "pro",
        "limits": [{"id": "codex", "secondary": {"window_duration_minutes": 10080,
                    "remaining_percent": 73, "resets_at": int((now + dt.timedelta(days=2, hours=3)).timestamp())},
                    "credits": {"has_credits": True, "unlimited": False, "balance": "1234"}}],
        "reset_credits_available": 4,
        "reset_credit_expirations": [
            {"count": 2, "expires_at": int((now + dt.timedelta(days=2)).timestamp()), "does_not_expire": False},
            {"count": 1, "expires_at": None, "does_not_expire": True},
            {"count": 1, "expires_at": None, "does_not_expire": False}],
    }
    if name in {"stale-usage", "warnings"}:
        data["generated_at"] = stamp(10)
    if name in {"stale-quota", "warnings"}:
        data["subscription"]["fetched_at"] = stamp(20)
    if name == "partial-pricing":
        data["health"] = "partial"
    if name in {"partial-pricing", "warnings"}:
        data["quality"]["pricing_complete"] = False
    if name in {"low-quota", "zero-quota", "warnings"}:
        data["subscription"]["limits"][0]["secondary"]["remaining_percent"] = 0 if name == "zero-quota" else 12
    if name == "signed-out":
        data["subscription"] = {"state": "not_applicable", "account_type": None}
    if name == "warnings":
        data["subscription"]["reset_credits_available"] = 5
        data["subscription"]["reset_credit_expirations"][0]["expires_at"] = int((now - dt.timedelta(hours=1)).timestamp())
    return data


def set_case(run, state, name):
    data = scenario(state, name)
    save(scratch(run) / "native-cache.json", data)
    state["case"] = name
    state["generatedAt"] = data["generated_at"]
    save(run / STATE, state)
    return data


def stage(run, state):
    checked(run, "pstack-native-checks", [APP_ROOT / "scripts/check.sh"], cwd=APP_ROOT)
    output = checked(run, "pstack-bin-path", [APP_ROOT / "scripts/swift.sh", "build", "--show-bin-path"], cwd=APP_ROOT)
    candidates = [Path(line.strip()) for line in output.splitlines() if line.strip().startswith(str(APP_ROOT / ".build") + "/")]
    if len(candidates) != 1 or not (candidates[0] / "UsageIsland").is_file():
        raise RuntimeError("SwiftPM did not return this app's usable build directory")
    binary_dir = candidates[0]
    contents = app(run) / "Contents"
    (contents / "MacOS").mkdir(parents=True)
    (contents / "Resources").mkdir()
    info = plistlib.loads((APP_ROOT / "packaging/Info.plist").read_bytes())
    info.update(CFBundleIdentifier=state["bundleID"], CFBundleName="Pstack Usage Island",
                CFBundleDisplayName="Pstack Usage Island")
    (contents / "Info.plist").write_bytes(plistlib.dumps(info))
    shutil.copy2(binary_dir / "UsageIsland", executable(run))
    shutil.copytree(binary_dir / "AIUsageIsland_UsageIslandApp.bundle",
                    contents / "Resources/AIUsageIsland_UsageIslandApp.bundle")
    shutil.copytree(APP_ROOT / "LICENSES", contents / "Resources/LICENSES")
    shutil.copy2(REPO / "LICENSE", contents / "Resources/LICENSES/AIUsageIsland.txt")
    checked(run, "pstack-sign", ["/usr/bin/codesign", "--force", "--sign", "-", "--options", "runtime", app(run)])
    checked(run, "pstack-signature", ["/usr/bin/codesign", "--verify", "--deep", "--strict", app(run)])
    assert digest(bridge_directory(run) / "dashboard_status_formatter.py") == digest(REPO / "scripts/dashboard_core/tmux_status.py")
    state["bundleHashes"] = {str(p.relative_to(app(run))): digest(p) for p in sorted(app(run).rglob("*")) if p.is_file()}
    state["sources"] = signatures()


def stop_owned(run, state):
    pid = state.get("pid")
    current = identity(pid)
    if not current:
        return
    if current != state.get("identity") or comm(pid) != str(executable(run)):
        raise RuntimeError("Owned PID identity changed; refusing to signal or delete scratch")
    os.kill(pid, signal.SIGTERM)
    for _ in range(60):
        if not identity(pid):
            return
        time.sleep(.1)
    if identity(pid) != current or comm(pid) != str(executable(run)):
        raise RuntimeError("Owned PID identity changed while waiting; refusing to escalate")
    os.kill(pid, signal.SIGKILL)
    for _ in range(30):
        if not identity(pid):
            return
        time.sleep(.1)
    raise RuntimeError("Owned app did not exit")


def launch(run, state, output=None):
    if state.get("stopped"):
        raise RuntimeError("A cleaned run cannot be relaunched; start a new run")
    diagnostic(run).unlink(missing_ok=True)
    arguments = [str(executable(run))]
    if output:
        arguments += ["--smoke-test", str(output)]
    with (run / "pstack-evidence" / f"pstack-app-{time.time_ns()}.log").open("w") as log:
        child = subprocess.Popen(arguments, cwd=scratch(run), env=environment(run), stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True)
    CHILDREN[child.pid] = child
    state["pid"] = child.pid
    state["identity"] = identity(child.pid)
    save(run / STATE, state)
    for _ in range(200):
        if not identity(child.pid):
            raise RuntimeError("Staged app exited; inspect retained pstack-app log")
        try:
            info = json.loads(diagnostic(run).read_text())
            if info["pid"] == child.pid and info["generated_at"] == state["generatedAt"]:
                return info
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(.1)
    raise RuntimeError("Staged app did not decode the private fixture within 20 seconds")


def doctor(run, state):
    assert not state.get("stopped"), "Run was cleaned; start a fresh run"
    assert state["sources"] == signatures(), "Source/build inputs drifted; start a fresh run"
    assert dt.date.today().isoformat() == state["today"], "Local date changed; start a fresh run"
    assert protected_unchanged(state), "Installed app/process/preferences changed; investigate before continuing"
    for relative, value in state["bundleHashes"].items():
        path = app(run) / relative
        assert not path.is_symlink() and digest(path) == value, "Staged bundle drifted"
    pid = state["pid"]
    assert identity(pid) == state["identity"] and comm(pid) == str(executable(run)), "Owned app is not running"
    info = json.loads(diagnostic(run).read_text())
    assert info["pid"] == pid and info["generated_at"] == state["generatedAt"], "Not this app/fixture diagnostic"
    assert plistlib.loads((app(run) / "Contents/Info.plist").read_bytes())["CFBundleIdentifier"] == state["bundleID"]
    payload = bridge(run)
    assert payload["generated_at"] == state["generatedAt"] and payload["health"] in {"ok", "stale"}
    assert info["health"] == payload["health"], "App and bundled adapter disagree on freshness"
    assert info["status"] == ("Up to date" if state["case"] == "baseline" else "Cached usage is stale")
    assert not (scratch(run) / "pstack-account-called").exists(), "Unexpected account invocation"
    env = environment(run)
    assert env["HOME"] == env["CFFIXED_USER_HOME"] == str(home(run))
    assert "AI_USAGE_BRIDGE_DIR" not in env, "Must use the real packaged bridge"
    result = {"ok": True, "ownedApp": True, "privateHomeProvenByAppDiagnostic": True,
              "sourceUnchanged": True, "installedAppUnchanged": True, "case": state["case"],
              "diagnostic": info, "bundleID": state["bundleID"], "pid": pid}
    evidence(run, "pstack-doctor.json", result)
    return result


def start():
    if sys.platform != "darwin":
        raise RuntimeError("BLOCKED: native Usage Island verification requires macOS 14+")
    for record in RUNS.glob(f"*/{STATE}"):
        old_run, old = load(record.parent)
        if identity(old.get("pid")):
            raise RuntimeError(f"Another recorded native run is active: {old_run}; stop it explicitly first")
    name = dt.datetime.now().strftime("%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:8]
    run = RUNS / name
    run.mkdir(mode=0o700)
    (run / "pstack-evidence").mkdir()
    state = {"owner": OWNER, "repo": str(REPO), "bundleID": "local.pstack.UsageIsland." + name.replace("-", ""),
             "today": dt.date.today().isoformat(), "protected": protected_state(), "pid": None}
    save(run / STATE, state)
    try:
        seed(run, state)
        stage(run, state)
        collect(run, state)
        set_case(run, state, "baseline")
        launch(run, state)
        result = doctor(run, state)
        return {"ok": True, "runDir": str(run), "pid": result["pid"], "bundleID": state["bundleID"]}
    except BaseException as error:
        evidence(run, "pstack-start-failure.json", {"error": str(error), "runDir": str(run)})
        save(run / STATE, state)
        cleanup(run, state)
        raise RuntimeError(f"Native start failed; evidence retained at {run}: {error}") from error


def matrix(run, state):
    doctor(run, state)
    results = []
    for name in ["baseline", "stale-usage", "stale-quota", "partial-pricing", "low-quota", "zero-quota",
                 "signed-out", "missing", "malformed", "collection-failure", "warnings"]:
        path = scratch(run) / "cases" / (name + ".json")
        if name == "malformed":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{"not": "a supported cache"}')
        elif name != "missing":
            save(path, scenario(state, name))
        payload = bridge(run, cache=path, refresh=True, fail=name == "collection-failure")
        evidence(run, f"pstack-case-{name}.json", {"case": name, "payload": payload, "cacheHash": digest(path)})
        fields = field_values(payload)
        if name == "baseline":
            assert all(fields.get(k) == v for k, v in state["expectedFields"].items()), fields
            assert payload["reset_credits"]["available_count"] == 4 and payload["reset_credits"]["details_complete"]
            assert payload["reset_credits"]["expirations"][1]["does_not_expire"] is True
            assert payload["reset_credits"]["expirations"][2]["does_not_expire"] is False
        if name in {"stale-usage", "warnings"}:
            assert payload["health"] == "stale"
        if name == "stale-usage":
            assert payload["subscription_state"] == "ok"
        if name in {"stale-quota", "warnings"}:
            assert payload["subscription_state"] == "stale"
        if name == "stale-quota":
            assert payload["health"] == "ok"
        if name in {"partial-pricing", "warnings"}:
            assert not payload["pricing_complete"] and "*" in fields["cost"]
        if name == "partial-pricing":
            assert payload["health"] == "partial"
        if name in {"low-quota", "zero-quota"}:
            weekly = next(f for f in payload["fields"] if f["key"] == "weekly")
            assert weekly["tone"] == ("error" if name == "zero-quota" else "warning")
            assert weekly["value"] == ("0% left" if name == "zero-quota" else "12% left")
        if name == "signed-out":
            assert "weekly" not in fields and "credits" not in fields and "today" in fields
        if name in {"missing", "malformed"}:
            assert payload["health"] == "unavailable" and "today" not in fields
        if name == "collection-failure":
            assert payload["health"] == "error" and "Collection failed" in payload["message"]
        if name == "warnings":
            assert not payload["reset_credits"]["details_complete"]
        results.append({"case": name, "ok": True, "payload": payload})
    calls = [json.loads(line) for line in (run / "pstack-evidence/pstack-fixture-collector.jsonl").read_text().splitlines()]
    assert all(any(call["cache"].endswith("/" + item["case"] + ".json") for call in calls) for item in results)
    assert any(call["failed"] for call in calls), "Failed refresh fixture was not actually invoked"
    resources = bridge(run, mode="resources")
    assert resources["available"] is True, "Real resource helper unavailable"
    assert all(re.fullmatch(r"\d{1,3}%", resources[k]) and 0 <= int(resources[k][:-1]) <= 100 for k in ["ram", "disk"])
    assert (home(run) / "Library/Caches/AIUsageIsland/tmux/status_disk_root").is_file()
    assert not (scratch(run) / "pstack-account-called").exists()
    result = {"ok": True, "cases": results, "resources": resources,
              "fixtureBoundary": "Normalized cache and collector subprocess replay; real packaged adapter/formatter/resources"}
    evidence(run, "pstack-bridge-matrix.json", result)
    doctor(run, state)
    return {"ok": True, "cases": len(results), "realResources": resources}


def native(run, state, *, preview=False):
    doctor(run, state)
    results = []
    for name in ["baseline", "warnings"]:
        stop_owned(run, state)
        fixture = set_case(run, state, name)
        output = run / "pstack-evidence" / f"pstack-native-{name}-{time.time_ns()}"
        output.mkdir()
        info = launch(run, state, output)
        deadline = time.monotonic() + 90
        while not (output / "smoke.json").exists():
            assert identity(state["pid"]) == state["identity"], "Staged app exited during smoke"
            if time.monotonic() > deadline:
                raise RuntimeError(f"Native smoke timed out; retained {output}")
            time.sleep(.2)
        report = json.loads((output / "smoke.json").read_text())
        payload = bridge(run)
        save(output / "pstack-fixture.json", fixture)
        save(output / "pstack-adapter.json", payload)
        save(output / "pstack-diagnostic.json", json.loads(diagnostic(run).read_text()))
        assert report["passed"] is True, report
        checks = report["checks"]
        assert "bundled adapter decoded the local cache" in checks
        assert "expanded details rendered offscreen without changing window visibility" in checks
        if report["skipped"]:
            locked_info = json.loads(diagnostic(run).read_text())
            assert locked_info["suspended"] and not locked_info["window_visible"] and not locked_info["visible"]
            assert "launch while locked keeps the island hidden" in checks
            if not preview:
                raise RuntimeError("BLOCKED: interactive native coverage requires an unlocked, active macOS session; "
                                   f"partial render/privacy evidence retained at {output}")
        else:
            for required in ["same-position clicks", "Escape collapses", "Refresh, Dashboard and More options",
                             "hide and show", "return to original display", "registered without Accessibility"]:
                assert any(required in text for text in checks), f"Missing native proof: {required}"
            assert (output / "compact.png").is_file() and (output / "expanded.png").is_file()
        assert (output / "expanded-preview.png").stat().st_size > 1000
        if name == "baseline":
            assert all(field_values(payload).get(k) == v for k, v in state["expectedFields"].items())
        else:
            assert payload["health"] == "stale" and payload["subscription_state"] == "stale"
            assert not payload["pricing_complete"] and not payload["reset_credits"]["details_complete"]
        results.append({"case": name, "ok": True, "checks": len(checks), "evidence": str(output.relative_to(run)),
                        "coverage": "offscreen-and-locked-startup" if report["skipped"] else "interactive-smoke",
                        "skipped": report["skipped"], "suspended": info["suspended"]})
        doctor(run, state)
    result = {"ok": True, "mode": "preview" if preview else "native",
              "interactivePassed": all(not r["skipped"] for r in results), "runs": results,
              "input": "Native app-local mouse events and Escape only when unlocked; not external physical input",
              "limits": ["Dashboard/More smoke callbacks count dispatch, not actual browser/menu navigation",
                         "Hide/show and display selection exercise production controller paths, not external menu clicks",
                         "Shortcut registration, not global physical key dispatch",
                         "No sleep/wake, lock transition, live display unplug or five-minute wall-clock certification"]}
    evidence(run, "pstack-preview-results.json" if preview else "pstack-native-results.json", result)
    return result


def cleanup(run, state):
    stop_owned(run, state)
    assert not identity(state.get("pid")), "Owned app is still running"
    if scratch(run).is_symlink():
        raise RuntimeError("Refusing to remove symlinked scratch")
    if scratch(run).exists():
        shutil.rmtree(scratch(run))
    state["stopped"] = True
    save(run / STATE, state)
    result = {"ok": True, "ownedAppExited": True, "scratchRemoved": not scratch(run).exists(),
              "installedAppUnchanged": protected_unchanged(state),
              "retainedEvidence": sorted(str(p.relative_to(run / "pstack-evidence"))
                                         for p in (run / "pstack-evidence").rglob("*") if p.is_file())}
    evidence(run, "pstack-cleanup.json", result)
    assert result["installedAppUnchanged"], "Installed app/process/preferences changed; inspect retained evidence"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "doctor", "bridge", "native", "preview", "stop"])
    parser.add_argument("run", nargs="?")
    args = parser.parse_args()
    run = None
    try:
        with exclusive():
            if args.command == "start":
                if args.run:
                    parser.error("start takes no run argument")
                result = start()
            else:
                if not args.run:
                    parser.error("run directory is required")
                run, state = load(args.run)
                operations = {"doctor": doctor, "bridge": matrix, "native": native,
                              "preview": lambda r, s: native(r, s, preview=True), "stop": cleanup}
                result = operations[args.command](run, state)
            print(json.dumps(result, indent=2))
    except Exception as error:
        blocked = str(error).startswith("BLOCKED:")
        if run is not None:
            evidence(run, "pstack-blocked.json" if blocked else "pstack-failure.json",
                     {"command": args.command, "status": "BLOCKED" if blocked else "FAIL", "error": str(error)})
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 2 if blocked else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
