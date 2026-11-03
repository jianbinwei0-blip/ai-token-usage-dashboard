#!/usr/bin/env python3
"""Isolated real-service verification for the AI Token Usage Dashboard."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, build_opener

REPO = Path(__file__).resolve().parents[4]
RUNS = REPO / "tmp" / "pstack-verification"
OWNER = "pstack-verify-ai-token-usage-dashboard-v1"
STATE = "pstack-run.json"


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def load(run):
    run = Path(run).resolve()
    if run.parent != RUNS.resolve() or run.is_symlink():
        raise ValueError("Expected an owned run directly under tmp/pstack-verification")
    state = json.loads((run / STATE).read_text())
    if state.get("owner") != OWNER or state.get("repo") != str(REPO):
        raise ValueError("Not an owned verification run for this repository")
    port = state.get("port")
    if not isinstance(port, int) or not 1024 <= port <= 65535 or port == 8765 or state.get("url") != f"http://127.0.0.1:{port}":
        raise ValueError("Only an owned non-production loopback port is allowed")
    return run, state


def identity(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="], capture_output=True, text=True)
    return " ".join(result.stdout.split()) if result.returncode == 0 else ""


def signatures():
    paths = [REPO / "dashboard/index.html", REPO / "scripts/run_local.sh",
             REPO / "scripts/ai_usage_recalc_server.py", REPO / "scripts/render_tmux_status.py"]
    paths += sorted((REPO / "scripts/dashboard_core").glob("*.py"))
    return {str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def evidence(run, name, value):
    path = run / "pstack-evidence" / name
    if path.exists():
        path = path.with_name(path.stem + "-" + str(time.time_ns()) + path.suffix)
    save(path, value)
    return path


def environment(run, state):
    scratch = run / "pstack-scratch"
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(scratch / "home"),
           "LANG": "en_US.UTF-8", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}
    if os.environ.get("TZ"):
        env["TZ"] = os.environ["TZ"]
    env.update({
        "AI_USAGE_SERVER_HOST": "127.0.0.1", "AI_USAGE_SERVER_PORT": str(state["port"]),
        "AI_USAGE_CODEX_SESSIONS_ROOT": str(scratch / "codex"),
        "AI_USAGE_CLAUDE_PROJECTS_ROOT": str(scratch / "claude"),
        "AI_USAGE_PI_AGENT_ROOT": str(scratch / "pi"), "AI_USAGE_DSH_HOME": str(scratch / "dsh"),
        "AI_USAGE_DASHBOARD_HTML": str(scratch / "index.runtime.html"),
        "AI_USAGE_PARSE_CACHE_FILE": str(scratch / "pstack-parse-cache.json"),
        "AI_USAGE_PRICING_FILE": str(scratch / "pstack-pricing.json"),
        "AI_USAGE_RECALC_LOG_FILE": str(run / "pstack-evidence/pstack-recalc-log.jsonl"),
        "AI_USAGE_CHATGPT_USAGE": "off", "AI_USAGE_CODEX_BIN": str(scratch / "pstack-forbidden-codex"),
    })
    return env


def stamp(day):
    return dt.datetime.combine(dt.date.fromisoformat(day), dt.time(12)).astimezone().astimezone(dt.timezone.utc).isoformat()


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def seed(run, state):
    scratch = run / "pstack-scratch"
    for name in ["home", "codex", "claude", "pi", "dsh"]:
        (scratch / name).mkdir(parents=True, exist_ok=True)
    today = state["today"]
    recent = (dt.date.fromisoformat(today) - dt.timedelta(days=1)).isoformat() if dt.date.fromisoformat(today).day > 1 else today
    old = (dt.date.fromisoformat(today) - dt.timedelta(days=40)).isoformat()
    state["gapDate"] = (dt.date.fromisoformat(old) + dt.timedelta(days=1)).isoformat()
    state["records"] = [
        {"provider": "codex", "date": today, "tokens": 1100000, "cost": 1.02},
        {"provider": "pi", "date": today, "tokens": 350000, "cost": 0.31},
        {"provider": "claude", "date": recent, "tokens": 410000, "cost": 0.429},
        {"provider": "codex", "date": old, "tokens": 550000, "cost": 0.51},
    ]
    for index, (day, input_tokens, cached, output) in enumerate([(today, 1000000, 200000, 100000), (old, 500000, 100000, 50000)]):
        write_jsonl(scratch / "codex" / day.replace("-", "/") / f"pstack-{index}.jsonl", [
            {"type": "session_meta", "timestamp": stamp(day), "payload": {"id": f"pstack-codex-{index}", "originator": "codex_cli_rs"}},
            {"type": "turn_context", "timestamp": stamp(day), "payload": {"model": "gpt-5.4"}},
            {"type": "event_msg", "timestamp": stamp(day), "payload": {"type": "token_count", "info": {"total_token_usage": {
                "input_tokens": input_tokens, "cached_input_tokens": cached, "output_tokens": output, "total_tokens": input_tokens + output}}}},
        ])
    write_jsonl(scratch / "claude/pstack-project/pstack-session.jsonl", [{
        "requestId": "pstack-request", "sessionId": "pstack-claude", "timestamp": stamp(recent),
        "message": {"model": "claude-sonnet-4-6", "usage": {"input_tokens": 300000, "output_tokens": 50000,
                     "cache_creation_input_tokens": 20000, "cache_read_input_tokens": 40000}},
    }])
    pi_path = scratch / "pi/sessions/pstack-project/pstack-session.jsonl"
    write_jsonl(pi_path, [
        {"type": "session", "id": "pstack-pi", "timestamp": stamp(today), "cwd": "/pstack-fixture"},
        {"type": "model_change", "id": "pstack-model", "timestamp": stamp(today), "provider": "openai-codex", "modelId": "gpt-5.4"},
        {"type": "message", "id": "pstack-assistant-1", "timestamp": stamp(today), "message": {"role": "assistant", "usage": {
            "input": 200000, "output": 50000, "cacheRead": 100000, "cacheWrite": 0, "totalTokens": 350000}}},
    ])
    rates = {"input_per_million": 1.0, "output_per_million": 2.0, "cache_read_per_million": 0.1, "cache_write_per_million": 1.25}
    save(scratch / "pstack-pricing.json", {"version": "pstack-fixture-v1", "providers": {
        "codex": {"gpt-5.4": rates}, "pi": {"gpt-5.4": rates}, "claude": {"claude-sonnet-4-6": rates}}})
    forbidden = scratch / "pstack-forbidden-codex"
    forbidden.write_text("#!/bin/sh\nprintf 'unexpected account lookup\\n' > \"$(dirname \"$0\")/pstack-account-called\"\nexit 97\n")
    forbidden.chmod(0o700)
    html = (REPO / "dashboard/index.html").read_text()
    html, count = re.subn(r'(<script id="usageDataset" type="application/json">).*?(</script>)', r'\1{}\2', html, flags=re.S)
    assert count == 1, "Expected exactly one real dashboard dataset tag"
    (scratch / "index.runtime.html").write_text(html)
    save(run / "pstack-evidence/pstack-fixture.json", {"today": today, "records": state["records"], "gapDate": state["gapDate"], "pricing": rates})


def request(state, route, timeout=15):
    with build_opener(ProxyHandler({})).open(state["url"] + route, timeout=timeout) as response:
        body = response.read().decode()
        return body, dict(response.headers)


def dataset(state, timeout=15):
    html, _ = request(state, "/", timeout=timeout)
    matches = re.findall(r'<script id="usageDataset" type="application/json">(.*?)</script>', html, re.S)
    assert len(matches) == 1, "Dataset tag missing or duplicated"
    return json.loads(matches[0])


def oracle(state, provider="combined", start=None, end=None):
    rows = [r for r in state["records"] if (provider == "combined" or r["provider"] == provider)
            and (not start or r["date"] >= start) and (not end or r["date"] <= end)]
    return {"tokens": sum(r["tokens"] for r in rows), "cost": round(sum(r["cost"] for r in rows), 9)}


def check_dataset(state, data):
    for provider in ["combined", "codex", "claude", "pi"]:
        rows = data["providers"][provider]["rows"]
        actual_tokens = sum(r["total_tokens"] for r in rows)
        actual_cost = sum(r["total_cost_usd"] for r in rows)
        expected = oracle(state, provider)
        assert actual_tokens == expected["tokens"], (provider, actual_tokens, expected)
        assert math.isclose(actual_cost, expected["cost"], abs_tol=1e-8), (provider, actual_cost, expected)
        assert all(r["cost_complete"] for r in rows), "Fixture pricing must be complete"
    assert data["providers_available"] == {"combined": True, "codex": True, "claude": True, "pi": True, "dsh": False}


def doctor(run, state):
    assert state.get("status") == "running", "Run is not marked running"
    assert identity(state["pid"]) == state["processIdentity"], "Server process identity changed"
    assert state["today"] == dt.date.today().isoformat(), "Date changed; start a fresh fixture"
    assert signatures() == state["sourceSignatures"], "Product source changed; relaunch before driving"
    health = json.loads(request(state, "/health", timeout=3)[0])
    assert health["ok"] is True and health["service"] == "ai_usage_recalc"
    env = environment(run, state)
    for field, key in [("dashboard", "AI_USAGE_DASHBOARD_HTML"), ("sessions_root", "AI_USAGE_CODEX_SESSIONS_ROOT"),
                       ("claude_projects_root", "AI_USAGE_CLAUDE_PROJECTS_ROOT"), ("pi_agent_root", "AI_USAGE_PI_AGENT_ROOT"),
                       ("dsh_home", "AI_USAGE_DSH_HOME"), ("pricing_file", "AI_USAGE_PRICING_FILE"), ("recalc_log_file", "AI_USAGE_RECALC_LOG_FILE")]:
        assert health[field] == env[key], (field, "not owned by this fixture")
    assert not (run / "pstack-scratch/pstack-account-called").exists(), "Account lookup occurred despite off mode"
    page = dataset(state, timeout=3)
    assert all(key in page.get("providers", {}) for key in ["combined", "codex", "claude", "pi"]), "Runtime page is not usable"
    return {"ok": True, "runDir": str(run), "url": state["url"], "pid": state["pid"], "today": state["today"], "ownedSources": True, "sourceUnchanged": True}


def start():
    RUNS.mkdir(parents=True, exist_ok=True)
    import uuid
    run = RUNS / (dt.datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])
    (run / "pstack-evidence").mkdir(parents=True)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    assert port != 8765
    state = {"owner": OWNER, "repo": str(REPO), "status": "starting", "today": dt.date.today().isoformat(),
             "port": port, "url": f"http://127.0.0.1:{port}", "sourceSignatures": signatures()}
    process = None
    try:
        seed(run, state)
        env = environment(run, state)
        cli = ["/usr/bin/python3", str(REPO / "scripts/render_tmux_status.py"), "--chatgpt-usage", "off",
               "--cache-file", str(run / "pstack-scratch/pstack-preflight.json"), "--max-width", "96"]
        prepared = subprocess.run(cli, cwd=REPO, env=env, capture_output=True, text=True, timeout=30)
        evidence(run, "pstack-preflight.json", {"command": cli, "code": prepared.returncode, "stdout": prepared.stdout, "stderr": prepared.stderr})
        assert prepared.returncode == 0 and (run / "pstack-scratch/pstack-preflight.json").exists(), "Real CLI did not seed usable runtime HTML"
        with (run / "pstack-evidence/pstack-server.log").open("w") as log:
            process = subprocess.Popen([str(REPO / "scripts/run_local.sh")], cwd=REPO, env=env,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        state["pid"] = process.pid
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Owned server exited during startup; inspect pstack-server.log")
            try:
                health = json.loads(request(state, "/health", timeout=1)[0])
                if health.get("dashboard") == env["AI_USAGE_DASHBOARD_HTML"]:
                    break
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError("Owned server did not become ready")
        state["processIdentity"] = identity(process.pid)
        assert str(REPO / "scripts/ai_usage_recalc_server.py") in state["processIdentity"]
        check_dataset(state, dataset(state))
        state["status"] = "running"
        save(run / STATE, state)
        result = doctor(run, state)
        evidence(run, "pstack-doctor-start.json", result)
        return result
    except Exception as error:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
        state["status"] = "failed"
        state["error"] = str(error)
        save(run / STATE, state)
        scratch = run / "pstack-scratch"
        if scratch.exists() and not scratch.is_symlink():
            shutil.rmtree(scratch)
        evidence(run, "pstack-start-failure.json", {"error": str(error), "runDir": str(run), "ownedProcessStopped": True})
        raise RuntimeError(f"Start failed; evidence retained at {run}: {error}") from error


def api(run, state):
    doctor(run, state)
    body, headers = request(state, "/recalc")
    payload = json.loads(body)
    assert payload["ok"] is True and "total" in payload["timings_ms"]
    assert any(k.lower() == "server-timing" for k in headers)
    expected = oracle(state, start=state["today"][:4] + "-01-01", end=state["today"])
    assert payload["ytd_total_tokens"] == expected["tokens"]
    check_dataset(state, dataset(state))
    try:
        request(state, "/pstack-not-a-route")
        raise AssertionError("Expected 404")
    except HTTPError as error:
        assert error.code == 404 and json.loads(error.read())["error"] == "not_found"
    assert (run / "pstack-scratch/pstack-parse-cache.json").is_file()
    result = {"ok": True, "yearTokens": payload["ytd_total_tokens"], "allTokens": oracle(state)["tokens"],
              "headers": headers, "payload": payload, "accountLookupCalled": False}
    evidence(run, "pstack-api.json", result)
    return result


def history(run, state):
    doctor(run, state)
    path = run / "pstack-scratch/pi/sessions/pstack-project/pstack-session.jsonl"
    if not state.get("historyComplete"):
        with path.open("a") as handle:
            handle.write(json.dumps({"type": "message", "id": "pstack-assistant-2", "timestamp": stamp(state["today"]),
                         "message": {"role": "assistant", "usage": {"input": 80000, "output": 20000,
                         "cacheRead": 0, "cacheWrite": 0, "totalTokens": 100000}}}) + "\n")
        state["records"].append({"provider": "pi", "date": state["today"], "tokens": 100000, "cost": 0.12})
        save(run / STATE, state)
        api(run, state)
        path.unlink()
        state["historyComplete"] = True
        save(run / STATE, state)
    result = api(run, state)
    assert not path.exists()
    assert oracle(state, "pi")["tokens"] == 450000
    evidence(run, "pstack-history.json", {"ok": True, "piTokensRetained": 450000, "deletedOnlyOwnedFixture": True, "allTokens": result["allTokens"]})
    return {"ok": True, "piTokensRetained": 450000, "allTokens": result["allTokens"]}


def tmux(run, state):
    doctor(run, state)
    cache = run / "pstack-scratch/pstack-tmux.json"
    command = ["/usr/bin/python3", str(REPO / "scripts/render_tmux_status.py"), "--chatgpt-usage", "off",
               "--cache-file", str(cache), "--max-width", "96"]
    result = subprocess.run(command, cwd=REPO, env=environment(run, state), capture_output=True, text=True, timeout=30)
    text = result.stdout.strip()
    snapshot = json.loads(cache.read_text())
    month = oracle(state, start=state["today"][:7] + "-01", end=state["today"])
    assert result.returncode == 0 and len(text.splitlines()) == 1 and len(text) <= 96, text
    assert all(word in text for word in ["Today", "MTD", "$"]) and "error" not in text.lower(), text
    assert snapshot["health"] == "ok" and snapshot["subscription_enabled"] is False
    assert "subscription" not in snapshot and not (run / "pstack-scratch/pstack-account-called").exists()
    assert snapshot["metrics"]["today_tokens"] == oracle(state, start=state["today"], end=state["today"])["tokens"]
    assert snapshot["metrics"]["range_tokens"] == month["tokens"]
    assert math.isclose(snapshot["metrics"]["range_cost_usd"], month["cost"], abs_tol=1e-8)
    captured = {"ok": True, "command": command, "stdout": result.stdout, "stderr": result.stderr, "code": result.returncode, "snapshot": snapshot}
    evidence(run, "pstack-tmux.json", captured)
    return captured


def stop(run, state):
    pid = state.get("pid")
    if pid and state.get("status") == "running":
        actual = identity(pid)
        if actual and actual != state.get("processIdentity"):
            raise RuntimeError("PID identity changed; refusing to kill or erase an unowned process")
        if actual:
            os.kill(pid, signal.SIGTERM)
            for _ in range(50):
                if identity(pid) != actual:
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError("Owned process did not stop; evidence and scratch retained")
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", state["port"])) != 0, "Port still listens; do not erase its state"
    scratch = run / "pstack-scratch"
    if scratch.is_symlink():
        raise RuntimeError("Scratch is a symlink; refusing cleanup")
    if scratch.exists():
        shutil.rmtree(scratch)
    state["status"] = "stopped"
    save(run / STATE, state)
    artifacts = sorted(str(p.relative_to(run / "pstack-evidence")) for p in (run / "pstack-evidence").rglob("*") if p.is_file())
    result = {"ok": True, "ownedProcessStopped": True, "ownedPortClosed": True, "scratchRemoved": not scratch.exists(),
              "productSourceUnchanged": signatures() == state["sourceSignatures"], "retainedEvidence": artifacts}
    assert result["productSourceUnchanged"], "Product source changed during verification"
    evidence(run, "pstack-cleanup.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "doctor", "api", "history", "tmux", "stop"])
    parser.add_argument("run", nargs="?")
    args = parser.parse_args()
    if args.command == "start":
        if args.run:
            parser.error("start creates its own run directory")
        result = start()
    else:
        if not args.run:
            parser.error("supply the owned run directory returned by start")
        run, state = load(args.run)
        result = {"doctor": doctor, "api": api, "history": history, "tmux": tmux, "stop": stop}[args.command](run, state)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"ok": False, "error": str(error)}, indent=2), file=sys.stderr)
        raise SystemExit(1)
