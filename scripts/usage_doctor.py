#!/usr/bin/env python3
"""Read-only setup diagnostics. Never collect usage, contact accounts or write settings."""
from __future__ import annotations

import argparse
import ast
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import shlex
import sys

from dashboard_core.config import DashboardConfig
from dashboard_core.settings import SETTINGS_VERSION, SettingsError, load_settings

REPO_ROOT = Path(__file__).resolve().parents[1]
MAX_METADATA_BYTES = 16 * 1024 * 1024
RUNTIME_FILES = (
    "scripts/render_tmux_status.py",
    *(str(path.relative_to(REPO_ROOT)) for path in sorted((REPO_ROOT / "scripts/dashboard_core").glob("*.py"))),
)


def _path_status(path: Path, *, directory: bool = False) -> dict:
    try:
        if not path.exists():
            state = "missing"
        elif (path.is_dir() if directory else path.is_file()):
            state = "ok" if os.access(path, os.R_OK | (os.X_OK if directory else 0)) else "unreadable"
        else:
            state = "wrong_type"
    except OSError:
        state = "unreadable"
    return {"path": str(path), "status": state}


def _source_tree(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, SyntaxError):
        return None


def _source_version(path: Path, name: str) -> int | None:
    tree = _source_tree(path)
    for node in tree.body if tree else []:
        if (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
                and isinstance(node.value, ast.Constant) and type(node.value.value) is int):
            return node.value.value
    return None


def _snapshot_version(path: Path) -> int | None:
    tree = _source_tree(path)
    for node in ast.walk(tree) if tree else []:
        if not isinstance(node, ast.Dict):
            continue
        values = {key.value: value for key, value in zip(node.keys, node.values) if isinstance(key, ast.Constant)}
        version = values.get("version")
        if "generated_at" in values and isinstance(version, ast.Constant) and type(version.value) is int:
            return version.value
    return None


def _read_metadata(path: Path) -> tuple[str, dict]:
    """Bounded parsing; callers emit only whitelisted schema/freshness metadata."""
    state = _path_status(path)["status"]
    if state != "ok":
        return state, {}
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_METADATA_BYTES + 1)
        if len(raw) > MAX_METADATA_BYTES:
            return "too_large_to_validate", {}
        value = json.loads(raw)
        return ("ok", value) if isinstance(value, dict) else ("invalid", {})
    except (OSError, ValueError, UnicodeError, RecursionError):
        return "invalid", {}


def _valid_snapshot_shape(snapshot: dict) -> bool:
    if not isinstance(snapshot.get("providers"), list) or not isinstance(snapshot.get("metrics"), dict):
        return False
    if any(key in snapshot and not isinstance(snapshot[key], dict) for key in ("quality", "range")):
        return False
    metrics = snapshot["metrics"]
    for key in ("today_tokens", "range_tokens", "range_cost_usd"):
        value = metrics.get(key)
        if key == "range_cost_usd" and value is None:
            continue
        if type(value) not in {int, float} or value < 0 or (type(value) is float and not math.isfinite(value)):
            return False
    return True


def _freshness(value: object, now: dt.datetime, max_age_seconds: int) -> dict:
    if not isinstance(value, str):
        return {"freshness": "unknown"}
    try:
        generated = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if generated.tzinfo is None:
            return {"freshness": "unknown"}
        age = int((now - generated).total_seconds())
        return {
            "generated_at": generated.isoformat(), "age_seconds": age,
            "freshness": "clock_skew" if age < -30 else "stale" if age > max_age_seconds else "fresh",
        }
    except (ValueError, OverflowError):
        return {"freshness": "unknown"}


def build_report(repo_root: Path = REPO_ROOT, *, now: dt.datetime | None = None,
                 max_age_seconds: int = 330) -> dict:
    settings = load_settings(repo_root)
    config = DashboardConfig.from_env(repo_root, settings=settings)
    now = now or dt.datetime.now(dt.timezone.utc)
    runtime = settings.runtime_dir
    recovery: list[str] = []
    runtime_status = _path_status(runtime, directory=True)
    missing, drifted = [], []
    for relative in RUNTIME_FILES:
        target, reference = runtime / relative, repo_root / relative
        if _path_status(target)["status"] != "ok":
            missing.append(relative)
            continue
        try:
            if hashlib.sha256(target.read_bytes()).digest() != hashlib.sha256(reference.read_bytes()).digest():
                drifted.append(relative)
        except OSError:
            missing.append(relative)
    if runtime_status["status"] == "ok":
        runtime_status["status"] = "incomplete" if missing else "drift" if drifted else "ok"
    runtime_status.update(missing_files=missing, differing_files=drifted)
    runtime_status["settings_version"] = _source_version(runtime / "scripts/dashboard_core/settings.py", "SETTINGS_VERSION")
    runtime_status["parse_cache_version"] = _source_version(runtime / "scripts/dashboard_core/collectors.py", "_PERSISTENT_CACHE_VERSION")
    runtime_status["snapshot_version"] = _snapshot_version(runtime / "scripts/dashboard_core/tmux_status.py")
    runtime_status["subscription_version"] = _source_version(runtime / "scripts/dashboard_core/chatgpt_subscription.py", "SUBSCRIPTION_SNAPSHOT_VERSION")
    if runtime_status["status"] != "ok":
        recovery.append("Select an existing matching runtime with saved runtime_dir or AI_USAGE_DASHBOARD_DIR. "
                        "Back it up before updating its scripts; preserve tmp/, pricing and all retained history. "
                        "Differing code is drift, not proof that a cache should be deleted.")

    sources = {}
    for name, path, setting in (
        ("codex", config.sessions_root, "codex_sessions_root"),
        ("claude", config.claude_projects_root, "claude_projects_root"),
        ("pi", config.pi_agent_root, "pi_agent_root"),
        ("dsh", config.dsh_home, "dsh_home"),
    ):
        sources[name] = dict(_path_status(path, directory=True), configured_by=settings.sources[setting])
        if sources[name]["status"] != "ok":
            recovery.append(f"Check {name} root existence/permissions or correct saved {setting} / its environment override. "
                            "A provider you do not use may remain missing; no source contents were read.")

    cache_path = Path(os.environ.get("AI_USAGE_CACHE_FILE", str(runtime / "tmp/tmux_status.json"))).expanduser()
    cache_state, snapshot = _read_metadata(cache_path)
    expected_snapshot = _snapshot_version(repo_root / "scripts/dashboard_core/tmux_status.py")
    version = snapshot.get("version") if type(snapshot.get("version")) is int else None
    cache = {"path": str(cache_path), "status": cache_state, "version": version, "expected_version": expected_snapshot}
    if cache_state == "ok":
        cache["status"] = "incompatible" if version != expected_snapshot else "ok" if _valid_snapshot_shape(snapshot) else "invalid"
        cache.update(_freshness(snapshot.get("generated_at"), now, max_age_seconds))
        health = snapshot.get("health")
        cache["collection_state"] = health if isinstance(health, str) and health in {"ok", "partial", "error", "stale"} else "unknown"
        subscription = snapshot.get("subscription")
        if isinstance(subscription, dict):
            sub_version = subscription.get("version") if type(subscription.get("version")) is int else None
            cache["subscription_version"] = sub_version
            expected_sub = _source_version(repo_root / "scripts/dashboard_core/chatgpt_subscription.py", "SUBSCRIPTION_SNAPSHOT_VERSION")
            cache["expected_subscription_version"] = expected_sub
            if snapshot.get("subscription_enabled") is True and sub_version != expected_sub:
                recovery.append("Cached account metadata has a different schema. Update runtime scripts together; "
                                "the next separately authorized account refresh can upgrade it. Doctor never calls an account API.")
    if (cache["status"] != "ok" or cache.get("freshness") != "fresh"
            or cache.get("collection_state") in {"error", "stale", "unknown"}):
        command = (f"AI_USAGE_CHATGPT_USAGE=off {shlex.quote(sys.executable)} "
                   f"{shlex.quote(str(runtime / 'scripts/render_tmux_status.py'))} "
                   f"--chatgpt-usage off --cache-file {shlex.quote(str(cache_path))}")
        recovery.append("After correcting setup, explicitly run the local-only collector if desired: " + command +
                        ". This writes cache/output files; doctor has not run it. Preserve history and do not delete parse caches to fix freshness.")

    parse_path = config.parse_cache_file
    parse_cache = {"status": "disabled"}
    if parse_path is not None:
        state, metadata = _read_metadata(parse_path)
        current = _source_version(repo_root / "scripts/dashboard_core/collectors.py", "_PERSISTENT_CACHE_VERSION")
        parse_version = metadata.get("version") if type(metadata.get("version")) is int else None
        parse_cache = {"path": str(parse_path), "status": state, "version": parse_version, "expected_version": current,
                       "storage_role": "durable_observed_history", "history_schema": metadata.get("history_schema") if type(metadata.get("history_schema")) is int else None}
        if state == "ok":
            parse_cache["status"] = ("ok" if parse_version == current else "legacy_compatible" if parse_version == 4 else "incompatible")
        if metadata.get("history_schema", 1) != 1:
            parse_cache["status"] = "incompatible"
        if config.legacy_parse_cache_file is not None:
            parse_cache["legacy_migration_available"] = not parse_path.exists() and config.legacy_parse_cache_file.is_file()
        if parse_cache["status"] not in {"ok", "legacy_compatible"}:
            recovery.append("The parse cache may contain retained usage from deleted logs. "
                            "For missing history, locate/restore a validated backup; for corruption or schema mismatch, "
                            "preserve the original and use the matching runtime before an authorized migration. Do not remove it as disposable cache.")

    pricing = ({"status": "built_in"} if config.pricing_file is None
               else dict(_path_status(config.pricing_file), configured_by=settings.sources["pricing_file"]))
    if pricing["status"] not in {"ok", "built_in"}:
        recovery.append("Correct pricing_file / AI_USAGE_PRICING_FILE, restore the selected file, or explicitly select built-in pricing with saved null. "
                        "Doctor checks file existence, not rates or amounts.")
    dashboard_html = _path_status(config.dashboard_html)
    if dashboard_html["status"] != "ok":
        recovery.append("Runtime HTML is missing/unreadable. Start the normal launcher after fixing setup so it can seed disposable HTML; "
                        "do not point generated output at the tracked starter template.")
    return {
        "doctor_schema_version": 1, "read_only": True, "ok": not recovery,
        "python_version": sys.version.split()[0],
        "settings": {"path": str(settings.settings_file), "version": SETTINGS_VERSION,
                     "status": "loaded" if settings.settings_loaded else "default_file_absent"},
        "runtime": runtime_status, "sources": sources,
        "dashboard": {"url": settings.dashboard_url, "configured_by": settings.sources["dashboard_url"], "html": dashboard_html},
        "pricing": pricing, "cache": cache, "parse_cache": parse_cache, "history": parse_cache,
        "recovery": recovery,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print structured, metadata-only diagnostics.")
    args = parser.parse_args(argv)
    try:
        report = build_report()
    except SettingsError as error:
        report = {"doctor_schema_version": 1, "read_only": True, "ok": False,
                  "settings": {"status": "invalid", "message": str(error)},
                  "recovery": ["Repair the version 1 settings file or correct/unset AI_USAGE_SETTINGS_FILE. "
                               "Keep the original file and retained history; doctor did not write or collect anything."]}
        code = 2
    else:
        code = 0 if report["ok"] else 1
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("AI usage setup doctor (read-only; no collection, account requests or writes)")
        for key, value in report.items():
            if key not in {"recovery", "doctor_schema_version", "read_only"}:
                print(f"{key}: {json.dumps(value, sort_keys=True)}")
        for step in report["recovery"]:
            print(f"Recovery: {step}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
