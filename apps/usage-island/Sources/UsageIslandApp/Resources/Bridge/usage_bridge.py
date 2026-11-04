#!/usr/bin/env python3
"""Read the existing usage cache; optionally run its lock-aware collector.

The bundled formatter is copied from scripts/dashboard_core/tmux_status.py
at build time. This adapter has no SketchyBar dependency.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys

import dashboard_status_formatter as formatter

# The build generates this resource verbatim from dashboard_core/settings.py.
# A source-tree fallback makes unprepared developer/test runs convenient, but an
# installed app never depends on an external runtime to read its saved settings.
try:
    import dashboard_settings as settings_module
except ModuleNotFoundError as error:
    if error.name != "dashboard_settings":
        raise
    checkout = next((p for p in Path(__file__).resolve().parents
                     if (p / "scripts/dashboard_core/settings.py").is_file()), None)
    if checkout is None:
        raise SystemExit("Shared settings reader is missing. Rebuild/reinstall the app from matching sources.") from None
    sys.path.insert(0, str(checkout / "scripts"))
    from dashboard_core import settings as settings_module

HOME = Path.home()
ACTIVE_CHILDREN: set[subprocess.Popen] = set()


def configuration() -> settings_module.SharedSettings:
    # Resolve lazily: importing formatter/test helpers must not read user settings.
    return settings_module.load_settings()


def cache_path(settings: settings_module.SharedSettings) -> Path:
    return Path(os.environ.get("AI_USAGE_CACHE_FILE", str(settings.runtime_dir / "tmp/tmux_status.json"))).expanduser()


def environment(settings: settings_module.SharedSettings | None = None, *, include_settings: bool = True) -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if include_settings:
        settings = settings if settings is not None else configuration()
        defaults = {
            "AI_USAGE_DASHBOARD_DIR": str(settings.runtime_dir),
            "AI_USAGE_DASHBOARD_URL": settings.dashboard_url,
            "AI_USAGE_SERVER_HOST": settings.server_host,
            "AI_USAGE_SERVER_PORT": str(settings.server_port),
            "AI_USAGE_CODEX_SESSIONS_ROOT": str(settings.codex_sessions_root),
            "AI_USAGE_CLAUDE_PROJECTS_ROOT": str(settings.claude_projects_root),
            "AI_USAGE_PI_AGENT_ROOT": str(settings.pi_agent_root),
            "AI_USAGE_DSH_HOME": str(settings.dsh_home),
            "AI_USAGE_PRICING_FILE": str(settings.pricing_file) if settings.pricing_file is not None else "",
            "AI_USAGE_CACHE_FILE": str(cache_path(settings)),
        }
        for key, value in defaults.items():
            env.setdefault(key, value)
    return env


def stop_child(child: subprocess.Popen, *, force_group: bool = False) -> None:
    if child.poll() is not None and not force_group:
        return
    try:
        os.killpg(child.pid, signal.SIGTERM)
        child.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.communicate()
    except ProcessLookupError:
        pass


def shutdown(_signum, _frame) -> None:
    for child in list(ACTIVE_CHILDREN):
        stop_child(child, force_group=True)
    raise SystemExit(143)


def run_command(arguments: list[str], *, timeout: float, cwd: Path | None = None,
                env: dict[str, str] | None = None) -> tuple[int, str]:
    # A process group lets timeouts/quit clean up the collector's app-server too.
    child = subprocess.Popen(
        arguments, cwd=cwd, env=env if env is not None else environment(),
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        text=True, start_new_session=True,
    )
    ACTIVE_CHILDREN.add(child)
    try:
        output, _ = child.communicate(timeout=timeout)
        return child.returncode, output
    except subprocess.TimeoutExpired:
        # A parent can exit while descendants still hold its stdout open.
        stop_child(child, force_group=True)
        raise
    finally:
        stop_child(child)
        ACTIVE_CHILDREN.discard(child)


def refresh_cache(settings: settings_module.SharedSettings | None = None) -> bool:
    try:
        settings = settings if settings is not None else configuration()
        code, output = run_command([
            sys.executable, str(settings.runtime_dir / "scripts/render_tmux_status.py"),
            "--cache-file", str(cache_path(settings)), "--scope", "combined", "--range", "mtd",
            "--refresh-interval-minutes", "5", "--max-width", "96",
        ], timeout=40, cwd=settings.runtime_dir, env=environment(settings))
        # The existing collector may return exit 0 with an explicit error label.
        return code == 0 and not re.match(r"AI(?: combined)? error\b", output)
    except (OSError, subprocess.SubprocessError, settings_module.SettingsError):
        return False


def read_cache(path: Path) -> dict | None:
    try:
        if path.stat().st_size > 1_000_000:
            return None
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(snapshot, dict) or not snapshot:
            return None
        for key in ("metrics", "quality", "range"):
            if key in snapshot and not isinstance(snapshot[key], dict):
                return None
        if not isinstance(snapshot.get("providers"), list):
            return None
        metrics = snapshot.get("metrics") or {}
        if not {"today_tokens", "range_tokens"}.issubset(metrics):
            return None
        for key in ("today_tokens", "range_tokens", "range_cost_usd"):
            value = metrics.get(key)
            if key != "range_cost_usd" and value is None:
                return None
            if value is not None and (isinstance(value, bool) or
                                      not isinstance(value, (int, float)) or
                                      not math.isfinite(value) or value < 0):
                return None
        return snapshot
    except (OSError, ValueError):
        return None


def fields_for_text(text: str) -> list[dict[str, str]]:
    # Group canonical caption/value segments without changing accounting rules.
    fields = []

    def add(key, caption, value, tone="text"):
        fields.append(dict(key=key, caption=caption, value=value, tone=tone))

    for part in text.split(" · "):
        part = part.strip()
        if not part:
            continue
        head, separator, reset = part.partition(" ↻")
        quota = re.fullmatch(r"(?:Weekly|7d) (.+)", head)
        if (part.startswith("GPT") or part == "AI") and not any(f["key"] == "plan" for f in fields):
            add("plan", "", part, "warning" if "stale" in part else "positive")
        elif part.startswith("AI "):
            add("health", "", part, "error" if "error" in part else "warning")
        elif quota:
            value = quota[1].strip()
            percentage = re.search(r"(\d+)%", value)
            remaining = int(percentage[1]) if percentage else 100
            add("weekly", "Weekly", value,
                "error" if remaining == 0 else "warning" if remaining <= 20 else "positive")
            if separator:
                add("reset", "Reset", reset.strip())
        elif part.startswith(("Credits ", "Cr ")):
            add("credits", "Credits", part.split(" ", 1)[1].strip())
        elif part.startswith("Today "):
            add("today", "Today", part[6:].strip())
        elif part.startswith("MTD "):
            add("month", "MTD", part[4:].strip())
        elif part.startswith("$") or part == "cost?":
            add("cost", "Est.", part, "warning" if "*" in part or "?" in part else "text")
        elif re.fullmatch(r"\d{2}:\d{2} → \d{2}:\d{2}", part):
            add("refresh", "Updated", part)
        else:
            add(f"extra{len(fields)}", "", part,
                "warning" if "stale" in part or "limits" in part else "text")
    return fields


def reset_credit_payload(subscription: object, *, scope: str) -> dict | None:
    if (scope not in {"combined", "codex"} or not isinstance(subscription, dict)
            or subscription.get("account_type") != "chatgpt"
            or subscription.get("state") == "not_applicable"):
        return None
    available = subscription.get("reset_credits_available")
    if type(available) is not int or not 0 <= available <= 2**63 - 1:
        available = None
    raw = subscription.get("reset_credit_expirations")
    expirations = None
    if isinstance(raw, list):
        expirations = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            count = entry.get("count")
            if type(count) is not int or not 0 < count <= 2**63 - 1:
                continue
            expires_at = entry.get("expires_at")
            if type(expires_at) is not int or not 0 < expires_at <= 253_402_300_799:
                expires_at = None
            does_not_expire = entry.get("does_not_expire") is True and entry.get("expires_at") is None
            expirations.append(dict(count=count, expires_at=expires_at, does_not_expire=does_not_expire))
    complete = (expirations is not None and available is not None
                and sum(entry["count"] for entry in expirations) == available)
    return dict(available_count=available, expirations=expirations, details_complete=complete)


def payload(snapshot: dict | None, *, failed: bool = False,
            now: dt.datetime | None = None) -> dict:
    now = now or dt.datetime.now().astimezone()
    common = dict(schema_version=1, read_at=now.isoformat())
    if not snapshot:
        return dict(common, generated_at=None, health="unavailable",
                    subscription_state="not_applicable", subscription_fetched_at=None, reset_credits=None,
                    pricing_complete=False, scope="combined", providers=[],
                    text="AI unavailable", fields=fields_for_text("AI unavailable"),
                    message="Usage cache is missing or unreadable. Check that the existing dashboard runtime is available.")
    effective = dict(snapshot, health="error") if failed else snapshot
    try:
        text = formatter.render_tmux_status(effective, now=now, max_width=None,
                                            use_tmux_style=False, refresh_interval_minutes=5)
        health = formatter.effective_health(effective, now, refresh_interval_minutes=5)
        subscription = snapshot.get("subscription")
        state = formatter.subscription_effective_state(subscription, now)
        message = "Collection failed. Showing only the cached information allowed by the existing formatter." if failed else None
        return dict(
            common, generated_at=snapshot.get("generated_at"), health=health,
            subscription_state=state,
            subscription_fetched_at=subscription.get("fetched_at") if isinstance(subscription, dict) else None,
            reset_credits=reset_credit_payload(subscription, scope=str(snapshot.get("scope") or "combined")),
            pricing_complete=bool((snapshot.get("quality") or {}).get("pricing_complete", True)),
            cost_explanation=formatter.cost_explanation(snapshot),
            scope=str(snapshot.get("scope") or "combined"),
            providers=[str(p) for p in snapshot.get("providers", [])],
            text=text, fields=fields_for_text(text), message=message,
        )
    except (AttributeError, TypeError, ValueError, KeyError, OverflowError):
        result = payload(None, now=now)
        result["message"] = "Usage cache has an unsupported or invalid format."
        return result


def resource_payload() -> dict:
    try:
        env = environment(include_settings=False)
        # Isolate the helper's disk cache from tmux's concurrent refreshes.
        env["XDG_CACHE_HOME"] = str(HOME / "Library/Caches/AIUsageIsland")
        code, text = run_command(
            ["/bin/sh", str(Path(__file__).with_name("status_resources.sh"))],
            timeout=5, env=env,
        )
        match = re.fullmatch(r"RAM (\d+%|n/a) DSK (\d+%|n/a)", text.strip())
        if code != 0 or not match:
            raise ValueError("Unavailable resources")
        ram, disk = match.groups()
        return dict(ram=ram, disk=disk, available=ram != "n/a" and disk != "n/a")
    except (OSError, ValueError, subprocess.SubprocessError):
        return dict(ram="n/a", disk="n/a", available=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("ai", "resources", "dashboard-url"), nargs="?", default="ai")
    parser.add_argument("--refresh", action="store_true", help="Use the existing five-minute collector cache/lock.")
    args = parser.parse_args()
    if args.refresh and args.mode != "ai":
        parser.error("--refresh is only valid in ai mode")
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    if args.mode == "resources":
        result = resource_payload()
    else:
        try:
            settings = configuration()
            if args.mode == "dashboard-url":
                result = {"dashboard_url": settings.dashboard_url}
            else:
                failed = args.refresh and not refresh_cache(settings)
                result = payload(read_cache(cache_path(settings)), failed=failed)
        except settings_module.SettingsError:
            if args.mode == "dashboard-url":
                parser.exit(2, "Invalid shared settings. Run scripts/usage_doctor.py in the dashboard checkout.\n")
            result = payload(None)
            result["message"] = "Invalid shared settings. Run scripts/usage_doctor.py in the dashboard checkout; no collection was attempted."
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
