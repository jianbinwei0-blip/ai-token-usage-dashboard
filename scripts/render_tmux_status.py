#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from dashboard_core.chatgpt_subscription import fetch_chatgpt_subscription_usage
from dashboard_core.config import DashboardConfig
from dashboard_core.pipeline import recalc_dashboard
from dashboard_core.runtime_html import seed_runtime_html
from dashboard_core.tmux_status import build_tmux_status_snapshot, next_refresh_boundary, read_snapshot, render_tmux_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render compact tmux status output for the AI token dashboard.")
    parser.add_argument("--scope", default="combined", choices=["combined", "codex", "claude", "pi"])
    parser.add_argument("--range", dest="range_preset", default="mtd")
    parser.add_argument("--cache-file", default=str(REPO_ROOT / "tmp" / "tmux_status.json"))
    parser.add_argument("--refresh-interval-minutes", type=int, default=5)
    parser.add_argument(
        "--chatgpt-usage",
        choices=["auto", "off"],
        default=os.environ.get("AI_USAGE_CHATGPT_USAGE", "auto").strip().lower(),
    )
    parser.add_argument("--codex-bin", default=os.environ.get("AI_USAGE_CODEX_BIN", "codex"))
    parser.add_argument(
        "--chatgpt-timeout-seconds",
        type=float,
        default=float(os.environ.get("AI_USAGE_CHATGPT_TIMEOUT_SECONDS", "3")),
    )
    parser.add_argument("--max-width", type=int, default=None)
    parser.add_argument("--tmux-style", action="store_true")
    return parser.parse_args()


@contextlib.contextmanager
def file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_config() -> DashboardConfig:
    config = DashboardConfig.from_env(REPO_ROOT)
    if os.environ.get("AI_USAGE_DASHBOARD_HTML"):
        return config

    runtime_html = REPO_ROOT / "tmp" / "index.runtime.html"
    source_html = REPO_ROOT / "dashboard" / "index.html"
    if not runtime_html.exists():
        seed_runtime_html(source_html, runtime_html)
    return DashboardConfig(
        host=config.host,
        port=config.port,
        dashboard_html=runtime_html,
        sessions_root=config.sessions_root,
        claude_projects_root=config.claude_projects_root,
        pi_agent_root=config.pi_agent_root,
        pricing_file=config.pricing_file,
        parse_cache_file=config.parse_cache_file,
        recalc_log_file=config.recalc_log_file,
    )


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
        temp_path = Path(handle.name)
    temp_path.replace(path)


def snapshot_is_fresh(
    snapshot: dict | None,
    *,
    scope: str,
    range_preset: str,
    refresh_interval_minutes: int,
    chatgpt_usage: str = "auto",
) -> bool:
    if not isinstance(snapshot, dict):
        return False
    generated_at = snapshot.get("generated_at")
    from dashboard_core.tmux_status import parse_iso_datetime

    if int(snapshot.get("version") or 0) < 3:
        return False
    subscription_enabled = chatgpt_usage == "auto"
    if bool(snapshot.get("subscription_enabled")) != subscription_enabled:
        return False
    if subscription_enabled and not isinstance(snapshot.get("subscription"), dict):
        return False
    if str(snapshot.get("scope") or "combined") != scope:
        return False
    cached_range = (snapshot.get("range") or {}).get("preset")
    if str(cached_range or "mtd") != range_preset:
        return False
    if str(snapshot.get("health") or "").lower() == "error":
        return False

    generated = parse_iso_datetime(str(generated_at or ""))
    if generated is None:
        return False

    now = effective_now()
    current_boundary = next_refresh_boundary(now, interval_minutes=refresh_interval_minutes, now=now)
    snapshot_boundary = next_refresh_boundary(generated, interval_minutes=refresh_interval_minutes, now=now)
    if current_boundary is None or snapshot_boundary is None:
        return False
    return snapshot_boundary >= current_boundary


def effective_now() -> dt.datetime:
    return dt.datetime.now().astimezone()


def refresh_subscription_snapshot(
    previous_snapshot: dict | None,
    *,
    codex_binary: str,
    timeout_seconds: float,
    now: dt.datetime | None = None,
) -> dict:
    attempted_at = now or effective_now()
    try:
        return fetch_chatgpt_subscription_usage(
            codex_binary=codex_binary,
            timeout_seconds=timeout_seconds,
            now=attempted_at,
        )
    except Exception:
        previous = previous_snapshot.get("subscription") if isinstance(previous_snapshot, dict) else None
        if isinstance(previous, dict):
            fallback = dict(previous)
            if (
                str(fallback.get("account_type") or "").strip().lower() == "chatgpt"
                and isinstance(fallback.get("limits"), list)
                and fallback.get("limits")
            ):
                fallback["state"] = "stale"
            fallback["last_attempted_at"] = attempted_at.astimezone(dt.timezone.utc).isoformat()
            return fallback
        return {
            "version": 1,
            "state": "unavailable",
            "fetched_at": attempted_at.astimezone(dt.timezone.utc).isoformat(),
            "account_type": None,
        }


def refresh_snapshot(
    config: DashboardConfig,
    scope: str,
    range_preset: str,
    *,
    previous_snapshot: dict | None = None,
    chatgpt_usage: str = "auto",
    codex_binary: str = "codex",
    chatgpt_timeout_seconds: float = 3.0,
) -> dict:
    payload = recalc_dashboard(config, include_dataset=True)
    dataset_payload = payload.get("dataset") or {}
    timings_ms = payload.get("timings_ms") if isinstance(payload.get("timings_ms"), dict) else {}
    snapshot = build_tmux_status_snapshot(dataset_payload, timings_ms, scope=scope, range_preset=range_preset)
    subscription_enabled = chatgpt_usage == "auto"
    snapshot["subscription_enabled"] = subscription_enabled
    if subscription_enabled:
        snapshot["subscription"] = refresh_subscription_snapshot(
            previous_snapshot,
            codex_binary=codex_binary,
            timeout_seconds=chatgpt_timeout_seconds,
        )
    return snapshot


def render_error_from_cache(
    snapshot: dict | None,
    max_width: int | None,
    *,
    use_tmux_style: bool,
    refresh_interval_minutes: int,
) -> str:
    if snapshot:
        failed = dict(snapshot)
        failed["health"] = "error"
        return render_tmux_status(
            failed,
            max_width=max_width,
            use_tmux_style=use_tmux_style,
            refresh_interval_minutes=refresh_interval_minutes,
        )
    return render_tmux_status(
        {"health": "error"},
        max_width=max_width,
        use_tmux_style=use_tmux_style,
        refresh_interval_minutes=refresh_interval_minutes,
    )


def main() -> int:
    args = parse_args()
    cache_path = Path(args.cache_file)
    lock_path = cache_path.with_suffix(cache_path.suffix + ".lock")
    config = build_config()

    with file_lock(lock_path):
        snapshot = read_snapshot(cache_path)
        if not snapshot_is_fresh(
            snapshot,
            scope=args.scope,
            range_preset=args.range_preset,
            refresh_interval_minutes=args.refresh_interval_minutes,
            chatgpt_usage=args.chatgpt_usage,
        ):
            try:
                snapshot = refresh_snapshot(
                    config,
                    args.scope,
                    args.range_preset,
                    previous_snapshot=snapshot,
                    chatgpt_usage=args.chatgpt_usage,
                    codex_binary=args.codex_bin,
                    chatgpt_timeout_seconds=args.chatgpt_timeout_seconds,
                )
                atomic_write_json(cache_path, snapshot)
            except Exception:
                print(
                    render_error_from_cache(
                        snapshot,
                        args.max_width,
                        use_tmux_style=args.tmux_style,
                        refresh_interval_minutes=args.refresh_interval_minutes,
                    )
                )
                return 0

    print(
        render_tmux_status(
            snapshot or {"health": "error"},
            max_width=args.max_width,
            use_tmux_style=args.tmux_style,
            refresh_interval_minutes=args.refresh_interval_minutes,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
