#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
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

from dashboard_core.config import DashboardConfig
from dashboard_core.pipeline import recalc_dashboard
from dashboard_core.runtime_html import seed_runtime_html
from dashboard_core.tmux_status import build_tmux_status_snapshot, next_refresh_boundary, read_snapshot, render_tmux_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render compact tmux status output for the AI token dashboard.")
    parser.add_argument("--scope", default="combined", choices=["combined", "codex", "claude", "pi"])
    parser.add_argument("--range", dest="range_preset", default="wtd")
    parser.add_argument("--cache-file", default=str(REPO_ROOT / "tmp" / "tmux_status.json"))
    parser.add_argument("--refresh-interval-minutes", type=int, default=5)
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


def snapshot_is_fresh(snapshot: dict | None, *, scope: str, range_preset: str, refresh_interval_minutes: int) -> bool:
    if not isinstance(snapshot, dict):
        return False
    generated_at = snapshot.get("generated_at")
    from dashboard_core.tmux_status import parse_iso_datetime

    if str(snapshot.get("scope") or "combined") != scope:
        return False
    cached_range = (snapshot.get("range") or {}).get("preset")
    if str(cached_range or "wtd") != range_preset:
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


def effective_now():
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc)


def refresh_snapshot(config: DashboardConfig, scope: str, range_preset: str) -> dict:
    payload = recalc_dashboard(config, include_dataset=True)
    dataset_payload = payload.get("dataset") or {}
    timings_ms = payload.get("timings_ms") if isinstance(payload.get("timings_ms"), dict) else {}
    return build_tmux_status_snapshot(dataset_payload, timings_ms, scope=scope, range_preset=range_preset)


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
        ):
            try:
                snapshot = refresh_snapshot(config, args.scope, args.range_preset)
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
