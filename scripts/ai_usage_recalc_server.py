#!/usr/bin/env python3
"""Local HTTP service to recalculate multi-provider AI token dashboard data on demand."""

from __future__ import annotations

import datetime as dt
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from dashboard_core.aggregation import (
    combine_daily_totals,
    current_week_end,
    providers_available,
    rows_from_daily,
    slice_daily,
    summary_from_daily,
    sum_range,
)
from dashboard_core.collectors import collect_claude_daily_totals, collect_codex_daily_totals
from dashboard_core.config import DashboardConfig
from dashboard_core.models import DailyTotals
from dashboard_core.pipeline import recalc_dashboard as recalc_dashboard_pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = DashboardConfig.from_env(REPO_ROOT)
HOST = CONFIG.host
PORT = CONFIG.port
DASHBOARD_HTML = CONFIG.dashboard_html
SESSIONS_ROOT = CONFIG.sessions_root
CLAUDE_PROJECTS_ROOT = CONFIG.claude_projects_root


# Compatibility helpers used by local tests and existing workflows.
def _fmt_num(value: int) -> str:
    return f"{value:,}"


def _collect_codex_daily_totals(sessions_root: Path | None = None) -> dict[dt.date, DailyTotals]:
    return collect_codex_daily_totals(sessions_root or SESSIONS_ROOT)


def _collect_claude_daily_totals(claude_projects_root: Path | None = None) -> dict[dt.date, DailyTotals]:
    return collect_claude_daily_totals(claude_projects_root or CLAUDE_PROJECTS_ROOT)


def _combine_daily_totals(*providers: dict[dt.date, DailyTotals]) -> dict[dt.date, DailyTotals]:
    return combine_daily_totals(*providers)


def _sum_range(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> tuple[int, int]:
    return sum_range(daily, from_date, to_date)


def _current_week_end(today: dt.date) -> dt.date:
    return current_week_end(today)


def _slice_daily(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> dict[dt.date, DailyTotals]:
    return slice_daily(daily, from_date, to_date)


def _rows_from_daily(daily: dict[dt.date, DailyTotals]) -> list[dict[str, int | str]]:
    return rows_from_daily(daily)


def _summary_from_daily(daily: dict[dt.date, DailyTotals]) -> dict[str, int]:
    return summary_from_daily(daily)


def _providers_available(codex_rows: list[dict[str, int | str]], claude_rows: list[dict[str, int | str]]) -> dict[str, bool]:
    return providers_available(codex_rows, claude_rows)


def recalc_dashboard() -> dict:
    return recalc_dashboard_pipeline(CONFIG)


class Handler(BaseHTTPRequestHandler):
    server_version = "AIUsageRecalc/4.0"

    def _set_headers(self, status_code: int = 200, content_type: str = "application/json") -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        if content_type == "text/html; charset=utf-8":
            self.send_header("Cache-Control", "no-cache")
        else:
            self.send_header("Cache-Control", "no-store, max-age=0")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _write_json(self, payload: dict, status_code: int = 200) -> None:
        self._set_headers(status_code=status_code)
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _write_html(self, html: str, status_code: int = 200) -> None:
        self._set_headers(status_code=status_code, content_type="text/html; charset=utf-8")
        self.wfile.write(html.encode("utf-8"))

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._set_headers(status_code=204)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in {"/", "/index.html"}:
            try:
                self._write_html(DASHBOARD_HTML.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                self._write_json(
                    {
                        "ok": False,
                        "error": "dashboard_unavailable",
                        "message": str(exc),
                        "dashboard": str(DASHBOARD_HTML),
                    },
                    status_code=500,
                )
            return

        if path == "/health":
            self._write_json(
                {
                    "ok": True,
                    "service": "ai_usage_recalc",
                    "dashboard": str(DASHBOARD_HTML),
                    "sessions_root": str(SESSIONS_ROOT),
                    "claude_projects_root": str(CLAUDE_PROJECTS_ROOT),
                    "providers_available": {
                        "codex": SESSIONS_ROOT.exists(),
                        "claude": CLAUDE_PROJECTS_ROOT.exists(),
                        "combined": SESSIONS_ROOT.exists() or CLAUDE_PROJECTS_ROOT.exists(),
                    },
                    "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                }
            )
            return

        if path == "/recalc":
            try:
                payload = recalc_dashboard()
                self._write_json(payload, status_code=200)
            except Exception as exc:  # noqa: BLE001
                self._write_json(
                    {
                        "ok": False,
                        "error": "recalc_failed",
                        "message": str(exc),
                        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                    },
                    status_code=500,
                )
            return

        self._write_json({"ok": False, "error": "not_found", "path": path}, status_code=404)

    def log_message(self, _format: str, *args: object) -> None:
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = _format % args if args else _format
        print(f"[{timestamp}] {self.client_address[0]} {self.command} {self.path} :: {message}")


def main() -> None:
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"AI usage recalc service listening on http://{HOST}:{PORT} (currently Codex and Claude)")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
