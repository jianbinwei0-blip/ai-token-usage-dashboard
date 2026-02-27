#!/usr/bin/env python3
"""Local HTTP service to recalculate Codex token dashboard data on demand."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
HOST = os.environ.get("CODEX_USAGE_SERVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("CODEX_USAGE_SERVER_PORT", "8765"))
DASHBOARD_HTML = Path(
    os.environ.get(
        "CODEX_USAGE_DASHBOARD_HTML",
        str(REPO_ROOT / "dashboard" / "index.html"),
    )
)
SESSIONS_ROOT = Path(
    os.environ.get(
        "CODEX_USAGE_SESSIONS_ROOT",
        str(Path.home() / ".codex" / "sessions"),
    )
)


@dataclass
class DailyTotals:
    date: dt.date
    sessions: int = 0
    total_tokens: int = 0


def _fmt_num(value: int) -> str:
    return f"{value:,}"


def _parse_session_usage(session_path: Path) -> int | None:
    latest_total = None
    with session_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") != "event_msg":
                continue
            payload = event.get("payload") or {}
            if payload.get("type") != "token_count":
                continue
            total = (((payload.get("info") or {}).get("total_token_usage")) or {}).get("total_tokens")
            if isinstance(total, int):
                latest_total = total
    return latest_total


def _collect_daily_totals() -> dict[dt.date, DailyTotals]:
    totals: dict[dt.date, DailyTotals] = {}
    if not SESSIONS_ROOT.exists():
        return totals

    for file_path in SESSIONS_ROOT.rglob("*.jsonl"):
        relative = file_path.relative_to(SESSIONS_ROOT).parts
        if len(relative) < 4:
            continue
        try:
            year = int(relative[0])
            month = int(relative[1])
            day = int(relative[2])
            usage_date = dt.date(year, month, day)
        except ValueError:
            continue

        usage_tokens = _parse_session_usage(file_path)
        if usage_tokens is None:
            continue

        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        daily.sessions += 1
        daily.total_tokens += usage_tokens

    return totals


def _sum_range(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> tuple[int, int]:
    if to_date < from_date:
        return (0, 0)
    sessions = 0
    total_tokens = 0
    for day, values in daily.items():
        if from_date <= day <= to_date:
            sessions += values.sessions
            total_tokens += values.total_tokens
    return (sessions, total_tokens)


def recalc_dashboard() -> dict:
    today = dt.date.today()
    ytd_from = dt.date(today.year, 1, 1)
    daily_all = _collect_daily_totals()
    ytd_days = [v for d, v in daily_all.items() if ytd_from <= d <= today]

    rows = sorted(ytd_days, key=lambda item: (-item.total_tokens, item.date.isoformat()))
    days_count = len(rows)
    sessions_total = sum(item.sessions for item in rows)
    ytd_total = sum(item.total_tokens for item in rows)
    highest = rows[0].total_tokens if rows else 0

    today_sessions, today_total = _sum_range(daily_all, today, today)

    current_monday = today - dt.timedelta(days=today.isoweekday() - 1)
    yesterday = today - dt.timedelta(days=1)
    current_week_sessions, current_week_total = _sum_range(daily_all, current_monday, yesterday)

    prev_week_monday = current_monday - dt.timedelta(days=7)
    prev_week_sunday = current_monday - dt.timedelta(days=1)
    prev_week_sessions, prev_week_total = _sum_range(daily_all, prev_week_monday, prev_week_sunday)

    prev2_week_monday = prev_week_monday - dt.timedelta(days=7)
    prev2_week_sunday = prev_week_monday - dt.timedelta(days=1)
    prev2_week_sessions, prev2_week_total = _sum_range(daily_all, prev2_week_monday, prev2_week_sunday)

    stats_section = f"""    <section class="stats">
      <article class="stat">
        <div class="label">YTD Total Tokens</div>
        <div class="value">{_fmt_num(ytd_total)}</div>
      </article>
      <article class="stat">
        <div class="label">Days With Usage</div>
        <div class="value">{days_count}</div>
      </article>
      <article class="stat">
        <div class="label">Total Sessions</div>
        <div class="value">{sessions_total}</div>
      </article>
      <article class="stat">
        <div class="label">Highest Single Day</div>
        <div class="value">{_fmt_num(highest)}</div>
      </article>
      <article class="stat">
        <div class="label">Today ({today.isoformat()}, {today_sessions} sessions)</div>
        <div class="value">{_fmt_num(today_total)}</div>
      </article>
      <article class="stat">
        <div class="label">Current Week ({current_monday.isoformat()} to {yesterday.isoformat()}, {current_week_sessions} sessions)</div>
        <div class="value">{_fmt_num(current_week_total)}</div>
      </article>
      <article class="stat">
        <div class="label">Previous Week ({prev_week_monday.isoformat()} to {prev_week_sunday.isoformat()}, {prev_week_sessions} sessions)</div>
        <div class="value">{_fmt_num(prev_week_total)}</div>
      </article>
      <article class="stat">
        <div class="label">2 Weeks Ago ({prev2_week_monday.isoformat()} to {prev2_week_sunday.isoformat()}, {prev2_week_sessions} sessions)</div>
        <div class="value">{_fmt_num(prev2_week_total)}</div>
      </article>
    </section>"""

    row_lines = []
    for idx, item in enumerate(rows, start=1):
        rank_class = " top-3" if idx <= 3 else ""
        row_lines.append(
            f'            <tr><td><span class="rank{rank_class}">{idx}</span></td><td>{item.date.isoformat()}</td>'
            f'<td class="num">{item.sessions}</td><td class="num total-col">{_fmt_num(item.total_tokens)}</td></tr>'
        )
    tbody = "<tbody>\n" + "\n".join(row_lines) + "\n          </tbody>"

    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    html = re.sub(
        r"<section class=\"stats\">.*?</section>",
        stats_section,
        html,
        count=1,
        flags=re.DOTALL,
    )
    html = re.sub(
        r"<tbody>\s*.*?\s*</tbody>",
        tbody,
        html,
        count=1,
        flags=re.DOTALL,
    )
    DASHBOARD_HTML.write_text(html, encoding="utf-8")

    return {
        "ok": True,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "today": today.isoformat(),
        "ytd_total_tokens": ytd_total,
        "days_with_usage": days_count,
        "sessions": sessions_total,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "CodexUsageRecalc/2.0"

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
                    "service": "codex_usage_recalc",
                    "dashboard": str(DASHBOARD_HTML),
                    "sessions_root": str(SESSIONS_ROOT),
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
    print(f"Codex usage recalc service listening on http://{HOST}:{PORT}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
