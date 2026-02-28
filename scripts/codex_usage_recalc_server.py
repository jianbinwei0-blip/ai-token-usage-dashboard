#!/usr/bin/env python3
"""Local HTTP service to recalculate multi-provider AI token dashboard data on demand."""

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
CLAUDE_PROJECTS_ROOT = Path(
    os.environ.get(
        "CODEX_USAGE_CLAUDE_PROJECTS_ROOT",
        str(Path.home() / ".claude" / "projects"),
    )
)


@dataclass
class DailyTotals:
    date: dt.date
    sessions: int = 0
    total_tokens: int = 0


def _fmt_num(value: int) -> str:
    return f"{value:,}"


def _safe_int(value: object) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


def _parse_codex_session_usage(session_path: Path) -> int | None:
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


def _collect_codex_daily_totals(sessions_root: Path | None = None) -> dict[dt.date, DailyTotals]:
    root = sessions_root or SESSIONS_ROOT
    totals: dict[dt.date, DailyTotals] = {}
    if not root.exists():
        return totals

    for file_path in root.rglob("*.jsonl"):
        relative = file_path.relative_to(root).parts
        if len(relative) < 4:
            continue
        try:
            year = int(relative[0])
            month = int(relative[1])
            day = int(relative[2])
            usage_date = dt.date(year, month, day)
        except ValueError:
            continue

        usage_tokens = _parse_codex_session_usage(file_path)
        if usage_tokens is None:
            continue

        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        daily.sessions += 1
        daily.total_tokens += usage_tokens

    return totals


def _parse_timestamp_local(value: object) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)

    return parsed.astimezone()


def _collect_claude_daily_totals(claude_projects_root: Path | None = None) -> dict[dt.date, DailyTotals]:
    root = claude_projects_root or CLAUDE_PROJECTS_ROOT
    totals: dict[dt.date, DailyTotals] = {}
    if not root.exists():
        return totals

    request_usage: dict[tuple[str, str], dict[str, object]] = {}

    for file_path in root.rglob("*.jsonl"):
        session_scope = file_path.stem
        with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                request_id = event.get("requestId")
                if not isinstance(request_id, str) or not request_id:
                    continue

                local_timestamp = _parse_timestamp_local(event.get("timestamp"))
                if local_timestamp is None:
                    continue

                message = event.get("message") or {}
                if not isinstance(message, dict):
                    continue

                usage = message.get("usage") or {}
                if not isinstance(usage, dict):
                    continue

                session_id = event.get("sessionId")
                if not isinstance(session_id, str) or not session_id:
                    session_id = session_scope

                dedupe_key = (session_id, request_id)
                current = request_usage.get(dedupe_key)

                input_tokens = _safe_int(usage.get("input_tokens"))
                cache_creation_input_tokens = _safe_int(usage.get("cache_creation_input_tokens"))
                cache_read_input_tokens = _safe_int(usage.get("cache_read_input_tokens"))
                output_tokens = _safe_int(usage.get("output_tokens"))

                if current is None:
                    request_usage[dedupe_key] = {
                        "session_id": session_id,
                        "timestamp": local_timestamp,
                        "input_tokens": input_tokens,
                        "cache_creation_input_tokens": cache_creation_input_tokens,
                        "cache_read_input_tokens": cache_read_input_tokens,
                        "output_tokens": output_tokens,
                    }
                    continue

                current["timestamp"] = max(current["timestamp"], local_timestamp)
                current["input_tokens"] = max(_safe_int(current.get("input_tokens")), input_tokens)
                current["cache_creation_input_tokens"] = max(
                    _safe_int(current.get("cache_creation_input_tokens")),
                    cache_creation_input_tokens,
                )
                current["cache_read_input_tokens"] = max(
                    _safe_int(current.get("cache_read_input_tokens")),
                    cache_read_input_tokens,
                )
                current["output_tokens"] = max(_safe_int(current.get("output_tokens")), output_tokens)

    daily_sessions: dict[dt.date, set[str]] = {}

    for request in request_usage.values():
        timestamp = request["timestamp"]
        if not isinstance(timestamp, dt.datetime):
            continue

        usage_date = timestamp.date()
        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        request_total = (
            _safe_int(request.get("input_tokens"))
            + _safe_int(request.get("cache_creation_input_tokens"))
            + _safe_int(request.get("cache_read_input_tokens"))
            + _safe_int(request.get("output_tokens"))
        )
        daily.total_tokens += request_total

        session_id = request.get("session_id")
        if isinstance(session_id, str) and session_id:
            daily_sessions.setdefault(usage_date, set()).add(session_id)

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    return totals


def _combine_daily_totals(*providers: dict[dt.date, DailyTotals]) -> dict[dt.date, DailyTotals]:
    combined: dict[dt.date, DailyTotals] = {}

    for provider in providers:
        for usage_date, values in provider.items():
            daily = combined.setdefault(usage_date, DailyTotals(date=usage_date))
            daily.sessions += values.sessions
            daily.total_tokens += values.total_tokens

    return combined


def _sum_range(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> tuple[int, int]:
    if to_date < from_date:
        return (0, 0)
    sessions = 0
    total_tokens = 0
    for usage_date, values in daily.items():
        if from_date <= usage_date <= to_date:
            sessions += values.sessions
            total_tokens += values.total_tokens
    return (sessions, total_tokens)


def _slice_daily(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> dict[dt.date, DailyTotals]:
    return {
        usage_date: values
        for usage_date, values in daily.items()
        if from_date <= usage_date <= to_date
    }


def _rows_from_daily(daily: dict[dt.date, DailyTotals]) -> list[dict[str, int | str]]:
    rows = []
    for usage_date, values in daily.items():
        rows.append(
            {
                "date": usage_date.isoformat(),
                "sessions": values.sessions,
                "total_tokens": values.total_tokens,
            }
        )
    rows.sort(key=lambda row: row["date"], reverse=True)
    return rows


def _summary_from_daily(daily: dict[dt.date, DailyTotals]) -> dict[str, int]:
    days = list(daily.values())
    highest = max((item.total_tokens for item in days), default=0)
    return {
        "ytd_total_tokens": sum(item.total_tokens for item in days),
        "days_with_usage": len(days),
        "sessions": sum(item.sessions for item in days),
        "highest_single_day": highest,
    }


def _providers_available(codex_rows: list[dict[str, int | str]], claude_rows: list[dict[str, int | str]]) -> dict[str, bool]:
    return {
        "codex": bool(codex_rows),
        "claude": bool(claude_rows),
        "combined": bool(codex_rows or claude_rows),
    }


def _inject_usage_dataset(html: str, dataset: dict) -> str:
    dataset_json = json.dumps(dataset, separators=(",", ":"))
    script = f'<script id="usageDataset" type="application/json">{dataset_json}</script>'
    pattern = r"<script id=\"usageDataset\" type=\"application/json\">.*?</script>"

    if re.search(pattern, html, flags=re.DOTALL):
        return re.sub(pattern, script, html, count=1, flags=re.DOTALL)

    if "</main>" in html:
        return html.replace("</main>", f"  {script}\n  </main>", 1)

    return html + "\n" + script


def recalc_dashboard() -> dict:
    today = dt.date.today()
    ytd_from = dt.date(today.year, 1, 1)

    codex_daily_all = _collect_codex_daily_totals()
    claude_daily_all = _collect_claude_daily_totals()
    combined_daily_all = _combine_daily_totals(codex_daily_all, claude_daily_all)

    codex_daily_ytd = _slice_daily(codex_daily_all, ytd_from, today)
    claude_daily_ytd = _slice_daily(claude_daily_all, ytd_from, today)
    combined_daily_ytd = _slice_daily(combined_daily_all, ytd_from, today)

    rows = sorted(combined_daily_ytd.values(), key=lambda item: (-item.total_tokens, item.date.isoformat()))
    days_count = len(rows)
    sessions_total = sum(item.sessions for item in rows)
    ytd_total = sum(item.total_tokens for item in rows)
    highest = rows[0].total_tokens if rows else 0

    today_sessions, today_total = _sum_range(combined_daily_all, today, today)

    current_monday = today - dt.timedelta(days=today.isoweekday() - 1)
    yesterday = today - dt.timedelta(days=1)
    current_week_sessions, current_week_total = _sum_range(combined_daily_all, current_monday, yesterday)

    prev_week_monday = current_monday - dt.timedelta(days=7)
    prev_week_sunday = current_monday - dt.timedelta(days=1)
    prev_week_sessions, prev_week_total = _sum_range(combined_daily_all, prev_week_monday, prev_week_sunday)

    prev2_week_monday = prev_week_monday - dt.timedelta(days=7)
    prev2_week_sunday = prev_week_monday - dt.timedelta(days=1)
    prev2_week_sessions, prev2_week_total = _sum_range(combined_daily_all, prev2_week_monday, prev2_week_sunday)

    stats_section = f"""    <section class=\"stats\">
      <article class=\"stat\">
        <div class=\"label\">YTD Total Tokens</div>
        <div class=\"value\">{_fmt_num(ytd_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Days With Usage</div>
        <div class=\"value\">{days_count}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Total Sessions</div>
        <div class=\"value\">{sessions_total}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Highest Single Day</div>
        <div class=\"value\">{_fmt_num(highest)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Today ({today.isoformat()}, {today_sessions} sessions)</div>
        <div class=\"value\">{_fmt_num(today_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Current Week ({current_monday.isoformat()} to {yesterday.isoformat()}, {current_week_sessions} sessions)</div>
        <div class=\"value\">{_fmt_num(current_week_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Previous Week ({prev_week_monday.isoformat()} to {prev_week_sunday.isoformat()}, {prev_week_sessions} sessions)</div>
        <div class=\"value\">{_fmt_num(prev_week_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">2 Weeks Ago ({prev2_week_monday.isoformat()} to {prev2_week_sunday.isoformat()}, {prev2_week_sessions} sessions)</div>
        <div class=\"value\">{_fmt_num(prev2_week_total)}</div>
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

    codex_rows_all = _rows_from_daily(codex_daily_all)
    claude_rows_all = _rows_from_daily(claude_daily_all)
    combined_rows_all = _rows_from_daily(combined_daily_all)
    providers_available = _providers_available(codex_rows_all, claude_rows_all)

    dataset_payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "timezone": dt.datetime.now().astimezone().tzname() or "local",
        "paths": {
            "codex_sessions_root": str(SESSIONS_ROOT),
            "claude_projects_root": str(CLAUDE_PROJECTS_ROOT),
        },
        "providers_available": providers_available,
        "providers": {
            "codex": {"rows": codex_rows_all},
            "claude": {"rows": claude_rows_all},
            "combined": {"rows": combined_rows_all},
        },
    }

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
    html = _inject_usage_dataset(html, dataset_payload)
    DASHBOARD_HTML.write_text(html, encoding="utf-8")

    return {
        "ok": True,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "today": today.isoformat(),
        "ytd_total_tokens": ytd_total,
        "days_with_usage": days_count,
        "sessions": sessions_total,
        "sources": {
            "codex_sessions_root": str(SESSIONS_ROOT),
            "claude_projects_root": str(CLAUDE_PROJECTS_ROOT),
        },
        "providers_available": providers_available,
        "providers": {
            "codex": _summary_from_daily(codex_daily_ytd),
            "claude": _summary_from_daily(claude_daily_ytd),
            "combined": _summary_from_daily(combined_daily_ytd),
        },
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "AIUsageRecalc/3.0"

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
