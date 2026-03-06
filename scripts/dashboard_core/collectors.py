from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from .models import DailyTotals


def safe_non_negative_int(value: object) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


def parse_codex_session_usage(session_path: Path) -> int | None:
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


def collect_codex_daily_totals(sessions_root: Path) -> dict[dt.date, DailyTotals]:
    totals: dict[dt.date, DailyTotals] = {}
    if not sessions_root.exists():
        return totals

    for file_path in sessions_root.rglob("*.jsonl"):
        relative = file_path.relative_to(sessions_root).parts
        if len(relative) < 4:
            continue
        try:
            year = int(relative[0])
            month = int(relative[1])
            day = int(relative[2])
            usage_date = dt.date(year, month, day)
        except ValueError:
            continue

        usage_tokens = parse_codex_session_usage(file_path)
        if usage_tokens is None:
            continue

        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        daily.sessions += 1
        daily.total_tokens += usage_tokens

    return totals


def parse_timestamp_local(value: object) -> dt.datetime | None:
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


def collect_claude_daily_totals(claude_projects_root: Path) -> dict[dt.date, DailyTotals]:
    totals: dict[dt.date, DailyTotals] = {}
    if not claude_projects_root.exists():
        return totals

    request_usage: dict[tuple[str, str], dict[str, object]] = {}

    for file_path in claude_projects_root.rglob("*.jsonl"):
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

                local_timestamp = parse_timestamp_local(event.get("timestamp"))
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

                input_tokens = safe_non_negative_int(usage.get("input_tokens"))
                cache_creation_input_tokens = safe_non_negative_int(usage.get("cache_creation_input_tokens"))
                cache_read_input_tokens = safe_non_negative_int(usage.get("cache_read_input_tokens"))
                output_tokens = safe_non_negative_int(usage.get("output_tokens"))

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
                current["input_tokens"] = max(safe_non_negative_int(current.get("input_tokens")), input_tokens)
                current["cache_creation_input_tokens"] = max(
                    safe_non_negative_int(current.get("cache_creation_input_tokens")),
                    cache_creation_input_tokens,
                )
                current["cache_read_input_tokens"] = max(
                    safe_non_negative_int(current.get("cache_read_input_tokens")),
                    cache_read_input_tokens,
                )
                current["output_tokens"] = max(safe_non_negative_int(current.get("output_tokens")), output_tokens)

    daily_sessions: dict[dt.date, set[str]] = {}

    for request in request_usage.values():
        timestamp = request["timestamp"]
        if not isinstance(timestamp, dt.datetime):
            continue

        usage_date = timestamp.date()
        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        request_total = (
            safe_non_negative_int(request.get("input_tokens"))
            + safe_non_negative_int(request.get("cache_creation_input_tokens"))
            + safe_non_negative_int(request.get("cache_read_input_tokens"))
            + safe_non_negative_int(request.get("output_tokens"))
        )
        daily.total_tokens += request_total

        session_id = request.get("session_id")
        if isinstance(session_id, str) and session_id:
            daily_sessions.setdefault(usage_date, set()).add(session_id)

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    return totals


def collect_pi_daily_totals(pi_agent_root: Path) -> dict[dt.date, DailyTotals]:
    totals: dict[dt.date, DailyTotals] = {}
    if not pi_agent_root.exists():
        return totals

    sessions_root = pi_agent_root / "sessions"
    if not sessions_root.exists():
        return totals

    daily_sessions: dict[dt.date, set[str]] = {}

    for file_path in sessions_root.rglob("*.jsonl"):
        session_id = file_path.stem
        with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if event.get("type") == "session":
                    event_session_id = event.get("id")
                    if isinstance(event_session_id, str) and event_session_id:
                        session_id = event_session_id
                    continue

                if event.get("type") != "message":
                    continue

                local_timestamp = parse_timestamp_local(event.get("timestamp"))
                if local_timestamp is None:
                    continue

                message = event.get("message") or {}
                if not isinstance(message, dict) or message.get("role") != "assistant":
                    continue

                usage = message.get("usage") or {}
                if not isinstance(usage, dict):
                    continue

                usage_date = local_timestamp.date()
                daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
                daily.total_tokens += safe_non_negative_int(usage.get("totalTokens"))
                daily_sessions.setdefault(usage_date, set()).add(session_id)

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    return totals
