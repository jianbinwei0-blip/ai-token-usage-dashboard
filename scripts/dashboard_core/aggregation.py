from __future__ import annotations

import datetime as dt

from .models import DailyTotals


def combine_daily_totals(*providers: dict[dt.date, DailyTotals]) -> dict[dt.date, DailyTotals]:
    combined: dict[dt.date, DailyTotals] = {}

    for provider in providers:
        for usage_date, values in provider.items():
            daily = combined.setdefault(usage_date, DailyTotals(date=usage_date))
            daily.sessions += values.sessions
            daily.total_tokens += values.total_tokens

    return combined


def sum_range(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> tuple[int, int]:
    if to_date < from_date:
        return (0, 0)
    sessions = 0
    total_tokens = 0
    for usage_date, values in daily.items():
        if from_date <= usage_date <= to_date:
            sessions += values.sessions
            total_tokens += values.total_tokens
    return (sessions, total_tokens)


def current_week_end(today: dt.date) -> dt.date:
    return today


def slice_daily(daily: dict[dt.date, DailyTotals], from_date: dt.date, to_date: dt.date) -> dict[dt.date, DailyTotals]:
    return {
        usage_date: values
        for usage_date, values in daily.items()
        if from_date <= usage_date <= to_date
    }


def rows_from_daily(daily: dict[dt.date, DailyTotals]) -> list[dict[str, int | str]]:
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


def summary_from_daily(daily: dict[dt.date, DailyTotals]) -> dict[str, int]:
    days = list(daily.values())
    highest = max((item.total_tokens for item in days), default=0)
    return {
        "ytd_total_tokens": sum(item.total_tokens for item in days),
        "days_with_usage": len(days),
        "sessions": sum(item.sessions for item in days),
        "highest_single_day": highest,
    }


def providers_available(codex_rows: list[dict[str, int | str]], claude_rows: list[dict[str, int | str]]) -> dict[str, bool]:
    return {
        "codex": bool(codex_rows),
        "claude": bool(claude_rows),
        "combined": bool(codex_rows or claude_rows),
    }
