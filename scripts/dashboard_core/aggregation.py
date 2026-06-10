from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from .models import ActivityTotals, AttributionTotals, BreakdownTotals, DailyTotals, round_cost


@dataclass
class DailyMaterialization:
    values: list[DailyTotals]
    rows: list[dict[str, int | float | str | bool | list[dict[str, int | float | str | bool]]]]
    ranked_values: list[DailyTotals]
    summary: dict[str, int | float | bool | str]
    breakdown_rows: list[dict[str, int | float | str | bool]]
    attribution_rows: list[dict[str, int | float | str | bool]]


def serialize_breakdown_rows(breakdowns: dict[tuple[str, str], BreakdownTotals]) -> list[dict[str, int | float | str | bool]]:
    rows = [
        {
            "agent_cli": bucket.agent_cli,
            "model": bucket.model,
            "sessions": bucket.sessions,
            "input_tokens": bucket.input_tokens,
            "output_tokens": bucket.output_tokens,
            "cached_tokens": bucket.cached_tokens,
            "total_tokens": bucket.total_tokens,
            "input_cost_usd": bucket.input_cost_usd,
            "output_cost_usd": bucket.output_cost_usd,
            "cached_cost_usd": bucket.cached_cost_usd,
            "total_cost_usd": bucket.total_cost_usd,
            "cost_complete": bucket.cost_complete,
            "cost_status": bucket.cost_status,
        }
        for bucket in breakdowns.values()
    ]
    rows.sort(
        key=lambda row: (
            -int(row["total_tokens"]),
            -int(row["sessions"]),
            str(row["agent_cli"]),
            str(row["model"]),
        )
    )
    return rows


def serialize_attribution_rows(attributions: dict[tuple[str, str], AttributionTotals]) -> list[dict[str, int | float | str | bool]]:
    rows = [
        {
            "category": bucket.category,
            "name": bucket.name,
            "sessions": bucket.sessions,
            "events": bucket.events,
            "input_tokens": bucket.input_tokens,
            "output_tokens": bucket.output_tokens,
            "cached_tokens": bucket.cached_tokens,
            "total_tokens": bucket.total_tokens,
            "input_cost_usd": bucket.input_cost_usd,
            "output_cost_usd": bucket.output_cost_usd,
            "cached_cost_usd": bucket.cached_cost_usd,
            "total_cost_usd": bucket.total_cost_usd,
            "cost_complete": bucket.cost_complete,
            "cost_status": bucket.cost_status,
        }
        for bucket in attributions.values()
    ]
    rows.sort(
        key=lambda row: (
            str(row["category"]),
            -int(row["total_tokens"]),
            -int(row["events"]),
            str(row["name"]),
        )
    )
    return rows


def serialize_activity_rows(activity_totals: dict[tuple[dt.date, int], ActivityTotals]) -> list[dict[str, int | float | str | bool]]:
    rows = [
        {
            "date": activity.date.isoformat(),
            "hour": activity.hour,
            "sessions": activity.sessions,
            "input_tokens": activity.input_tokens,
            "output_tokens": activity.output_tokens,
            "cached_tokens": activity.cached_tokens,
            "total_tokens": activity.total_tokens,
            "input_cost_usd": activity.input_cost_usd,
            "output_cost_usd": activity.output_cost_usd,
            "cached_cost_usd": activity.cached_cost_usd,
            "total_cost_usd": activity.total_cost_usd,
            "cost_complete": activity.cost_complete,
            "cost_status": activity.cost_status,
        }
        for activity in activity_totals.values()
    ]
    rows.sort(key=lambda row: (str(row["date"]), int(row["hour"])))
    return rows


def combine_daily_totals(*providers: dict[dt.date, DailyTotals]) -> dict[dt.date, DailyTotals]:
    combined: dict[dt.date, DailyTotals] = {}

    for provider in providers:
        for usage_date, values in provider.items():
            daily = combined.setdefault(usage_date, DailyTotals(date=usage_date))
            daily.merge_from(values)

    return combined


def combine_activity_totals(*providers: dict[tuple[dt.date, int], ActivityTotals]) -> dict[tuple[dt.date, int], ActivityTotals]:
    combined: dict[tuple[dt.date, int], ActivityTotals] = {}

    for provider in providers:
        for key, values in provider.items():
            activity = combined.setdefault(key, ActivityTotals(date=values.date, hour=values.hour))
            activity.merge_from(values)

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


def materialize_daily(
    daily: dict[dt.date, DailyTotals],
    from_date: dt.date | None = None,
    to_date: dt.date | None = None,
    *,
    include_breakdown_rows: bool = False,
) -> DailyMaterialization:
    values: list[DailyTotals] = []
    rows: list[dict[str, int | float | str | bool | list[dict[str, int | float | str | bool]]]] = []
    breakdown_totals: dict[tuple[str, str], BreakdownTotals] = {}
    attribution_totals: dict[tuple[str, str], AttributionTotals] = {}
    input_tokens = 0
    output_tokens = 0
    cached_tokens = 0
    total_tokens = 0
    input_cost_usd = 0.0
    output_cost_usd = 0.0
    cached_cost_usd = 0.0
    total_cost_usd = 0.0
    sessions = 0
    highest_single_day = 0
    cost_complete = True

    for usage_date, item in daily.items():
        if from_date is not None and usage_date < from_date:
            continue
        if to_date is not None and usage_date > to_date:
            continue

        values.append(item)
        row = {
            "date": usage_date.isoformat(),
            "sessions": item.sessions,
            "input_tokens": item.input_tokens,
            "output_tokens": item.output_tokens,
            "cached_tokens": item.cached_tokens,
            "total_tokens": item.total_tokens,
            "input_cost_usd": item.input_cost_usd,
            "output_cost_usd": item.output_cost_usd,
            "cached_cost_usd": item.cached_cost_usd,
            "total_cost_usd": item.total_cost_usd,
            "cost_complete": item.cost_complete,
            "cost_status": item.cost_status,
            "breakdown_rows": serialize_breakdown_rows(item.breakdowns),
        }
        if item.attributions:
            row["attribution_rows"] = serialize_attribution_rows(item.attributions)
        rows.append(row)

        input_tokens += item.input_tokens
        output_tokens += item.output_tokens
        cached_tokens += item.cached_tokens
        total_tokens += item.total_tokens
        input_cost_usd = round_cost(input_cost_usd + item.input_cost_usd)
        output_cost_usd = round_cost(output_cost_usd + item.output_cost_usd)
        cached_cost_usd = round_cost(cached_cost_usd + item.cached_cost_usd)
        total_cost_usd = round_cost(total_cost_usd + item.total_cost_usd)
        sessions += item.sessions
        highest_single_day = max(highest_single_day, item.total_tokens)
        cost_complete = cost_complete and item.cost_complete

        if include_breakdown_rows:
            for key, bucket in item.breakdowns.items():
                aggregate = breakdown_totals.get(key)
                if aggregate is None:
                    aggregate = BreakdownTotals(agent_cli=bucket.agent_cli, model=bucket.model)
                    breakdown_totals[key] = aggregate
                aggregate.sessions += bucket.sessions
                aggregate.input_tokens += bucket.input_tokens
                aggregate.output_tokens += bucket.output_tokens
                aggregate.cached_tokens += bucket.cached_tokens
                aggregate.total_tokens += bucket.total_tokens
                aggregate.input_cost_usd = round_cost(aggregate.input_cost_usd + bucket.input_cost_usd)
                aggregate.output_cost_usd = round_cost(aggregate.output_cost_usd + bucket.output_cost_usd)
                aggregate.cached_cost_usd = round_cost(aggregate.cached_cost_usd + bucket.cached_cost_usd)
                aggregate.total_cost_usd = round_cost(aggregate.total_cost_usd + bucket.total_cost_usd)
                aggregate.cost_complete = aggregate.cost_complete and bucket.cost_complete

            for key, bucket in item.attributions.items():
                aggregate_attr = attribution_totals.get(key)
                if aggregate_attr is None:
                    aggregate_attr = AttributionTotals(category=bucket.category, name=bucket.name)
                    attribution_totals[key] = aggregate_attr
                aggregate_attr.sessions += bucket.sessions
                aggregate_attr.events += bucket.events
                aggregate_attr.input_tokens += bucket.input_tokens
                aggregate_attr.output_tokens += bucket.output_tokens
                aggregate_attr.cached_tokens += bucket.cached_tokens
                aggregate_attr.total_tokens += bucket.total_tokens
                aggregate_attr.input_cost_usd = round_cost(aggregate_attr.input_cost_usd + bucket.input_cost_usd)
                aggregate_attr.output_cost_usd = round_cost(aggregate_attr.output_cost_usd + bucket.output_cost_usd)
                aggregate_attr.cached_cost_usd = round_cost(aggregate_attr.cached_cost_usd + bucket.cached_cost_usd)
                aggregate_attr.total_cost_usd = round_cost(aggregate_attr.total_cost_usd + bucket.total_cost_usd)
                aggregate_attr.cost_complete = aggregate_attr.cost_complete and bucket.cost_complete

    rows.sort(key=lambda row: str(row["date"]), reverse=True)
    ranked_values = sorted(values, key=lambda item: (-item.total_tokens, item.date.isoformat()))
    materialized_breakdown_rows = serialize_breakdown_rows(breakdown_totals) if include_breakdown_rows else []
    materialized_attribution_rows = serialize_attribution_rows(attribution_totals) if include_breakdown_rows else []

    return DailyMaterialization(
        values=values,
        rows=rows,
        ranked_values=ranked_values,
        summary={
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_tokens": cached_tokens,
            "ytd_total_tokens": total_tokens,
            "input_cost_usd": input_cost_usd,
            "output_cost_usd": output_cost_usd,
            "cached_cost_usd": cached_cost_usd,
            "total_cost_usd": total_cost_usd,
            "cost_complete": cost_complete,
            "cost_status": "complete" if cost_complete else "partial",
            "days_with_usage": len(values),
            "sessions": sessions,
            "highest_single_day": highest_single_day,
        },
        breakdown_rows=materialized_breakdown_rows,
        attribution_rows=materialized_attribution_rows,
    )


def rows_from_daily(
    daily: dict[dt.date, DailyTotals],
) -> list[dict[str, int | float | str | bool | list[dict[str, int | float | str | bool]]]]:
    return materialize_daily(daily).rows


def breakdown_rows_from_daily(daily: dict[dt.date, DailyTotals]) -> list[dict[str, int | float | str | bool]]:
    return materialize_daily(daily, include_breakdown_rows=True).breakdown_rows


def activity_rows_from_totals(
    activity_totals: dict[tuple[dt.date, int], ActivityTotals],
) -> list[dict[str, int | float | str | bool]]:
    return serialize_activity_rows(activity_totals)


def summary_from_daily(daily: dict[dt.date, DailyTotals]) -> dict[str, int | float | bool | str]:
    return materialize_daily(daily).summary


def providers_available(codex_source: object, claude_source: object, pi_source: object = False) -> dict[str, bool]:
    codex_present = bool(codex_source)
    claude_present = bool(claude_source)
    pi_present = bool(pi_source)
    return {
        "codex": codex_present,
        "claude": claude_present,
        "pi": pi_present,
        "combined": bool(codex_present or claude_present or pi_present),
    }
