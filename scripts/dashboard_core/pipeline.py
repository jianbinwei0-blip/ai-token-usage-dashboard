from __future__ import annotations

import datetime as dt
from pathlib import Path

from .aggregation import (
    activity_rows_from_totals,
    combine_activity_totals,
    combine_daily_totals,
    current_week_end,
    materialize_daily,
    providers_available,
    sum_range,
)
from .collectors import (
    collect_claude_usage_data,
    collect_codex_usage_data,
    collect_pi_usage_data,
    load_persistent_parse_caches,
    save_persistent_parse_caches,
)
from .config import DashboardConfig
from .models import round_cost
from .pricing import PricingCatalog
from .render import build_breakdown_table_body, build_stats_sections, build_table_body, rewrite_dashboard_html


_HTML_CACHE: dict[str, tuple[int, str]] = {}


def read_dashboard_html(path: Path) -> str:
    cache_key = str(path)
    mtime_ns = path.stat().st_mtime_ns
    cached = _HTML_CACHE.get(cache_key)
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]

    html = path.read_text(encoding="utf-8")
    _HTML_CACHE[cache_key] = (mtime_ns, html)
    return html


def write_dashboard_html(path: Path, html: str) -> None:
    cache_key = str(path)
    cached = _HTML_CACHE.get(cache_key)
    if cached is not None and cached[1] == html and path.exists():
        return

    path.write_text(html, encoding="utf-8")
    _HTML_CACHE[cache_key] = (path.stat().st_mtime_ns, html)


def recalc_dashboard(config: DashboardConfig, now: dt.datetime | None = None) -> dict:
    now_utc = now.astimezone(dt.timezone.utc) if now is not None else dt.datetime.now(dt.timezone.utc)
    now_local = now_utc.astimezone()
    today = now_local.date()
    ytd_from = dt.date(today.year, 1, 1)
    pricing_catalog = PricingCatalog.from_file(config.pricing_file)

    load_persistent_parse_caches(config.parse_cache_file)
    codex_daily_all, codex_activity_all = collect_codex_usage_data(config.sessions_root, pricing_catalog=pricing_catalog)
    claude_daily_all, claude_activity_all = collect_claude_usage_data(
        config.claude_projects_root,
        pricing_catalog=pricing_catalog,
    )
    pi_daily_all, pi_activity_all = collect_pi_usage_data(config.pi_agent_root, pricing_catalog=pricing_catalog)
    save_persistent_parse_caches(config.parse_cache_file)
    combined_daily_all = combine_daily_totals(codex_daily_all, claude_daily_all, pi_daily_all)
    combined_activity_all = combine_activity_totals(codex_activity_all, claude_activity_all, pi_activity_all)

    codex_all = materialize_daily(codex_daily_all)
    claude_all = materialize_daily(claude_daily_all)
    pi_all = materialize_daily(pi_daily_all)
    combined_all = materialize_daily(combined_daily_all)

    codex_ytd = materialize_daily(codex_daily_all, ytd_from, today)
    claude_ytd = materialize_daily(claude_daily_all, ytd_from, today)
    pi_ytd = materialize_daily(pi_daily_all, ytd_from, today)
    combined_ytd = materialize_daily(combined_daily_all, ytd_from, today, include_breakdown_rows=True)

    rows = combined_ytd.ranked_values
    summary = combined_ytd.summary
    days_count = int(summary["days_with_usage"])
    sessions_total = int(summary["sessions"])
    input_total = int(summary["input_tokens"])
    output_total = int(summary["output_tokens"])
    cached_total = int(summary["cached_tokens"])
    ytd_total = int(summary["ytd_total_tokens"])
    input_cost_total = round_cost(float(summary["input_cost_usd"]))
    output_cost_total = round_cost(float(summary["output_cost_usd"]))
    cached_cost_total = round_cost(float(summary["cached_cost_usd"]))
    total_cost = round_cost(float(summary["total_cost_usd"]))
    cost_complete = bool(summary["cost_complete"])
    highest = int(summary["highest_single_day"])

    today_sessions, today_total = sum_range(combined_daily_all, today, today)

    current_monday = today - dt.timedelta(days=today.isoweekday() - 1)
    week_end = current_week_end(today)
    current_week_sessions, current_week_total = sum_range(combined_daily_all, current_monday, week_end)

    prev_week_monday = current_monday - dt.timedelta(days=7)
    prev_week_sunday = current_monday - dt.timedelta(days=1)
    prev_week_sessions, prev_week_total = sum_range(combined_daily_all, prev_week_monday, prev_week_sunday)

    prev2_week_monday = prev_week_monday - dt.timedelta(days=7)
    prev2_week_sunday = prev_week_monday - dt.timedelta(days=1)
    prev2_week_sessions, prev2_week_total = sum_range(combined_daily_all, prev2_week_monday, prev2_week_sunday)

    fixed_stats_section, range_stats_section = build_stats_sections(
        today=today,
        ytd_total=ytd_total,
        days_count=days_count,
        sessions_total=sessions_total,
        highest=highest,
        input_total=input_total,
        output_total=output_total,
        cached_total=cached_total,
        total_cost=total_cost,
        input_cost_total=input_cost_total,
        output_cost_total=output_cost_total,
        cached_cost_total=cached_cost_total,
        cost_complete=cost_complete,
        today_sessions=today_sessions,
        today_total=today_total,
        current_monday=current_monday,
        current_week_end=week_end,
        current_week_sessions=current_week_sessions,
        current_week_total=current_week_total,
        prev_week_monday=prev_week_monday,
        prev_week_sunday=prev_week_sunday,
        prev_week_sessions=prev_week_sessions,
        prev_week_total=prev_week_total,
        prev2_week_monday=prev2_week_monday,
        prev2_week_sunday=prev2_week_sunday,
        prev2_week_sessions=prev2_week_sessions,
        prev2_week_total=prev2_week_total,
    )

    table_body = build_table_body(rows)
    breakdown_body = build_breakdown_table_body(combined_ytd.breakdown_rows)

    provider_flags = providers_available(
        config.sessions_root.exists(),
        config.claude_projects_root.exists(),
        config.pi_agent_root.exists(),
    )
    pricing_metadata = pricing_catalog.metadata()

    dataset_payload = {
        "generated_at": now_utc.isoformat(),
        "timezone": now_local.tzname() or "local",
        "paths": {
            "codex_sessions_root": str(config.sessions_root),
            "claude_projects_root": str(config.claude_projects_root),
            "pi_agent_root": str(config.pi_agent_root),
            "pi_sessions_root": str(config.pi_agent_root / "sessions"),
            "pricing_file": str(config.pricing_file) if config.pricing_file else None,
        },
        "providers_available": provider_flags,
        "pricing": pricing_metadata,
        "providers": {
            "codex": {
                "rows": codex_all.rows,
                "activity_rows": activity_rows_from_totals(codex_activity_all),
            },
            "claude": {
                "rows": claude_all.rows,
                "activity_rows": activity_rows_from_totals(claude_activity_all),
            },
            "pi": {
                "rows": pi_all.rows,
                "activity_rows": activity_rows_from_totals(pi_activity_all),
            },
            "combined": {
                "rows": combined_all.rows,
                "activity_rows": activity_rows_from_totals(combined_activity_all),
            },
        },
    }

    html = read_dashboard_html(config.dashboard_html)
    html = rewrite_dashboard_html(
        html,
        fixed_stats_section,
        range_stats_section,
        table_body,
        breakdown_body,
        dataset_payload,
    )
    write_dashboard_html(config.dashboard_html, html)

    return {
        "ok": True,
        "updated_at": now_utc.isoformat(),
        "today": today.isoformat(),
        "input_tokens": input_total,
        "output_tokens": output_total,
        "cached_tokens": cached_total,
        "ytd_total_tokens": ytd_total,
        "input_cost_usd": input_cost_total,
        "output_cost_usd": output_cost_total,
        "cached_cost_usd": cached_cost_total,
        "total_cost_usd": total_cost,
        "cost_complete": cost_complete,
        "cost_status": "complete" if cost_complete else "partial",
        "days_with_usage": days_count,
        "sessions": sessions_total,
        "sources": {
            "codex_sessions_root": str(config.sessions_root),
            "claude_projects_root": str(config.claude_projects_root),
            "pi_agent_root": str(config.pi_agent_root),
            "pricing_file": str(config.pricing_file) if config.pricing_file else None,
        },
        "providers_available": provider_flags,
        "pricing": pricing_metadata,
        "providers": {
            "codex": codex_ytd.summary,
            "claude": claude_ytd.summary,
            "pi": pi_ytd.summary,
            "combined": combined_ytd.summary,
        },
    }
