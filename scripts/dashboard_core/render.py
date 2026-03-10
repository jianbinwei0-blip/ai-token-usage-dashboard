from __future__ import annotations

import datetime as dt
import json
import re


USAGE_DATASET_PATTERN = re.compile(r'<script id="usageDataset" type="application/json">.*?</script>', re.DOTALL)
PROVIDER_SELECT_OPEN = '<select id="usageProvider"'
PROVIDER_SELECT_CLOSE = "</select>"
FIXED_STATS_SECTION_PATTERN = re.compile(
    r'^[ \t]*<section id="fixedStats" class="stats(?: [^"]*)?">.*?</section>',
    re.DOTALL | re.MULTILINE,
)
RANGE_STATS_SECTION_PATTERN = re.compile(
    r'^[ \t]*<section id="rangeStats" class="stats(?: [^"]*)?">.*?</section>',
    re.DOTALL | re.MULTILINE,
)
LEGACY_STATS_SECTION_PATTERN = re.compile(r'^[ \t]*<section class="stats">.*?</section>', re.DOTALL | re.MULTILINE)
DAILY_USAGE_TBODY_PATTERN = re.compile(r'<tbody id="dailyUsageTableBody">.*?</tbody>', re.DOTALL)
BREAKDOWN_TBODY_PATTERN = re.compile(r'<tbody id="usageBreakdownTableBody">.*?</tbody>', re.DOTALL)


def format_number(value: int) -> str:
    return f"{value:,}"


def format_usd(value: float) -> str:
    if value < 1:
        return f"${value:,.4f}"
    return f"${value:,.2f}"


def format_cost_display(value: float, cost_complete: bool) -> str:
    rendered = format_usd(value)
    return rendered if cost_complete else f"{rendered} (partial)"


def build_stats_sections(
    *,
    today: dt.date,
    ytd_total: int,
    days_count: int,
    sessions_total: int,
    highest: int,
    input_total: int,
    output_total: int,
    cached_total: int,
    total_cost: float,
    input_cost_total: float,
    output_cost_total: float,
    cached_cost_total: float,
    cost_complete: bool,
    today_sessions: int,
    today_total: int,
    current_monday: dt.date,
    current_week_end: dt.date,
    current_week_sessions: int,
    current_week_total: int,
    prev_week_monday: dt.date,
    prev_week_sunday: dt.date,
    prev_week_sessions: int,
    prev_week_total: int,
    prev2_week_monday: dt.date,
    prev2_week_sunday: dt.date,
    prev2_week_sessions: int,
    prev2_week_total: int,
) -> tuple[str, str]:
    fixed_stats_section = f"""    <section id=\"fixedStats\" class=\"stats stats-fixed\">
      <article class=\"stat\">
        <div class=\"label\">Today ({today.isoformat()}, {today_sessions} sessions)</div>
        <div class=\"value\">{format_number(today_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Current Week ({current_monday.isoformat()} to {current_week_end.isoformat()}, {current_week_sessions} sessions)</div>
        <div class=\"value\">{format_number(current_week_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">Previous Week ({prev_week_monday.isoformat()} to {prev_week_sunday.isoformat()}, {prev_week_sessions} sessions)</div>
        <div class=\"value\">{format_number(prev_week_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">2 Weeks Ago ({prev2_week_monday.isoformat()} to {prev2_week_sunday.isoformat()}, {prev2_week_sessions} sessions)</div>
        <div class=\"value\">{format_number(prev2_week_total)}</div>
      </article>
    </section>"""

    range_stats_section = f"""    <section id=\"rangeStats\" class=\"stats\">
      <article class=\"stat\">
        <div class=\"label\">YTD Total Tokens</div>
        <div class=\"value\">{format_number(ytd_total)}</div>
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
        <div class=\"value\">{format_number(highest)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Input Tokens</div>
        <div class=\"value\">{format_number(input_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Output Tokens</div>
        <div class=\"value\">{format_number(output_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Cached Tokens</div>
        <div class=\"value\">{format_number(cached_total)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Total Cost</div>
        <div class=\"value\">{format_cost_display(total_cost, cost_complete)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Input Cost</div>
        <div class=\"value\">{format_cost_display(input_cost_total, cost_complete)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Output Cost</div>
        <div class=\"value\">{format_cost_display(output_cost_total, cost_complete)}</div>
      </article>
      <article class=\"stat\">
        <div class=\"label\">YTD Cached Cost</div>
        <div class=\"value\">{format_cost_display(cached_cost_total, cost_complete)}</div>
      </article>
    </section>"""

    return fixed_stats_section, range_stats_section


def build_table_body(rows: list[object]) -> str:
    row_lines = []
    for idx, item in enumerate(rows, start=1):
        rank_class = " top-3" if idx <= 3 else ""
        row_lines.append(
            f'            <tr><td><span class="rank{rank_class}">{idx}</span></td><td>{item.date.isoformat()}</td>'
            f'<td class="num">{item.sessions}</td><td class="num">{format_number(item.input_tokens)}</td>'
            f'<td class="num">{format_number(item.output_tokens)}</td><td class="num">{format_number(item.cached_tokens)}</td>'
            f'<td class="num">{format_number(item.total_tokens)}</td><td class="num total-col">{format_cost_display(item.total_cost_usd, item.cost_complete)}</td></tr>'
        )
    return "<tbody id=\"dailyUsageTableBody\">\n" + "\n".join(row_lines) + "\n          </tbody>"


def build_breakdown_table_body(rows: list[dict[str, int | float | str | bool]]) -> str:
    row_lines = []
    for idx, item in enumerate(rows, start=1):
        rank_class = " top-3" if idx <= 3 else ""
        row_lines.append(
            "            <tr>"
            f'<td><span class="rank{rank_class}">{idx}</span></td>'
            f'<td>{item["agent_cli"]}</td>'
            f'<td>{item["model"]}</td>'
            f'<td class="num">{item["sessions"]}</td>'
            f'<td class="num">{format_number(int(item["input_tokens"]))}</td>'
            f'<td class="num">{format_number(int(item["output_tokens"]))}</td>'
            f'<td class="num">{format_number(int(item["cached_tokens"]))}</td>'
            f'<td class="num">{format_number(int(item["total_tokens"]))}</td>'
            f'<td class="num total-col">{format_cost_display(float(item["total_cost_usd"]), bool(item["cost_complete"]))}</td>'
            "</tr>"
        )
    return "<tbody id=\"usageBreakdownTableBody\">\n" + "\n".join(row_lines) + "\n          </tbody>"


def inject_usage_dataset(html: str, dataset: dict) -> str:
    dataset_json = json.dumps(dataset, separators=(",", ":"))
    script = f'<script id="usageDataset" type="application/json">{dataset_json}</script>'

    if USAGE_DATASET_PATTERN.search(html):
        return USAGE_DATASET_PATTERN.sub(script, html, count=1)

    if "</main>" in html:
        return html.replace("</main>", f"  {script}\n  </main>", 1)

    return html + "\n" + script


def build_provider_options(dataset: dict) -> str:
    provider_flags = dataset.get("providers_available")
    if not isinstance(provider_flags, dict):
        provider_flags = {}

    provider_labels = {
        "codex": "Codex",
        "claude": "Claude",
        "pi": "PI",
    }
    present_providers = [key for key in ("codex", "claude", "pi") if provider_flags.get(key)]

    option_lines = []
    if len(present_providers) > 1:
        option_lines.append('              <option value="combined">Combined</option>')
    for provider in present_providers:
        option_lines.append(f'              <option value="{provider}">{provider_labels[provider]}</option>')

    if not option_lines:
        option_lines.append('              <option value="combined">Combined</option>')

    return "\n".join(option_lines)


def rewrite_provider_select(html: str, dataset: dict) -> str:
    options = build_provider_options(dataset)
    start = html.find(PROVIDER_SELECT_OPEN)
    if start < 0:
        return html

    open_end = html.find(">", start)
    if open_end < 0:
        return html

    close_start = html.find(PROVIDER_SELECT_CLOSE, open_end)
    if close_start < 0:
        return html

    close_end = close_start + len(PROVIDER_SELECT_CLOSE)
    return (
        html[: open_end + 1]
        + "\n"
        + options
        + "\n            "
        + PROVIDER_SELECT_CLOSE
        + html[close_end:]
    )


def rewrite_dashboard_html(
    html: str,
    fixed_stats_section: str,
    range_stats_section: str,
    table_body: str,
    breakdown_body: str,
    dataset: dict,
) -> str:
    updated = html
    if FIXED_STATS_SECTION_PATTERN.search(updated) and RANGE_STATS_SECTION_PATTERN.search(updated):
        updated = FIXED_STATS_SECTION_PATTERN.sub(fixed_stats_section, updated, count=1)
        updated = RANGE_STATS_SECTION_PATTERN.sub(range_stats_section, updated, count=1)
    else:
        updated = LEGACY_STATS_SECTION_PATTERN.sub(
            f"{fixed_stats_section}\n\n{range_stats_section}",
            updated,
            count=1,
        )
    updated = DAILY_USAGE_TBODY_PATTERN.sub(table_body, updated, count=1)
    updated = BREAKDOWN_TBODY_PATTERN.sub(breakdown_body, updated, count=1)
    updated = rewrite_provider_select(updated, dataset)
    return inject_usage_dataset(updated, dataset)
