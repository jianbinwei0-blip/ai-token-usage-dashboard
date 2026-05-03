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


def format_session_count(count: int) -> str:
    noun = "session" if count == 1 else "sessions"
    return f"{count:,} {noun}"


def format_period_label(title: str, start_date: dt.date, end_date: dt.date, sessions: int) -> str:
    range_text = start_date.isoformat() if start_date == end_date else f"{start_date.isoformat()} → {end_date.isoformat()}"
    return f"{title} · {range_text} · {format_session_count(sessions)}"


def format_cost_display(value: float, cost_complete: bool) -> str:
    rendered = format_usd(value)
    return rendered if cost_complete else f"{rendered} (partial)"


def format_average_tokens(total_tokens: int, selected_day_span: int, period_days: float) -> str:
    normalized_total = round(total_tokens * period_days / max(selected_day_span, 1))
    return format_number(int(normalized_total))


def format_average_cost(total_cost: float, selected_day_span: int, period_days: float, cost_complete: bool) -> str:
    normalized_total = total_cost * period_days / max(selected_day_span, 1)
    return format_cost_display(normalized_total, cost_complete)


def build_stats_sections(
    *,
    today: dt.date,
    ytd_total: int,
    selected_day_span: int,
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
    fixed_stats_section = f"""    <section id="fixedStats" class="stats stats-fixed">
      <div class="stat-group stat-group--pulse-history">
        <div class="stat-group__header">
          <div class="stat-group__eyebrow">Token Usage · Current + History</div>
          <div class="stat-group__hint">Latest movement and weekly comparison</div>
        </div>
        <div class="stat-group__grid">
          <article class="stat stat--signal stat--executive stat--today">
            <div class="label">Today</div>
            <div class="value">{format_number(today_total)}</div>
          </article>
          <article class="stat stat--signal">
            <div class="label">Current Week</div>
            <div class="value">{format_number(current_week_total)}</div>
          </article>
          <article class="stat stat--history">
            <div class="label">Previous Week</div>
            <div class="value">{format_number(prev_week_total)}</div>
          </article>
          <article class="stat stat--history">
            <div class="label">2 Weeks Ago</div>
            <div class="value">{format_number(prev2_week_total)}</div>
          </article>
        </div>
      </div>
    </section>"""

    range_stats_section = f"""    <section id="rangeStats" class="stats stats-clustered">
      <div class="stat-group stat-group--overview">
        <div class="stat-group__header">
          <div class="stat-group__eyebrow">Range Snapshot</div>
          <div class="stat-group__hint">Selected window overview</div>
        </div>
        <div class="stat-group__grid">
          <article class="stat stat--overview stat--executive stat--range-total">
            <div class="label">Total</div>
            <div class="value">{format_number(ytd_total)}</div>
          </article>
          <article class="stat stat--overview">
            <div class="label">Active Days</div>
            <div class="value">{days_count}</div>
          </article>
          <article class="stat stat--overview">
            <div class="label">Sessions</div>
            <div class="value">{sessions_total}</div>
          </article>
          <article class="stat stat--overview">
            <div class="label">Peak Day</div>
            <div class="value">{format_number(highest)}</div>
          </article>
        </div>
      </div>
      <div class="stat-group stat-group--averages">
        <div class="stat-group__header">
          <div class="stat-group__eyebrow">Average Pace</div>
          <div class="stat-group__hint">Normalized to the selected span</div>
        </div>
        <div class="stat-group__grid">
          <article class="stat stat--average">
            <div class="label">Monthly</div>
            <div class="value">{format_average_tokens(ytd_total, selected_day_span, 365.2425 / 12)}</div>
          </article>
          <article class="stat stat--average">
            <div class="label">Weekly</div>
            <div class="value">{format_average_tokens(ytd_total, selected_day_span, 7)}</div>
          </article>
          <article class="stat stat--average">
            <div class="label">Daily</div>
            <div class="value">{format_average_tokens(ytd_total, selected_day_span, 1)}</div>
          </article>
        </div>
      </div>
      <div class="stat-group stat-group--tokens">
        <div class="stat-group__header">
          <div class="stat-group__eyebrow">Token Flow</div>
          <div class="stat-group__hint">Volume mix</div>
        </div>
        <div class="stat-group__grid">
          <article class="stat stat--tokens">
            <div class="label">Input</div>
            <div class="value">{format_number(input_total)}</div>
          </article>
          <article class="stat stat--tokens">
            <div class="label">Output</div>
            <div class="value">{format_number(output_total)}</div>
          </article>
          <article class="stat stat--tokens">
            <div class="label">Total</div>
            <div class="value">{format_number(ytd_total)}</div>
          </article>
        </div>
      </div>
      <div class="stat-group stat-group--cost-averages">
        <div class="stat-group__header">
          <div class="stat-group__eyebrow">Average Spend</div>
          <div class="stat-group__hint">Normalized to the selected span</div>
        </div>
        <div class="stat-group__grid">
          <article class="stat stat--average-cost">
            <div class="label">Monthly</div>
            <div class="value">{format_average_cost(total_cost, selected_day_span, 365.2425 / 12, cost_complete)}</div>
          </article>
          <article class="stat stat--average-cost">
            <div class="label">Weekly</div>
            <div class="value">{format_average_cost(total_cost, selected_day_span, 7, cost_complete)}</div>
          </article>
          <article class="stat stat--average-cost">
            <div class="label">Daily</div>
            <div class="value">{format_average_cost(total_cost, selected_day_span, 1, cost_complete)}</div>
          </article>
        </div>
      </div>
      <div class="stat-group stat-group--costs">
        <div class="stat-group__header">
          <div class="stat-group__eyebrow">Cost Surface</div>
          <div class="stat-group__hint">Spend profile</div>
        </div>
        <div class="stat-group__grid">
          <article class="stat stat--cost">
            <div class="label">Total</div>
            <div class="value">{format_cost_display(total_cost, cost_complete)}</div>
          </article>
          <article class="stat stat--cost">
            <div class="label">Input</div>
            <div class="value">{format_cost_display(input_cost_total, cost_complete)}</div>
          </article>
          <article class="stat stat--cost">
            <div class="label">Output</div>
            <div class="value">{format_cost_display(output_cost_total, cost_complete)}</div>
          </article>
          <article class="stat stat--cost">
            <div class="label">Cached</div>
            <div class="value">{format_cost_display(cached_cost_total, cost_complete)}</div>
          </article>
        </div>
      </div>
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
