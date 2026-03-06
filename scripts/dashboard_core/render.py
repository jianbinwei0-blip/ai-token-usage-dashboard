from __future__ import annotations

import datetime as dt
import json
import re

from .models import DailyTotals


def format_number(value: int) -> str:
    return f"{value:,}"


def build_stats_section(
    *,
    today: dt.date,
    ytd_total: int,
    days_count: int,
    sessions_total: int,
    highest: int,
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
) -> str:
    return f"""    <section class=\"stats\">
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


def build_table_body(rows: list[DailyTotals]) -> str:
    row_lines = []
    for idx, item in enumerate(rows, start=1):
        rank_class = " top-3" if idx <= 3 else ""
        row_lines.append(
            f'            <tr><td><span class="rank{rank_class}">{idx}</span></td><td>{item.date.isoformat()}</td>'
            f'<td class="num">{item.sessions}</td><td class="num total-col">{format_number(item.total_tokens)}</td></tr>'
        )
    return "          <tbody>\n" + "\n".join(row_lines) + "\n          </tbody>"


def inject_usage_dataset(html: str, dataset: dict) -> str:
    dataset_json = json.dumps(dataset, separators=(",", ":"))
    script = f'<script id="usageDataset" type="application/json">{dataset_json}</script>'
    pattern = r"<script id=\"usageDataset\" type=\"application/json\">.*?</script>"

    if re.search(pattern, html, flags=re.DOTALL):
        return re.sub(pattern, script, html, count=1, flags=re.DOTALL)

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
    pattern = r"(<select id=\"usageProvider\"[^>]*>)(.*?)(</select>)"
    options = build_provider_options(dataset)

    if not re.search(pattern, html, flags=re.DOTALL):
        return html

    return re.sub(
        pattern,
        lambda match: f"{match.group(1)}\n{options}\n            {match.group(3)}",
        html,
        count=1,
        flags=re.DOTALL,
    )


def rewrite_dashboard_html(html: str, stats_section: str, table_body: str, dataset: dict) -> str:
    updated = re.sub(
        r"^[ \t]*<section class=\"stats\">.*?</section>",
        stats_section,
        html,
        count=1,
        flags=re.DOTALL | re.MULTILINE,
    )
    updated = re.sub(
        r"^[ \t]*<tbody>\s*.*?\s*</tbody>",
        table_body,
        updated,
        count=1,
        flags=re.DOTALL | re.MULTILINE,
    )
    updated = rewrite_provider_select(updated, dataset)
    return inject_usage_dataset(updated, dataset)
