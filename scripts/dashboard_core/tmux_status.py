from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any


Snapshot = dict[str, Any]


HEALTH_COLORS = {
    "ok": "#7EE787",
    "partial": "#F2CC60",
    "stale": "#F2A65A",
    "error": "#FF7B72",
}

AI_COLOR = "#58A6FF"
SCOPE_COLOR = "#A5D6FF"
MUTED_COLOR = "#8B949E"
TODAY_VALUE_COLOR = "#E6EDF3"
RANGE_VALUE_COLOR = "#79C0FF"
TOTAL_TOKEN_HIGHLIGHT_FG = "#1F2328"
TOTAL_TOKEN_HIGHLIGHT_BG = "#F2CC60"
COST_OK_COLOR = "#7EE787"
UNAVAILABLE_COLOR = "#F2CC60"


def _to_utc(now: dt.datetime | None) -> dt.datetime:
    reference = now or dt.datetime.now(dt.timezone.utc)
    if reference.tzinfo is None:
        return reference.replace(tzinfo=dt.timezone.utc)
    return reference.astimezone(dt.timezone.utc)


def parse_iso_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_iso_date(value: str | None, fallback: dt.date) -> dt.date:
    if not value:
        return fallback
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
        return fallback


def range_label_for_preset(preset: str) -> str:
    return {
        "wtd": "Week to Date",
        "mtd": "Month to Date",
        "ytd": "Year to Date",
        "last7": "Past 7 Days",
        "last30": "Past 30 Days",
        "last90": "Past 90 Days",
        "all": "All Available",
    }.get(preset, "Selected Range")


def range_short_for_preset(preset: str) -> str:
    return {
        "wtd": "WTD",
        "mtd": "MTD",
        "ytd": "YTD",
        "last7": "7D",
        "last30": "30D",
        "last90": "90D",
        "all": "ALL",
    }.get(preset, "RNG")


def compute_range_dates(
    preset: str,
    today: dt.date,
    rows: list[dict[str, Any]],
) -> tuple[dt.date, dt.date, str]:
    end_date = today
    if preset == "wtd":
        start_date = today - dt.timedelta(days=today.isoweekday() - 1)
    elif preset == "mtd":
        start_date = today.replace(day=1)
    elif preset == "ytd":
        start_date = dt.date(today.year, 1, 1)
    elif preset == "last7":
        start_date = today - dt.timedelta(days=6)
    elif preset == "last30":
        start_date = today - dt.timedelta(days=29)
    elif preset == "last90":
        start_date = today - dt.timedelta(days=89)
    elif preset == "all":
        dated_rows = [parse_iso_date(row.get("date"), today) for row in rows]
        start_date = min(dated_rows) if dated_rows else today
    else:
        start_date = today - dt.timedelta(days=today.isoweekday() - 1)
        preset = "wtd"
    return start_date, end_date, range_label_for_preset(preset)


def available_providers(dataset_payload: dict[str, Any]) -> list[str]:
    provider_flags = dataset_payload.get("providers_available") or {}
    return [
        provider
        for provider in ("codex", "claude", "pi")
        if bool(provider_flags.get(provider))
    ]


def filter_pricing_warnings(pricing_metadata: dict[str, Any], scope: str) -> list[dict[str, Any]]:
    warnings = pricing_metadata.get("warnings")
    if not isinstance(warnings, list):
        return []
    if scope == "combined":
        return [warning for warning in warnings if isinstance(warning, dict)]
    return [
        warning
        for warning in warnings
        if isinstance(warning, dict) and str(warning.get("provider") or "").strip().lower() == scope
    ]


def summary_for_rows(rows: list[dict[str, Any]], start_date: dt.date, end_date: dt.date) -> dict[str, Any]:
    today_tokens = 0
    today_input_tokens = 0
    today_output_tokens = 0
    today_cached_tokens = 0
    range_tokens = 0
    range_input_tokens = 0
    range_output_tokens = 0
    range_cached_tokens = 0
    range_cost_usd = 0.0
    pricing_complete = True

    for row in rows:
        row_date = parse_iso_date(row.get("date"), end_date)
        input_tokens = int(row.get("input_tokens") or 0)
        output_tokens = int(row.get("output_tokens") or 0)
        cached_tokens = int(row.get("cached_tokens") or 0)
        total_tokens = int(row.get("total_tokens") or 0)
        if row_date == end_date:
            today_tokens += total_tokens
            today_input_tokens += input_tokens
            today_output_tokens += output_tokens
            today_cached_tokens += cached_tokens
        if not (start_date <= row_date <= end_date):
            continue
        range_tokens += total_tokens
        range_input_tokens += input_tokens
        range_output_tokens += output_tokens
        range_cached_tokens += cached_tokens
        range_cost_usd += float(row.get("total_cost_usd") or 0.0)
        pricing_complete = pricing_complete and bool(row.get("cost_complete", True))

    return {
        "today_tokens": today_tokens,
        "today_input_tokens": today_input_tokens,
        "today_output_tokens": today_output_tokens,
        "today_cached_tokens": today_cached_tokens,
        "range_tokens": range_tokens,
        "range_input_tokens": range_input_tokens,
        "range_output_tokens": range_output_tokens,
        "range_cached_tokens": range_cached_tokens,
        "range_cost_usd": round(range_cost_usd, 9),
        "pricing_complete": pricing_complete,
    }


def build_tmux_status_snapshot(
    dataset_payload: dict[str, Any],
    timings_ms: dict[str, Any] | None = None,
    *,
    scope: str = "combined",
    range_preset: str = "wtd",
    now: dt.datetime | None = None,
    base_health: str = "ok",
) -> Snapshot:
    generated_at_text = str(dataset_payload.get("generated_at") or "")
    generated_at = parse_iso_datetime(generated_at_text) or _to_utc(now)
    local_reference = now.astimezone() if now is not None else dt.datetime.now().astimezone()
    today = generated_at.astimezone(local_reference.tzinfo).date() if local_reference.tzinfo else generated_at.astimezone().date()

    providers_payload = dataset_payload.get("providers") or {}
    if scope not in {"combined", "codex", "claude", "pi"}:
        scope = "combined"

    provider_rows = providers_payload.get(scope) or {}
    rows = provider_rows.get("rows") if isinstance(provider_rows, dict) else []
    if not isinstance(rows, list):
        rows = []

    start_date, end_date, range_label = compute_range_dates(range_preset, today, rows)
    summary = summary_for_rows(rows, start_date, end_date)

    pricing_metadata = dataset_payload.get("pricing") or {}
    warnings = filter_pricing_warnings(pricing_metadata, scope)
    warning_count = len(warnings)
    pricing_complete = bool(summary["pricing_complete"]) and warning_count == 0

    health = base_health if base_health in {"ok", "partial", "stale", "error"} else "error"
    if health not in {"error", "stale"} and (not pricing_complete or warning_count):
        health = "partial"
    elif health not in {"error", "stale"}:
        health = "ok"

    snapshot: Snapshot = {
        "version": 2,
        "generated_at": generated_at.isoformat(),
        "health": health,
        "scope": scope,
        "providers": available_providers(dataset_payload),
        "range": {
            "mode": "preset",
            "preset": range_preset,
            "label": range_label,
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
        },
        "metrics": {
            "today_tokens": int(summary["today_tokens"]),
            "today_input_tokens": int(summary["today_input_tokens"]),
            "today_output_tokens": int(summary["today_output_tokens"]),
            "today_cached_tokens": int(summary["today_cached_tokens"]),
            "range_tokens": int(summary["range_tokens"]),
            "range_input_tokens": int(summary["range_input_tokens"]),
            "range_output_tokens": int(summary["range_output_tokens"]),
            "range_cached_tokens": int(summary["range_cached_tokens"]),
            "range_cost_usd": float(summary["range_cost_usd"]),
            "recalc_ms": float((timings_ms or {}).get("total") or 0.0),
        },
        "quality": {
            "pricing_complete": pricing_complete,
            "warning_count": warning_count,
            "pricing_source": str(pricing_metadata.get("source") or "unavailable"),
            "pricing_version": str(pricing_metadata.get("version") or ""),
        },
    }
    return snapshot


def format_tokens_short(value: int | float) -> str:
    amount = float(value or 0)
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount < 1_000:
        return f"{sign}{int(round(amount))}"

    thresholds = (
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    )
    for threshold, suffix in thresholds:
        if amount >= threshold:
            scaled = amount / threshold
            precision = 2 if scaled < 10 and suffix == "B" else 1
            text = f"{scaled:.{precision}f}".rstrip("0").rstrip(".")
            return f"{sign}{text}{suffix}"
    return f"{sign}{int(round(amount))}"


def format_usd_short(value: int | float | None) -> str:
    if value is None:
        return "cost?"
    amount = float(value)
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount < 1_000:
        rounded = int(round(amount))
        return f"{sign}${rounded}"
    if amount < 1_000_000:
        text = f"{amount / 1_000:.1f}".rstrip("0").rstrip(".")
        return f"{sign}${text}k"
    text = f"{amount / 1_000_000:.2f}".rstrip("0").rstrip(".")
    return f"{sign}${text}M"


def format_recalc_short(value: int | float | None) -> str:
    if value is None:
        return "--"
    amount = float(value)
    if amount < 1000:
        return f"{int(round(amount))}ms"
    return f"{amount / 1000:.1f}".rstrip("0").rstrip(".") + "s"


def _coerce_generated_datetime(generated_at: str | dt.datetime | None) -> dt.datetime | None:
    if isinstance(generated_at, dt.datetime):
        return generated_at.astimezone(dt.timezone.utc) if generated_at.tzinfo else generated_at.replace(tzinfo=dt.timezone.utc)
    return parse_iso_datetime(generated_at)


def format_age_short(generated_at: str | dt.datetime | None, now: dt.datetime | None = None) -> str:
    reference = _to_utc(now)
    generated = _coerce_generated_datetime(generated_at)
    if generated is None:
        return "--"

    seconds = max(0, int(round((reference - generated).total_seconds())))
    if seconds < 30:
        return "now"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def format_refresh_time(generated_at: str | dt.datetime | None, now: dt.datetime | None = None) -> str:
    reference = now or dt.datetime.now().astimezone()
    generated = _coerce_generated_datetime(generated_at)
    if generated is None:
        return "--:--"
    local_generated = generated.astimezone(reference.tzinfo) if reference.tzinfo else generated.astimezone()
    return local_generated.strftime("%H:%M")


def next_refresh_boundary(
    moment: str | dt.datetime | None,
    *,
    interval_minutes: int = 5,
    now: dt.datetime | None = None,
) -> dt.datetime | None:
    reference = now or dt.datetime.now().astimezone()
    generated = _coerce_generated_datetime(moment)
    if generated is None:
        return None
    local_generated = generated.astimezone(reference.tzinfo) if reference.tzinfo else generated.astimezone()
    bucket_minute = (local_generated.minute // interval_minutes) * interval_minutes
    bucket_start = local_generated.replace(minute=bucket_minute, second=0, microsecond=0)
    next_boundary = bucket_start + dt.timedelta(minutes=interval_minutes)
    return next_boundary


def format_next_refresh_time(
    generated_at: str | dt.datetime | None,
    *,
    interval_minutes: int = 5,
    now: dt.datetime | None = None,
) -> str:
    next_boundary = next_refresh_boundary(generated_at, interval_minutes=interval_minutes, now=now)
    if next_boundary is None:
        return "--:--"
    return next_boundary.strftime("%H:%M")


def effective_health(
    snapshot: Snapshot,
    now: dt.datetime | None = None,
    *,
    refresh_interval_minutes: int = 5,
    grace_seconds: int = 30,
) -> str:
    base = str(snapshot.get("health") or "error").strip().lower()
    if base == "error":
        return "error"
    if base == "partial":
        return "partial"
    if base == "stale":
        return "stale"

    generated = parse_iso_datetime(str(snapshot.get("generated_at") or ""))
    if generated is None:
        return "error"
    age_seconds = (_to_utc(now) - generated).total_seconds()
    stale_after_seconds = max(60, refresh_interval_minutes * 60 + grace_seconds)
    if age_seconds > stale_after_seconds:
        return "stale"
    return "ok"


def tmux_style(text: str, *, fg: str | None = None, bg: str | None = None, bold: bool = False) -> str:
    parts: list[str] = []
    if fg:
        parts.append(f"fg={fg}")
    if bg:
        parts.append(f"bg={bg}")
    if bold:
        parts.append("bold")
    if not parts:
        return text
    return f"#[{','.join(parts)}]{text}#[default]"


def render_tmux_status(
    snapshot: Snapshot,
    now: dt.datetime | None = None,
    *,
    max_width: int | None = None,
    use_tmux_style: bool = False,
    refresh_interval_minutes: int = 5,
) -> str:
    if not isinstance(snapshot, dict):
        return "AI unavailable"

    metrics = snapshot.get("metrics") or {}
    quality = snapshot.get("quality") or {}
    raw_providers = snapshot.get("providers")
    providers = [str(provider).strip().lower() for provider in (raw_providers or []) if str(provider).strip()]
    provider_set = set(providers)
    scope = str(snapshot.get("scope") or "combined").strip().lower()

    def join_plain(parts: list[str]) -> str:
        return " · ".join(part for part in parts if part)

    def join_styled(parts: list[str]) -> str:
        separator = tmux_style(" · ", fg=MUTED_COLOR)
        return separator.join(part for part in parts if part)

    unavailable = raw_providers is not None and (not provider_set or (scope != "combined" and scope not in provider_set))
    if unavailable:
        if not use_tmux_style:
            return "AI unavailable"
        return tmux_style("AI", fg=AI_COLOR, bold=True) + " " + tmux_style("unavailable", fg=UNAVAILABLE_COLOR, bold=True)

    range_preset = str((snapshot.get("range") or {}).get("preset") or "wtd").strip().lower()
    range_short = range_short_for_preset(range_preset)
    today_input_tokens = format_tokens_short(metrics.get("today_input_tokens") or 0)
    today_output_tokens = format_tokens_short(metrics.get("today_output_tokens") or 0)
    today_total_tokens = format_tokens_short(metrics.get("today_tokens") or 0)
    range_tokens = format_tokens_short(metrics.get("range_tokens") or 0)
    range_input_tokens = format_tokens_short(metrics.get("range_input_tokens") or 0)
    range_output_tokens = format_tokens_short(metrics.get("range_output_tokens") or 0)
    range_total_tokens = range_tokens
    today_breakdown = f"T I {today_input_tokens} O {today_output_tokens} Σ {today_total_tokens}"
    range_breakdown = f"{range_short} I {range_input_tokens} O {range_output_tokens} Σ {range_total_tokens}"
    cost = format_usd_short(metrics.get("range_cost_usd"))
    if not bool(quality.get("pricing_complete", True)) and cost != "cost?":
        cost = f"{cost}*"
    range_cost = f"{range_short} {cost}"
    refreshed_at = format_refresh_time(snapshot.get("generated_at"), now)
    next_refresh_at = format_next_refresh_time(snapshot.get("generated_at"), interval_minutes=refresh_interval_minutes, now=now)
    status = effective_health(snapshot, now, refresh_interval_minutes=refresh_interval_minutes)

    lead_plain = f"AI {status}" if not (scope and scope != "combined") else f"AI {scope} {status}"

    if status == "error":
        plain = join_plain([lead_plain, f"{refreshed_at} → {next_refresh_at}"])
        variant = "error_cached"
    else:
        full = join_plain([lead_plain, today_breakdown, range_breakdown, range_cost, f"{refreshed_at} → {next_refresh_at}"])
        compact = join_plain([lead_plain, today_breakdown, range_breakdown, f"{refreshed_at} → {next_refresh_at}"])
        short = join_plain([lead_plain, range_breakdown, f"{refreshed_at} → {next_refresh_at}"])
        minimum = join_plain([lead_plain, f"{range_short} {range_tokens}"])

        if max_width is None or len(full) <= max_width:
            plain = full
            variant = "full"
        elif len(compact) <= max_width:
            plain = compact
            variant = "compact"
        elif len(short) <= max_width:
            plain = short
            variant = "short"
        elif len(minimum) <= max_width:
            plain = minimum
            variant = "minimum"
        else:
            plain = minimum[:max_width]
            variant = "minimum"

    if not use_tmux_style:
        return plain

    health_label = status if status in HEALTH_COLORS else "error"
    health_color = HEALTH_COLORS.get(health_label, HEALTH_COLORS["error"])
    ai_segment = tmux_style("AI", fg=AI_COLOR, bold=True)
    health_segment = tmux_style(health_label, fg=health_color, bold=True)
    if scope and scope != "combined":
        lead_segment = ai_segment + " " + tmux_style(scope, fg=SCOPE_COLOR, bold=True) + " " + health_segment
    else:
        lead_segment = ai_segment + " " + health_segment

    today_segment = (
        tmux_style("T", fg=MUTED_COLOR)
        + " "
        + tmux_style("I", fg=MUTED_COLOR)
        + " "
        + tmux_style(today_input_tokens, fg=TODAY_VALUE_COLOR, bold=True)
        + " "
        + tmux_style("O", fg=MUTED_COLOR)
        + " "
        + tmux_style(today_output_tokens, fg=TODAY_VALUE_COLOR, bold=True)
        + " "
        + tmux_style("Σ", fg=MUTED_COLOR)
        + " "
        + tmux_style(
            today_total_tokens,
            fg=TOTAL_TOKEN_HIGHLIGHT_FG,
            bg=TOTAL_TOKEN_HIGHLIGHT_BG,
            bold=True,
        )
    )
    range_segment = (
        tmux_style(range_short, fg=MUTED_COLOR)
        + " "
        + tmux_style("I", fg=MUTED_COLOR)
        + " "
        + tmux_style(range_input_tokens, fg=RANGE_VALUE_COLOR, bold=True)
        + " "
        + tmux_style("O", fg=MUTED_COLOR)
        + " "
        + tmux_style(range_output_tokens, fg=RANGE_VALUE_COLOR, bold=True)
        + " "
        + tmux_style("Σ", fg=MUTED_COLOR)
        + " "
        + tmux_style(
            range_total_tokens,
            fg=TOTAL_TOKEN_HIGHLIGHT_FG,
            bg=TOTAL_TOKEN_HIGHLIGHT_BG,
            bold=True,
        )
    )
    cost_segment = tmux_style(range_short, fg=MUTED_COLOR) + " " + tmux_style(cost, fg=health_color if cost.endswith("*") else COST_OK_COLOR, bold=True)
    time_segment = (
        tmux_style(refreshed_at, fg=TODAY_VALUE_COLOR)
        + tmux_style(" → ", fg=MUTED_COLOR)
        + tmux_style(next_refresh_at, fg=RANGE_VALUE_COLOR)
    )

    if variant == "error_cached":
        return join_styled([lead_segment, time_segment])
    if variant == "full":
        return join_styled([lead_segment, today_segment, range_segment, cost_segment, time_segment])
    if variant == "compact":
        return join_styled([lead_segment, today_segment, range_segment, time_segment])
    if variant == "short":
        return join_styled([lead_segment, range_segment, time_segment])
    return join_styled([lead_segment, range_segment])


def read_snapshot(path: Path) -> Snapshot | None:
    try:
        import json

        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None
