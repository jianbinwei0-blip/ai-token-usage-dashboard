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
COST_OK_COLOR = "#7EE787"
UNAVAILABLE_COLOR = "#F2CC60"
CHATGPT_COLOR = "#7EE787"
QUOTA_GOOD_COLOR = "#7EE787"
QUOTA_WARNING_COLOR = "#F2CC60"
QUOTA_CRITICAL_COLOR = "#FF7B72"


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
        start_date = today.replace(day=1)
        preset = "mtd"
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
    range_preset: str = "mtd",
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
        "version": 3,
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


def format_chatgpt_plan(plan: Any) -> str:
    normalized = str(plan or "").strip().lower()
    labels = {
        "free": "Free",
        "go": "Go",
        "plus": "Plus",
        "pro": "Pro",
        "prolite": "Pro Lite",
        "team": "Team",
        "self_serve_business_usage_based": "Business",
        "business": "Business",
        "enterprise_cbp_usage_based": "Enterprise",
        "enterprise": "Enterprise",
        "edu": "Edu",
    }
    if normalized in labels:
        return labels[normalized]
    if not normalized or normalized == "unknown":
        return ""
    return " ".join(part.capitalize() for part in normalized.replace("-", "_").split("_") if part)


def subscription_effective_state(
    subscription: Any,
    now: dt.datetime | None = None,
    *,
    stale_minutes: int = 15,
) -> str:
    if not isinstance(subscription, dict):
        return "not_applicable"
    if str(subscription.get("account_type") or "").strip().lower() != "chatgpt":
        return "not_applicable"

    state = str(subscription.get("state") or "unavailable").strip().lower()
    if state == "not_applicable":
        return state
    if state not in {"ok", "stale", "unavailable"}:
        return "unavailable"
    if state != "ok":
        return state

    fetched_at = parse_iso_datetime(str(subscription.get("fetched_at") or ""))
    if fetched_at is None:
        return "stale"
    if (_to_utc(now) - fetched_at).total_seconds() > max(60, stale_minutes * 60):
        return "stale"
    return "ok"


def format_quota_reset_time(
    resets_at: Any,
    now: dt.datetime | None = None,
    *,
    compact: bool = False,
) -> str:
    try:
        reset_epoch = int(resets_at)
    except (TypeError, ValueError, OverflowError):
        return ""
    if reset_epoch <= 0:
        return ""

    reference = now or dt.datetime.now().astimezone()
    if reference.tzinfo is None:
        reference = reference.astimezone()
    try:
        reset = dt.datetime.fromtimestamp(reset_epoch, tz=dt.timezone.utc).astimezone(reference.tzinfo)
    except (OSError, OverflowError, ValueError):
        return ""
    if reset <= reference:
        return "now"

    day_delta = (reset.date() - reference.date()).days
    if day_delta == 0:
        return reset.strftime("%H:%M")
    if day_delta < 7:
        return reset.strftime("%a") if compact else reset.strftime("%a %H:%M")
    if reset.year == reference.year:
        date_label = f"{reset.strftime('%b')} {reset.day}"
        return date_label.replace(" ", "") if compact else f"{date_label} {reset.strftime('%H:%M')}"
    return reset.strftime("%Y-%m-%d")


def _quota_window_labels(duration_minutes: Any, kind: str) -> tuple[str, str]:
    try:
        minutes = int(duration_minutes)
    except (TypeError, ValueError, OverflowError):
        minutes = 0
    if minutes == 300:
        return "5h", "5h"
    if minutes == 10_080:
        return "Weekly", "7d"
    if minutes > 0 and minutes % 1_440 == 0:
        label = f"{minutes // 1_440}d"
        return label, label
    if minutes > 0 and minutes % 60 == 0:
        label = f"{minutes // 60}h"
        return label, label
    if minutes > 0:
        label = f"{minutes}m"
        return label, label
    return ("Usage", "U") if kind == "primary" else ("Secondary", "S")


def _quota_remaining(window: Any) -> int | None:
    if not isinstance(window, dict):
        return None
    try:
        return max(0, min(100, int(window.get("remaining_percent"))))
    except (TypeError, ValueError, OverflowError):
        return None


def _quota_color(remaining: int, *, stale: bool, reached: bool) -> str:
    if stale:
        return HEALTH_COLORS["stale"]
    if reached or remaining <= 20:
        return QUOTA_CRITICAL_COLOR
    if remaining <= 50:
        return QUOTA_WARNING_COLOR
    return QUOTA_GOOD_COLOR


def _short_limit_name(value: Any) -> str:
    text = str(value or "").strip().replace("_", "-")
    parts = [part for part in text.split("-") if part]
    candidate = parts[-1] if parts else text
    if not candidate:
        return "Model"
    if candidate.islower():
        candidate = candidate.capitalize()
    return candidate[:12]


def _quota_segment(
    window: dict[str, Any],
    kind: str,
    *,
    now: dt.datetime | None,
    stale: bool,
    reached: bool,
    prefix: str = "",
    compact: bool,
    include_reset: bool,
) -> tuple[str, str] | None:
    remaining = _quota_remaining(window)
    if remaining is None:
        return None
    full_label, compact_label = _quota_window_labels(window.get("window_duration_minutes"), kind)
    label = compact_label if compact else full_label
    if prefix:
        label = f"{prefix} {label}"
    reset = format_quota_reset_time(window.get("resets_at"), now, compact=False)
    if (
        not reset
        and bool(window.get("inferred"))
        and remaining == 100
        and window.get("window_duration_minutes") == 300
    ):
        reset = "now"
    remaining_text = f"{remaining}%" if compact else f"{remaining}% left"
    if reached:
        remaining_text += "!"
    plain = f"{label} {remaining_text}"
    if include_reset and reset:
        plain += f" ↻{reset}"

    color = _quota_color(remaining, stale=stale, reached=reached)
    styled = tmux_style(label, fg=MUTED_COLOR) + " " + tmux_style(remaining_text, fg=color, bold=True)
    if include_reset and reset:
        styled += tmux_style(" ↻", fg=MUTED_COLOR) + tmux_style(reset, fg=RANGE_VALUE_COLOR)
    return plain, styled


def _subscription_render_segments(
    subscription: Any,
    now: dt.datetime | None,
) -> dict[str, list[tuple[str, str]]]:
    empty = {"full": [], "compact": [], "short": [], "minimum": [], "five_hour": []}
    state = subscription_effective_state(subscription, now)
    if state == "not_applicable" or not isinstance(subscription, dict):
        return empty

    stale = state == "stale"
    plan = format_chatgpt_plan(subscription.get("plan"))
    plan_plain = "GPT" + (f" {plan}" if plan else "") + (" stale" if stale else "")
    plan_color = HEALTH_COLORS["stale"] if stale else CHATGPT_COLOR
    plan_styled = tmux_style("GPT", fg=CHATGPT_COLOR, bold=True)
    if plan:
        plan_styled += " " + tmux_style(plan, fg=plan_color, bold=True)
    if stale:
        plan_styled += " " + tmux_style("stale", fg=plan_color, bold=True)
    plan_segment = (plan_plain, plan_styled)

    raw_limits = subscription.get("limits")
    limits = [limit for limit in raw_limits if isinstance(limit, dict)] if isinstance(raw_limits, list) else []
    if not limits:
        unavailable_plain = "limits?" if state == "unavailable" else "limits stale"
        unavailable_segment = (unavailable_plain, tmux_style(unavailable_plain, fg=UNAVAILABLE_COLOR, bold=True))
        return {
            "full": [plan_segment, unavailable_segment],
            "compact": [plan_segment, unavailable_segment],
            "short": [plan_segment],
            "minimum": [plan_segment],
            "five_hour": [],
        }

    canonical = next((limit for limit in limits if str(limit.get("id") or "").lower() == "codex"), limits[0])
    canonical_reached = bool(canonical.get("rate_limit_reached_type"))
    full_segments = [plan_segment]
    compact_segments = [plan_segment]
    short_segments = [plan_segment]
    minimum_segments = [plan_segment]
    five_hour_segments: list[tuple[str, str]] = []

    canonical_windows: dict[str, dict[str, Any]] = {}
    for kind in ("primary", "secondary"):
        window = canonical.get(kind)
        if not isinstance(window, dict) or _quota_remaining(window) is None:
            continue
        canonical_windows[kind] = window
        full = _quota_segment(
            window,
            kind,
            now=now,
            stale=stale,
            reached=canonical_reached,
            compact=False,
            include_reset=True,
        )
        compact_value = _quota_segment(
            window,
            kind,
            now=now,
            stale=stale,
            reached=canonical_reached,
            compact=True,
            include_reset=True,
        )
        is_five_hour = window.get("window_duration_minutes") == 300
        short = _quota_segment(
            window,
            kind,
            now=now,
            stale=stale,
            reached=canonical_reached,
            compact=True,
            include_reset=is_five_hour,
        )
        if full is not None:
            full_segments.append(full)
        if compact_value is not None:
            compact_segments.append(compact_value)
            if is_five_hour:
                five_hour_segments = [compact_value]
        if short is not None:
            short_segments.append(short)
            if len(minimum_segments) == 1:
                minimum_segments.append(short)

    named_candidates: list[tuple[bool, int, str, str, dict[str, Any]]] = []
    for limit in limits:
        if limit is canonical:
            continue
        reached = bool(limit.get("rate_limit_reached_type"))
        name = _short_limit_name(limit.get("name") or limit.get("id"))
        for kind in ("primary", "secondary"):
            window = limit.get(kind)
            remaining = _quota_remaining(window)
            if remaining is None or not isinstance(window, dict):
                continue
            canonical_remaining = _quota_remaining(canonical_windows.get(kind))
            if reached or canonical_remaining is None or remaining < canonical_remaining:
                named_candidates.append((reached, remaining, name, kind, window))

    if named_candidates:
        reached, _remaining, name, kind, window = min(
            named_candidates,
            key=lambda candidate: (not candidate[0], candidate[1]),
        )
        named_full = _quota_segment(
            window,
            kind,
            now=now,
            stale=stale,
            reached=reached,
            prefix=name,
            compact=False,
            include_reset=True,
        )
        named_compact = _quota_segment(
            window,
            kind,
            now=now,
            stale=stale,
            reached=reached,
            prefix=name,
            compact=True,
            include_reset=True,
        )
        named_short = _quota_segment(
            window,
            kind,
            now=now,
            stale=stale,
            reached=reached,
            prefix=name,
            compact=True,
            include_reset=False,
        )
        if named_full is not None:
            full_segments.append(named_full)
        if named_compact is not None:
            compact_segments.append(named_compact)
        if named_short is not None:
            short_segments.append(named_short)

    credits = canonical.get("credits")
    if isinstance(credits, dict) and bool(credits.get("has_credits")):
        if bool(credits.get("unlimited")):
            credit_value = "∞"
        else:
            try:
                balance = float(credits.get("balance"))
            except (TypeError, ValueError, OverflowError):
                balance = 0.0
            credit_value = str(int(round(balance))) if balance > 0 else ""
        if credit_value:
            credit_plain = f"Credits {credit_value}"
            credit_styled = tmux_style("Credits", fg=MUTED_COLOR) + " " + tmux_style(credit_value, fg=COST_OK_COLOR, bold=True)
            full_segments.append((credit_plain, credit_styled))
            compact_credit = (
                f"Cr {credit_value}",
                tmux_style("Cr", fg=MUTED_COLOR) + " " + tmux_style(credit_value, fg=COST_OK_COLOR, bold=True),
            )
            compact_segments.append(compact_credit)
            short_segments.append(compact_credit)
            if canonical_reached:
                minimum_segments.append(compact_credit)

    return {
        "full": full_segments,
        "compact": compact_segments,
        "short": short_segments,
        "minimum": minimum_segments,
        "five_hour": five_hour_segments,
    }


def _subscription_group(segments: list[tuple[str, str]]) -> tuple[str, str]:
    if not segments:
        return "", ""
    plan, *details = segments
    if not details:
        return plan
    detail_plain = " · ".join(segment[0] for segment in details)
    detail_styled = tmux_style(" · ", fg=MUTED_COLOR).join(segment[1] for segment in details)
    separator_plain = " · "
    separator_styled = tmux_style(separator_plain, fg=MUTED_COLOR)
    return (
        f"{plan[0]}{separator_plain}{detail_plain}",
        plan[1] + separator_styled + detail_styled,
    )


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
    if max_width is not None and max_width <= 0:
        return ""

    metrics = snapshot.get("metrics") or {}
    quality = snapshot.get("quality") or {}
    raw_providers = snapshot.get("providers")
    providers = [str(provider).strip().lower() for provider in (raw_providers or []) if str(provider).strip()]
    provider_set = set(providers)
    scope = str(snapshot.get("scope") or "combined").strip().lower()
    subscription_tiers = (
        _subscription_render_segments(snapshot.get("subscription"), now)
        if scope in {"combined", "codex"}
        else {"full": [], "compact": [], "short": [], "minimum": [], "five_hour": []}
    )
    subscription_groups = {
        tier: _subscription_group(segments)
        for tier, segments in subscription_tiers.items()
    }
    has_subscription = bool(subscription_groups["minimum"][0])

    local_unavailable = raw_providers is not None and (
        not provider_set or (scope != "combined" and scope not in provider_set)
    )
    if local_unavailable and not has_subscription:
        if not use_tmux_style:
            return "AI unavailable"
        return tmux_style("AI", fg=AI_COLOR, bold=True) + " " + tmux_style("unavailable", fg=UNAVAILABLE_COLOR, bold=True)

    range_preset = str((snapshot.get("range") or {}).get("preset") or "mtd").strip().lower()
    range_short = range_short_for_preset(range_preset)
    today_total_tokens = format_tokens_short(metrics.get("today_tokens") or 0)
    range_total_tokens = format_tokens_short(metrics.get("range_tokens") or 0)
    cost = format_usd_short(metrics.get("range_cost_usd"))
    if not bool(quality.get("pricing_complete", True)) and cost != "cost?":
        cost = f"{cost}*"
    refreshed_at = format_refresh_time(snapshot.get("generated_at"), now)
    next_refresh_at = format_next_refresh_time(
        snapshot.get("generated_at"),
        interval_minutes=refresh_interval_minutes,
        now=now,
    )
    status = effective_health(snapshot, now, refresh_interval_minutes=refresh_interval_minutes)
    health_label = status if status in HEALTH_COLORS else "error"
    health_color = HEALTH_COLORS.get(health_label, HEALTH_COLORS["error"])

    ai_segment = tmux_style("AI", fg=AI_COLOR, bold=True)
    if status == "ok":
        if has_subscription:
            lead = ("", "")
        elif scope and scope != "combined":
            lead = (f"AI {scope}", ai_segment + " " + tmux_style(scope, fg=SCOPE_COLOR, bold=True))
        else:
            lead = ("AI", ai_segment)
    else:
        health_segment = tmux_style(health_label, fg=health_color, bold=True)
        if scope and scope != "combined":
            lead = (
                f"AI {scope} {status}",
                ai_segment + " " + tmux_style(scope, fg=SCOPE_COLOR, bold=True) + " " + health_segment,
            )
        else:
            lead = (f"AI {status}", ai_segment + " " + health_segment)
    ai_only = ("AI", ai_segment)

    today_group = (
        f"Today {today_total_tokens}",
        tmux_style("Today", fg=MUTED_COLOR)
        + " "
        + tmux_style(today_total_tokens, fg=TODAY_VALUE_COLOR, bold=True),
    )
    range_group = (
        f"{range_short} {range_total_tokens} · {cost}",
        tmux_style(range_short, fg=MUTED_COLOR)
        + " "
        + tmux_style(range_total_tokens, fg=RANGE_VALUE_COLOR, bold=True)
        + tmux_style(" · ", fg=MUTED_COLOR)
        + tmux_style(cost, fg=health_color if cost.endswith("*") else COST_OK_COLOR, bold=True),
    )
    time_segment = (
        f"{refreshed_at} → {next_refresh_at}",
        tmux_style(refreshed_at, fg=TODAY_VALUE_COLOR)
        + tmux_style(" → ", fg=MUTED_COLOR)
        + tmux_style(next_refresh_at, fg=RANGE_VALUE_COLOR),
    )

    local_full = [] if local_unavailable else [today_group, range_group, time_segment]
    local_compact = [] if local_unavailable else [today_group, range_group]
    local_short = [] if local_unavailable else [range_group]
    local_minimum = local_short

    if status == "error":
        candidates = [
            [lead, subscription_groups["compact"], time_segment],
            [lead, subscription_groups["short"]],
            [subscription_groups["five_hour"]],
            [lead],
            [ai_only],
        ]
    else:
        candidates = [
            [lead, subscription_groups["full"], *local_full],
            [lead, subscription_groups["compact"], *local_full],
            [lead, subscription_groups["compact"], *local_compact],
            [lead, subscription_groups["compact"], *local_short],
            [lead, subscription_groups["compact"]],
            [lead, subscription_groups["short"], *local_compact],
            [lead, subscription_groups["short"], *local_short],
            [lead, subscription_groups["minimum"], *local_minimum],
            [lead, subscription_groups["minimum"]],
            [subscription_groups["five_hour"]],
            [lead],
            [ai_only],
        ]

    separator_plain = " · "
    separator_styled = tmux_style(separator_plain, fg=MUTED_COLOR)
    selected_plain = "AI"
    selected_styled = ai_only[1]
    for candidate in candidates:
        pairs = [pair for pair in candidate if pair[0]]
        plain = separator_plain.join(pair[0] for pair in pairs)
        if max_width is None or len(plain) <= max_width:
            selected_plain = plain
            selected_styled = separator_styled.join(pair[1] for pair in pairs)
            break

    if max_width is not None and len(selected_plain) > max_width:
        selected_plain = selected_plain[:max_width]
        selected_styled = tmux_style(selected_plain, fg=AI_COLOR, bold=True)
    return selected_styled if use_tmux_style else selected_plain


def read_snapshot(path: Path) -> Snapshot | None:
    try:
        import json

        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None
