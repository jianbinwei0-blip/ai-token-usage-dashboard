from __future__ import annotations

import datetime as dt
import json
import os
import re
from pathlib import Path

from .models import ActivityTotals, DailyTotals
from .pricing import PricingCatalog


DEFAULT_MODEL = "unknown"
LOCAL_TIMEZONE = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
CODEX_ROLLOUT_TIMESTAMP_PATTERN = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})"
)
_CODEX_SESSION_USAGE_CACHE: dict[str, tuple[int, int, str, dict[str, object] | None]] = {}
_CLAUDE_REQUEST_RECORDS_CACHE: dict[str, tuple[int, int, list[dict[str, object]]]] = {}
_PI_SESSION_RECORDS_CACHE: dict[str, tuple[int, int, tuple[str, list[dict[str, object]]]]] = {}
_PERSISTENT_CACHE_VERSION = 1
_PERSISTENT_CACHE_LOADED_FROM: str | None = None
_PERSISTENT_CACHE_DIRTY = False


def safe_non_negative_int(value: object) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


def normalized_bucket_value(value: object, fallback: str) -> str:
    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            return normalized
    return fallback


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


def serialize_timestamp(value: object) -> str | None:
    return value.isoformat() if isinstance(value, dt.datetime) else None


def deserialize_timestamp(value: object) -> dt.datetime | None:
    return parse_timestamp_local(value)


def _mark_persistent_cache_dirty() -> None:
    global _PERSISTENT_CACHE_DIRTY
    _PERSISTENT_CACHE_DIRTY = True


def _serialize_record(record: dict[str, object]) -> dict[str, object]:
    serialized = dict(record)
    serialized["timestamp"] = serialize_timestamp(record.get("timestamp"))
    return serialized


def _deserialize_record(record: object) -> dict[str, object] | None:
    if not isinstance(record, dict):
        return None
    deserialized = dict(record)
    deserialized["timestamp"] = deserialize_timestamp(record.get("timestamp"))
    return deserialized


def _serialize_codex_contribution(contribution: dict[str, object] | None) -> dict[str, object] | None:
    if contribution is None:
        return None
    serialized = dict(contribution)
    serialized["timestamp"] = serialize_timestamp(contribution.get("timestamp"))
    return serialized


def _deserialize_codex_contribution(contribution: object) -> dict[str, object] | None:
    if contribution is None:
        return None
    if not isinstance(contribution, dict):
        return None
    deserialized = dict(contribution)
    deserialized["timestamp"] = deserialize_timestamp(contribution.get("timestamp"))
    return deserialized


def merge_native_cost(target: dict[str, float] | None, native_cost: object) -> dict[str, float] | None:
    if not isinstance(native_cost, dict) or not native_cost:
        return target

    merged = dict(target or {})
    for key in ("input", "output", "cacheRead", "cacheWrite", "total"):
        value = native_cost.get(key)
        if isinstance(value, (int, float)) and value >= 0:
            merged[key] = float(merged.get(key) or 0.0) + float(value)
    return merged or target


def serialize_pi_contribution(contribution: object) -> dict[str, object] | None:
    if not isinstance(contribution, dict):
        return None
    usage_rows = contribution.get("usage_rows")
    activity_rows = contribution.get("activity_rows")
    if not isinstance(usage_rows, list) or not isinstance(activity_rows, list):
        return None
    return {
        "session_id": normalized_bucket_value(contribution.get("session_id"), "unknown-session"),
        "usage_rows": [dict(row) for row in usage_rows if isinstance(row, dict)],
        "activity_rows": [_serialize_record(row) for row in activity_rows if isinstance(row, dict)],
    }


def deserialize_pi_contribution(contribution: object) -> dict[str, object] | None:
    if not isinstance(contribution, dict):
        return None
    usage_rows = contribution.get("usage_rows")
    activity_rows = contribution.get("activity_rows")
    if not isinstance(usage_rows, list) or not isinstance(activity_rows, list):
        return None
    return {
        "session_id": normalized_bucket_value(contribution.get("session_id"), "unknown-session"),
        "usage_rows": [dict(row) for row in usage_rows if isinstance(row, dict)],
        "activity_rows": [row for item in activity_rows if (row := _deserialize_record(item)) is not None],
    }


def load_persistent_parse_caches(cache_path: Path | None) -> None:
    global _PERSISTENT_CACHE_LOADED_FROM, _PERSISTENT_CACHE_DIRTY

    target = str(cache_path.resolve()) if cache_path is not None else None
    if _PERSISTENT_CACHE_LOADED_FROM == target:
        return

    _CODEX_SESSION_USAGE_CACHE.clear()
    _CLAUDE_REQUEST_RECORDS_CACHE.clear()
    _PI_SESSION_RECORDS_CACHE.clear()
    _PERSISTENT_CACHE_LOADED_FROM = target
    _PERSISTENT_CACHE_DIRTY = False

    if cache_path is None or not cache_path.exists():
        return

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    if not isinstance(payload, dict) or payload.get("version") != _PERSISTENT_CACHE_VERSION:
        return

    codex_payload = payload.get("codex")
    if isinstance(codex_payload, dict):
        for file_path, entry in codex_payload.items():
            if not isinstance(entry, dict):
                continue
            size = safe_non_negative_int(entry.get("size"))
            mtime_ns = safe_non_negative_int(entry.get("mtime_ns"))
            pricing_key = normalized_bucket_value(entry.get("pricing_key"), "")
            contribution = _deserialize_codex_contribution(entry.get("contribution"))
            if not pricing_key:
                continue
            _CODEX_SESSION_USAGE_CACHE[file_path] = (size, mtime_ns, pricing_key, contribution)

    claude_payload = payload.get("claude")
    if isinstance(claude_payload, dict):
        for file_path, entry in claude_payload.items():
            if not isinstance(entry, dict):
                continue
            size = safe_non_negative_int(entry.get("size"))
            mtime_ns = safe_non_negative_int(entry.get("mtime_ns"))
            records_payload = entry.get("records")
            if not isinstance(records_payload, list):
                continue
            records = [record for item in records_payload if (record := _deserialize_record(item)) is not None]
            _CLAUDE_REQUEST_RECORDS_CACHE[file_path] = (size, mtime_ns, records)

    pi_payload = payload.get("pi")
    if isinstance(pi_payload, dict):
        for file_path, entry in pi_payload.items():
            if not isinstance(entry, dict):
                continue
            size = safe_non_negative_int(entry.get("size"))
            mtime_ns = safe_non_negative_int(entry.get("mtime_ns"))
            contribution = deserialize_pi_contribution(entry.get("contribution"))
            if contribution is None:
                continue
            _PI_SESSION_RECORDS_CACHE[file_path] = (size, mtime_ns, contribution)


def save_persistent_parse_caches(cache_path: Path | None) -> None:
    global _PERSISTENT_CACHE_DIRTY

    if cache_path is None or not _PERSISTENT_CACHE_DIRTY:
        return

    payload = {
        "version": _PERSISTENT_CACHE_VERSION,
        "codex": {
            file_path: {
                "size": size,
                "mtime_ns": mtime_ns,
                "pricing_key": pricing_key,
                "contribution": _serialize_codex_contribution(contribution),
            }
            for file_path, (size, mtime_ns, pricing_key, contribution) in _CODEX_SESSION_USAGE_CACHE.items()
        },
        "claude": {
            file_path: {
                "size": size,
                "mtime_ns": mtime_ns,
                "records": [_serialize_record(record) for record in records],
            }
            for file_path, (size, mtime_ns, records) in _CLAUDE_REQUEST_RECORDS_CACHE.items()
        },
        "pi": {
            file_path: {
                "size": size,
                "mtime_ns": mtime_ns,
                "contribution": serialize_pi_contribution(contribution),
            }
            for file_path, (size, mtime_ns, contribution) in _PI_SESSION_RECORDS_CACHE.items()
        },
    }

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(cache_path)
        _PERSISTENT_CACHE_DIRTY = False
    except OSError:
        return


def parse_codex_rollout_timestamp_local(session_path: Path) -> dt.datetime | None:
    match = CODEX_ROLLOUT_TIMESTAMP_PATTERN.search(session_path.stem)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(part) for part in match.groups())
    return dt.datetime(year, month, day, hour, minute, second, tzinfo=LOCAL_TIMEZONE)


def codex_usage_date_from_path(session_path: Path, sessions_root: Path) -> dt.date | None:
    relative = session_path.relative_to(sessions_root).parts
    if len(relative) < 4:
        return None
    try:
        year = int(relative[0])
        month = int(relative[1])
        day = int(relative[2])
        return dt.date(year, month, day)
    except ValueError:
        return None


def apply_usage_to_daily(
    daily: DailyTotals,
    *,
    agent_cli: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int,
    total_tokens: int,
    input_cost_usd: float,
    output_cost_usd: float,
    cached_cost_usd: float,
    total_cost_usd: float,
    cost_complete: bool,
) -> None:
    daily.add_usage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=cached_tokens,
        total_tokens=total_tokens,
        input_cost_usd=input_cost_usd,
        output_cost_usd=output_cost_usd,
        cached_cost_usd=cached_cost_usd,
        total_cost_usd=total_cost_usd,
        cost_complete=cost_complete,
    )
    daily.add_breakdown(
        agent_cli=agent_cli,
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=cached_tokens,
        total_tokens=total_tokens,
        input_cost_usd=input_cost_usd,
        output_cost_usd=output_cost_usd,
        cached_cost_usd=cached_cost_usd,
        total_cost_usd=total_cost_usd,
        cost_complete=cost_complete,
    )


def add_usage_to_activity(
    activity_totals: dict[tuple[dt.date, int], ActivityTotals],
    timestamp: dt.datetime,
    *,
    sessions: int,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int,
    total_tokens: int,
    input_cost_usd: float,
    output_cost_usd: float,
    cached_cost_usd: float,
    total_cost_usd: float,
    cost_complete: bool,
) -> None:
    key = (timestamp.date(), timestamp.hour)
    activity = activity_totals.get(key)
    if activity is None:
        activity = ActivityTotals(date=timestamp.date(), hour=timestamp.hour)
        activity_totals[key] = activity
    activity.add_usage(
        sessions=sessions,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=cached_tokens,
        total_tokens=total_tokens,
        input_cost_usd=input_cost_usd,
        output_cost_usd=output_cost_usd,
        cached_cost_usd=cached_cost_usd,
        total_cost_usd=total_cost_usd,
        cost_complete=cost_complete,
    )


def iter_jsonl_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for filename in sorted(filenames):
            if filename.endswith(".jsonl"):
                yield Path(dirpath) / filename


def pricing_cache_key(pricing_catalog: PricingCatalog) -> str:
    return f"{pricing_catalog.source}|{pricing_catalog.version}"


def parse_codex_session_usage(session_path: Path) -> dict[str, int | str | dt.datetime] | None:
    latest_usage: dict[str, int] | None = None
    latest_model = DEFAULT_MODEL
    agent_cli = "codex"
    session_id = session_path.stem
    activity_timestamp = parse_codex_rollout_timestamp_local(session_path)

    with session_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            event_timestamp = parse_timestamp_local(event.get("timestamp"))
            if activity_timestamp is None and event_timestamp is not None:
                activity_timestamp = event_timestamp

            event_type = event.get("type")

            if event_type == "session_meta":
                payload = event.get("payload") or {}
                if isinstance(payload, dict):
                    session_id = normalized_bucket_value(payload.get("id"), session_id)
                    agent_cli = normalized_bucket_value(payload.get("originator"), "")
                    if not agent_cli:
                        agent_cli = normalized_bucket_value(payload.get("source"), "codex")
                    payload_timestamp = parse_timestamp_local(payload.get("timestamp"))
                    if payload_timestamp is not None:
                        activity_timestamp = payload_timestamp
                    elif activity_timestamp is None and event_timestamp is not None:
                        activity_timestamp = event_timestamp
                continue

            if event_type == "turn_context":
                payload = event.get("payload") or {}
                if isinstance(payload, dict):
                    latest_model = normalized_bucket_value(payload.get("model"), latest_model)
                continue

            if event_type != "event_msg":
                continue

            payload = event.get("payload") or {}
            if not isinstance(payload, dict) or payload.get("type") != "token_count":
                continue

            info = payload.get("info") or {}
            if not isinstance(info, dict):
                continue

            total_usage = info.get("total_token_usage") or {}
            if not isinstance(total_usage, dict):
                continue

            input_tokens = safe_non_negative_int(total_usage.get("input_tokens"))
            cached_tokens = safe_non_negative_int(total_usage.get("cached_input_tokens"))
            output_tokens = safe_non_negative_int(total_usage.get("output_tokens"))
            total_tokens = safe_non_negative_int(total_usage.get("total_tokens"))
            if total_tokens == 0 and (input_tokens or cached_tokens or output_tokens):
                total_tokens = input_tokens + output_tokens

            if total_tokens == 0 and input_tokens == 0 and cached_tokens == 0 and output_tokens == 0:
                continue

            if activity_timestamp is None and event_timestamp is not None:
                activity_timestamp = event_timestamp

            latest_usage = {
                "input_tokens": input_tokens,
                "cached_tokens": cached_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
            }

    if latest_usage is None:
        return None

    return {
        "session_id": session_id,
        "agent_cli": normalized_bucket_value(agent_cli, "codex"),
        "model": normalized_bucket_value(latest_model, DEFAULT_MODEL),
        "timestamp": activity_timestamp,
        **latest_usage,
    }


def build_codex_session_contribution(
    session_path: Path,
    sessions_root: Path,
    usage: dict[str, int | str | dt.datetime],
    pricing_catalog: PricingCatalog,
) -> dict[str, object] | None:
    fallback_usage_date = codex_usage_date_from_path(session_path, sessions_root)
    if fallback_usage_date is None:
        return None

    activity_timestamp = usage.get("timestamp") if isinstance(usage.get("timestamp"), dt.datetime) else None
    usage_date = activity_timestamp.date() if activity_timestamp is not None else fallback_usage_date
    if activity_timestamp is None:
        activity_timestamp = dt.datetime.combine(usage_date, dt.time(hour=0), tzinfo=LOCAL_TIMEZONE)

    session_id = normalized_bucket_value(usage.get("session_id"), session_path.stem)
    agent_cli = normalized_bucket_value(usage.get("agent_cli"), "codex")
    model = normalized_bucket_value(usage.get("model"), DEFAULT_MODEL)
    input_tokens = safe_non_negative_int(usage.get("input_tokens"))
    output_tokens = safe_non_negative_int(usage.get("output_tokens"))
    cached_tokens = safe_non_negative_int(usage.get("cached_tokens"))
    total_tokens = safe_non_negative_int(usage.get("total_tokens"))
    priced = pricing_catalog.price_usage(
        "codex",
        model,
        uncached_input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_tokens=cached_tokens,
    )

    return {
        "usage_date": usage_date.isoformat(),
        "timestamp": activity_timestamp,
        "session_id": session_id,
        "agent_cli": agent_cli,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cached_tokens": cached_tokens,
        "total_tokens": total_tokens,
        "input_cost_usd": priced.input_cost_usd,
        "output_cost_usd": priced.output_cost_usd,
        "cached_cost_usd": priced.cached_cost_usd,
        "total_cost_usd": priced.total_cost_usd,
        "cost_complete": priced.cost_complete,
    }


def parse_codex_session_usage_cached(
    session_path: Path,
    sessions_root: Path,
    pricing_catalog: PricingCatalog,
) -> dict[str, object] | None:
    try:
        stat = session_path.stat()
    except OSError:
        return None

    cache_key = str(session_path)
    cached = _CODEX_SESSION_USAGE_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    current_pricing_key = pricing_cache_key(pricing_catalog)
    if cached is not None and cached[:2] == signature and cached[2] == current_pricing_key:
        return cached[3]

    usage = parse_codex_session_usage(session_path)
    contribution = None if usage is None else build_codex_session_contribution(session_path, sessions_root, usage, pricing_catalog)
    _CODEX_SESSION_USAGE_CACHE[cache_key] = (signature[0], signature[1], current_pricing_key, contribution)
    _mark_persistent_cache_dirty()
    return contribution


def collect_codex_usage_data(
    sessions_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> tuple[dict[dt.date, DailyTotals], dict[tuple[dt.date, int], ActivityTotals]]:
    totals: dict[dt.date, DailyTotals] = {}
    activity_totals: dict[tuple[dt.date, int], ActivityTotals] = {}
    if not sessions_root.exists():
        return totals, activity_totals

    catalog = pricing_catalog or PricingCatalog.from_file(None)
    bucket_sessions: dict[tuple[dt.date, str, str], set[str]] = {}

    for file_path in iter_jsonl_files(sessions_root):
        contribution = parse_codex_session_usage_cached(file_path, sessions_root, catalog)
        if contribution is None:
            continue

        usage_date_value = contribution.get("usage_date")
        if not isinstance(usage_date_value, str):
            continue
        try:
            usage_date = dt.date.fromisoformat(usage_date_value)
        except ValueError:
            continue

        activity_timestamp = contribution.get("timestamp")
        if not isinstance(activity_timestamp, dt.datetime):
            activity_timestamp = dt.datetime.combine(usage_date, dt.time(hour=0), tzinfo=LOCAL_TIMEZONE)

        session_id = normalized_bucket_value(contribution.get("session_id"), file_path.stem)
        agent_cli = normalized_bucket_value(contribution.get("agent_cli"), "codex")
        model = normalized_bucket_value(contribution.get("model"), DEFAULT_MODEL)
        input_tokens = safe_non_negative_int(contribution.get("input_tokens"))
        output_tokens = safe_non_negative_int(contribution.get("output_tokens"))
        cached_tokens = safe_non_negative_int(contribution.get("cached_tokens"))
        total_tokens = safe_non_negative_int(contribution.get("total_tokens"))
        input_cost_usd = float(contribution.get("input_cost_usd") or 0.0)
        output_cost_usd = float(contribution.get("output_cost_usd") or 0.0)
        cached_cost_usd = float(contribution.get("cached_cost_usd") or 0.0)
        total_cost_usd = float(contribution.get("total_cost_usd") or 0.0)
        cost_complete = bool(contribution.get("cost_complete", True))

        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        daily.sessions += 1
        apply_usage_to_daily(
            daily,
            agent_cli=agent_cli,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            total_tokens=total_tokens,
            input_cost_usd=input_cost_usd,
            output_cost_usd=output_cost_usd,
            cached_cost_usd=cached_cost_usd,
            total_cost_usd=total_cost_usd,
            cost_complete=cost_complete,
        )
        bucket_sessions.setdefault((usage_date, agent_cli, model), set()).add(session_id)
        add_usage_to_activity(
            activity_totals,
            activity_timestamp,
            sessions=1,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            total_tokens=total_tokens,
            input_cost_usd=input_cost_usd,
            output_cost_usd=output_cost_usd,
            cached_cost_usd=cached_cost_usd,
            total_cost_usd=total_cost_usd,
            cost_complete=cost_complete,
        )

    for (usage_date, agent_cli, model), sessions in bucket_sessions.items():
        daily = totals.get(usage_date)
        if daily is not None:
            daily.add_breakdown(agent_cli=agent_cli, model=model, sessions=len(sessions))

    return totals, activity_totals


def collect_codex_daily_totals(
    sessions_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> dict[dt.date, DailyTotals]:
    totals, _activity_totals = collect_codex_usage_data(sessions_root, pricing_catalog=pricing_catalog)
    return totals


def parse_claude_request_records(file_path: Path) -> list[dict[str, object]]:
    session_scope = file_path.stem
    request_usage: dict[tuple[str, str], dict[str, object]] = {}

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
            cached_tokens = cache_creation_input_tokens + cache_read_input_tokens
            model = normalized_bucket_value(message.get("model"), DEFAULT_MODEL)

            if current is None:
                request_usage[dedupe_key] = {
                    "session_id": session_id,
                    "request_id": request_id,
                    "timestamp": local_timestamp,
                    "input_tokens": input_tokens,
                    "cache_creation_input_tokens": cache_creation_input_tokens,
                    "cache_read_input_tokens": cache_read_input_tokens,
                    "cached_tokens": cached_tokens,
                    "output_tokens": output_tokens,
                    "model": model,
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
            current["cached_tokens"] = max(safe_non_negative_int(current.get("cached_tokens")), cached_tokens)
            current["output_tokens"] = max(safe_non_negative_int(current.get("output_tokens")), output_tokens)
            if model != DEFAULT_MODEL:
                current["model"] = model

    return list(request_usage.values())


def parse_claude_request_records_cached(file_path: Path) -> list[dict[str, object]]:
    try:
        stat = file_path.stat()
    except OSError:
        return []

    cache_key = str(file_path)
    cached = _CLAUDE_REQUEST_RECORDS_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    if cached is not None and cached[:2] == signature:
        return cached[2]

    records = parse_claude_request_records(file_path)
    _CLAUDE_REQUEST_RECORDS_CACHE[cache_key] = (signature[0], signature[1], records)
    return records


def collect_claude_usage_data(
    claude_projects_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> tuple[dict[dt.date, DailyTotals], dict[tuple[dt.date, int], ActivityTotals]]:
    totals: dict[dt.date, DailyTotals] = {}
    activity_totals: dict[tuple[dt.date, int], ActivityTotals] = {}
    if not claude_projects_root.exists():
        return totals, activity_totals

    catalog = pricing_catalog or PricingCatalog.from_file(None)
    request_usage: dict[tuple[str, str], dict[str, object]] = {}

    for file_path in iter_jsonl_files(claude_projects_root):
        for record in parse_claude_request_records_cached(file_path):
            session_id = normalized_bucket_value(record.get("session_id"), file_path.stem)
            request_id = normalized_bucket_value(record.get("request_id"), "")
            if not request_id:
                continue

            dedupe_key = (session_id, request_id)
            current = request_usage.get(dedupe_key)
            local_timestamp = record.get("timestamp")
            if not isinstance(local_timestamp, dt.datetime):
                continue

            input_tokens = safe_non_negative_int(record.get("input_tokens"))
            cache_creation_input_tokens = safe_non_negative_int(record.get("cache_creation_input_tokens"))
            cache_read_input_tokens = safe_non_negative_int(record.get("cache_read_input_tokens"))
            cached_tokens = safe_non_negative_int(record.get("cached_tokens"))
            output_tokens = safe_non_negative_int(record.get("output_tokens"))
            model = normalized_bucket_value(record.get("model"), DEFAULT_MODEL)

            if current is None:
                request_usage[dedupe_key] = {
                    "session_id": session_id,
                    "timestamp": local_timestamp,
                    "input_tokens": input_tokens,
                    "cache_creation_input_tokens": cache_creation_input_tokens,
                    "cache_read_input_tokens": cache_read_input_tokens,
                    "cached_tokens": cached_tokens,
                    "output_tokens": output_tokens,
                    "model": model,
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
            current["cached_tokens"] = max(safe_non_negative_int(current.get("cached_tokens")), cached_tokens)
            current["output_tokens"] = max(safe_non_negative_int(current.get("output_tokens")), output_tokens)
            if model != DEFAULT_MODEL:
                current["model"] = model

    daily_sessions: dict[dt.date, set[str]] = {}
    bucket_sessions: dict[tuple[dt.date, str, str], set[str]] = {}
    daily_session_usage: dict[tuple[dt.date, str], dict[str, object]] = {}

    for request in request_usage.values():
        timestamp = request["timestamp"]
        if not isinstance(timestamp, dt.datetime):
            continue

        usage_date = timestamp.date()
        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        input_tokens = safe_non_negative_int(request.get("input_tokens"))
        cache_creation_input_tokens = safe_non_negative_int(request.get("cache_creation_input_tokens"))
        cache_read_input_tokens = safe_non_negative_int(request.get("cache_read_input_tokens"))
        cached_tokens = safe_non_negative_int(request.get("cached_tokens"))
        output_tokens = safe_non_negative_int(request.get("output_tokens"))
        request_total = input_tokens + cached_tokens + output_tokens
        agent_cli = "claude-code"
        model = normalized_bucket_value(request.get("model"), DEFAULT_MODEL)
        priced = catalog.price_usage(
            "claude",
            model,
            uncached_input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_input_tokens,
            cache_write_tokens=cache_creation_input_tokens,
        )

        apply_usage_to_daily(
            daily,
            agent_cli=agent_cli,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            total_tokens=request_total,
            input_cost_usd=priced.input_cost_usd,
            output_cost_usd=priced.output_cost_usd,
            cached_cost_usd=priced.cached_cost_usd,
            total_cost_usd=priced.total_cost_usd,
            cost_complete=priced.cost_complete,
        )

        session_id = normalized_bucket_value(request.get("session_id"), "unknown-session")
        daily_sessions.setdefault(usage_date, set()).add(session_id)
        bucket_sessions.setdefault((usage_date, agent_cli, model), set()).add(session_id)

        session_key = (usage_date, session_id)
        session_activity = daily_session_usage.get(session_key)
        if session_activity is None:
            daily_session_usage[session_key] = {
                "timestamp": timestamp,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cached_tokens": cached_tokens,
                "total_tokens": request_total,
                "input_cost_usd": priced.input_cost_usd,
                "output_cost_usd": priced.output_cost_usd,
                "cached_cost_usd": priced.cached_cost_usd,
                "total_cost_usd": priced.total_cost_usd,
                "cost_complete": priced.cost_complete,
            }
        else:
            session_activity["timestamp"] = min(session_activity["timestamp"], timestamp)
            session_activity["input_tokens"] = safe_non_negative_int(session_activity.get("input_tokens")) + input_tokens
            session_activity["output_tokens"] = safe_non_negative_int(session_activity.get("output_tokens")) + output_tokens
            session_activity["cached_tokens"] = safe_non_negative_int(session_activity.get("cached_tokens")) + cached_tokens
            session_activity["total_tokens"] = safe_non_negative_int(session_activity.get("total_tokens")) + request_total
            session_activity["input_cost_usd"] = float(session_activity.get("input_cost_usd") or 0.0) + priced.input_cost_usd
            session_activity["output_cost_usd"] = float(session_activity.get("output_cost_usd") or 0.0) + priced.output_cost_usd
            session_activity["cached_cost_usd"] = float(session_activity.get("cached_cost_usd") or 0.0) + priced.cached_cost_usd
            session_activity["total_cost_usd"] = float(session_activity.get("total_cost_usd") or 0.0) + priced.total_cost_usd
            session_activity["cost_complete"] = bool(session_activity.get("cost_complete", True)) and priced.cost_complete

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    for (usage_date, agent_cli, model), sessions in bucket_sessions.items():
        daily = totals.get(usage_date)
        if daily is not None:
            daily.add_breakdown(agent_cli=agent_cli, model=model, sessions=len(sessions))

    for session_activity in daily_session_usage.values():
        timestamp = session_activity.get("timestamp")
        if not isinstance(timestamp, dt.datetime):
            continue
        add_usage_to_activity(
            activity_totals,
            timestamp,
            sessions=1,
            input_tokens=safe_non_negative_int(session_activity.get("input_tokens")),
            output_tokens=safe_non_negative_int(session_activity.get("output_tokens")),
            cached_tokens=safe_non_negative_int(session_activity.get("cached_tokens")),
            total_tokens=safe_non_negative_int(session_activity.get("total_tokens")),
            input_cost_usd=float(session_activity.get("input_cost_usd") or 0.0),
            output_cost_usd=float(session_activity.get("output_cost_usd") or 0.0),
            cached_cost_usd=float(session_activity.get("cached_cost_usd") or 0.0),
            total_cost_usd=float(session_activity.get("total_cost_usd") or 0.0),
            cost_complete=bool(session_activity.get("cost_complete", True)),
        )

    return totals, activity_totals


def collect_claude_daily_totals(
    claude_projects_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> dict[dt.date, DailyTotals]:
    totals, _activity_totals = collect_claude_usage_data(claude_projects_root, pricing_catalog=pricing_catalog)
    return totals


def parse_pi_session_contribution(file_path: Path) -> dict[str, object]:
    session_id = file_path.stem
    active_model = DEFAULT_MODEL
    usage_rows: dict[tuple[str, str], dict[str, object]] = {}
    activity_rows: dict[str, dict[str, object]] = {}

    with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            event_type = event.get("type")
            if event_type == "session":
                event_session_id = event.get("id")
                if isinstance(event_session_id, str) and event_session_id:
                    session_id = event_session_id
                continue

            if event_type == "model_change":
                active_model = normalized_bucket_value(event.get("modelId"), active_model)
                continue

            if event_type != "message":
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

            usage_date = local_timestamp.date().isoformat()
            model = normalized_bucket_value(message.get("model"), active_model)
            input_tokens = safe_non_negative_int(usage.get("input"))
            output_tokens = safe_non_negative_int(usage.get("output"))
            cache_read_tokens = safe_non_negative_int(usage.get("cacheRead"))
            cache_write_tokens = safe_non_negative_int(usage.get("cacheWrite"))
            cached_tokens = cache_read_tokens + cache_write_tokens
            total_tokens = safe_non_negative_int(usage.get("totalTokens"))
            if total_tokens == 0 and (input_tokens or output_tokens or cached_tokens):
                total_tokens = input_tokens + output_tokens + cached_tokens

            usage_bucket = usage_rows.setdefault(
                (usage_date, model),
                {
                    "date": usage_date,
                    "model": model,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_read_tokens": 0,
                    "cache_write_tokens": 0,
                    "cached_tokens": 0,
                    "total_tokens": 0,
                    "native_cost": None,
                },
            )
            usage_bucket["input_tokens"] = safe_non_negative_int(usage_bucket.get("input_tokens")) + input_tokens
            usage_bucket["output_tokens"] = safe_non_negative_int(usage_bucket.get("output_tokens")) + output_tokens
            usage_bucket["cache_read_tokens"] = safe_non_negative_int(usage_bucket.get("cache_read_tokens")) + cache_read_tokens
            usage_bucket["cache_write_tokens"] = safe_non_negative_int(usage_bucket.get("cache_write_tokens")) + cache_write_tokens
            usage_bucket["cached_tokens"] = safe_non_negative_int(usage_bucket.get("cached_tokens")) + cached_tokens
            usage_bucket["total_tokens"] = safe_non_negative_int(usage_bucket.get("total_tokens")) + total_tokens
            usage_bucket["native_cost"] = merge_native_cost(
                usage_bucket.get("native_cost") if isinstance(usage_bucket.get("native_cost"), dict) else None,
                usage.get("cost"),
            )

            activity_bucket = activity_rows.get(usage_date)
            if activity_bucket is None:
                activity_rows[usage_date] = {"date": usage_date, "timestamp": local_timestamp}
            else:
                existing_timestamp = activity_bucket.get("timestamp")
                if isinstance(existing_timestamp, dt.datetime):
                    activity_bucket["timestamp"] = min(existing_timestamp, local_timestamp)
                else:
                    activity_bucket["timestamp"] = local_timestamp

    return {
        "session_id": session_id,
        "usage_rows": sorted(usage_rows.values(), key=lambda row: (str(row["date"]), str(row["model"]))),
        "activity_rows": sorted(activity_rows.values(), key=lambda row: str(row["date"])),
    }


def parse_pi_session_contribution_cached(file_path: Path) -> dict[str, object]:
    try:
        stat = file_path.stat()
    except OSError:
        return {"session_id": file_path.stem, "usage_rows": [], "activity_rows": []}

    cache_key = str(file_path)
    cached = _PI_SESSION_RECORDS_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    if cached is not None and cached[:2] == signature:
        contribution = cached[2]
        return contribution if isinstance(contribution, dict) else {"session_id": file_path.stem, "usage_rows": [], "activity_rows": []}

    contribution = parse_pi_session_contribution(file_path)
    _PI_SESSION_RECORDS_CACHE[cache_key] = (signature[0], signature[1], contribution)
    _mark_persistent_cache_dirty()
    return contribution


def collect_pi_usage_data(
    pi_agent_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> tuple[dict[dt.date, DailyTotals], dict[tuple[dt.date, int], ActivityTotals]]:
    totals: dict[dt.date, DailyTotals] = {}
    activity_totals: dict[tuple[dt.date, int], ActivityTotals] = {}
    if not pi_agent_root.exists():
        return totals, activity_totals

    sessions_root = pi_agent_root / "sessions"
    if not sessions_root.exists():
        return totals, activity_totals

    catalog = pricing_catalog or PricingCatalog.from_file(None)
    daily_sessions: dict[dt.date, set[str]] = {}
    bucket_sessions: dict[tuple[dt.date, str, str], set[str]] = {}

    for file_path in iter_jsonl_files(sessions_root):
        contribution = parse_pi_session_contribution_cached(file_path)
        session_id = normalized_bucket_value(contribution.get("session_id"), file_path.stem)
        usage_rows = contribution.get("usage_rows")
        activity_rows = contribution.get("activity_rows")
        if not isinstance(usage_rows, list):
            usage_rows = []
        if not isinstance(activity_rows, list):
            activity_rows = []

        session_activity: dict[dt.date, dict[str, object]] = {}
        for activity_row in activity_rows:
            if not isinstance(activity_row, dict):
                continue
            timestamp = activity_row.get("timestamp")
            if not isinstance(timestamp, dt.datetime):
                continue
            usage_date = timestamp.date()
            session_activity[usage_date] = {
                "timestamp": timestamp,
                "input_tokens": 0,
                "output_tokens": 0,
                "cached_tokens": 0,
                "total_tokens": 0,
                "input_cost_usd": 0.0,
                "output_cost_usd": 0.0,
                "cached_cost_usd": 0.0,
                "total_cost_usd": 0.0,
                "cost_complete": True,
            }

        for usage_row in usage_rows:
            if not isinstance(usage_row, dict):
                continue
            usage_date_value = usage_row.get("date")
            if not isinstance(usage_date_value, str):
                continue
            try:
                usage_date = dt.date.fromisoformat(usage_date_value)
            except ValueError:
                continue

            input_tokens = safe_non_negative_int(usage_row.get("input_tokens"))
            output_tokens = safe_non_negative_int(usage_row.get("output_tokens"))
            cache_read_tokens = safe_non_negative_int(usage_row.get("cache_read_tokens"))
            cache_write_tokens = safe_non_negative_int(usage_row.get("cache_write_tokens"))
            cached_tokens = safe_non_negative_int(usage_row.get("cached_tokens"))
            total_tokens = safe_non_negative_int(usage_row.get("total_tokens"))
            native_cost = usage_row.get("native_cost") if isinstance(usage_row.get("native_cost"), dict) else None
            model = normalized_bucket_value(usage_row.get("model"), DEFAULT_MODEL)
            priced = catalog.price_usage(
                "pi",
                model,
                uncached_input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_write_tokens=cache_write_tokens,
                native_cost=native_cost,
            )

            daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
            agent_cli = "pi"
            apply_usage_to_daily(
                daily,
                agent_cli=agent_cli,
                model=model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                total_tokens=total_tokens,
                input_cost_usd=priced.input_cost_usd,
                output_cost_usd=priced.output_cost_usd,
                cached_cost_usd=priced.cached_cost_usd,
                total_cost_usd=priced.total_cost_usd,
                cost_complete=priced.cost_complete,
            )
            daily_sessions.setdefault(usage_date, set()).add(session_id)
            bucket_sessions.setdefault((usage_date, agent_cli, model), set()).add(session_id)

            activity = session_activity.get(usage_date)
            if activity is None:
                activity = {
                    "timestamp": dt.datetime.combine(usage_date, dt.time(hour=0), tzinfo=LOCAL_TIMEZONE),
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cached_tokens": 0,
                    "total_tokens": 0,
                    "input_cost_usd": 0.0,
                    "output_cost_usd": 0.0,
                    "cached_cost_usd": 0.0,
                    "total_cost_usd": 0.0,
                    "cost_complete": True,
                }
                session_activity[usage_date] = activity
            activity["input_tokens"] = safe_non_negative_int(activity.get("input_tokens")) + input_tokens
            activity["output_tokens"] = safe_non_negative_int(activity.get("output_tokens")) + output_tokens
            activity["cached_tokens"] = safe_non_negative_int(activity.get("cached_tokens")) + cached_tokens
            activity["total_tokens"] = safe_non_negative_int(activity.get("total_tokens")) + total_tokens
            activity["input_cost_usd"] = float(activity.get("input_cost_usd") or 0.0) + priced.input_cost_usd
            activity["output_cost_usd"] = float(activity.get("output_cost_usd") or 0.0) + priced.output_cost_usd
            activity["cached_cost_usd"] = float(activity.get("cached_cost_usd") or 0.0) + priced.cached_cost_usd
            activity["total_cost_usd"] = float(activity.get("total_cost_usd") or 0.0) + priced.total_cost_usd
            activity["cost_complete"] = bool(activity.get("cost_complete", True)) and priced.cost_complete

        for activity in session_activity.values():
            timestamp = activity.get("timestamp")
            if not isinstance(timestamp, dt.datetime):
                continue
            add_usage_to_activity(
                activity_totals,
                timestamp,
                sessions=1,
                input_tokens=safe_non_negative_int(activity.get("input_tokens")),
                output_tokens=safe_non_negative_int(activity.get("output_tokens")),
                cached_tokens=safe_non_negative_int(activity.get("cached_tokens")),
                total_tokens=safe_non_negative_int(activity.get("total_tokens")),
                input_cost_usd=float(activity.get("input_cost_usd") or 0.0),
                output_cost_usd=float(activity.get("output_cost_usd") or 0.0),
                cached_cost_usd=float(activity.get("cached_cost_usd") or 0.0),
                total_cost_usd=float(activity.get("total_cost_usd") or 0.0),
                cost_complete=bool(activity.get("cost_complete", True)),
            )

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    for (usage_date, agent_cli, model), sessions in bucket_sessions.items():
        daily = totals.get(usage_date)
        if daily is not None:
            daily.add_breakdown(agent_cli=agent_cli, model=model, sessions=len(sessions))

    return totals, activity_totals


def collect_pi_daily_totals(
    pi_agent_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> dict[dt.date, DailyTotals]:
    totals, _activity_totals = collect_pi_usage_data(pi_agent_root, pricing_catalog=pricing_catalog)
    return totals
