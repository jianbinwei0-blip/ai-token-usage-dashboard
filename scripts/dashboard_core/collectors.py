from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

from .models import ActivityTotals, BreakdownTotals, DailyTotals, round_cost
from .pricing import PricingCatalog, native_cost_values, normalize_native_cost


DEFAULT_MODEL = "unknown"
NANODOLLARS_PER_DOLLAR = 1_000_000_000
LOCAL_TIMEZONE = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
CODEX_ROLLOUT_TIMESTAMP_PATTERN = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})"
)
CLAUDE_COMMAND_MESSAGE_PATTERN = re.compile(r"<command-message>([^<]+)</command-message>")
CLAUDE_BUILTIN_COMMANDS = frozenset(
    {
        "exit",
        "help",
        "clear",
        "compact",
        "cost",
        "doctor",
        "init",
        "login",
        "logout",
        "memory",
        "permissions",
        "review",
        "status",
        "terminal-setup",
        "vim",
        "fast",
        "effort",
    }
)
CodexContribution = tuple[object, ...]
ClaudeRequestRecord = tuple[str, str, dt.datetime, int, int, int, int, int, str]
ClaudeAttributionEvent = tuple[str, str, str, dt.datetime]
PiUsageRow = tuple[str, str, int, int, int, int, int, int, object, object]
PiActivityRow = tuple[str, dt.datetime]


_CODEX_SESSION_USAGE_CACHE: dict[str, tuple[int, int, str, CodexContribution | None]] = {}
_CLAUDE_REQUEST_RECORDS_CACHE: dict[str, tuple[int, int, list[ClaudeRequestRecord]]] = {}
_CLAUDE_ATTRIBUTION_EVENTS_CACHE: dict[str, tuple[int, int, list[ClaudeAttributionEvent]]] = {}
_PI_SESSION_RECORDS_CACHE: dict[str, tuple[int, int, dict[str, object]]] = {}
_JSONL_FILE_INDEX_CACHE: dict[str, tuple[dict[str, int], tuple[str, ...]]] = {}
_PERSISTENT_CACHE_VERSION = 4
_PERSISTENT_CACHE_LOADED_FROM: str | None = None
_PERSISTENT_CACHE_DIRTY = False
_PI_APPEND_FAST_PATH_WINDOW_BYTES = 4096


def safe_non_negative_int(value: object) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


def cost_to_nanodollars(value: object) -> int:
    return int(round(float(value or 0.0) * NANODOLLARS_PER_DOLLAR))


def normalized_bucket_value(value: object, fallback: str) -> str:
    if isinstance(value, str):
        normalized = value.strip()
        if normalized:
            return normalized
    return fallback


def path_stem(path: object) -> str:
    name = os.path.basename(os.fspath(path))
    return name[:-6] if name.endswith(".jsonl") else os.path.splitext(name)[0]


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
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _mark_persistent_cache_dirty() -> None:
    global _PERSISTENT_CACHE_DIRTY
    _PERSISTENT_CACHE_DIRTY = True


def persistent_parse_caches_dirty() -> bool:
    return _PERSISTENT_CACHE_DIRTY


def _cache_entries_for_root(cache: dict, root: Path) -> list[tuple[str, object]]:
    """Return every session record observed below root, including deleted source files."""
    root_prefix = os.path.join(os.fspath(root), "")
    return [(key, value) for key, value in cache.items() if key.startswith(root_prefix)]


def _ordered_observed_paths(live_paths: list[str], cached_paths: set[str]) -> list[str]:
    """Keep the legacy live traversal order, then append deleted log paths deterministically."""
    live_path_set = set(live_paths)
    return [path for path in live_paths if path in cached_paths] + sorted(cached_paths - live_path_set)


def _serialize_claude_request_record(record: ClaudeRequestRecord) -> list[object]:
    (
        session_id,
        request_id,
        timestamp,
        input_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        cached_tokens,
        output_tokens,
        model,
    ) = record
    return [
        session_id,
        request_id,
        serialize_timestamp(timestamp),
        input_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        cached_tokens,
        output_tokens,
        model,
    ]


def _deserialize_claude_request_record(record: object) -> ClaudeRequestRecord | None:
    if isinstance(record, list) and len(record) == 9:
        request_id = record[1] if isinstance(record[1], str) else ""
        timestamp = deserialize_timestamp(record[2])
        if not request_id or timestamp is None:
            return None
        return (
            record[0] if isinstance(record[0], str) else "",
            request_id,
            timestamp,
            record[3],
            record[4],
            record[5],
            record[6],
            record[7],
            record[8] if isinstance(record[8], str) and record[8] else DEFAULT_MODEL,
        )
    if not isinstance(record, dict):
        return None

    request_id = normalized_bucket_value(record.get("request_id"), "")
    timestamp_value = record.get("timestamp")
    timestamp = timestamp_value if isinstance(timestamp_value, dt.datetime) else deserialize_timestamp(timestamp_value)
    if not request_id or timestamp is None:
        return None
    return (
        normalized_bucket_value(record.get("session_id"), ""),
        request_id,
        timestamp,
        safe_non_negative_int(record.get("input_tokens")),
        safe_non_negative_int(record.get("cache_creation_input_tokens")),
        safe_non_negative_int(record.get("cache_read_input_tokens")),
        safe_non_negative_int(record.get("cached_tokens")),
        safe_non_negative_int(record.get("output_tokens")),
        normalized_bucket_value(record.get("model"), DEFAULT_MODEL),
    )


def _serialize_claude_attribution_event(event: ClaudeAttributionEvent) -> list[object]:
    category, name, session_id, timestamp = event
    return [category, name, session_id, serialize_timestamp(timestamp)]


def _deserialize_claude_attribution_event(event: object) -> ClaudeAttributionEvent | None:
    if isinstance(event, list) and len(event) == 4:
        category = event[0] if isinstance(event[0], str) else ""
        name = event[1] if isinstance(event[1], str) else ""
        timestamp = deserialize_timestamp(event[3])
        if not category or not name or timestamp is None:
            return None
        return category, name, event[2] if isinstance(event[2], str) else "", timestamp
    if not isinstance(event, dict):
        return None

    category = normalized_bucket_value(event.get("category"), "")
    name = normalized_bucket_value(event.get("name"), "")
    timestamp_value = event.get("timestamp")
    timestamp = timestamp_value if isinstance(timestamp_value, dt.datetime) else deserialize_timestamp(timestamp_value)
    if not category or not name or timestamp is None:
        return None
    return category, name, normalized_bucket_value(event.get("session_id"), ""), timestamp


def _serialize_codex_contribution(contribution: CodexContribution | None) -> list[object] | None:
    if contribution is None:
        return None
    (
        usage_date,
        activity_date,
        activity_hour,
        session_id,
        agent_cli,
        model,
        input_tokens,
        output_tokens,
        cached_tokens,
        total_tokens,
        input_cost_usd,
        output_cost_usd,
        cached_cost_usd,
        total_cost_usd,
        cost_complete,
    ) = contribution
    return [
        "t",
        usage_date.toordinal(),
        activity_date.toordinal(),
        activity_hour,
        session_id,
        agent_cli,
        model,
        input_tokens,
        output_tokens,
        cached_tokens,
        total_tokens,
        input_cost_usd,
        output_cost_usd,
        cached_cost_usd,
        total_cost_usd,
        cost_complete,
    ]


def _legacy_codex_dates(usage_date_value: object, timestamp_value: object) -> tuple[dt.date, dt.date, int] | None:
    if not isinstance(usage_date_value, str):
        return None
    try:
        usage_date = dt.date.fromisoformat(usage_date_value)
    except ValueError:
        return None
    timestamp = timestamp_value if isinstance(timestamp_value, dt.datetime) else deserialize_timestamp(timestamp_value)
    if timestamp is None:
        return usage_date, usage_date, 0
    return usage_date, timestamp.date(), timestamp.hour


def _deserialize_codex_contribution(contribution: object) -> CodexContribution | None:
    if contribution is None:
        return None
    if isinstance(contribution, list) and len(contribution) == 16 and contribution[0] == "t":
        try:
            usage_date = dt.date.fromordinal(contribution[1])
            activity_date = dt.date.fromordinal(contribution[2])
        except (TypeError, ValueError):
            return None
        return (
            usage_date,
            activity_date,
            contribution[3],
            contribution[4] if isinstance(contribution[4], str) else "",
            contribution[5] if isinstance(contribution[5], str) and contribution[5] else "codex",
            contribution[6] if isinstance(contribution[6], str) and contribution[6] else DEFAULT_MODEL,
            contribution[7],
            contribution[8],
            contribution[9],
            contribution[10],
            contribution[11],
            contribution[12],
            contribution[13],
            contribution[14],
            bool(contribution[15]),
        )
    if isinstance(contribution, list) and len(contribution) == 15 and contribution[0] == "n":
        dates = _legacy_codex_dates(contribution[1], contribution[2])
        if dates is None:
            return None
        return (
            *dates,
            contribution[3] if isinstance(contribution[3], str) else "",
            contribution[4] if isinstance(contribution[4], str) and contribution[4] else "codex",
            contribution[5] if isinstance(contribution[5], str) and contribution[5] else DEFAULT_MODEL,
            contribution[6],
            contribution[7],
            contribution[8],
            contribution[9],
            contribution[10],
            contribution[11],
            contribution[12],
            contribution[13],
            bool(contribution[14]),
        )
    if isinstance(contribution, list) and len(contribution) == 14:
        dates = _legacy_codex_dates(contribution[0], contribution[1])
        if dates is None:
            return None
        return (
            *dates,
            contribution[2] if isinstance(contribution[2], str) else "",
            contribution[3] if isinstance(contribution[3], str) and contribution[3] else "codex",
            contribution[4] if isinstance(contribution[4], str) and contribution[4] else DEFAULT_MODEL,
            contribution[5],
            contribution[6],
            contribution[7],
            contribution[8],
            cost_to_nanodollars(contribution[9]),
            cost_to_nanodollars(contribution[10]),
            cost_to_nanodollars(contribution[11]),
            cost_to_nanodollars(contribution[12]),
            bool(contribution[13]),
        )
    if not isinstance(contribution, dict):
        return None

    dates = _legacy_codex_dates(contribution.get("usage_date"), contribution.get("timestamp"))
    if dates is None:
        return None
    return (
        *dates,
        normalized_bucket_value(contribution.get("session_id"), ""),
        normalized_bucket_value(contribution.get("agent_cli"), "codex"),
        normalized_bucket_value(contribution.get("model"), DEFAULT_MODEL),
        safe_non_negative_int(contribution.get("input_tokens")),
        safe_non_negative_int(contribution.get("output_tokens")),
        safe_non_negative_int(contribution.get("cached_tokens")),
        safe_non_negative_int(contribution.get("total_tokens")),
        cost_to_nanodollars(contribution.get("input_cost_usd")),
        cost_to_nanodollars(contribution.get("output_cost_usd")),
        cost_to_nanodollars(contribution.get("cached_cost_usd")),
        cost_to_nanodollars(contribution.get("total_cost_usd")),
        bool(contribution.get("cost_complete", True)),
    )


def merge_native_cost(target: dict[str, float] | None, native_cost: object) -> dict[str, float] | None:
    if not isinstance(native_cost, dict) or not native_cost:
        return target

    merged = dict(target or {})
    for key in ("input", "output", "cacheRead", "cacheWrite", "total"):
        value = native_cost.get(key)
        if isinstance(value, (int, float)) and value >= 0:
            merged[key] = float(merged.get(key) or 0.0) + float(value)
    return merged or target


def _hash_bytes(value: bytes) -> str:
    if not value:
        return ""
    return hashlib.blake2b(value, digest_size=16).hexdigest()


def _read_file_signature_window(file_path: Path, start: int, end: int) -> str:
    bounded_start = max(0, start)
    bounded_end = max(bounded_start, end)
    if bounded_end <= bounded_start:
        return ""

    try:
        with file_path.open("rb") as handle:
            handle.seek(bounded_start)
            return _hash_bytes(handle.read(bounded_end - bounded_start))
    except OSError:
        return ""


def _pi_native_cost_state(native_cost: object) -> tuple[object, object]:
    normalized = normalize_native_cost(native_cost)
    values = native_cost_values(normalized)
    if normalized is None or values is None:
        return None, None
    return normalized, tuple(cost_to_nanodollars(value) for value in values)


def _pi_native_cost_mapping(native_cost: object) -> dict[str, float] | None:
    normalized = normalize_native_cost(native_cost)
    if normalized is None:
        return None
    mapped = {
        "input": normalized[0],
        "output": normalized[1],
        "cacheRead": normalized[2],
        "cacheWrite": normalized[3],
    }
    if normalized[4] is not None:
        mapped["total"] = normalized[4]
    return mapped


def _pi_usage_row_from_object(row: object) -> PiUsageRow | None:
    if isinstance(row, tuple) and len(row) == 10:
        return row
    if isinstance(row, list) and len(row) == 10:
        date_value = row[0]
        if not isinstance(date_value, str):
            return None
        if isinstance(row[8], list) and len(row[8]) == 5 and isinstance(row[9], list) and len(row[9]) == 4:
            native_cost = tuple(row[8])
            priced_cost = tuple(row[9])
        else:
            native_cost, priced_cost = _pi_native_cost_state(row[8])
        return (
            date_value,
            row[1] if isinstance(row[1], str) and row[1] else DEFAULT_MODEL,
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            native_cost,
            priced_cost,
        )
    if isinstance(row, list) and len(row) == 9:
        date_value = row[0]
        if not isinstance(date_value, str):
            return None
        native_cost, priced_cost = _pi_native_cost_state(row[8])
        return (
            date_value,
            row[1] if isinstance(row[1], str) and row[1] else DEFAULT_MODEL,
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            native_cost,
            priced_cost,
        )
    if not isinstance(row, dict):
        return None
    date_value = row.get("date")
    if not isinstance(date_value, str):
        return None
    native_cost, priced_cost = _pi_native_cost_state(row.get("native_cost"))
    return (
        date_value,
        normalized_bucket_value(row.get("model"), DEFAULT_MODEL),
        safe_non_negative_int(row.get("input_tokens")),
        safe_non_negative_int(row.get("output_tokens")),
        safe_non_negative_int(row.get("cache_read_tokens")),
        safe_non_negative_int(row.get("cache_write_tokens")),
        safe_non_negative_int(row.get("cached_tokens")),
        safe_non_negative_int(row.get("total_tokens")),
        native_cost,
        priced_cost,
    )


def _pi_activity_row_from_object(row: object) -> PiActivityRow | None:
    if isinstance(row, tuple) and len(row) == 2 and isinstance(row[0], str) and isinstance(row[1], dt.datetime):
        return row
    if isinstance(row, list) and len(row) == 2:
        timestamp = deserialize_timestamp(row[1])
        if isinstance(row[0], str) and timestamp is not None:
            return row[0], timestamp
        return None
    if not isinstance(row, dict):
        return None
    date_value = row.get("date")
    timestamp_value = row.get("timestamp")
    timestamp = timestamp_value if isinstance(timestamp_value, dt.datetime) else deserialize_timestamp(timestamp_value)
    if not isinstance(date_value, str) or timestamp is None:
        return None
    return date_value, timestamp


def _serialize_pi_usage_row(row: object) -> list[object] | None:
    normalized = _pi_usage_row_from_object(row)
    return list(normalized) if normalized is not None else None


def _serialize_pi_activity_row(row: object) -> list[object] | None:
    normalized = _pi_activity_row_from_object(row)
    if normalized is None:
        return None
    return [normalized[0], serialize_timestamp(normalized[1])]


def empty_pi_contribution(session_id: str) -> dict[str, object]:
    return {
        "session_id": normalized_bucket_value(session_id, "unknown-session"),
        "active_model": DEFAULT_MODEL,
        "offset": 0,
        "head_signature": "",
        "boundary_signature": "",
        "usage_rows": [],
        "activity_rows": [],
    }


def serialize_pi_contribution(contribution: object) -> dict[str, object] | None:
    if not isinstance(contribution, dict):
        return None
    usage_rows = contribution.get("usage_rows")
    activity_rows = contribution.get("activity_rows")
    if not isinstance(usage_rows, list) or not isinstance(activity_rows, list):
        return None
    return {
        "session_id": normalized_bucket_value(contribution.get("session_id"), "unknown-session"),
        "active_model": normalized_bucket_value(contribution.get("active_model"), DEFAULT_MODEL),
        "offset": safe_non_negative_int(contribution.get("offset")),
        "head_signature": normalized_bucket_value(contribution.get("head_signature"), ""),
        "boundary_signature": normalized_bucket_value(contribution.get("boundary_signature"), ""),
        "usage_rows": [
            serialized
            for row in usage_rows
            if (serialized := _serialize_pi_usage_row(row)) is not None
        ],
        "activity_rows": [
            serialized
            for row in activity_rows
            if (serialized := _serialize_pi_activity_row(row)) is not None
        ],
    }


def deserialize_pi_contribution(contribution: object) -> dict[str, object] | None:
    if not isinstance(contribution, dict):
        return None
    usage_rows = contribution.get("usage_rows")
    activity_rows = contribution.get("activity_rows")
    if not isinstance(usage_rows, list) or not isinstance(activity_rows, list):
        return None
    contribution["session_id"] = normalized_bucket_value(contribution.get("session_id"), "unknown-session")
    contribution["active_model"] = normalized_bucket_value(contribution.get("active_model"), DEFAULT_MODEL)
    contribution["offset"] = safe_non_negative_int(contribution.get("offset"))
    contribution["head_signature"] = normalized_bucket_value(contribution.get("head_signature"), "")
    contribution["boundary_signature"] = normalized_bucket_value(contribution.get("boundary_signature"), "")
    contribution["usage_rows"] = [
        row for item in usage_rows if (row := _pi_usage_row_from_object(item)) is not None
    ]
    contribution["activity_rows"] = [
        row for item in activity_rows if (row := _pi_activity_row_from_object(item)) is not None
    ]
    return contribution


def clone_pi_contribution(contribution: dict[str, object] | None, *, session_id: str) -> dict[str, object]:
    cloned = deserialize_pi_contribution(serialize_pi_contribution(contribution))
    if cloned is not None:
        return cloned
    return empty_pi_contribution(session_id)


def update_pi_contribution_signatures(file_path: Path, contribution: dict[str, object]) -> None:
    processed_offset = safe_non_negative_int(contribution.get("offset"))
    head_end = min(processed_offset, _PI_APPEND_FAST_PATH_WINDOW_BYTES)
    boundary_start = max(0, processed_offset - _PI_APPEND_FAST_PATH_WINDOW_BYTES)
    contribution["head_signature"] = _read_file_signature_window(file_path, 0, head_end)
    contribution["boundary_signature"] = _read_file_signature_window(file_path, boundary_start, processed_offset)


def load_persistent_parse_caches(cache_path: Path | None) -> None:
    global _PERSISTENT_CACHE_LOADED_FROM, _PERSISTENT_CACHE_DIRTY

    target = str(cache_path.resolve()) if cache_path is not None else None
    if _PERSISTENT_CACHE_LOADED_FROM == target:
        return

    _CODEX_SESSION_USAGE_CACHE.clear()
    _CLAUDE_REQUEST_RECORDS_CACHE.clear()
    _CLAUDE_ATTRIBUTION_EVENTS_CACHE.clear()
    _PI_SESSION_RECORDS_CACHE.clear()
    _JSONL_FILE_INDEX_CACHE.clear()
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

    file_indexes_payload = payload.get("file_indexes")
    if isinstance(file_indexes_payload, dict):
        for root_path, entry in file_indexes_payload.items():
            if not isinstance(root_path, str) or not isinstance(entry, dict):
                continue
            directories_payload = entry.get("directories")
            files_payload = entry.get("files")
            if not isinstance(directories_payload, dict) or not isinstance(files_payload, list):
                continue
            directories = {
                path: mtime_ns
                for path, value in directories_payload.items()
                if isinstance(path, str) and (mtime_ns := safe_non_negative_int(value))
            }
            files = tuple(path for path in files_payload if isinstance(path, str))
            if directories:
                _JSONL_FILE_INDEX_CACHE[root_path] = (directories, files)

    codex_payload = payload.get("codex")
    if isinstance(codex_payload, dict):
        for file_path, entry in codex_payload.items():
            legacy_entry = isinstance(entry, dict)
            if legacy_entry:
                size_value = entry.get("size")
                mtime_ns_value = entry.get("mtime_ns")
                pricing_key_value = entry.get("pricing_key")
                contribution_value = entry.get("contribution")
            elif isinstance(entry, list) and len(entry) == 4:
                size_value, mtime_ns_value, pricing_key_value, contribution_value = entry
            else:
                continue
            size = safe_non_negative_int(size_value)
            mtime_ns = safe_non_negative_int(mtime_ns_value)
            pricing_key = normalized_bucket_value(pricing_key_value, "")
            contribution = _deserialize_codex_contribution(contribution_value)
            if not pricing_key:
                continue
            _CODEX_SESSION_USAGE_CACHE[file_path] = (size, mtime_ns, pricing_key, contribution)
            if legacy_entry or (
                contribution_value is not None
                and not (
                    isinstance(contribution_value, list)
                    and len(contribution_value) == 16
                    and contribution_value[0] == "t"
                )
            ):
                _PERSISTENT_CACHE_DIRTY = True

    claude_payload = payload.get("claude")
    if isinstance(claude_payload, dict):
        for file_path, entry in claude_payload.items():
            legacy_entry = isinstance(entry, dict)
            if legacy_entry:
                size_value = entry.get("size")
                mtime_ns_value = entry.get("mtime_ns")
                records_payload = entry.get("records")
            elif isinstance(entry, list) and len(entry) == 3:
                size_value, mtime_ns_value, records_payload = entry
            else:
                continue
            if not isinstance(records_payload, list):
                continue
            records = [
                record
                for item in records_payload
                if (record := _deserialize_claude_request_record(item)) is not None
            ]
            _CLAUDE_REQUEST_RECORDS_CACHE[file_path] = (
                safe_non_negative_int(size_value),
                safe_non_negative_int(mtime_ns_value),
                records,
            )
            if legacy_entry or any(isinstance(item, dict) for item in records_payload):
                _PERSISTENT_CACHE_DIRTY = True

    claude_attribution_payload = payload.get("claude_attribution")
    if isinstance(claude_attribution_payload, dict):
        for file_path, entry in claude_attribution_payload.items():
            legacy_entry = isinstance(entry, dict)
            if legacy_entry:
                size_value = entry.get("size")
                mtime_ns_value = entry.get("mtime_ns")
                events_payload = entry.get("events")
            elif isinstance(entry, list) and len(entry) == 3:
                size_value, mtime_ns_value, events_payload = entry
            else:
                continue
            if not isinstance(events_payload, list):
                continue
            events = [
                event
                for item in events_payload
                if (event := _deserialize_claude_attribution_event(item)) is not None
            ]
            _CLAUDE_ATTRIBUTION_EVENTS_CACHE[file_path] = (
                safe_non_negative_int(size_value),
                safe_non_negative_int(mtime_ns_value),
                events,
            )
            if legacy_entry or any(isinstance(item, dict) for item in events_payload):
                _PERSISTENT_CACHE_DIRTY = True

    pi_payload = payload.get("pi")
    if isinstance(pi_payload, dict):
        for file_path, entry in pi_payload.items():
            if not isinstance(entry, dict):
                continue
            size = safe_non_negative_int(entry.get("size"))
            mtime_ns = safe_non_negative_int(entry.get("mtime_ns"))
            contribution_payload = entry.get("contribution")
            usage_rows_payload = contribution_payload.get("usage_rows") if isinstance(contribution_payload, dict) else None
            activity_rows_payload = (
                contribution_payload.get("activity_rows") if isinstance(contribution_payload, dict) else None
            )
            legacy_rows = (
                isinstance(usage_rows_payload, list)
                and any(
                    isinstance(row, dict) or (isinstance(row, list) and len(row) != 10)
                    for row in usage_rows_payload
                )
            ) or (
                isinstance(activity_rows_payload, list)
                and any(isinstance(row, dict) for row in activity_rows_payload)
            )
            contribution = deserialize_pi_contribution(contribution_payload)
            if contribution is None:
                continue
            _PI_SESSION_RECORDS_CACHE[file_path] = (size, mtime_ns, contribution)
            if legacy_rows:
                _PERSISTENT_CACHE_DIRTY = True


def save_persistent_parse_caches(cache_path: Path | None) -> None:
    global _PERSISTENT_CACHE_DIRTY

    if cache_path is None or not _PERSISTENT_CACHE_DIRTY:
        return

    payload = {
        "version": _PERSISTENT_CACHE_VERSION,
        "file_indexes": {
            root_path: {
                "directories": directories,
                "files": list(files),
            }
            for root_path, (directories, files) in _JSONL_FILE_INDEX_CACHE.items()
        },
        "codex": {
            file_path: [
                size,
                mtime_ns,
                pricing_key,
                _serialize_codex_contribution(contribution),
            ]
            for file_path, (size, mtime_ns, pricing_key, contribution) in _CODEX_SESSION_USAGE_CACHE.items()
        },
        "claude": {
            file_path: [
                size,
                mtime_ns,
                [_serialize_claude_request_record(record) for record in records],
            ]
            for file_path, (size, mtime_ns, records) in _CLAUDE_REQUEST_RECORDS_CACHE.items()
        },
        "claude_attribution": {
            file_path: [
                size,
                mtime_ns,
                [_serialize_claude_attribution_event(event) for event in events],
            ]
            for file_path, (size, mtime_ns, events) in _CLAUDE_ATTRIBUTION_EVENTS_CACHE.items()
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


def _round_accumulated_costs(totals: ActivityTotals | BreakdownTotals | DailyTotals) -> None:
    totals.input_cost_usd = round_cost(totals.input_cost_usd)
    totals.output_cost_usd = round_cost(totals.output_cost_usd)
    totals.cached_cost_usd = round_cost(totals.cached_cost_usd)
    totals.total_cost_usd = round_cost(totals.total_cost_usd)


def _materialize_nanodollar_costs(totals: ActivityTotals | BreakdownTotals | DailyTotals) -> None:
    totals.input_cost_usd /= NANODOLLARS_PER_DOLLAR
    totals.output_cost_usd /= NANODOLLARS_PER_DOLLAR
    totals.cached_cost_usd /= NANODOLLARS_PER_DOLLAR
    totals.total_cost_usd /= NANODOLLARS_PER_DOLLAR


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


def _jsonl_file_index_is_current(directories: dict[str, int]) -> bool:
    for directory, expected_mtime_ns in directories.items():
        try:
            if os.stat(directory).st_mtime_ns != expected_mtime_ns:
                return False
        except OSError:
            return False
    return True


def _scan_jsonl_file_index(root: Path) -> tuple[dict[str, int], tuple[str, ...], bool]:
    directories: dict[str, int] = {}
    files: list[str] = []
    stable = True
    pending = [str(root)]

    while pending:
        directory = pending.pop()
        try:
            before_mtime_ns = os.stat(directory).st_mtime_ns
            with os.scandir(directory) as scanner:
                entries = list(scanner)
            after_mtime_ns = os.stat(directory).st_mtime_ns
        except OSError:
            stable = False
            continue

        if before_mtime_ns != after_mtime_ns:
            stable = False
        directories[directory] = after_mtime_ns

        child_directories: list[str] = []
        directory_files: list[str] = []
        for entry in entries:
            try:
                is_directory = entry.is_dir()
                is_symlink = entry.is_symlink()
            except OSError:
                stable = False
                continue
            if is_directory:
                if not is_symlink:
                    child_directories.append(entry.path)
            elif entry.name.endswith(".jsonl"):
                directory_files.append(entry.path)

        files.extend(sorted(directory_files))
        pending.extend(reversed(sorted(child_directories)))

    return directories, tuple(files), stable


def iter_jsonl_files(root: Path):
    root_key = str(root)
    cached = _JSONL_FILE_INDEX_CACHE.get(root_key)
    if cached is not None and _jsonl_file_index_is_current(cached[0]):
        yield from cached[1]
        return

    directories, files, stable = _scan_jsonl_file_index(root)
    if stable and directories:
        refreshed = (directories, files)
        if cached != refreshed:
            _JSONL_FILE_INDEX_CACHE[root_key] = refreshed
            _mark_persistent_cache_dirty()
    elif cached is not None:
        del _JSONL_FILE_INDEX_CACHE[root_key]
        _mark_persistent_cache_dirty()

    yield from files


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
) -> CodexContribution | None:
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

    return (
        usage_date,
        activity_timestamp.date(),
        activity_timestamp.hour,
        session_id,
        agent_cli,
        model,
        input_tokens,
        output_tokens,
        cached_tokens,
        total_tokens,
        cost_to_nanodollars(priced.input_cost_usd),
        cost_to_nanodollars(priced.output_cost_usd),
        cost_to_nanodollars(priced.cached_cost_usd),
        cost_to_nanodollars(priced.total_cost_usd),
        priced.cost_complete,
    )


def parse_codex_session_usage_cached(
    session_path,
    sessions_root: Path,
    pricing_catalog: PricingCatalog,
    current_pricing_key: str | None = None,
) -> CodexContribution | None:
    try:
        stat = os.stat(session_path)
    except OSError:
        return None

    cache_key = os.fspath(session_path)
    cached = _CODEX_SESSION_USAGE_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    if current_pricing_key is None:
        current_pricing_key = pricing_cache_key(pricing_catalog)
    if cached is not None and cached[:2] == signature and cached[2] == current_pricing_key:
        return cached[3]

    path = session_path if isinstance(session_path, Path) else Path(session_path)
    usage = parse_codex_session_usage(path)
    contribution = None if usage is None else build_codex_session_contribution(path, sessions_root, usage, pricing_catalog)
    _CODEX_SESSION_USAGE_CACHE[cache_key] = (signature[0], signature[1], current_pricing_key, contribution)
    _mark_persistent_cache_dirty()
    return contribution


def _reprice_codex_contribution(
    contribution: CodexContribution,
    pricing_catalog: PricingCatalog,
) -> CodexContribution:
    priced = pricing_catalog.price_usage(
        "codex",
        contribution[5],
        uncached_input_tokens=contribution[6],
        output_tokens=contribution[7],
        cache_read_tokens=contribution[8],
    )
    return (
        *contribution[:10],
        cost_to_nanodollars(priced.input_cost_usd),
        cost_to_nanodollars(priced.output_cost_usd),
        cost_to_nanodollars(priced.cached_cost_usd),
        cost_to_nanodollars(priced.total_cost_usd),
        priced.cost_complete,
    )


def iter_observed_codex_contributions(
    sessions_root: Path,
    pricing_catalog: PricingCatalog,
    current_pricing_key: str,
):
    """Yield every observed log contribution, even after its source file is deleted."""
    live_paths: list[str] = []
    for file_path in iter_jsonl_files(sessions_root):
        live_paths.append(os.fspath(file_path))
        parse_codex_session_usage_cached(
            file_path,
            sessions_root,
            pricing_catalog,
            current_pricing_key=current_pricing_key,
        )

    entries = dict(_cache_entries_for_root(_CODEX_SESSION_USAGE_CACHE, sessions_root))
    for cache_path in _ordered_observed_paths(live_paths, set(entries)):
        entry = entries[cache_path]
        size, mtime_ns, stored_pricing_key, contribution = entry
        if contribution is None:
            continue
        if stored_pricing_key != current_pricing_key:
            contribution = _reprice_codex_contribution(contribution, pricing_catalog)
            _CODEX_SESSION_USAGE_CACHE[cache_path] = (
                size,
                mtime_ns,
                current_pricing_key,
                contribution,
            )
            _mark_persistent_cache_dirty()
        yield cache_path, contribution


def collect_codex_usage_data(
    sessions_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> tuple[dict[dt.date, DailyTotals], dict[tuple[dt.date, int], ActivityTotals]]:
    totals: dict[dt.date, DailyTotals] = {}
    activity_totals: dict[tuple[dt.date, int], ActivityTotals] = {}
    catalog = pricing_catalog or PricingCatalog.from_file(None)
    current_pricing_key = pricing_cache_key(catalog)
    bucket_sessions: dict[tuple[dt.date, str, str], set[str]] = {}

    for file_path, contribution in iter_observed_codex_contributions(
        sessions_root,
        catalog,
        current_pricing_key,
    ):
        (
            usage_date,
            activity_date,
            activity_hour,
            session_id,
            agent_cli,
            model,
            input_tokens,
            output_tokens,
            cached_tokens,
            total_tokens,
            input_cost_usd,
            output_cost_usd,
            cached_cost_usd,
            total_cost_usd,
            cost_complete,
        ) = contribution
        if not session_id:
            session_id = path_stem(file_path)

        daily = totals.get(usage_date)
        if daily is None:
            daily = DailyTotals(date=usage_date)
            daily.input_cost_usd = daily.output_cost_usd = daily.cached_cost_usd = daily.total_cost_usd = 0
            totals[usage_date] = daily
        daily.sessions += 1
        daily.input_tokens += input_tokens
        daily.output_tokens += output_tokens
        daily.cached_tokens += cached_tokens
        daily.total_tokens += total_tokens
        daily.input_cost_usd += input_cost_usd
        daily.output_cost_usd += output_cost_usd
        daily.cached_cost_usd += cached_cost_usd
        daily.total_cost_usd += total_cost_usd
        daily.cost_complete = daily.cost_complete and cost_complete

        breakdown_key = (agent_cli, model)
        breakdown = daily.breakdowns.get(breakdown_key)
        if breakdown is None:
            breakdown = BreakdownTotals(agent_cli=agent_cli, model=model)
            breakdown.input_cost_usd = breakdown.output_cost_usd = breakdown.cached_cost_usd = breakdown.total_cost_usd = 0
            daily.breakdowns[breakdown_key] = breakdown
        breakdown.input_tokens += input_tokens
        breakdown.output_tokens += output_tokens
        breakdown.cached_tokens += cached_tokens
        breakdown.total_tokens += total_tokens
        breakdown.input_cost_usd += input_cost_usd
        breakdown.output_cost_usd += output_cost_usd
        breakdown.cached_cost_usd += cached_cost_usd
        breakdown.total_cost_usd += total_cost_usd
        breakdown.cost_complete = breakdown.cost_complete and cost_complete
        bucket_sessions.setdefault((usage_date, agent_cli, model), set()).add(session_id)

        activity_key = (activity_date, activity_hour)
        activity = activity_totals.get(activity_key)
        if activity is None:
            activity = ActivityTotals(date=activity_key[0], hour=activity_key[1])
            activity.input_cost_usd = activity.output_cost_usd = activity.cached_cost_usd = activity.total_cost_usd = 0
            activity_totals[activity_key] = activity
        activity.sessions += 1
        activity.input_tokens += input_tokens
        activity.output_tokens += output_tokens
        activity.cached_tokens += cached_tokens
        activity.total_tokens += total_tokens
        activity.input_cost_usd += input_cost_usd
        activity.output_cost_usd += output_cost_usd
        activity.cached_cost_usd += cached_cost_usd
        activity.total_cost_usd += total_cost_usd
        activity.cost_complete = activity.cost_complete and cost_complete

    for (usage_date, agent_cli, model), sessions in bucket_sessions.items():
        daily = totals.get(usage_date)
        if daily is not None:
            daily.breakdowns[(agent_cli, model)].sessions = len(sessions)

    for daily in totals.values():
        _materialize_nanodollar_costs(daily)
        for breakdown in daily.breakdowns.values():
            _materialize_nanodollar_costs(breakdown)
    for activity in activity_totals.values():
        _materialize_nanodollar_costs(activity)

    return totals, activity_totals


def collect_codex_daily_totals(
    sessions_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> dict[dt.date, DailyTotals]:
    totals, _activity_totals = collect_codex_usage_data(sessions_root, pricing_catalog=pricing_catalog)
    return totals


def parse_claude_request_records(file_path: Path) -> list[ClaudeRequestRecord]:
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

    return [
        record
        for value in request_usage.values()
        if (record := _deserialize_claude_request_record(value)) is not None
    ]


def parse_claude_request_records_cached(file_path) -> list[ClaudeRequestRecord]:
    try:
        stat = os.stat(file_path)
    except OSError:
        return []

    cache_key = os.fspath(file_path)
    cached = _CLAUDE_REQUEST_RECORDS_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    if cached is not None and cached[:2] == signature:
        return cached[2]

    path = file_path if isinstance(file_path, Path) else Path(file_path)
    records = parse_claude_request_records(path)
    _CLAUDE_REQUEST_RECORDS_CACHE[cache_key] = (signature[0], signature[1], records)
    _mark_persistent_cache_dirty()
    return records


def extract_claude_message_texts(message: object) -> list[str]:
    texts: list[str] = []
    if isinstance(message, str):
        texts.append(message)
    elif isinstance(message, list):
        for item in message:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    texts.append(text)
            elif isinstance(item, str):
                texts.append(item)
    elif isinstance(message, dict):
        content = message.get("content", "")
        if isinstance(content, str):
            texts.append(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        texts.append(text)
                elif isinstance(item, str):
                    texts.append(item)
    return texts


def normalize_claude_skill_name(value: object) -> str:
    command = normalized_bucket_value(value, "")
    if not command:
        return ""
    command = command.lstrip("/")
    if not command or command in CLAUDE_BUILTIN_COMMANDS:
        return ""
    return f"/{command}"


def add_claude_attribution_event(
    events: list[ClaudeAttributionEvent],
    seen: set[tuple[str, str, str, str]],
    *,
    category: str,
    name: str,
    session_id: str,
    timestamp: dt.datetime,
    source_id: object = None,
) -> None:
    normalized_name = normalized_bucket_value(name, "")
    if not normalized_name:
        return
    event_key = normalized_bucket_value(source_id, timestamp.isoformat())
    dedupe_key = (category, normalized_name, session_id, event_key)
    if dedupe_key in seen:
        return
    seen.add(dedupe_key)
    events.append((category, normalized_name, session_id, timestamp))


def normalize_claude_tool_name(tool_name: object) -> str:
    name = normalized_bucket_value(tool_name, "")
    if name.startswith("mcp__"):
        parts = name.split("__")
        server = parts[1] if len(parts) > 1 and parts[1] else "unknown"
        method = "__".join(part for part in parts[2:] if part)
        return f"mcp:{server}/{method}" if method else f"mcp:{server}"
    if name.startswith("plugin__") or name.startswith("extension__"):
        parts = name.split("__")
        namespace = parts[1] if len(parts) > 1 and parts[1] else "unknown"
        method = "__".join(part for part in parts[2:] if part)
        prefix = "extension" if name.startswith("extension__") else "plugin"
        return f"{prefix}:{namespace}/{method}" if method else f"{prefix}:{namespace}"
    return name


def extract_claude_extension_names(event: dict[str, object]) -> list[str]:
    names: list[str] = []
    candidate_keys = (
        "plugin",
        "pluginName",
        "plugin_name",
        "pluginId",
        "plugin_id",
        "extension",
        "extensionName",
        "extension_name",
        "extensionId",
        "extension_id",
    )

    def append_candidate(value: object) -> None:
        if isinstance(value, str):
            normalized = normalized_bucket_value(value, "")
        elif isinstance(value, dict):
            normalized = normalized_bucket_value(
                value.get("name") or value.get("id") or value.get("label") or value.get("source"),
                "",
            )
        else:
            normalized = ""
        if normalized and normalized not in names:
            names.append(normalized)

    for key in candidate_keys:
        append_candidate(event.get(key))

    message = event.get("message")
    if isinstance(message, dict):
        for key in candidate_keys:
            append_candidate(message.get(key))

    return names


def extension_name_from_tool_name(tool_name: str) -> str:
    if not (tool_name.startswith("plugin__") or tool_name.startswith("extension__")):
        return ""
    parts = tool_name.split("__")
    return normalized_bucket_value(parts[1] if len(parts) > 1 else "", "")


def parse_claude_attribution_events(file_path: Path) -> list[ClaudeAttributionEvent]:
    session_scope = file_path.stem
    events: list[ClaudeAttributionEvent] = []
    seen: set[tuple[str, str, str, str]] = set()

    with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue

            timestamp = parse_timestamp_local(event.get("timestamp"))
            if timestamp is None:
                continue
            session_id = normalized_bucket_value(event.get("sessionId"), session_scope)

            for extension_name in extract_claude_extension_names(event):
                add_claude_attribution_event(
                    events,
                    seen,
                    category="plugin",
                    name=extension_name,
                    session_id=session_id,
                    timestamp=timestamp,
                    source_id=f"extension:{extension_name}:{timestamp.isoformat()}",
                )

            if event.get("type") == "user":
                for text in extract_claude_message_texts(event.get("message")):
                    for match in CLAUDE_COMMAND_MESSAGE_PATTERN.finditer(text):
                        skill_name = normalize_claude_skill_name(match.group(1))
                        if not skill_name:
                            continue
                        add_claude_attribution_event(
                            events,
                            seen,
                            category="skill",
                            name=skill_name,
                            session_id=session_id,
                            timestamp=timestamp,
                            source_id=f"skill:{skill_name}:{timestamp.isoformat()}",
                        )
                        plugin_name = skill_name.lstrip("/").split(":", 1)[0] if ":" in skill_name else ""
                        if plugin_name:
                            add_claude_attribution_event(
                                events,
                                seen,
                                category="plugin",
                                name=plugin_name,
                                session_id=session_id,
                                timestamp=timestamp,
                                source_id=f"plugin-command:{plugin_name}:{timestamp.isoformat()}",
                            )

            message = event.get("message")
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue

            for block in content:
                if not isinstance(block, dict) or block.get("type") != "tool_use":
                    continue
                tool_name = normalized_bucket_value(block.get("name"), "")
                if not tool_name:
                    continue
                tool_source_id = block.get("id") or f"tool:{tool_name}:{timestamp.isoformat()}"
                add_claude_attribution_event(
                    events,
                    seen,
                    category="tool",
                    name=normalize_claude_tool_name(tool_name),
                    session_id=session_id,
                    timestamp=timestamp,
                    source_id=tool_source_id,
                )
                tool_extension_name = extension_name_from_tool_name(tool_name)
                if tool_extension_name:
                    add_claude_attribution_event(
                        events,
                        seen,
                        category="plugin",
                        name=tool_extension_name,
                        session_id=session_id,
                        timestamp=timestamp,
                        source_id=f"tool-extension:{tool_source_id}",
                    )
                if tool_name.startswith("mcp__"):
                    parts = tool_name.split("__")
                    server_name = parts[1] if len(parts) > 1 else "unknown"
                    add_claude_attribution_event(
                        events,
                        seen,
                        category="mcp_server",
                        name=server_name,
                        session_id=session_id,
                        timestamp=timestamp,
                        source_id=tool_source_id,
                    )
                elif tool_name in {"Agent", "Task"}:
                    tool_input = block.get("input")
                    if not isinstance(tool_input, dict):
                        tool_input = {}
                    agent_name = normalized_bucket_value(
                        tool_input.get("subagent_type")
                        or tool_input.get("subagentType")
                        or tool_input.get("agent_type")
                        or tool_input.get("agentType")
                        or tool_input.get("agent")
                        or tool_input.get("name"),
                        "unknown-agent",
                    )
                    add_claude_attribution_event(
                        events,
                        seen,
                        category="agent",
                        name=agent_name,
                        session_id=session_id,
                        timestamp=timestamp,
                        source_id=tool_source_id,
                    )

    return events


def parse_claude_attribution_events_cached(file_path) -> list[ClaudeAttributionEvent]:
    try:
        stat = os.stat(file_path)
    except OSError:
        return []

    cache_key = os.fspath(file_path)
    cached = _CLAUDE_ATTRIBUTION_EVENTS_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    if cached is not None and cached[:2] == signature:
        return cached[2]

    path = file_path if isinstance(file_path, Path) else Path(file_path)
    events = parse_claude_attribution_events(path)
    _CLAUDE_ATTRIBUTION_EVENTS_CACHE[cache_key] = (signature[0], signature[1], events)
    _mark_persistent_cache_dirty()
    return events


def apply_claude_session_attribution(
    daily: DailyTotals,
    events: list[ClaudeAttributionEvent],
    session_activity: dict[str, object],
) -> None:
    if not events:
        return

    grouped: Counter[tuple[str, str]] = Counter()
    category_events: Counter[str] = Counter()
    for category, name, _session_id, _timestamp in events:
        grouped[(category, name)] += 1
        category_events[category] += 1

    if not grouped:
        return

    input_tokens = safe_non_negative_int(session_activity.get("input_tokens"))
    output_tokens = safe_non_negative_int(session_activity.get("output_tokens"))
    cached_tokens = safe_non_negative_int(session_activity.get("cached_tokens"))
    total_tokens = safe_non_negative_int(session_activity.get("total_tokens"))
    input_cost_usd = float(session_activity.get("input_cost_usd") or 0.0)
    output_cost_usd = float(session_activity.get("output_cost_usd") or 0.0)
    cached_cost_usd = float(session_activity.get("cached_cost_usd") or 0.0)
    total_cost_usd = float(session_activity.get("total_cost_usd") or 0.0)
    cost_complete = bool(session_activity.get("cost_complete", True))

    for (category, name), event_count in grouped.items():
        category_count = max(category_events[category], 1)
        share = event_count / category_count
        daily.add_attribution(
            category=category,
            name=name,
            sessions=1,
            events=event_count,
            input_tokens=round(input_tokens * share),
            output_tokens=round(output_tokens * share),
            cached_tokens=round(cached_tokens * share),
            total_tokens=round(total_tokens * share),
            input_cost_usd=input_cost_usd * share,
            output_cost_usd=output_cost_usd * share,
            cached_cost_usd=cached_cost_usd * share,
            total_cost_usd=total_cost_usd * share,
            cost_complete=cost_complete,
        )


def collect_observed_claude_records(
    claude_projects_root: Path,
) -> tuple[
    dict[tuple[str, str], ClaudeRequestRecord],
    dict[tuple[dt.date, str], list[ClaudeAttributionEvent]],
]:
    """Merge live and previously observed Claude records into a deletion-safe history."""
    live_paths: list[str] = []
    for file_path in iter_jsonl_files(claude_projects_root):
        live_paths.append(os.fspath(file_path))
        parse_claude_request_records_cached(file_path)
        parse_claude_attribution_events_cached(file_path)

    request_entries = dict(_cache_entries_for_root(_CLAUDE_REQUEST_RECORDS_CACHE, claude_projects_root))
    attribution_entries = dict(
        _cache_entries_for_root(_CLAUDE_ATTRIBUTION_EVENTS_CACHE, claude_projects_root)
    )
    history_paths = _ordered_observed_paths(
        live_paths,
        request_entries.keys() | attribution_entries.keys(),
    )

    request_usage: dict[tuple[str, str], ClaudeRequestRecord] = {}
    attribution_events_by_session_date: dict[tuple[dt.date, str], list[ClaudeAttributionEvent]] = defaultdict(list)

    for file_path in history_paths:
        file_stem = path_stem(file_path)
        attribution_entry = attribution_entries.get(file_path)
        if attribution_entry is not None:
            for category, name, session_id, timestamp in attribution_entry[2]:
                effective_session_id = normalized_bucket_value(session_id, file_stem)
                effective_event = (category, name, effective_session_id, timestamp)
                attribution_events_by_session_date[(timestamp.date(), effective_session_id)].append(
                    effective_event
                )

        request_entry = request_entries.get(file_path)
        if request_entry is None:
            continue
        for record in request_entry[2]:
            (
                session_id,
                request_id,
                local_timestamp,
                input_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                cached_tokens,
                output_tokens,
                model,
            ) = record
            session_id = normalized_bucket_value(session_id, file_stem)
            dedupe_key = (session_id, request_id)
            current = request_usage.get(dedupe_key)
            if current is None:
                request_usage[dedupe_key] = (
                    session_id,
                    request_id,
                    local_timestamp,
                    input_tokens,
                    cache_creation_input_tokens,
                    cache_read_input_tokens,
                    cached_tokens,
                    output_tokens,
                    model,
                )
                continue

            request_usage[dedupe_key] = (
                session_id,
                request_id,
                max(current[2], local_timestamp),
                max(current[3], input_tokens),
                max(current[4], cache_creation_input_tokens),
                max(current[5], cache_read_input_tokens),
                max(current[6], cached_tokens),
                max(current[7], output_tokens),
                model if model != DEFAULT_MODEL else current[8],
            )

    return request_usage, attribution_events_by_session_date


def collect_claude_usage_data(
    claude_projects_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> tuple[dict[dt.date, DailyTotals], dict[tuple[dt.date, int], ActivityTotals]]:
    totals: dict[dt.date, DailyTotals] = {}
    activity_totals: dict[tuple[dt.date, int], ActivityTotals] = {}
    catalog = pricing_catalog or PricingCatalog.from_file(None)
    request_usage, attribution_events_by_session_date = collect_observed_claude_records(
        claude_projects_root
    )

    daily_sessions: dict[dt.date, set[str]] = {}
    bucket_sessions: dict[tuple[dt.date, str, str], set[str]] = {}
    daily_session_usage: dict[tuple[dt.date, str], dict[str, object]] = {}
    request_groups: dict[tuple[dt.date, str, str], list[ClaudeRequestRecord]] = defaultdict(list)
    for request in request_usage.values():
        request_groups[(request[2].date(), request[0], request[8])].append(request)

    for (usage_date, session_id, model), requests in request_groups.items():
        timestamp = min(request[2] for request in requests)
        input_tokens = sum(request[3] for request in requests)
        cache_creation_input_tokens = sum(request[4] for request in requests)
        cache_read_input_tokens = sum(request[5] for request in requests)
        cached_tokens = sum(request[6] for request in requests)
        output_tokens = sum(request[7] for request in requests)
        request_total = input_tokens + cached_tokens + output_tokens

        if catalog.supports_exact_usage_aggregation("claude", model):
            priced = catalog.price_usage(
                "claude",
                model,
                uncached_input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read_input_tokens,
                cache_write_tokens=cache_creation_input_tokens,
            )
            input_cost_usd = priced.input_cost_usd
            output_cost_usd = priced.output_cost_usd
            cached_cost_usd = priced.cached_cost_usd
            total_cost_usd = priced.total_cost_usd
            cost_complete = priced.cost_complete
        else:
            input_cost_usd = 0.0
            output_cost_usd = 0.0
            cached_cost_usd = 0.0
            total_cost_usd = 0.0
            cost_complete = True
            for request in requests:
                priced = catalog.price_usage(
                    "claude",
                    model,
                    uncached_input_tokens=request[3],
                    output_tokens=request[7],
                    cache_read_tokens=request[5],
                    cache_write_tokens=request[4],
                )
                input_cost_usd += priced.input_cost_usd
                output_cost_usd += priced.output_cost_usd
                cached_cost_usd += priced.cached_cost_usd
                total_cost_usd += priced.total_cost_usd
                cost_complete = cost_complete and priced.cost_complete

        daily = totals.setdefault(usage_date, DailyTotals(date=usage_date))
        daily.input_tokens += input_tokens
        daily.output_tokens += output_tokens
        daily.cached_tokens += cached_tokens
        daily.total_tokens += request_total
        daily.input_cost_usd += input_cost_usd
        daily.output_cost_usd += output_cost_usd
        daily.cached_cost_usd += cached_cost_usd
        daily.total_cost_usd += total_cost_usd
        daily.cost_complete = daily.cost_complete and cost_complete

        agent_cli = "claude-code"
        breakdown_key = (agent_cli, model)
        breakdown = daily.breakdowns.get(breakdown_key)
        if breakdown is None:
            breakdown = BreakdownTotals(agent_cli=agent_cli, model=model)
            daily.breakdowns[breakdown_key] = breakdown
        breakdown.input_tokens += input_tokens
        breakdown.output_tokens += output_tokens
        breakdown.cached_tokens += cached_tokens
        breakdown.total_tokens += request_total
        breakdown.input_cost_usd += input_cost_usd
        breakdown.output_cost_usd += output_cost_usd
        breakdown.cached_cost_usd += cached_cost_usd
        breakdown.total_cost_usd += total_cost_usd
        breakdown.cost_complete = breakdown.cost_complete and cost_complete

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
                "input_cost_usd": input_cost_usd,
                "output_cost_usd": output_cost_usd,
                "cached_cost_usd": cached_cost_usd,
                "total_cost_usd": total_cost_usd,
                "cost_complete": cost_complete,
            }
        else:
            session_activity["timestamp"] = min(session_activity["timestamp"], timestamp)
            session_activity["input_tokens"] = safe_non_negative_int(session_activity.get("input_tokens")) + input_tokens
            session_activity["output_tokens"] = safe_non_negative_int(session_activity.get("output_tokens")) + output_tokens
            session_activity["cached_tokens"] = safe_non_negative_int(session_activity.get("cached_tokens")) + cached_tokens
            session_activity["total_tokens"] = safe_non_negative_int(session_activity.get("total_tokens")) + request_total
            session_activity["input_cost_usd"] = float(session_activity.get("input_cost_usd") or 0.0) + input_cost_usd
            session_activity["output_cost_usd"] = float(session_activity.get("output_cost_usd") or 0.0) + output_cost_usd
            session_activity["cached_cost_usd"] = float(session_activity.get("cached_cost_usd") or 0.0) + cached_cost_usd
            session_activity["total_cost_usd"] = float(session_activity.get("total_cost_usd") or 0.0) + total_cost_usd
            session_activity["cost_complete"] = bool(session_activity.get("cost_complete", True)) and cost_complete

    for session_key, session_activity in daily_session_usage.items():
        usage_date, session_id = session_key
        daily = totals.get(usage_date)
        if daily is None:
            continue
        apply_claude_session_attribution(
            daily,
            attribution_events_by_session_date.get((usage_date, session_id), []),
            session_activity,
        )

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    for (usage_date, agent_cli, model), sessions in bucket_sessions.items():
        daily = totals.get(usage_date)
        if daily is not None:
            daily.breakdowns[(agent_cli, model)].sessions = len(sessions)

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

    for daily in totals.values():
        _round_accumulated_costs(daily)
        for breakdown in daily.breakdowns.values():
            _round_accumulated_costs(breakdown)

    return totals, activity_totals


def collect_claude_daily_totals(
    claude_projects_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> dict[dt.date, DailyTotals]:
    totals, _activity_totals = collect_claude_usage_data(claude_projects_root, pricing_catalog=pricing_catalog)
    return totals


def _pi_usage_rows_index(contribution: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    indexed: dict[tuple[str, str], dict[str, object]] = {}
    usage_rows = contribution.get("usage_rows")
    if not isinstance(usage_rows, list):
        usage_rows = []
    normalized_rows: list[dict[str, object]] = []
    for row in usage_rows:
        compact_row = _pi_usage_row_from_object(row)
        if compact_row is None:
            continue
        (
            date_value,
            model,
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            cached_tokens,
            total_tokens,
            native_cost,
            _priced_cost,
        ) = compact_row
        normalized_row: dict[str, object] = {
            "date": date_value,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_read_tokens": cache_read_tokens,
            "cache_write_tokens": cache_write_tokens,
            "cached_tokens": cached_tokens,
            "total_tokens": total_tokens,
            "native_cost": _pi_native_cost_mapping(native_cost),
        }
        normalized_rows.append(normalized_row)
        indexed[(date_value, model)] = normalized_row
    contribution["usage_rows"] = normalized_rows
    return indexed


def _pi_activity_rows_index(contribution: dict[str, object]) -> dict[str, dict[str, object]]:
    indexed: dict[str, dict[str, object]] = {}
    activity_rows = contribution.get("activity_rows")
    if not isinstance(activity_rows, list):
        activity_rows = []
    normalized_rows: list[dict[str, object]] = []
    for row in activity_rows:
        compact_row = _pi_activity_row_from_object(row)
        if compact_row is None:
            continue
        date_value, timestamp = compact_row
        normalized_row = {"date": date_value, "timestamp": timestamp}
        normalized_rows.append(normalized_row)
        indexed[date_value] = normalized_row
    contribution["activity_rows"] = normalized_rows
    return indexed


def apply_pi_event_to_contribution(
    contribution: dict[str, object],
    event: dict[str, object],
    usage_rows: dict[tuple[str, str], dict[str, object]],
    activity_rows: dict[str, dict[str, object]],
) -> None:
    event_type = event.get("type")
    session_id = normalized_bucket_value(contribution.get("session_id"), "unknown-session")
    active_model = normalized_bucket_value(contribution.get("active_model"), DEFAULT_MODEL)

    if event_type == "session":
        contribution["session_id"] = normalized_bucket_value(event.get("id"), session_id)
        return

    if event_type == "model_change":
        contribution["active_model"] = normalized_bucket_value(event.get("modelId"), active_model)
        return

    if event_type != "message":
        return

    local_timestamp = parse_timestamp_local(event.get("timestamp"))
    if local_timestamp is None:
        return

    message = event.get("message") or {}
    if not isinstance(message, dict) or message.get("role") != "assistant":
        return

    usage = message.get("usage") or {}
    if not isinstance(usage, dict):
        return

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


def parse_pi_session_contribution(
    file_path: Path,
    previous_contribution: dict[str, object] | None = None,
    *,
    start_offset: int = 0,
) -> dict[str, object]:
    contribution = (
        clone_pi_contribution(previous_contribution, session_id=file_path.stem)
        if start_offset > 0 and previous_contribution is not None
        else empty_pi_contribution(file_path.stem)
    )
    contribution["session_id"] = normalized_bucket_value(contribution.get("session_id"), file_path.stem)
    contribution["active_model"] = normalized_bucket_value(contribution.get("active_model"), DEFAULT_MODEL)

    usage_rows = _pi_usage_rows_index(contribution)
    activity_rows = _pi_activity_rows_index(contribution)
    processed_offset = max(0, start_offset)

    with file_path.open("rb") as handle:
        if processed_offset:
            handle.seek(processed_offset)
        buffer = b""
        while chunk := handle.read(1024 * 1024):
            buffer += chunk
            lines = buffer.split(b"\n")
            buffer = lines.pop()
            for raw_line in lines:
                line = raw_line.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    apply_pi_event_to_contribution(contribution, event, usage_rows, activity_rows)

        file_size = handle.tell()
        processed_offset = file_size - len(buffer)
        tail_line = buffer.decode("utf-8", errors="ignore").strip()
        if tail_line:
            try:
                event = json.loads(tail_line)
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(event, dict):
                    apply_pi_event_to_contribution(contribution, event, usage_rows, activity_rows)
                    processed_offset = file_size

    contribution["usage_rows"] = [
        row
        for item in sorted(usage_rows.values(), key=lambda value: (str(value["date"]), str(value["model"])))
        if (row := _pi_usage_row_from_object(item)) is not None
    ]
    contribution["activity_rows"] = [
        row
        for item in sorted(activity_rows.values(), key=lambda value: str(value["date"]))
        if (row := _pi_activity_row_from_object(item)) is not None
    ]
    contribution["offset"] = processed_offset
    update_pi_contribution_signatures(file_path, contribution)
    return contribution


def can_resume_pi_contribution(file_path: Path, current_size: int, cached_size: int, contribution: dict[str, object]) -> bool:
    processed_offset = safe_non_negative_int(contribution.get("offset"))
    if current_size <= cached_size or processed_offset <= 0:
        return False
    if processed_offset > cached_size or processed_offset > current_size:
        return False

    head_end = min(processed_offset, _PI_APPEND_FAST_PATH_WINDOW_BYTES)
    if normalized_bucket_value(contribution.get("head_signature"), "") != _read_file_signature_window(file_path, 0, head_end):
        return False

    boundary_start = max(0, processed_offset - _PI_APPEND_FAST_PATH_WINDOW_BYTES)
    return normalized_bucket_value(contribution.get("boundary_signature"), "") == _read_file_signature_window(
        file_path,
        boundary_start,
        processed_offset,
    )


def parse_pi_session_contribution_cached(file_path) -> dict[str, object]:
    stem = path_stem(file_path)
    try:
        stat = os.stat(file_path)
    except OSError:
        return empty_pi_contribution(stem)

    cache_key = os.fspath(file_path)
    cached = _PI_SESSION_RECORDS_CACHE.get(cache_key)
    signature = (stat.st_size, stat.st_mtime_ns)
    if cached is not None and cached[:2] == signature:
        contribution = cached[2]
        return contribution if isinstance(contribution, dict) else empty_pi_contribution(stem)

    path = file_path if isinstance(file_path, Path) else Path(file_path)
    if cached is not None and isinstance(cached[2], dict) and can_resume_pi_contribution(path, stat.st_size, cached[0], cached[2]):
        contribution = parse_pi_session_contribution(path, cached[2], start_offset=safe_non_negative_int(cached[2].get("offset")))
    else:
        contribution = parse_pi_session_contribution(path)

    _PI_SESSION_RECORDS_CACHE[cache_key] = (signature[0], signature[1], contribution)
    _mark_persistent_cache_dirty()
    return contribution


def iter_observed_pi_contributions(sessions_root: Path):
    """Yield every observed log contribution, even after its source file is deleted."""
    live_paths: list[str] = []
    for file_path in iter_jsonl_files(sessions_root):
        live_paths.append(os.fspath(file_path))
        parse_pi_session_contribution_cached(file_path)

    entries = dict(_cache_entries_for_root(_PI_SESSION_RECORDS_CACHE, sessions_root))
    for cache_path in _ordered_observed_paths(live_paths, set(entries)):
        entry = entries[cache_path]
        _size, _mtime_ns, contribution = entry
        if isinstance(contribution, dict):
            yield cache_path, contribution


def collect_pi_usage_data(
    pi_agent_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> tuple[dict[dt.date, DailyTotals], dict[tuple[dt.date, int], ActivityTotals]]:
    totals: dict[dt.date, DailyTotals] = {}
    activity_totals: dict[tuple[dt.date, int], ActivityTotals] = {}
    sessions_root = pi_agent_root / "sessions"
    catalog = pricing_catalog or PricingCatalog.from_file(None)
    daily_sessions: dict[dt.date, set[str]] = {}
    bucket_sessions: dict[tuple[dt.date, str, str], set[str]] = {}

    for file_path, contribution in iter_observed_pi_contributions(sessions_root):
        session_id = normalized_bucket_value(contribution.get("session_id"), path_stem(file_path))
        usage_rows = contribution.get("usage_rows")
        activity_rows = contribution.get("activity_rows")
        if not isinstance(usage_rows, list):
            usage_rows = []
        if not isinstance(activity_rows, list):
            activity_rows = []

        session_activity: dict[dt.date, ActivityTotals] = {}
        for activity_row in activity_rows:
            compact_activity = _pi_activity_row_from_object(activity_row)
            if compact_activity is None:
                continue
            _usage_date_value, timestamp = compact_activity
            usage_date = timestamp.date()
            session_totals = ActivityTotals(date=usage_date, hour=timestamp.hour)
            session_totals.input_cost_usd = session_totals.output_cost_usd = 0
            session_totals.cached_cost_usd = session_totals.total_cost_usd = 0
            session_activity[usage_date] = session_totals

        for usage_row in usage_rows:
            compact_usage = _pi_usage_row_from_object(usage_row)
            if compact_usage is None:
                continue
            (
                usage_date_value,
                model,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                cached_tokens,
                total_tokens,
                native_cost,
                priced_cost,
            ) = compact_usage
            try:
                usage_date = dt.date.fromisoformat(usage_date_value)
            except ValueError:
                continue
            if isinstance(priced_cost, tuple) and len(priced_cost) == 4:
                input_cost, output_cost, cached_cost, total_cost = priced_cost
                cost_complete = True
            else:
                priced = catalog.price_usage(
                    "pi",
                    model,
                    uncached_input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_write_tokens=cache_write_tokens,
                    native_cost=native_cost,
                )
                input_cost = cost_to_nanodollars(priced.input_cost_usd)
                output_cost = cost_to_nanodollars(priced.output_cost_usd)
                cached_cost = cost_to_nanodollars(priced.cached_cost_usd)
                total_cost = cost_to_nanodollars(priced.total_cost_usd)
                cost_complete = priced.cost_complete

            daily = totals.get(usage_date)
            if daily is None:
                daily = DailyTotals(date=usage_date)
                daily.input_cost_usd = daily.output_cost_usd = daily.cached_cost_usd = daily.total_cost_usd = 0
                totals[usage_date] = daily
            daily.input_tokens += input_tokens
            daily.output_tokens += output_tokens
            daily.cached_tokens += cached_tokens
            daily.total_tokens += total_tokens
            daily.input_cost_usd += input_cost
            daily.output_cost_usd += output_cost
            daily.cached_cost_usd += cached_cost
            daily.total_cost_usd += total_cost
            daily.cost_complete = daily.cost_complete and cost_complete

            agent_cli = "pi"
            breakdown_key = (agent_cli, model)
            breakdown = daily.breakdowns.get(breakdown_key)
            if breakdown is None:
                breakdown = BreakdownTotals(agent_cli=agent_cli, model=model)
                breakdown.input_cost_usd = breakdown.output_cost_usd = 0
                breakdown.cached_cost_usd = breakdown.total_cost_usd = 0
                daily.breakdowns[breakdown_key] = breakdown
            breakdown.input_tokens += input_tokens
            breakdown.output_tokens += output_tokens
            breakdown.cached_tokens += cached_tokens
            breakdown.total_tokens += total_tokens
            breakdown.input_cost_usd += input_cost
            breakdown.output_cost_usd += output_cost
            breakdown.cached_cost_usd += cached_cost
            breakdown.total_cost_usd += total_cost
            breakdown.cost_complete = breakdown.cost_complete and cost_complete
            daily_sessions.setdefault(usage_date, set()).add(session_id)
            bucket_sessions.setdefault((usage_date, agent_cli, model), set()).add(session_id)

            activity = session_activity.get(usage_date)
            if activity is None:
                activity = ActivityTotals(date=usage_date, hour=0)
                activity.input_cost_usd = activity.output_cost_usd = 0
                activity.cached_cost_usd = activity.total_cost_usd = 0
                session_activity[usage_date] = activity
            activity.input_tokens += input_tokens
            activity.output_tokens += output_tokens
            activity.cached_tokens += cached_tokens
            activity.total_tokens += total_tokens
            activity.input_cost_usd += input_cost
            activity.output_cost_usd += output_cost
            activity.cached_cost_usd += cached_cost
            activity.total_cost_usd += total_cost
            activity.cost_complete = activity.cost_complete and cost_complete

        for activity in session_activity.values():
            activity_key = (activity.date, activity.hour)
            total_activity = activity_totals.get(activity_key)
            if total_activity is None:
                total_activity = ActivityTotals(date=activity.date, hour=activity.hour)
                total_activity.input_cost_usd = total_activity.output_cost_usd = 0
                total_activity.cached_cost_usd = total_activity.total_cost_usd = 0
                activity_totals[activity_key] = total_activity
            total_activity.sessions += 1
            total_activity.input_tokens += activity.input_tokens
            total_activity.output_tokens += activity.output_tokens
            total_activity.cached_tokens += activity.cached_tokens
            total_activity.total_tokens += activity.total_tokens
            total_activity.input_cost_usd += activity.input_cost_usd
            total_activity.output_cost_usd += activity.output_cost_usd
            total_activity.cached_cost_usd += activity.cached_cost_usd
            total_activity.total_cost_usd += activity.total_cost_usd
            total_activity.cost_complete = total_activity.cost_complete and activity.cost_complete

    for usage_date, sessions in daily_sessions.items():
        if usage_date in totals:
            totals[usage_date].sessions = len(sessions)

    for (usage_date, agent_cli, model), sessions in bucket_sessions.items():
        daily = totals.get(usage_date)
        if daily is not None:
            daily.breakdowns[(agent_cli, model)].sessions = len(sessions)

    for daily in totals.values():
        _materialize_nanodollar_costs(daily)
        for breakdown in daily.breakdowns.values():
            _materialize_nanodollar_costs(breakdown)
    for activity in activity_totals.values():
        _materialize_nanodollar_costs(activity)

    return totals, activity_totals


def collect_pi_daily_totals(
    pi_agent_root: Path,
    pricing_catalog: PricingCatalog | None = None,
) -> dict[dt.date, DailyTotals]:
    totals, _activity_totals = collect_pi_usage_data(pi_agent_root, pricing_catalog=pricing_catalog)
    return totals
