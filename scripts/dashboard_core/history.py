"""Versioned durable observed history, with fail-closed validation and safe transfer."""
from __future__ import annotations

import copy
import datetime as dt
import json
import math
from pathlib import Path

from .storage import accounting_lock, atomic_write_text

HISTORY_SCHEMA = 1
PARSE_VERSIONS = {4, 5}
PROVIDER_SECTIONS = ('codex', 'claude', 'claude_attribution', 'pi', 'dsh')


class HistoryError(ValueError):
    pass


def validate_history(payload: object) -> dict:
    """Reject malformed records rather than accepting a deceptively partial restore."""
    from . import collectors as c

    def invalid():
        raise HistoryError('History is corrupt or incompatible. Keep the original file; validate a backup with scripts/usage_history.py before restoring it.')

    if (not isinstance(payload, dict) or type(payload.get('version')) is not int
            or payload.get('version') not in PARSE_VERSIONS
            or type(payload.get('history_schema', 1)) is not int
            or payload.get('history_schema', 1) != HISTORY_SCHEMA):
        invalid()
    if not any(section in payload for section in PROVIDER_SECTIONS):
        invalid()

    def finite(value):
        if isinstance(value, float) and (not math.isfinite(value) or value < 0):
            invalid()
        if isinstance(value, int) and not isinstance(value, bool) and value < 0:
            invalid()
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {'input_tokens', 'output_tokens', 'cached_tokens', 'total_tokens', 'cache_read_tokens', 'cache_write_tokens', 'cache_creation_input_tokens', 'cached_input_tokens'} and (type(item) is not int or item < 0):
                    invalid()
                finite(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                finite(item)
    finite(payload)
    try:
        for section in PROVIDER_SECTIONS:
            entries = payload.get(section, {})
            if not isinstance(entries, dict):
                invalid()
            for key, entry in entries.items():
                if not isinstance(key, str) or not isinstance(entry, (dict, list)):
                    invalid()
                if section == 'codex':
                    if isinstance(entry, list) and len(entry) != 4:
                        invalid()
                    record = entry.get('contribution') if isinstance(entry, dict) else entry[3]
                    parsed = c._deserialize_codex_contribution(record)
                    if record is not None and parsed is None:
                        invalid()
                    if parsed is not None:
                        rows = parsed[1] if len(parsed) == 2 and parsed[0] == 'codex-events-v1' else [parsed]
                        for row in rows:
                            if type(row[2]) is not int or not 0 <= row[2] <= 23 or any(type(value) is not int or value < 0 for value in row[6:14]):
                                invalid()
                elif section in {'claude', 'claude_attribution'}:
                    if isinstance(entry, list) and len(entry) != 3:
                        invalid()
                    records = entry.get('records' if section == 'claude' else 'events') if isinstance(entry, dict) else entry[2]
                    parse = c._deserialize_claude_request_record if section == 'claude' else c._deserialize_claude_attribution_event
                    if not isinstance(records, list) or any(parse(row) is None for row in records):
                        invalid()
                    if section == 'claude' and any(type(value) is not int or value < 0 for row in records for value in parse(row)[3:8]):
                        invalid()
                else:
                    if not isinstance(entry, dict):
                        invalid()
                    record = copy.deepcopy(entry.get('contribution'))
                    parse = c.deserialize_pi_contribution if section == 'pi' else c.deserialize_dsh_contribution
                    usage_count = len(record.get('usage_rows', [])) if isinstance(record, dict) else -1
                    activity_count = len(record.get('activity_rows', [])) if isinstance(record, dict) else -1
                    if isinstance(record, dict):
                        for row in record.get('usage_rows', []):
                            if isinstance(row, (list, tuple)):
                                metrics = row[2:8] if section == 'pi' else row[4:9]
                                if section == 'pi' and len(row) in {9, 10} and row[8] is not None:
                                    native = row[8]
                                    amounts = native.values() if isinstance(native, dict) else native if isinstance(native, (list, tuple)) else [native]
                                    if any(value is not None and type(value) not in {int, float} for value in amounts):
                                        invalid()
                                if section == 'dsh' and (type(row[1]) is not int or not 0 <= row[1] <= 23):
                                    invalid()
                            elif isinstance(row, dict):
                                names = ('input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'cached_tokens', 'total_tokens')
                                metrics = [row[name] for name in names if name in row]
                            else:
                                invalid()
                            if any(type(value) is not int or value < 0 for value in metrics):
                                invalid()
                    restored = parse(record)
                    if restored is not None and section == 'pi':
                        for row in restored['usage_rows']:
                            dt.date.fromisoformat(row[0])
                            if any(type(value) is not int or value < 0 for value in row[2:8]):
                                invalid()
                        for row in restored['activity_rows']:
                            dt.date.fromisoformat(row[0])
                    if restored is None or len(restored['usage_rows']) != usage_count:
                        invalid()
                    if section == 'pi' and len(restored['activity_rows']) != activity_count:
                        invalid()
    except (TypeError, KeyError, IndexError, OverflowError, AttributeError, ValueError) as exc:
        if isinstance(exc, HistoryError):
            raise
        invalid()
    return payload


def read_history(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HistoryError('History cannot be read. Keep the original file and restore a validated backup with scripts/usage_history.py; collection was not saved.') from exc
    return validate_history(payload)


def backup_path(path: Path) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    return path.with_name(f'{path.name}.backup-{stamp}.json')


def prepare_history(path: Path | None, legacy_path: Path | None = None) -> dict:
    status = {'schema': HISTORY_SCHEMA, 'state': 'loaded', 'warnings': []}
    if path is None:
        status.update(state='disabled', warnings=['Durable history is disabled; deleted source logs will not survive this process.'])
        return status
    if not path.exists() and legacy_path is not None and legacy_path.exists() and legacy_path.resolve() != path.resolve():
        payload = copy.deepcopy(read_history(legacy_path))
        backup = backup_path(legacy_path)
        atomic_write_text(backup, json.dumps(payload, separators=(',', ':'), allow_nan=False))
        payload['history_schema'] = HISTORY_SCHEMA
        atomic_write_text(path, json.dumps(payload, separators=(',', ':'), allow_nan=False))
        status['state'] = 'migrated'
        status['warnings'].append('Retained history was copied from the legacy parse cache. The original and a validated backup were kept.')
    elif not path.exists():
        status.update(state='new', warnings=['Durable history is missing or new. Only currently readable logs can be recovered; restore a backup if older usage is expected.'])
    return status


def export_history(source: Path, destination: Path) -> None:
    with accounting_lock(source):
        payload = read_history(source)
        if destination.exists():
            raise HistoryError('Export destination already exists; choose a new filename.')
        atomic_write_text(destination, json.dumps(payload, separators=(',', ':'), allow_nan=False))
        read_history(destination)


def restore_history(source: Path, destination: Path, *, replace: bool = False) -> Path | None:
    payload = read_history(source)
    with accounting_lock(destination):
        backup = None
        if destination.exists():
            if not replace:
                raise HistoryError('History already exists. Stop collectors and use --replace to create a backup before replacement.')
            # Preserve even an unreadable old file for diagnosis/recovery.
            backup = backup_path(destination)
            atomic_write_text(backup, destination.read_text(encoding='utf-8'))
        payload = copy.deepcopy(payload)
        payload['history_schema'] = HISTORY_SCHEMA
        atomic_write_text(destination, json.dumps(payload, separators=(',', ':'), allow_nan=False))
        read_history(destination)
        return backup
