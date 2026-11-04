"""Privacy-safe views of canonical contributions, never a second pricing engine."""
from __future__ import annotations

import hashlib
from copy import deepcopy

from .models import DailyTotals, round_cost

COST_SOURCES = ('native', 'rate_card', 'unpriced')
METRICS = ('input_tokens', 'output_tokens', 'cached_tokens', 'total_tokens',
           'input_cost_usd', 'output_cost_usd', 'cached_cost_usd', 'total_cost_usd')


def opaque_identity(kind: str, value: str) -> str:
    return hashlib.blake2b(f'{kind}:{value}'.encode('utf-8'), digest_size=12).hexdigest()


def empty_cost_sources() -> dict:
    return {source: {'total_tokens': 0, 'total_cost_usd': 0.0} for source in COST_SOURCES}


def merge_cost_sources(target: dict, source: dict) -> None:
    for name in COST_SOURCES:
        values = source.get(name) or {}
        target[name]['total_tokens'] += int(values.get('total_tokens', 0))
        target[name]['total_cost_usd'] = round_cost(target[name]['total_cost_usd'] + float(values.get('total_cost_usd', 0)))


def daily_investigation(daily: DailyTotals) -> tuple[list[dict], dict]:
    """Fill unsupported/legacy buckets explicitly so every token and dollar reconciles."""
    rows = deepcopy(daily.investigation_rows)
    assigned: dict[tuple[str, str], dict] = {}
    for row in rows:
        bucket = assigned.setdefault((row['agent_cli'], row['model']), dict.fromkeys(METRICS, 0))
        for metric in METRICS:
            bucket[metric] += row[metric]
    for key, bucket in daily.breakdowns.items():
        covered = assigned.get(key, {})
        remainder = {metric: round_cost(getattr(bucket, metric) - covered.get(metric, 0)) if metric.endswith('_usd')
                     else getattr(bucket, metric) - covered.get(metric, 0) for metric in METRICS}
        if key not in assigned or any(remainder.values()):
            rows.append({
                'agent_cli': bucket.agent_cli, 'model': bucket.model,
                'project_id': '', 'project_label': 'Unassigned / legacy',
                'session_id': '', 'session_label': 'Unsupported / legacy',
                'cost_source': 'rate_card' if bucket.cost_complete else 'unpriced',
                'cost_complete': bucket.cost_complete, **remainder,
            })
    # Old manually imported daily totals may not contain model buckets at all.
    if not rows and (daily.total_tokens or daily.total_cost_usd):
        rows.append({'agent_cli': 'unknown', 'model': 'unknown',
                     'project_id': '', 'project_label': 'Unassigned / legacy',
                     'session_id': '', 'session_label': 'Unsupported / legacy',
                     'cost_source': 'rate_card' if daily.cost_complete else 'unpriced',
                     'cost_complete': daily.cost_complete,
                     **{metric: getattr(daily, metric) for metric in METRICS}})
    sources = empty_cost_sources()
    for row in rows:
        source = row['cost_source']
        sources[source]['total_tokens'] += row['total_tokens']
        sources[source]['total_cost_usd'] = round_cost(sources[source]['total_cost_usd'] + row['total_cost_usd'])
    return rows, sources
