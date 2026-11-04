import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_core.aggregation import combine_daily_totals, materialize_daily
from dashboard_core.collectors import collect_pi_daily_totals, load_persistent_parse_caches, save_persistent_parse_caches
from dashboard_core.models import DailyTotals
from dashboard_core.tmux_status import build_tmux_status_snapshot


class InvestigationTests(unittest.TestCase):
    def test_pi_identity_and_provenance_survive_deletion_without_exposing_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / 'sessions' / 'session.jsonl'
            log.parent.mkdir()
            events = [
                {'type': 'session', 'id': 'private-session-identity', 'cwd': '/Users/private/secret-project'},
                {'type': 'message', 'timestamp': '2026-11-03T12:00:00Z', 'message': {
                    'role': 'assistant', 'model': 'unknown-private-model', 'usage': {
                        'input': 100, 'output': 10, 'totalTokens': 110, 'cost': {'total': 0.5}}}},
            ]
            log.write_text(''.join(json.dumps(e) + '\n' for e in events))
            history = root / 'history.json'
            load_persistent_parse_caches(history)
            first = materialize_daily(collect_pi_daily_totals(root)).rows[0]
            detail = first['investigation_rows'][0]
            self.assertEqual(detail['total_tokens'], first['total_tokens'])
            self.assertEqual(detail['total_cost_usd'], first['total_cost_usd'])
            self.assertEqual(detail['cost_source'], 'native')
            self.assertTrue(detail['project_id'])
            self.assertTrue(detail['session_id'])
            self.assertNotIn('secret-project', json.dumps(first))
            self.assertNotIn('private-session-identity', json.dumps(first))
            self.assertNotIn('/Users/', json.dumps(first))
            save_persistent_parse_caches(history)
            log.unlink()
            load_persistent_parse_caches(root / 'other.json')
            load_persistent_parse_caches(history)
            retained = materialize_daily(collect_pi_daily_totals(root)).rows[0]
            self.assertEqual(retained, first)

    def test_mixed_native_and_derived_pi_usage_keep_separate_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / 'sessions' / 'mixed.jsonl'
            log.parent.mkdir()
            events = [{'type': 'session', 'id': 'mixed', 'cwd': '/private/project'}]
            for cost in ({'total': 0.5}, None):
                usage = {'input': 100, 'output': 10, 'totalTokens': 110}
                if cost is not None:
                    usage['cost'] = cost
                events.append({'type': 'message', 'timestamp': '2026-11-03T12:00:00Z',
                               'message': {'role': 'assistant', 'model': 'gpt-6-astra', 'usage': usage}})
            log.write_text(''.join(json.dumps(e) + '\n' for e in events))
            row = materialize_daily(collect_pi_daily_totals(root)).rows[0]
            self.assertEqual(row['cost_sources']['native']['total_tokens'], 110)
            self.assertEqual(row['cost_sources']['rate_card']['total_tokens'], 110)
            self.assertAlmostEqual(row['total_cost_usd'], 0.5015)
            unsupported = DailyTotals(date=dt.date(2026, 11, 3), sessions=1)
            unsupported.add_usage(input_tokens=10, total_tokens=10, cost_complete=False)
            unsupported.add_breakdown(agent_cli='codex', model='unknown', input_tokens=10,
                                      total_tokens=10, cost_complete=False)
            combined = materialize_daily(combine_daily_totals(collect_pi_daily_totals(root), {unsupported.date: unsupported})).rows[0]
            self.assertEqual(sum(r['total_tokens'] for r in combined['investigation_rows']), combined['total_tokens'])
            self.assertAlmostEqual(sum(r['total_cost_usd'] for r in combined['investigation_rows']), combined['total_cost_usd'])
            self.assertEqual(combined['cost_sources']['unpriced']['total_tokens'], 10)
            self.assertFalse(combined['cost_complete'])
            snapshot = build_tmux_status_snapshot({
                "generated_at": "2026-11-03T12:00:00+00:00",
                "providers": {"combined": {"rows": [combined]}},
                "pricing": {"version": "fixture"},
            }, range_preset="mtd", now=dt.datetime(2026, 11, 3, 12, tzinfo=dt.timezone.utc))
            self.assertEqual(snapshot["quality"]["cost_sources"], combined["cost_sources"])
            self.assertFalse(snapshot["quality"]["pricing_complete"])
            self.assertTrue(any(r['project_label'] == 'Unassigned / legacy' for r in combined['investigation_rows']))


if __name__ == '__main__':
    unittest.main()
