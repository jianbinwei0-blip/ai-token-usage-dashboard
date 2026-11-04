import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_core.collectors import load_persistent_parse_caches
from dashboard_core.config import DashboardConfig
from dashboard_core.pipeline import recalc_dashboard
from dashboard_core.history import HistoryError, export_history, prepare_history, read_history, restore_history


class HistoryTests(unittest.TestCase):
    def test_corrupt_history_fails_closed_without_overwriting_original(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'history.json'
            path.write_text('{broken')
            with self.assertRaises(HistoryError):
                load_persistent_parse_caches(path)
            self.assertEqual(path.read_text(), '{broken')

    def test_migration_export_restore_preserve_deleted_log_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy, durable, export, restored = [root / name for name in ('legacy.json', 'history.json', 'export.json', 'restored.json')]
            payload = {'version': 5, 'pi': {'/deleted/session.jsonl': {'size': 0, 'mtime_ns': 0, 'contribution': {
                'session_id': 'deleted', 'active_model': 'unknown', 'usage_rows': [['2026-11-03', 'unknown', 100, 10, 0, 0, 0, 110, None, None]],
                'activity_rows': [['2026-11-03', '2026-11-03T12:00:00+00:00']]}}}}
            original = json.dumps(payload)
            legacy.write_text(original)
            status = prepare_history(durable, legacy)
            self.assertEqual(status['state'], 'migrated')
            self.assertEqual(legacy.read_text(), original)
            self.assertEqual(read_history(durable)['pi'], payload['pi'])
            self.assertEqual(len(list(root.glob('legacy.json.backup-*'))), 1)
            export_history(durable, export)
            restore_history(export, restored)
            self.assertEqual(read_history(restored), read_history(durable))
            with self.assertRaises(HistoryError):
                restore_history(export, restored)
            backup = restore_history(export, restored, replace=True)
            self.assertTrue(backup.is_file())
            self.assertEqual(read_history(backup), read_history(durable))
            with self.assertRaises(HistoryError):
                export_history(durable, export)

    def test_incompatible_history_and_malformed_restore_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / 'bad.json', Path(tmp) / 'target.json'
            invalid_pi_rows = [None, ['bad-date', 'x', 100, 10, 0, 0, 0, 110, None, None],
                               ['2026-11-03', 'x', '100', 10, 0, 0, 0, 110, None, None]]
            payloads = [{'version': 999, 'pi': {}}, {'version': 5.0, 'pi': {}}]
            payloads.extend({'version': 5, 'pi': {'x': {'contribution': {'usage_rows': [row], 'activity_rows': []}}}} for row in invalid_pi_rows)
            for payload in payloads:
                source.write_text(json.dumps(payload))
                with self.assertRaises(HistoryError):
                    restore_history(source, target)
                self.assertFalse(target.exists())

    def test_pipeline_migration_retains_unassigned_usage_and_corruption_preserves_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            html = root / 'runtime.html'
            html.write_text((Path(__file__).resolve().parents[2] / 'dashboard/index.html').read_text())
            legacy, history = root / 'legacy.json', root / 'data/history.json'
            key = str(root / 'pi/sessions/deleted.jsonl')
            legacy.write_text(json.dumps({'version': 4, 'pi': {key: {'size': 0, 'mtime_ns': 0, 'contribution': {
                'session_id': 'deleted', 'usage_rows': [['2026-11-03', 'unknown', 100, 10, 0, 0, 0, 110, None, None]],
                'activity_rows': [['2026-11-03', '2026-11-03T12:00:00+00:00']]}}}}))
            config = DashboardConfig('127.0.0.1', 9321, html, root / 'codex', root / 'claude', root / 'pi', root / 'dsh',
                                     parse_cache_file=history, legacy_parse_cache_file=legacy)
            now = dt.datetime(2026, 11, 3, 12, tzinfo=dt.timezone.utc)
            first = recalc_dashboard(config, now, include_dataset=True)
            pi = first['dataset']['providers']['pi']['rows'][0]
            self.assertEqual(pi['total_tokens'], 110)
            self.assertEqual(pi['investigation_rows'][0]['project_label'], 'Unassigned / legacy')
            self.assertEqual(pi['cost_sources']['unpriced']['total_tokens'], 110)
            self.assertFalse(pi['cost_complete'])
            self.assertTrue(first['providers_available']['pi'])
            self.assertTrue(legacy.exists())
            self.assertEqual(recalc_dashboard(config, now, include_dataset=True), first)
            previous_html = html.read_text()
            history.write_text('{broken-after-load')
            with self.assertRaises(HistoryError):
                recalc_dashboard(config, now)
            self.assertEqual(history.read_text(), '{broken-after-load')
            self.assertEqual(html.read_text(), previous_html)


if __name__ == '__main__':
    unittest.main()
