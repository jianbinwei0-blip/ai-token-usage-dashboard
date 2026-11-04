import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_core.storage import accounting_lock, atomic_write_text


class StorageTests(unittest.TestCase):
    def test_failed_atomic_promotion_preserves_previous_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'snapshot.json'
            atomic_write_text(output, '{"old":true}')
            with patch.object(Path, 'replace', side_effect=OSError('promotion failed')):
                with self.assertRaises(OSError):
                    atomic_write_text(output, '{"new":true}')
            self.assertEqual(json.loads(output.read_text()), {'old': True})
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_shared_refresh_lock_prevents_lost_updates_and_partial_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'state.json'
            atomic_write_text(output, '{"value":0}')
            failures = []
            done = threading.Event()
            def writer():
                try:
                    for _ in range(15):
                        with accounting_lock(output):
                            current = json.loads(output.read_text())
                            current['value'] += 1
                            atomic_write_text(output, json.dumps(current))
                except Exception as exc:
                    failures.append(exc)
            def reader():
                try:
                    while not done.is_set():
                        self.assertIsInstance(json.loads(output.read_text())['value'], int)
                except Exception as exc:
                    failures.append(exc)
            monitor = threading.Thread(target=reader)
            writers = [threading.Thread(target=writer) for _ in range(3)]
            monitor.start()
            for thread in writers:
                thread.start()
            for thread in writers:
                thread.join()
            done.set()
            monitor.join()
            self.assertEqual(failures, [])
            self.assertEqual(json.loads(output.read_text())['value'], 45)


if __name__ == '__main__':
    unittest.main()
