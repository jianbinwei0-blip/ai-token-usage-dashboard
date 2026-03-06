import os
import re
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.runtime_html import seed_runtime_html


class RuntimeHtmlTests(unittest.TestCase):
    def test_seed_runtime_html_overwrites_newer_runtime_with_source_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_html = root / "dashboard" / "index.html"
            runtime_html = root / "tmp" / "index.runtime.html"

            source_html.parent.mkdir(parents=True, exist_ok=True)
            runtime_html.parent.mkdir(parents=True, exist_ok=True)

            source_html.write_text("new-template-currentWeekEnd=today", encoding="utf-8")
            runtime_html.write_text("old-template-currentWeekEnd=yesterday", encoding="utf-8")

            newer_runtime = time.time() + 10
            source_older = newer_runtime - 5
            os.utime(source_html, (source_older, source_older))
            os.utime(runtime_html, (newer_runtime, newer_runtime))

            seed_runtime_html(source_html, runtime_html)

            self.assertEqual(runtime_html.read_text(encoding="utf-8"), "new-template-currentWeekEnd=today")

    def test_dashboard_groups_refresh_chips_and_uses_untruncated_refresh_copy(self) -> None:
        dashboard_html = Path(__file__).resolve().parents[2] / "dashboard" / "index.html"
        html = dashboard_html.read_text(encoding="utf-8")

        refresh_row_match = re.search(
            r'<div class="meta-row meta-row--refresh">(.*?)</div>',
            html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresh_row_match)
        refresh_row = refresh_row_match.group(1)
        self.assertIn('id="autoRecalcStatus"', refresh_row)
        self.assertIn('id="lastRefreshTime"', refresh_row)
        self.assertNotIn("Auto-recalc", html)
        self.assertNotIn("Auto-refresh", html)
        self.assertIn("Auto refresh: waiting for refresh", html)
        self.assertRegex(
            html,
            r"\.meta-row--refresh \.chip \{\s*flex: 0 0 auto;\s*max-width: none;\s*white-space: nowrap;\s*overflow: visible;\s*text-overflow: clip;",
        )


if __name__ == "__main__":
    unittest.main()
