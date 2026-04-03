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

    def test_dashboard_groups_refresh_chips_and_exposes_breakdown_headers(self) -> None:
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
        self.assertIn('<div class="label">Active Days</div>', html)
        self.assertIn('<div class="label">Peak Day</div>', html)
        self.assertGreaterEqual(html.count('<div class="label">Monthly</div>'), 2)
        self.assertGreaterEqual(html.count('<div class="label">Weekly</div>'), 2)
        self.assertGreaterEqual(html.count('<div class="label">Daily</div>'), 2)
        self.assertGreaterEqual(html.count('<div class="label">Input</div>'), 2)
        self.assertGreaterEqual(html.count('<div class="label">Output</div>'), 2)
        self.assertGreaterEqual(html.count('<div class="label">Cached</div>'), 2)
        self.assertNotIn("Monthly Avg Cost", html)
        self.assertNotIn("Weekly Avg Cost", html)
        self.assertNotIn("Daily Avg Cost", html)
        self.assertIn("Agent CLI + Model Breakdown", html)
        self.assertIn("Activity Rhythm", html)
        self.assertIn('id="activityHeatmap"', html)
        self.assertIn('id="activitySummaryTableBody"', html)
        self.assertIn('id="activityRhythmSummary"', html)
        self.assertIn('id="dailyUsageTableBody"', html)
        self.assertIn('id="usageBreakdownTableBody"', html)
        self.assertIn('>Input</th>', html)
        self.assertIn('>Cached</th>', html)
        self.assertIn('>Total Cost</th>', html)
        self.assertIn('const pricingMetadata = usageDataset?.pricing || {};', html)
        self.assertIn('const timeOfDayBuckets = [', html)
        self.assertIn('const renderActivityRhythm = (rows, providerLabel, rangeLabelText) => {', html)
        self.assertIn('id="fixedStats"', html)
        self.assertIn('id="rangeStats"', html)
        self.assertLess(html.index('id="fixedStats"'), html.index('class="panel range-panel"'))
        self.assertLess(html.index('class="panel range-panel"'), html.index('id="rangeStats"'))
        self.assertIn('const fixedStatCards = Array.from(document.querySelectorAll("#fixedStats .stat"));', html)
        self.assertIn('const rangeStatCards = Array.from(document.querySelectorAll("#rangeStats .stat"));', html)
        self.assertIn('value="input-desc">Input Tokens (High to Low)</option>', html)
        self.assertIn('value="output-desc">Output Tokens (High to Low)</option>', html)
        self.assertIn('value="cached-desc">Cached Tokens (High to Low)</option>', html)
        self.assertIn('value="total-cost-desc">Total Cost (High to Low)</option>', html)
        self.assertIn('value="input-cost-desc">Input Cost (High to Low)</option>', html)
        self.assertIn('value="output-cost-desc">Output Cost (High to Low)</option>', html)
        self.assertIn('value="cached-cost-desc">Cached Cost (High to Low)</option>', html)
        self.assertNotIn("Xbox 360 calm, Her-era warmth, and liquid-glass instrumentation for Codex, Claude, and PI.", html)
        self.assertIn('usage-chart-control-group--action', html)
        self.assertIn('<div class="label">Today</div>', html)
        self.assertNotIn("Total Sessions", html)
        self.assertNotIn("Highest Single Day", html)
        self.assertNotIn("Days With Usage", html)
        self.assertNotIn("YTD Input Tokens", html)


if __name__ == "__main__":
    unittest.main()
