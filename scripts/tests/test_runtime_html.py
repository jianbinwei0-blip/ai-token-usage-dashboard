import os
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


if __name__ == "__main__":
    unittest.main()
