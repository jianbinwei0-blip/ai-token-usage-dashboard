import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_core.render import build_breakdown_table_body, build_usage_dataset_script, inject_usage_dataset


class RenderSafetyTests(unittest.TestCase):
    def test_log_labels_are_text_and_script_json_round_trips(self):
        hostile = '</script><img src=x onerror="alert(1)">&\\name'
        row = dict(agent_cli=hostile, model=hostile, sessions=1, input_tokens=1,
                   output_tokens=0, cached_tokens=0, total_tokens=1,
                   total_cost_usd=0, cost_complete=False)
        body = build_breakdown_table_body([row])
        self.assertNotIn('<img', body)
        self.assertIn('&lt;/script&gt;', body)
        dataset = {'label': hostile}
        script = build_usage_dataset_script(dataset)
        self.assertEqual(script.count('</script>'), 1)
        self.assertEqual(json.loads(script.partition('>')[2].removesuffix('</script>')), dataset)
        self.assertEqual(inject_usage_dataset('<script id="usageDataset" type="application/json">{}</script>', dataset), script)


if __name__ == '__main__':
    unittest.main()
