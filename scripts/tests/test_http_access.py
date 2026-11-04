import http.client
import sys
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ai_usage_recalc_server as service


class LocalAccessTests(unittest.TestCase):
    def test_cross_origin_and_rebound_hosts_cannot_trigger_collection(self):
        with ThreadingHTTPServer(('127.0.0.1', 0), service.Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            port = server.server_address[1]
            def request(headers):
                connection = http.client.HTTPConnection('127.0.0.1', port, timeout=3)
                connection.request('GET', '/recalc', headers=headers)
                response = connection.getresponse()
                body = response.read()
                result = response.status, dict(response.getheaders()), body
                connection.close()
                return result
            try:
                with patch.object(service, 'append_recalc_log'), patch.object(service, 'recalc_dashboard', return_value={'ok': True}) as collect:
                    denied = [
                        {'Host': f'attacker.invalid:{port}'},
                        {'Origin': 'https://attacker.invalid'},
                        {'Origin': 'null'},
                        {'Sec-Fetch-Site': 'cross-site'},
                        {'Host': f'127.0.0.1:{port + 1}'},
                    ]
                    for headers in denied:
                        status, _, _ = request(headers)
                        self.assertEqual(status, 403, headers)
                    collect.assert_not_called()
                    status, headers, _ = request({'Origin': f'http://127.0.0.1:{port}'})
                    self.assertEqual(status, 200)
                    self.assertNotIn('Access-Control-Allow-Origin', headers)
                    collect.assert_called_once()
            finally:
                server.shutdown()
                thread.join()


if __name__ == '__main__':
    unittest.main()
