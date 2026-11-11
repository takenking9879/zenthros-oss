# SPDX-License-Identifier: Apache-2.0

"""The job list and the logs come in gzip, and nothing else is asked so.

zenthros-api compresses GET /jobs and the logs when the request accepts it
(pkg/api gzip.go): the SDK asks only there, and reads both the whole answer
and a followed log line by line, each piece as it comes. Any other call
(a job, a secret) does not ask, so an answer with a secret is never
compressed (BREACH reads it from its size).
"""

import gzip
import json
import threading
import time
import unittest
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI


def _gz(raw):
    return gzip.compress(raw)


class GzipTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "t")
        self.addCleanup(self.client.close)

    def test_the_list_and_the_logs_come_compressed(self):
        listed = {"items": [{"metadata": {"name": f"j{i}"}} for i in range(300)], "metadata": {}}
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (
            200, _gz(json.dumps(listed).encode()), {"Content-Encoding": "gzip"})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (
            200, _gz(b"line 1\nline 2\n"), {"Content-Encoding": "gzip"})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = (200, {"metadata": {"name": "j"}, "status": {}})
        self.assertEqual(len(self.client.list_jobs()), 300)
        self.assertEqual(self.client.logs("j"), "line 1\nline 2\n")
        self.client.job_status("j")
        asked = {r["path"].split("?")[0]: r["headers"].get("Accept-Encoding") for r in self.api.requests}
        # http.client says identity on its own: no compression.
        self.assertEqual(asked, {"/v1/workspaces/risk/jobs": "gzip", "/v1/workspaces/risk/jobs/j/logs": "gzip",
                                 "/v1/workspaces/risk/jobs/j": "identity"})

    def test_a_followed_log_in_gzip_reads_each_line_as_it_comes(self):
        second = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                zip_ = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)

                def piece(raw):
                    data = zip_.compress(raw) + zip_.flush(zlib.Z_SYNC_FLUSH)
                    self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
                    self.wfile.flush()

                piece(b"line 1\n")
                second.wait(5)
                piece(b"line 2\npartial")
                end = zip_.flush()
                self.wfile.write(f"{len(end):x}\r\n".encode() + end + b"\r\n0\r\n\r\n")

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        client = HTTPJobClient(f"http://127.0.0.1:{server.server_address[1]}", "risk", "t")
        stream = client.follow_logs("j")
        started = time.monotonic()
        self.assertEqual(next(stream.stdout), "line 1\n")
        self.assertLess(time.monotonic() - started, 4, "the first line waited for the second")
        second.set()
        self.assertEqual(list(stream.stdout), ["line 2\n", "partial"])


if __name__ == "__main__":
    unittest.main()
