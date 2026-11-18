# SPDX-License-Identifier: Apache-2.0

"""The job list comes in gzip, and nothing else is asked so.

zenthros-api compresses GET /jobs when the request accepts it (pkg/api
gzip.go): the SDK asks only there. A log's records come packed by its agent,
a zstd frame each (logrecords.unpack), so a read of a log, whole or followed,
asks for no gzip. Any other call (a job, a secret) does not ask, so an answer
with a secret is never compressed (BREACH reads it from its size).
"""

import gzip
import json
import unittest

from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI, record


class GzipTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "t")
        self.addCleanup(self.client.close)

    def test_only_the_list_comes_compressed(self):
        listed = {"items": [{"metadata": {"name": f"j{i}"}} for i in range(300)], "metadata": {}}
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (
            200, gzip.compress(json.dumps(listed).encode()), {"Content-Encoding": "gzip"})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (200, record(b"line 1\nline 2\n", cursor="14"))
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = (200, {"metadata": {"name": "j"}, "status": {}})
        self.assertEqual(len(self.client.list_jobs()), 300)
        self.assertEqual(self.client.logs("j"), "line 1\nline 2\n")
        stream = self.client.follow_logs("j")
        self.assertEqual(list(stream.stdout), ["line 1\n", "line 2\n"])
        self.client.job_status("j")
        asked = [(r["path"].split("?")[0], r["headers"].get("Accept-Encoding")) for r in self.api.requests]
        # http.client says identity on its own: no compression.
        self.assertEqual(asked, [("/v1/workspaces/risk/jobs", "gzip"), ("/v1/workspaces/risk/jobs/j/logs", "identity"),
                                 ("/v1/workspaces/risk/jobs/j/logs", "identity"), ("/v1/workspaces/risk/jobs/j", "identity")])


if __name__ == "__main__":
    unittest.main()
