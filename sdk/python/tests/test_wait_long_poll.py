# SPDX-License-Identifier: Apache-2.0

"""wait() on zenthros-api asks to be told of a change (GET .../wait).

The first ask, with seen "", answers at once with the job and its token;
each next one sends that token back, and the API answers when the job's
state is another (or after the wait's seconds, with the same token). A wait
so asks no more than every 25 s while nothing changes and knows of an end in
milliseconds; it sleeps poll_seconds only after an ask that failed, and asks
again with the token it had, so a cut misses nothing. Near its deadline it
asks for no more than is left.
"""

import unittest
import urllib.parse
from unittest import mock

from zenthros.jobs import client as jobs_client
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI

WAIT = "/v1/workspaces/risk/jobs/j/wait"


def _job(phase):
    return {"metadata": {"name": "j", "generation": 1}, "status": {"phase": phase, "message": f"job is {phase}"}}


class LongPollTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "t", poll_seconds=7)
        self.addCleanup(self.client.close)
        self.slept = []
        patch = mock.patch.object(jobs_client.time, "sleep", self.slept.append)
        patch.start()
        self.addCleanup(patch.stop)

    def answers(self, *answers):
        queue = list(answers)
        self.api.routes[("GET", WAIT)] = lambda _body: queue.pop(0) if len(queue) > 1 else queue[0]

    def asked(self):
        return [urllib.parse.parse_qs(urllib.parse.urlsplit(r["path"]).query, keep_blank_values=True)
                for r in self.api.requests if r["path"].startswith(WAIT)]

    def test_each_ask_sends_back_the_token_it_was_given(self):
        self.answers((200, {"job": _job("Running"), "seen": "a"}),
                     (200, {"job": _job("Running"), "seen": "a"}),
                     (200, {"job": _job("Succeeded"), "seen": "b"}))
        self.assertEqual(self.client.wait("j"), "Succeeded")
        self.assertEqual([q["seen"] for q in self.asked()], [[""], ["a"], ["a"]])
        self.assertTrue(all(q["timeoutSeconds"] == ["25"] for q in self.asked()), self.asked())
        self.assertEqual(self.slept, [], "a wait the API holds needs no sleep of its own")

    def test_a_cut_asks_again_with_the_same_token(self):
        self.answers((200, {"job": _job("Running"), "seen": "a"}),
                     (503, {"kind": "Status", "message": "rolling out", "code": 503}),
                     (200, {"job": _job("Failed"), "seen": "c"}))
        with self.assertRaises(Exception) as caught:
            self.client.wait("j")
        self.assertEqual(type(caught.exception).__name__, "JobFailed")
        self.assertEqual([q["seen"] for q in self.asked()], [[""], ["a"], ["a"]])
        self.assertEqual(self.slept, [7], "only the failed ask waits poll_seconds")

    def test_near_its_deadline_it_asks_for_what_is_left(self):
        self.answers((200, {"job": _job("Running"), "seen": "a"}))
        with self.assertRaises(Exception) as caught:
            self.client.wait("j", timeout=3)
        self.assertEqual(type(caught.exception).__name__, "WaitTimeout")
        self.assertTrue(all(int(q["timeoutSeconds"][0]) <= 3 for q in self.asked()), self.asked())


if __name__ == "__main__":
    unittest.main()
