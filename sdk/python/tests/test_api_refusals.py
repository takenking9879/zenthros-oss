# SPDX-License-Identifier: Apache-2.0

"""The 404s and 413s zenthros-api answers ↔ what the SDK's HTTP client takes them for.

operator/pkg/api/testdata/wire/refusals.json holds each 404 as the API
writes it (pkg/api TestThe404sTheAPIAnswers): a job gone only when the
Status names a zenthrosjob, as Kubernetes and the Gateway say it; any other
404 (a Workspace the hub does not route, a route it does not have) is
another thing not there. Every 404 read as a job gone, a Workspace the hub
does not route was a job collected after its TTL (review 2026-10-29). A
413 (a body over its route's most) is the caller's to fix: a
ValidationError, never asked again. A 429 for a Workspace that keeps its
most jobs has no Retry-After: a TransientError at once, with what to do. A
426 (a hub of another wire) is a WireMismatch that says what to install. A
410 is a log its machine no longer keeps: a LogGone, never asked again. A
409 for a job's machine that is not connected (detached, stopped, between
two connections) is a Conflict at once: the hub answered, about the
machine. A 503 with Retry-After is the hub restarting: waited out by
hub_away_retry, Retry-After first, then a TransientError.
"""

import json
import pathlib
import unittest
from unittest import mock

from zenthros import errors
from zenthros.jobs import http
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI

SAMPLES = pathlib.Path(__file__).resolve().parents[3] / "operator" / "pkg" / "api" / "testdata" / "wire" / "refusals.json"

MEANS = {"job-absent": errors.JobNotFound, "not-found": errors.NotFound, "too-large": errors.ValidationError,
         "workspace-full": errors.TransientError, "other-wire": errors.WireMismatch, "code-absent": errors.CodeAbsent,
         "log-gone": errors.LogGone, "machine-away": errors.Conflict, "hub-away": errors.TransientError}


class APIRefusalsTest(unittest.TestCase):
    def test_each_refusal_is_taken_for_what_it_means(self):
        refusals = json.loads(SAMPLES.read_text())["refusals"]
        self.assertEqual({r["means"] for r in refusals}, set(MEANS))
        api = FakeAPI()
        self.addCleanup(api.close)
        client = HTTPJobClient(api.url, "risk", "t", poll_seconds=0.01)
        waits = []
        for refusal in refusals:
            said = {"Retry-After": refusal["retryAfter"]} if refusal.get("retryAfter") else {}
            api.routes[("GET", "/v1/workspaces/risk/jobs/train")] = (refusal["code"], refusal["body"].encode(), said)
            del waits[:]
            with self.subTest(case=refusal["case"]), self.assertRaises(MEANS[refusal["means"]]) as caught, \
                    mock.patch.object(http, "_sleep", waits.append):
                client.job_status("train")
            if refusal["means"] != "job-absent":
                self.assertNotIsInstance(caught.exception, errors.JobNotFound, refusal["case"])
            if refusal["means"] == "hub-away":
                # Waited out, the first wait the one the hub said, about two
                # minutes in all and never more.
                self.assertEqual(waits[0], float(refusal["retryAfter"]), refusal["case"])
                self.assertLessEqual(sum(waits), sum(http.HUB_AWAY_WAITS))
                self.assertGreater(sum(waits), sum(http.HUB_AWAY_WAITS) - max(http.HUB_AWAY_WAITS))
                self.assertIn("the hub is restarting", str(caught.exception))
                continue
            # None is asked again: none says when.
            self.assertEqual(waits, [], refusal["case"])
            if refusal["means"] == "workspace-full":
                self.assertIn("delete finished ones", str(caught.exception))
            if refusal["means"] == "other-wire":
                self.assertIn("install", str(caught.exception))
            if refusal["means"] == "log-gone":
                self.assertIn("logs=", str(caught.exception))
            if refusal["means"] == "machine-away":
                self.assertIn("machine is not connected", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
