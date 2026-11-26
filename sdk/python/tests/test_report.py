#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

import json
import os
import unittest
from unittest import mock

from zenthros import errors
from zenthros.core import agent as agent_mod
from zenthros.core.report import MAX_PAYLOAD_BYTES, Reporter, progress, report

from support import no_job

IN_JOB = {"ZENTHROS_REPORT_ENDPOINT": "http://agent:9445", "ZENTHROS_JOB_ID": "job", "ZENTHROS_REPORT_TOKEN": "t"}


class Sent:
    """The agent's socket: records every POST."""

    def __init__(self, answers=None):
        self.calls = []
        self.answers = list(answers or [])

    def __call__(self, path, body, headers, timeout):
        self.calls.append((path, json.loads(body)))
        if self.answers:
            answer = self.answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return 202, json.dumps({"report_id": "r", "sequence": len(self.calls)}).encode()


class ReportSDKTest(unittest.TestCase):
    def test_publish_sends_complete_snapshot(self):
        sent = Sent()
        with mock.patch.dict(os.environ, IN_JOB), mock.patch.object(agent_mod.AgentClient, "_send", sent):
            r = Reporter(producer_id="worker-0")
            r.set("epoch", 1)
            r.set("accuracy", 0.9)
            receipt = r.publish()
        self.assertEqual(receipt["sequence"], 1)
        path, snapshot = sent.calls[0]
        self.assertEqual(path, "/v1/reports")
        self.assertEqual(snapshot["payload"], {"epoch": 1, "accuracy": 0.9})
        self.assertEqual(snapshot["kind"], "ExecutionReport")
        self.assertEqual(snapshot["producer_id"], "worker-0")
        self.assertEqual(snapshot["job_id"], "job")

    def test_oneshot_report(self):
        sent = Sent()
        with mock.patch.dict(os.environ, IN_JOB), mock.patch.object(agent_mod.AgentClient, "_send", sent):
            report({"ok": True})
        self.assertEqual(sent.calls[0][1]["payload"], {"ok": True})

    def test_payload_too_large(self):
        with self.assertRaises(errors.PayloadTooLarge):
            report({"k": "x" * MAX_PAYLOAD_BYTES})

    def test_non_object_rejected(self):
        with self.assertRaises(errors.InvalidSnapshot):
            report([1, 2, 3])

    def test_strict_outside_a_job_raises_progress_drops(self):
        with no_job():
            with self.assertRaises(errors.ReportError):
                report({"n": 1})
            self.assertIsNone(progress({"n": 1}))

    def test_retries_transient_ingest_errors(self):
        sent = Sent([ConnectionRefusedError("Connection refused")] * 2)
        with mock.patch.dict(os.environ, IN_JOB), mock.patch.object(agent_mod.AgentClient, "_send", sent), \
                mock.patch("zenthros.core.agent.time.sleep"):
            data = report({"n": 1})
        self.assertEqual(len(sent.calls), 3)
        self.assertEqual(data["sequence"], 3)


if __name__ == "__main__":
    unittest.main()
