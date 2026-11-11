# SPDX-License-Identifier: Apache-2.0

"""The SDK sends the bytes operator/pkg/jobcli/testdata/wire.json pins.

`zenthros-agent job ...` (Go, for any language) is checked against the same
fixture by TestAgentJobCLICheckpointMatchesSDKWire, so a Python trainer and
a shell job speak one protocol to the agent.
"""

import json
import os
import unittest
from pathlib import Path
from unittest import mock

from zenthros.core import agent as client
from zenthros.core.agent import AgentClient
from zenthros.core.checkpoint import Checkpointer, write_blob
from zenthros.core.report import new_snapshot
from zenthros.core.storage import MemoryBlobStore

from support import AckAgent

FIXTURE = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "jobcli" / "testdata" / "wire.json"


def load():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


class JobProtocolTest(unittest.TestCase):
    def test_checkpoint_commit_matches_fixture(self):
        fx = load()
        sent = []
        store = MemoryBlobStore()
        # The cursor in env is gen 2's seq 1: the job resumes from it and
        # commits seq 2 under its own generation.
        cursor = json.loads(fx["env"]["ZENTHROS_CHECKPOINT_CURSORS"])["train"]
        write_blob(cursor["uri"], {"step": 1}, store=store)
        with mock.patch.dict(os.environ, fx["env"], clear=False):
            ckpt = Checkpointer(fx["checkpoint"]["phase"], store=store,
                                agent=AckAgent(lambda c: sent.append(c) or {"accepted": True}))
            self.assertEqual((ckpt.seq, ckpt.state), (1, {"step": 1}))
            ckpt.save(fx["checkpoint"]["state"])
        self.assertEqual(sent, [fx["checkpoint"]["body"]])
        # The sha256 is of the bytes stored, which the fixture carries as the
        # file the Go CLI hashes.
        self.assertEqual(store.data[fx["checkpoint"]["body"]["uri"]].decode("utf-8"), fx["checkpoint"]["blob"])

    def test_checkpoint_wire_post(self):
        fx = load()
        calls = []

        def fake_unix_post(socket_path, http_path, body, headers, timeout):
            calls.append((http_path, headers, json.loads(body)))
            return 202, b'{"accepted":true}'

        with mock.patch.dict(os.environ, {**fx["env"], "ZENTHROS_REPORT_ENDPOINT": "unix:///run/r.sock"}), \
                mock.patch.object(client, "_unix_post", fake_unix_post):
            AgentClient().ack_checkpoint(fx["checkpoint"]["body"])
        path, headers, body = calls[0]
        self.assertEqual(path, fx["checkpoint"]["path"])
        self.assertEqual(headers["Authorization"], fx["authorization"])
        self.assertEqual(body, fx["checkpoint"]["body"])

    def test_report_matches_fixture(self):
        fx = load()
        rep = fx["report"]
        calls = []

        def fake_unix_post(socket_path, http_path, body, headers, timeout):
            calls.append((http_path, headers, json.loads(body)))
            return 202, b"{}"

        with mock.patch.dict(os.environ, {**fx["env"], "ZENTHROS_REPORT_ENDPOINT": "unix:///run/r.sock"}), \
                mock.patch.object(client, "_unix_post", fake_unix_post):
            snapshot = new_snapshot(rep["payload"], final=rep["final"], report_id=rep["report_id"])
            snapshot["produced_at"] = rep["produced_at"]
            AgentClient().report(snapshot)
        path, headers, body = calls[0]
        self.assertEqual(path, rep["path"])
        self.assertEqual(headers["Authorization"], fx["authorization"])
        self.assertEqual(body, rep["body"])


if __name__ == "__main__":
    unittest.main()
