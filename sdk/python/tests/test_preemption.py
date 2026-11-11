#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The interruption notice the agent writes, as a job reads it."""
import json
import os
import tempfile
import unittest
from unittest import mock

from zenthros import errors, preemption
from zenthros.core.checkpoint import Checkpointer
from zenthros.core.storage import MemoryBlobStore

from support import AckAgent


class PreemptionTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "uid.json")
        self.env = mock.patch.dict(os.environ, {"ZENTHROS_PREEMPT_FILE": self.path}, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.dir.cleanup()

    def _notice(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"source": "aws-imds", "action": "terminate", "deadline": "2026-09-25T12:00:00Z"}, f)

    def test_requested_reads_the_file(self):
        self.assertFalse(preemption.requested())
        self._notice()
        self.assertTrue(preemption.requested())
        self.assertEqual(preemption.notice()["deadline"], "2026-09-25T12:00:00Z")

    def test_no_file_env_means_no_notice(self):
        with mock.patch.dict(os.environ, {"ZENTHROS_PREEMPT_FILE": ""}, clear=False):
            self.assertIsNone(preemption.notice())

    def test_preempt_triggers_checkpoint(self):
        """A loop asking every iteration checkpoints once when the notice
        appears, then goes back to its own cadence."""
        ckpt = Checkpointer("train", store=MemoryBlobStore())
        self.assertFalse(ckpt.notice())
        self._notice()
        self.assertTrue(ckpt.notice())
        self.assertFalse(ckpt.notice())

    def test_replacement_submitted_raises_at_once(self):
        """Once the replacement took its cursor, no retry can land: the
        refusal is raised on the first answer, not after eight."""
        calls = []

        def ack(commit):
            calls.append(commit)
            raise errors.from_code("CHECKPOINT_FROZEN", "cursor frozen: replacement submitted at 2026-09-25T12:00:05Z")

        env = {"ZENTHROS_JOB_ID": "u", "ZENTHROS_CHECKPOINT_PREFIX": "memory://out"}
        with mock.patch.dict(os.environ, env, clear=False), mock.patch("zenthros.core.checkpoint.time.sleep"):
            with self.assertRaises(errors.CheckpointFrozen) as caught:
                Checkpointer("train", store=MemoryBlobStore(), agent=AckAgent(ack)).save({"epoch": 1})
        self.assertEqual(len(calls), 1)
        self.assertIn("replacement submitted at 2026-09-25T12:00:05Z", str(caught.exception))


if __name__ == "__main__":
    unittest.main()

