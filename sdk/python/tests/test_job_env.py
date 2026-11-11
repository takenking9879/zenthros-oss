# SPDX-License-Identifier: Apache-2.0

"""What the SDK reads about its job is what Zenthros puts in the job.

operator/pkg/jobcli/testdata/wire.json's job_env lists every variable a job
finds in its environment, by who sets it. Go tests hold the control plane,
the agent, a task and a child to exactly their variables; this holds the
SDK to reading only those, and to giving its own children only its own. A
variable renamed on one side fails here or there, without a cluster.
"""
import json
import os
import unittest
from pathlib import Path
from unittest import mock

from zenthros import fanout
from zenthros.core import env
from zenthros.remote import call, envs

FIXTURE = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "jobcli" / "testdata" / "wire.json"
JOB_ENV = {k: v for k, v in json.loads(FIXTURE.read_text())["job_env"].items() if k != "_about"}


class Looked(dict):
    """An environment that notes every variable looked up."""

    def __init__(self, items=()):
        super().__init__(items)
        self.names = set()

    def get(self, key, default=None):
        self.names.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.names.add(key)
        return super().__getitem__(key)

    def __contains__(self, key):
        self.names.add(key)
        return super().__contains__(key)


def _owned(names):
    return {n for n in names if n.startswith("ZENTHROS_") or n in ("TASK_INDEX", "TASK_COUNT")}


class JobEnvTest(unittest.TestCase):
    def test_the_sdk_reads_only_variables_a_job_is_given(self):
        reads = set(env.JOB_VARS) | {call.UPSTREAM_RESULTS_ENV, envs.CACHE_ENV}
        self.assertFalse(sorted(reads - set(JOB_ENV)),
                         "the SDK reads job variables wire.json's job_env does not list: nobody sets them")

    def test_reading_a_job_looks_up_only_its_variables(self):
        looked = Looked()
        env.JobEnv.current(looked)
        self.assertLessEqual(looked.names, set(env.JOB_VARS), "JobEnv reads a variable JOB_VARS does not name")
        self.assertLessEqual(looked.names, set(JOB_ENV))

    def test_the_sdk_gives_its_children_only_its_own_variables(self):
        sent = {}

        def spawn(name, count, cmd=None, env=None, **_):
            sent.update(env or {})
            return mock.Mock()

        with mock.patch.dict(os.environ, {"ZENTHROS_DATA_OUTPUT": "s3://bucket/runs/r1"}), \
                mock.patch.object(fanout, "spawn", spawn):
            fanout.map(square, [1, 2], blob_store=mock.Mock())
        self.assertEqual(_owned(sent), {n for n, by in JOB_ENV.items() if by == "sdk"})


def square(x):
    return x * x


if __name__ == "__main__":
    unittest.main()
