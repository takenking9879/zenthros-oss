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
from pathlib import Path, PurePosixPath
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

        with mock.patch.dict(os.environ, {"ZENTHROS_DATA_OUTPUT": "s3://bucket/runs/r1",
                                          "ZENTHROS_DATA_OUTPUT_URI": "s3://bucket/runs/r1"}), \
                mock.patch.object(fanout, "spawn", spawn):
            fanout.map(square, [1, 2], blob_store=mock.Mock())
        self.assertEqual(_owned(sent), {n for n, by in JOB_ENV.items() if by == "sdk"})


def square(x):
    return x * x


if __name__ == "__main__":
    unittest.main()


class OutputsFolderTest(unittest.TestCase):
    """On a machine ZENTHROS_DATA_OUTPUT is a folder its agent saves to
    ZENTHROS_DATA_OUTPUT_URI once the job ended (19): what is in the bucket
    while the job runs (a checkpoint) goes to the URI, and what bears a name
    of zenthros' own is taken out of the folder, never over the result."""

    def test_on_a_machine_the_output_is_a_folder_and_checkpoints_go_to_its_uri(self):
        job = env.JobEnv.current({"ZENTHROS_JOB_ID": "uid", "ZENTHROS_DATA_OUTPUT": "/var/lib/zenthros/jobs/uid/output",
                                  "ZENTHROS_DATA_OUTPUT_URI": "s3://b/calls/c1"})
        self.assertEqual(job.output_folder, "/var/lib/zenthros/jobs/uid/output")
        self.assertEqual(job.prefix, "s3://b/calls/c1")
        self.assertTrue(job.in_job)
        # A pod's output is the URI itself; a call on this computer writes in
        # the call's own folder: neither is a folder its agent saves.
        for same in ("s3://b/calls/c1", "/home/me/.zenthros/local/calls/c1"):
            job = env.JobEnv.current({"ZENTHROS_DATA_OUTPUT": same, "ZENTHROS_DATA_OUTPUT_URI": same})
            self.assertEqual(job.output_folder, "")

    def test_a_file_named_as_zenthros_own_is_taken_out_of_the_folder(self):
        import io
        import tempfile

        from zenthros.remote import runner

        with tempfile.TemporaryDirectory() as root:
            folder = Path(root) / "output"
            (folder / "results").mkdir(parents=True)
            for name in ("result.pkl", "results/0.pkl", "model.pt"):
                (folder / name).write_bytes(b"mine")
            said = io.StringIO()
            runner._keep_the_sdks_own(env.JobEnv(data_output=str(folder), data_output_uri="s3://b/calls/c1"), said)
            self.assertEqual(sorted(p.name for p in folder.rglob("*") if p.is_file()), ["model.pt"])
            self.assertIn("result.pkl in $ZENTHROS_DATA_OUTPUT is the name of zenthros' own", said.getvalue())
            # The call's own folder on this computer holds the call and its
            # result: nothing of it is taken.
            (folder / "result.pkl").write_bytes(b"the runner's")
            runner._keep_the_sdks_own(env.JobEnv(data_output=str(folder), data_output_uri=str(folder)), said)
            self.assertTrue((folder / "result.pkl").exists())

    def test_a_path_into_the_folder_is_returned_as_where_its_agent_saves_it(self):
        # The next step runs on another machine: it reads a step's file in
        # the bucket, as it read a URI the step wrote to itself (19).
        from zenthros.remote import runner

        # A machine's path: the agent's machines are Linux, wherever the
        # test runs (a Path on Windows would be another machine's path).
        job = env.JobEnv(data_output="/jobs/j/output", data_output_uri="s3://b/calls/c1")
        value = {"rows": "/jobs/j/output/rows.parquet", "plots": [PurePosixPath("/jobs/j/output/plots/a.png"), "/elsewhere/x"],
                 "pair": ("/jobs/j/output", 3), "n": 3}
        self.assertEqual(runner._where_saved(value, job), {
            "rows": "s3://b/calls/c1/rows.parquet", "plots": ["s3://b/calls/c1/plots/a.png", "/elsewhere/x"],
            "pair": ("s3://b/calls/c1", 3), "n": 3})
        self.assertEqual(runner._where_saved("/jobs/j/output2/x", job), "/jobs/j/output2/x")
        here = env.JobEnv(data_output="/home/me/calls/c1", data_output_uri="/home/me/calls/c1")
        self.assertEqual(runner._where_saved("/home/me/calls/c1/x", here), "/home/me/calls/c1/x")
