# SPDX-License-Identifier: Apache-2.0

"""A call with no bucket: what it needs rides in the job, and its result comes
back in the job's final report, verified as any. C7: a Workspace with no store and
a machine of yours."""

import base64
import io
import os
import tarfile
import tempfile
import unittest
from unittest import mock

from zenthros import errors, remote
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import carried

from test_cache import _placed
from test_remote import ExecutingClient


class NoBucket(ExecutingClient):
    """A hub with no store: the machine reads the call from its own files."""

    def __init__(self):
        super().__init__(None)
        self.store = ""
        self.machine_store = None

    def submit(self, spec):
        return FakeJobClient.submit(self, spec)


def double(x):
    return x * 2


def large(n):
    return b"x" * n


class Carried(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory()
        self.addCleanup(self.home.cleanup)
        env = mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": f"{self.home.name}/config"})
        env.start()
        self.addCleanup(env.stop)
        self.client = NoBucket()
        configured = remote.configure(store="", client=self.client)
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        placed = mock.patch("zenthros.machines.place", return_value=_placed("demo"))
        placed.start()
        self.addCleanup(placed.stop)

    def run_call(self, fn, *args, **kwargs):
        kwargs.setdefault("code_dir", None)
        return remote.run(fn, *args, **kwargs)

    def inline_tar(self, call):
        template = self.client.submitted(call.name).spec.workload.template
        with tarfile.open(fileobj=io.BytesIO(base64.b64decode(template.source.inline))) as tar:
            return tar.getnames(), template

    def test_the_cache_keeps_on_this_computer_what_this_computer_checked(self):
        os.chdir(self.home.name)
        self.addCleanup(os.chdir, os.path.dirname(os.path.abspath(__file__)))
        first = self.run_call(double, 21, cache=True)
        self.assertEqual(first.result(stream_logs=False), 42)
        again = self.run_call(double, 21, cache=True)
        self.assertEqual(again.result(), 42)
        self.assertEqual(again.action, "reuse")
        self.assertEqual(len(self.client._jobs), 1)

    def test_rerun_finds_the_call_in_the_job_it_went_in(self):
        first = self.run_call(double, 21)
        first.result(stream_logs=False)
        again = first.rerun()
        self.assertNotEqual(again.name, first.name)
        self.assertEqual(again.result(stream_logs=False), 42)

    def test_a_call_from_another_process_is_read_from_its_job(self):
        first = self.run_call(double, 21)
        first.result(stream_logs=False)
        self.assertEqual(remote.get(first.name).result(stream_logs=False), 42)

    def test_a_large_call_is_asked_for_apart_as_the_api_keeps_it(self):
        # zenthros-api keeps code of 8 KiB and more out of the job, by reference.
        whole = self.client.job_object

        def by_reference(name):
            found = whole(name)
            source = found["spec"]["workload"]["template"].get("source") or {}
            return {**found, "spec": {**found["spec"], "workload": {"template": {
                **found["spec"]["workload"]["template"], "source": {key: value for key, value in source.items() if key != "inline"}}}}}

        first = self.run_call(double, 21)
        first.result(stream_logs=False)
        with mock.patch.object(self.client, "source_of", lambda name: (whole(name)["spec"]["workload"]["template"]["source"]["inline"])), \
                mock.patch.object(self.client, "job_object", by_reference):
            self.assertEqual(remote.get(first.name).result(stream_logs=False), 42)
            self.assertEqual(first.rerun().result(stream_logs=False), 42)

    def test_a_small_call_there_and_back(self):
        call = self.run_call(double, 21)
        self.assertEqual(call.result(stream_logs=False), 42)

    def test_its_result_is_in_the_report_with_its_digest(self):
        call = self.run_call(double, 21)
        call.result(stream_logs=False)
        entry = self.client.reports(call.name)[-1].payload["zenthros.result"]
        self.assertTrue(entry["uri"].startswith(carried.ROOT))
        self.assertTrue(entry[carried.BYTES_KEY])
        self.assertEqual(len(entry["sha256"]), 64)

    def test_the_call_rides_in_the_job_and_nothing_points_at_a_bucket(self):
        call = self.run_call(double, 21)
        names, template = self.inline_tar(call)
        self.assertTrue(any(name.startswith(f"{carried.FOLDER}/job/{call.name}/call-") for name in names), names)
        self.assertTrue(any(name.startswith("zenthros/") for name in names), "the runner has to start")
        self.assertIn(carried.ROOT, template.command[-1])
        self.assertFalse(template.artifacts.output, "no bucket to keep its output in")
        self.assertEqual(template.secret_sources or [], [], "no store, no credentials")
        packages = {e.name: e.value for e in template.env}["ZENTHROS_RUNNER_PACKAGES"]
        self.assertNotIn("boto3", packages)

    def test_a_result_written_over_is_refused(self):
        """The digest in the job's status is the word; what a report brings is checked against it."""
        call = self.run_call(double, 21)
        call.handle.wait()
        view = self.client.reports(call.name)[-1]
        view.payload["zenthros.result"][carried.BYTES_KEY] = base64.b64encode(b"forged").decode()
        with self.assertRaisesRegex(remote.RemoteError, "refusing to unpickle"):
            call.result(stream_logs=False)

    def test_a_result_over_the_limit_says_the_three_ways_out(self):
        call = self.run_call(large, carried.MAX_RESULT_BYTES + 1)
        with self.assertRaises(remote.RemoteError) as caught:
            call.result(stream_logs=False)
        said = str(caught.exception)
        self.assertIn("zenthros store", said)
        self.assertIn("return less", said)
        self.assertIn("return its path", said)

    def test_a_result_within_the_limit_comes_back_whole(self):
        size = carried.MAX_RESULT_BYTES - 1024
        self.assertEqual(len(self.run_call(large, size).result(stream_logs=False)), size)

    def test_a_call_over_what_a_job_carries_says_so_and_sends_nothing(self):
        with self.assertRaisesRegex(errors.ValidationError, "with no bucket a job carries"):
            self.run_call(large, os.urandom(400 << 10))
        self.assertEqual(self.client._jobs, {})

    def test_a_project_rides_too(self):
        with tempfile.TemporaryDirectory() as project:
            with open(f"{project}/helper.py", "w") as f:
                f.write("VALUE = 7\n")
            call = self.run_call(double, 21, code_dir=project)
            names, _ = self.inline_tar(call)
            self.assertTrue(any(f"{carried.FOLDER}/job/code/" in name for name in names), names)
            self.assertEqual(call.result(stream_logs=False), 42)

    def test_a_command_needs_none_of_it(self):
        call = remote.run(["echo", "hello"], code_dir=None)
        template = self.client.submitted(call.name).spec.workload.template
        self.assertEqual(template.command, ["echo", "hello"])

    def test_a_result_as_an_argument_needs_a_bucket(self):
        first = self.run_call(double, 21)
        with self.assertRaisesRegex(errors.ValidationError, "travels by a bucket"):
            self.run_call(double, first)

    def test_a_store_of_the_hub_goes_as_before(self):
        self.client.store = "s3://hub/w"
        self.assertEqual(remote.call._store_or_carried(remote.settings(), self.client), "s3://hub/w")


if __name__ == "__main__":
    unittest.main()
