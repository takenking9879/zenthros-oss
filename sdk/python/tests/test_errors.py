# SPDX-License-Identifier: Apache-2.0

"""A job that no longer exists ends the wait (audit 2026-10-03 SDK-4).

The controller collects a finished job after its TTL (1 h by default). A
404 from zenthros-api, or zctl's "not found", was a TransientError that
wait() retried for ever, so result() from another terminal the next day
never returned. It is errors.JobNotFound now: after a short grace (a job
read just after its create may not be there yet), wait() raises it, and
result() says the job was collected instead of hanging.

What a refusal means decides what the caller does next (audit
2026-10-16 AUD-040).

A 409 (a name taken, a template's limit reached) was a TransientError,
which tells callers to retry; a missing zctl was transient too, so wait()
waited for it in silence, for ever with no timeout; any zctl "not found",
a missing context included, became a job "collected after its TTL"; and an
expired key read as "not authorized for this Workspace".
"""
import subprocess
import time
import unittest
from unittest import mock

from zenthros import errors, zctl
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs import client as jobclient
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import JobHandle
from zenthros.jobs.http import HTTPJobClient
from zenthros.remote.call import RemoteCall

from support import FakeAPI, status


class PollingClient(FakeJobClient):
    """FakeJobClient with the real wait(): the loop every client shares."""

    wait = jobclient.JobClient.wait


class GoneClient(PollingClient):
    """A client whose job is gone: every read of it is JobNotFound."""

    def job_status(self, job_id):
        raise errors.JobNotFound(f"job {job_id} not found")

    def status(self, job_id):
        raise errors.JobNotFound(f"job {job_id} not found")


class JobNotFoundTest(unittest.TestCase):
    def test_a_404_from_the_api_is_job_not_found(self):
        """A 404 is a job gone only when its Status names a zenthrosjob, as
        Kubernetes and the Gateway say it: a Workspace the hub does not
        route, or a page that is not the API's, is another thing not there,
        never a job collected after its TTL (review 2026-10-29)."""
        api = FakeAPI()
        self.addCleanup(api.close)
        code, gone = status(404, 'zenthrosjobs.zenthros.ai "j" not found', "NotFound")
        gone["details"] = {"name": "j", "group": "zenthros.ai", "kind": "zenthrosjobs"}
        api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = (code, gone)
        client = HTTPJobClient(api.url, "risk", "t", poll_seconds=0.01)
        with self.assertRaises(errors.JobNotFound):
            client.job_status("j")
        for answer in (status(404, "Workspace risk not found", "NotFound"), (404, b"404 page not found")):
            api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = answer
            with self.subTest(answer=answer), self.assertRaises(errors.NotFound) as caught:
                client.job_status("j")
            self.assertNotIsInstance(caught.exception, errors.JobNotFound)
        self.assertNotIsInstance(errors.JobNotFound("x"), errors.TransientError,
                                 "a TransientError is retried for ever by wait()")

    def test_zctl_not_found_is_job_not_found(self):
        refused = zctl.refusal('zctl: zenthrosjob "j" not found', zctl.EXIT_JOB_ABSENT)
        self.assertIsInstance(refused, errors.JobNotFound)

    def test_a_transient_zctl_exit_is_never_read_by_its_words(self):
        """zctl says a transient failure by its exit code (75, EX_TEMPFAIL):
        a webhook outage whose words hold "not found" and "zenthrosjob" was
        a job gone for good (fourth review of Ola J, zone C)."""
        text = ('Error: Internal error occurred: failed calling webhook "vzenthrosjob.zenthros.ai": '
                'service "zenthros-webhook" not found')
        refused = zctl.refusal(text, zctl.EXIT_TRANSIENT)
        self.assertIsInstance(refused, errors.ZctlError)
        self.assertNotIsInstance(refused, errors.JobNotFound)

    def test_a_zctl_create_of_unknown_end_names_the_job_it_may_have_made(self):
        """A create zctl could not say the end of may have made the job:
        the error names it, so a call keeps the machine placed for it. A
        transient exit made nothing."""
        from zenthros.jobs import spec as S
        from zenthros.jobs.client import ZctlJobClient

        client = ZctlJobClient(zctl="/bin/true")
        job = S.Job(name="zr-1", spec=S.Spec(workspace="ws-a"))
        for returncode, named in ((1, True), (zctl.EXIT_TRANSIENT, False)):
            with mock.patch.object(client, "_run", side_effect=errors.ZctlError("zctl create: deadline", returncode)), \
                    self.assertRaises(errors.ZctlError) as caught:
                client.submit(job)
            self.assertEqual(getattr(caught.exception, "job", None), "zr-1" if named else None)

    def test_wait_ends_on_a_job_that_stays_gone(self):
        client = GoneClient()
        client.poll_seconds = 0.01
        started = time.monotonic()
        with mock.patch.object(jobclient, "JOB_NOT_FOUND_GRACE", 0.2):
            with self.assertRaises(errors.JobNotFound):
                client.wait("j", timeout=None)
        self.assertLess(time.monotonic() - started, 5)

    def test_wait_rides_out_a_job_not_there_yet(self):
        client = PollingClient()
        client.poll_seconds = 0.01
        reads = []

        def job_status(job_id):
            reads.append(job_id)
            if len(reads) < 3:
                raise errors.JobNotFound("not yet")
            return {"phase": "Succeeded"}

        client.job_status = job_status
        with mock.patch.object(jobclient, "JOB_NOT_FOUND_GRACE", 5):
            self.assertEqual(client.wait("j"), "Succeeded")

    def test_result_of_a_collected_job_says_so(self):
        store = MemoryBlobStore()
        prefix = "s3://zr/calls/zr-gone"
        store.put(f"{prefix}/result.pkl", b"whatever is there")
        client = GoneClient()
        client.poll_seconds = 0.01
        call = RemoteCall("zr-gone", prefix, JobHandle("zr-gone", client), client, store, time.time())
        with mock.patch.object(jobclient, "JOB_NOT_FOUND_GRACE", 0.1):
            with self.assertRaises(errors.JobNotFound) as caught:
                call.result(stream_logs=False)
        message = str(caught.exception)
        self.assertIn("collected", message)
        self.assertIn(f"{prefix}/result.pkl", message)


class ZctlRefusalTest(unittest.TestCase):
    def test_only_a_job_absent_code_is_job_not_found(self):
        """zctl says a job is gone by its code (66), whatever the words:
        'context "lab" not found in the zctl config' was a job collected
        after its TTL (AUD-040), and so was a Workspace the hub does not
        route."""
        self.assertIsInstance(zctl.refusal('zctl: zenthrosjob "j" not found', zctl.EXIT_JOB_ABSENT), errors.JobNotFound)
        for text in ('zctl: context "lab" not found in the zctl config', "zctl: get zenthrosjob: Workspace risk not found"):
            refused = zctl.refusal(text, zctl.EXIT_INVALID)
            self.assertIsInstance(refused, errors.ValidationError, text)
            self.assertNotIsInstance(refused, errors.JobNotFound, text)
        self.assertIs(type(zctl.refusal('zctl: zenthrosjob "j" not found', 1)), errors.ZctlError)

    def test_unauthorized_is_a_login_to_renew(self):
        refused = zctl.refusal("zctl: get zenthrosjob: Unauthorized", zctl.EXIT_LOGIN)
        self.assertIsInstance(refused, errors.LoginRequired)
        self.assertIn("zctl user token", str(refused))
        self.assertIsInstance(zctl.refusal('zenthrosjobs is forbidden: User "bob" cannot list', zctl.EXIT_FORBIDDEN),
                              errors.WorkspaceForbidden)

    def test_a_zctl_that_does_not_answer_is_a_transient_error(self):
        # A controller that never answers left wait() hanging for good.
        hung = subprocess.TimeoutExpired(["zctl", "get"], zctl.TIMEOUT)
        with mock.patch.object(zctl.subprocess, "run", side_effect=hung) as run:
            with self.assertRaises(errors.ZctlError) as caught:
                zctl.Zctl("zctl")("get", "zenthrosjob", "j")
        self.assertEqual(run.call_args.kwargs["timeout"], zctl.TIMEOUT)
        self.assertIsInstance(caught.exception, errors.TransientError)
        self.assertIn("did not answer", str(caught.exception))

    def test_a_followed_log_closes_its_pipe_once_read(self):
        # The remote E2E (2026-10-20): each call's zctl logs --watch left its
        # pipe for the GC, with a ResourceWarning.
        import sys

        proc = zctl.Zctl(sys.executable).popen("-c", "print('a'); print('b')")
        pipe = proc.stdout.gi_frame.f_locals["pipe"]
        self.assertEqual(list(proc.stdout), ["a\n", "b\n"])
        proc.wait()
        self.assertTrue(pipe.closed)

    def test_a_conflict_is_not_transient(self):
        self.assertNotIsInstance(errors.Conflict("taken"), errors.TransientError)
        self.assertEqual(errors.Conflict("taken").status, 409)


class FailingClient(PollingClient):
    """A client whose store keeps failing."""

    def job_status(self, job_id):
        raise errors.TransientError("zenthros-api 503: no Gateway")

    status = job_status


class WaitWarnsTest(unittest.TestCase):
    def test_a_wait_names_a_failure_that_does_not_clear(self):
        clock = [0.0]

        def monotonic():
            clock[0] += 30.0
            return clock[0]

        client = FailingClient()
        client.poll_seconds = 0
        with mock.patch.object(jobclient.time, "monotonic", monotonic), \
                self.assertLogs("zenthros.jobs", level="WARNING") as logs, \
                self.assertRaises(errors.WaitTimeout) as caught:
            client.wait("j", timeout=700)
        warnings = [line for line in logs.output if "no Gateway" in line]
        # After a minute, then every five: 60 s, ~6 min, ~11 min.
        self.assertTrue(2 <= len(warnings) <= 3, logs.output)
        self.assertIn("no Gateway", str(caught.exception))


class ZenthrosErrorTest(unittest.TestCase):
    def test_every_error_the_sdk_raises_is_a_zenthros_error_and_its_builtin(self):
        import zenthros
        from zenthros.fanout import ChildFailed
        from zenthros.remote.call import RemoteError
        from zenthros.remote.envs import EnvironmentFailed

        raised = [errors.ValidationError("x"), errors.JobNotFound("x"), errors.WaitTimeout("x"),
                  errors.LoginRequired("x"), errors.ZctlError("x"), errors.CheckpointError("C", "x"),
                  errors.StoppedOnNotice("x"), RemoteError("x"), ChildFailed([]), EnvironmentFailed("x")]
        for err in raised:
            self.assertIsInstance(err, zenthros.ZenthrosError, type(err).__name__)
        self.assertIsInstance(errors.ValidationError("x"), ValueError)
        self.assertIsInstance(errors.JobNotFound("x"), LookupError)
        self.assertIsInstance(errors.LoginRequired("x"), PermissionError)



if __name__ == "__main__":
    unittest.main()
