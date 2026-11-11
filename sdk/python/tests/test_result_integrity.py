# SPDX-License-Identifier: Apache-2.0

"""The caller unpickles only what the machine wrote (audit 2026-10-03 SEC-4).

Unpickling runs code. result.pkl and call.pkl live in the store, which more
than the caller can write: a Workspace member who may presign a PUT, a job
with the bucket's credentials, a third party's host. The runner reports
each result's SHA-256 to its agent, which only that job can do, and
result() compares the bytes with it before it unpickles them; get() reads
the call it names from the job, whose spec pins the call's digest, and never
unpickles a call.pkl that does not match.

A call is unpickled only if it is the bytes its caller stored.

The store may be writable by more than the caller (anyone the Workspace lets
create objects), and unpickling runs code. The caller pins the SHA-256 of
call.pkl in the job's spec, which only an editor of the job can change; the
runner checks it before pickle.loads, never after.
"""
import hashlib
import io
import os
import pickle
import tempfile
import threading
import unittest
from unittest import mock

from zenthros import errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import ReportView
from zenthros.remote import runner

from test_remote import ExecutingClient, call, double

RAN = []


def ran(what):
    RAN.append(what)


class Payload:
    """A pickle that runs code when it is loaded. It calls a function of
    this module by name, so loading it appends to this RAN: a bound
    RAN.append pickles the list with it, and the copy it appended to left
    RAN empty however the bytes were read (post-fix review 2026-10-03 R-3)."""

    def __reduce__(self):
        return (ran, ("CODE RAN",))


def sum_values(values):
    return sum(values)


def evil():
    return pickle.dumps({"ok": True, "value": pickle.dumps(Payload())}, protocol=4)


class ResultIntegrityTest(unittest.TestCase):
    def setUp(self):
        RAN.clear()
        # A refusal waits out the grace for a digest that never comes: 60 s
        # a test. test_a_digest_reported_a_moment_after_the_result_is_waited_for
        # pins the wait itself.
        grace = mock.patch("zenthros.remote.call.DIGEST_GRACE", 0.2)
        grace.start()
        self.addCleanup(grace.stop)

    def test_a_digest_reported_a_moment_after_the_result_is_waited_for(self):
        # The runner stores the result, then reports its digest: a reader in
        # between waits for the digest instead of refusing the result.
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        job = client._jobs[handle.name]
        results, reports = job.get("results"), job["reports"]
        job["results"], job["reports"] = [], []

        def report():
            job["results"], job["reports"] = results, reports

        threading.Timer(0.3, report).start()
        with mock.patch("zenthros.remote.call.DIGEST_GRACE", 10.0):
            self.assertEqual(handle.result(stream_logs=False), 42)

    def test_result_written_over_after_the_job_is_refused_unread(self):
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        store.put(f"{handle.prefix}/result.pkl", evil())
        with self.assertRaises(remote.RemoteError) as caught:
            handle.result(stream_logs=False)
        self.assertEqual(RAN, [], "the store's bytes were unpickled")
        self.assertIn("SHA-256", str(caught.exception))

    def test_a_result_without_a_reported_digest_is_refused_unread(self):
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        client._jobs[handle.name]["reports"] = []
        store.put(f"{handle.prefix}/result.pkl", evil())
        with self.assertRaises(remote.RemoteError):
            handle.result(stream_logs=False)
        self.assertEqual(RAN, [])

    def test_an_api_that_does_not_answer_is_no_missing_digest(self):
        # An outage of the API through the grace is no answer: the result
        # is neither refused as unvouched nor cached as such, and result()
        # reads it once the API is back (R4 ronda 4, M3).
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        down = errors.TransientError("zenthros-api 503: hub down")
        with mock.patch.object(client, "job_object", side_effect=down), \
                mock.patch.object(client, "reports", side_effect=down):
            with self.assertRaises(errors.TransientError):
                handle.result(stream_logs=False)
        self.assertEqual(handle.result(stream_logs=False), 42)

    def test_reports_that_do_not_answer_are_no_missing_digest(self):
        # The status answering without the digest yet, and the reports'
        # route away (the Hub restarting with the API server up), is no
        # answer either: refused as unvouched, the handle kept the refusal
        # and never returned the result (final review of Ola J, P3).
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        down = errors.TransientError("zenthros-api 503: hub down")
        with mock.patch.object(client, "job_object", return_value={"status": {}}), \
                mock.patch.object(client, "reports", side_effect=down):
            with self.assertRaises(errors.TransientError):
                handle.result(stream_logs=False)
        self.assertEqual(handle.result(stream_logs=False), 42)

    def test_an_api_that_keeps_no_reports_has_answered(self):
        # 501 from the reports: the API keeps none, which is an answer. Read
        # as "away", a result with no digest was asked about for ever
        # instead of refused (final review of Ola J, second pass).
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        client._jobs[handle.name]["reports"] = []
        store.put(f"{handle.prefix}/result.pkl", evil())
        none = errors.TransientError("zenthros-api 501: no report store")
        none.status = 501
        with mock.patch.object(client, "job_object", return_value={"status": {}}), \
                mock.patch.object(client, "reports", side_effect=none):
            with self.assertRaises(remote.RemoteError):
                handle.result(stream_logs=False)
        self.assertEqual(RAN, [])

    def test_the_result_the_machine_wrote_is_returned(self):
        handle, _, _ = call(double, 21)
        self.assertEqual(handle.result(stream_logs=False), 42)

    def test_each_map_item_is_checked(self):
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            calls = remote.map(double, [1, 2, 3], code_dir=None)
        client.wait(calls.name)
        store.put(f"{calls.prefix}/results/1.pkl", evil())
        with self.assertRaises(remote.RemoteError):
            calls.result(stream_logs=False)
        self.assertEqual(RAN, [])

    def test_the_result_is_stored_before_its_digest_is_reported(self):
        # ⟂ A reported digest always names bytes already in the store.
        events = []
        store = MemoryBlobStore()
        real_put = store.put
        store.put = lambda uri, payload: (events.append(("put", uri)), real_put(uri, payload))[1]

        class Recorder:
            def __init__(self, producer_id=None, strict=True):
                self.producer = producer_id

            def set(self, key, value):
                events.append(("report", self.producer, value["uri"]))
                return self

            def publish(self, final=False):
                return {"accepted": True}

        with mock.patch.object(runner, "Reporter", Recorder):
            handle, client, _ = call(double, 21, store=store)
            client.wait(handle.name)
        result = f"{handle.prefix}/result.pkl"
        self.assertLess(events.index(("put", result)), events.index(("report", "zenthros-result-g1", result)))

    def test_a_digest_the_agent_could_not_take_for_a_call_is_sent_again(self):
        # A spool full while the controller is away (a restart, 2-3 min) gave
        # only 507s for a call's 45 s: the digest was dropped and the result,
        # its bytes in the store, refused (R4 ronda 5, P2).
        sent = []

        class Busy:
            def __init__(self, producer_id=None, strict=True):
                pass

            def set(self, key, value):
                return self

            def publish(self, final=False):
                sent.append(1)
                if len(sent) < 4:
                    raise errors.AgentUnanswered("SPOOL_FULL", "local report spool is full")
                return {"accepted": True}

        out = io.StringIO()
        with mock.patch.object(runner, "Reporter", Busy), mock.patch.object(runner.time, "sleep", lambda _: None):
            runner.store_result(MemoryBlobStore(), "mem://b/result.pkl", {"ok": True}, "zenthros-result-g1", out)
        self.assertEqual(len(sent), 4)
        self.assertNotIn("could not report", out.getvalue())

    def test_a_replacements_digest_vouches_for_its_result(self):
        # A replacement's agent counts report sequences from 1 again: its
        # digest comes from a producer of its own generation, and result()
        # takes the result that matches any digest the job reported.
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        raw = pickle.dumps({"ok": True, "value": pickle.dumps(99)}, protocol=4)
        store.put(f"{handle.prefix}/result.pkl", raw)
        client._jobs[handle.name]["reports"].append(ReportView(
            job_id=handle.name, producer_id="zenthros-result-g2",
            payload={"zenthros.result": {"uri": f"{handle.prefix}/result.pkl",
                                         "sha256": hashlib.sha256(raw).hexdigest()}}))
        self.assertEqual(handle.result(stream_logs=False), 99)

    def test_the_payload_runs_code_when_unpickled(self):
        # ⟂ The asserts above can fail: the payload's code reaches RAN.
        pickle.loads(pickle.loads(evil())["value"])
        self.assertEqual(RAN, ["CODE RAN"])

    def test_the_status_vouches_once_the_report_store_lost_the_digest(self):
        # Without a database the report store is in memory: a controller
        # restart, or 50 000 newer reports, empties it. The controller also
        # keeps each digest in the job's status (post-fix review R-4).
        handle, client, _ = call(double, 21)
        client.wait(handle.name)
        client._jobs[handle.name]["reports"] = []
        self.assertEqual(handle.result(stream_logs=False), 42)

    def test_a_status_digest_does_not_vouch_for_other_bytes(self):
        handle, client, store = call(double, 21)
        client.wait(handle.name)
        client._jobs[handle.name]["reports"] = []
        store.put(f"{handle.prefix}/result.pkl", evil())
        with self.assertRaises(remote.RemoteError):
            handle.result(stream_logs=False)
        self.assertEqual(RAN, [])

    def test_an_upstream_written_over_is_refused_on_the_machine(self):
        # The downstream's machine reads the upstream's result from the
        # store too. It unpickles it only when it hashes to the pin the
        # controller put in its env from the upstream's status (R-1).
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            up = remote.run(double, 21, code_dir=None)
            client.wait(up.name)
            store.put(f"{up.prefix}/result.pkl", evil())
            down = remote.run(double, up, code_dir=None)
        with self.assertRaises(remote.RemoteError) as caught:
            down.result(stream_logs=False)
        self.assertEqual(RAN, [], "the machine unpickled an upstream result nobody vouched for")
        self.assertIn(up.name, str(caught.exception))

    def test_an_upstream_map_item_written_over_is_refused_on_the_machine(self):
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            up = remote.map(double, [1, 2, 3], code_dir=None)
            client.wait(up.name)
            store.put(f"{up.prefix}/results/2.pkl", evil())
            down = remote.run(sum_values, up, code_dir=None)
        with self.assertRaises(remote.RemoteError):
            down.result(stream_logs=False)
        self.assertEqual(RAN, [])

    def test_an_upstream_the_controller_did_not_pin_is_refused(self):
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        client.pin_upstreams = False
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            up = remote.run(double, 21, code_dir=None)
            down = remote.run(double, up, code_dir=None)
        with self.assertRaises(remote.RemoteError) as caught:
            down.result(stream_logs=False)
        self.assertIn("pinned", str(caught.exception))

    def test_a_vouched_upstream_reaches_the_downstream(self):
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            up = remote.map(double, [1, 2, 3], code_dir=None)
            down = remote.run(sum_values, up, code_dir=None)
        self.assertEqual(down.result(stream_logs=False), 12)

    def test_a_call_says_how_many_results_it_writes(self):
        # The controller holds a dependent until that many digests are in
        # the upstream's status: they come through the agent's spool, which
        # can land after the phase.
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            one = remote.run(double, 1, code_dir=None)
            many = remote.map(double, [1, 2, 3], code_dir=None)
            single = remote.map(double, [1], code_dir=None)
            command = remote.run(["true"], code_dir=None)
        count = lambda c: client.submitted(c.name).annotations.get("zenthros.ai/result-count")  # noqa: E731
        self.assertEqual([count(one), count(many), count(single), count(command)], ["1", "3", "1", None])

    def test_the_pin_is_the_controllers(self):
        # ⟂ resultdigest.Pin in Go: the latest digest per URI, sorted by URI,
        # one "uri sha256\n" line each.
        a, b = "a" * 64, "b" * 64
        lines = f"s3://b/r/results/0.pkl {b}\ns3://b/r/results/1.pkl {a}\n"
        self.assertEqual(remote.results_pin([("s3://b/r/results/1.pkl", b), ("s3://b/r/results/0.pkl", b),
                                             ("s3://b/r/results/1.pkl", a)]),
                         hashlib.sha256(lines.encode()).hexdigest())

    def test_get_never_unpickles_a_call_written_over(self):
        handle, client, store = call(double, 21)
        store.put(handle.call_uri, pickle.dumps(Payload()))
        with remote.configure(store=STORE, client=client, blob_store=store):
            try:
                again = remote.get(handle.name)
            except (errors.ValidationError, remote.RemoteError):
                again = None
        self.assertEqual(RAN, [], "get() unpickled a call.pkl other than the submitted one")
        if again is not None:
            self.assertEqual(again.name, handle.name)

STORE = "s3://calls/zenthros/risk"


def add(a, b):
    return a + b


def _touch(path):
    open(path, "w").close()
    return "evil"


class Evil:
    def __init__(self, path):
        self.path = path

    def __reduce__(self):
        return _touch, (self.path,)


def pinned(client, name):
    env = client.submitted(name).spec.workload.template.env
    return {e.name: e.value for e in env}.get("ZENTHROS_CALL_SHA256", "")


class CallDigestTest(unittest.TestCase):
    def setUp(self):
        self.store = MemoryBlobStore()
        self.client = FakeJobClient()
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)

    def test_submit_pins_the_digest_of_what_it_stored(self):
        call = remote.run(add, 1, 2, code_dir=None)
        stored = self.store.get(call.call_uri)
        self.assertEqual(pinned(self.client, call.name), hashlib.sha256(stored).hexdigest())
        command = remote.run(["echo", "hi"], code_dir=None)
        self.assertEqual(pinned(self.client, command.name), "", "a command is not unpickled: nothing to pin")

    def test_runner_refuses_a_changed_call_before_unpickling_it(self):
        call = remote.run(add, 1, 2, code_dir=None)
        uri = call.call_uri
        original = self.store.get(uri)
        digest = pinned(self.client, call.name)
        with tempfile.TemporaryDirectory() as tmp:
            marker = os.path.join(tmp, "ran")
            self.store.put(uri, pickle.dumps(Evil(marker)))
            out = io.StringIO()
            with mock.patch.dict(os.environ, {runner.CALL_SHA256_ENV: digest}):
                code = runner.execute(uri, blob_store=self.store, out=out)
            self.assertEqual(code, runner.EXIT_TAMPERED)
            self.assertFalse(os.path.exists(marker), "the replaced call was unpickled")
            self.assertIn("not the call that was submitted", out.getvalue())
            with self.assertRaises(FileNotFoundError):
                self.store.get(f"{call.prefix}/result.pkl")

            self.store.put(uri, original)
            with mock.patch.dict(os.environ, {runner.CALL_SHA256_ENV: digest}):
                self.assertEqual(runner.execute(uri, blob_store=self.store, out=io.StringIO()), 0)
        self.assertEqual(pickle.loads(self.store.get(f"{call.prefix}/result.pkl"))["ok"], True)

    def test_runner_refuses_a_call_whose_spec_pins_no_digest(self):
        # submit always pins it: a job without it was not made by submit
        # (another spec pointed at a call.pkl), and is not unpickled.
        call = remote.run(add, 1, 2, code_dir=None)
        with tempfile.TemporaryDirectory() as tmp:
            marker = os.path.join(tmp, "ran")
            self.store.put(call.call_uri, pickle.dumps(Evil(marker)))
            out = io.StringIO()
            with mock.patch.dict(os.environ):
                os.environ.pop(runner.CALL_SHA256_ENV, None)
                code = runner.execute(call.call_uri, blob_store=self.store, out=out)
            self.assertEqual(code, runner.EXIT_TAMPERED)
            self.assertFalse(os.path.exists(marker), "a call with no pinned digest was unpickled")
        with self.assertRaises(FileNotFoundError):
            self.store.get(f"{call.prefix}/result.pkl")


if __name__ == "__main__":
    unittest.main()
