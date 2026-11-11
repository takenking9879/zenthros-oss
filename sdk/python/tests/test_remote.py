# SPDX-License-Identifier: Apache-2.0

"""zenthros.run / zenthros.cloud and the remote runner, without a cluster.

ExecutingClient runs each submitted job's command in-process (the call's
code extracted into a directory of its own, as the agent does), so a whole
round trip — store the call, submit, run it, read the result — runs here.
The e2e against Floci is tests/e2e/test_remote_floci.py.

Smaller edges of zenthros.run (audit 2026-10-03 SDK-5, SDK-6, SDK-9,
SDK-10, SDK-12).
"""
import base64
import contextlib
import hashlib
import io
import json
import os
import pickle
import subprocess
import tarfile
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import cloudpickle

import zenthros
from zenthros import errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import FAILED, SUCCEEDED
from zenthros.jobs.http import HTTPJobClient
from zenthros.remote.call import runner_packages
from zenthros.view import Backend

from support import FakeAPI, ReportAgent, execute, machine_side, reporting

STORE = "s3://zr-test/calls"


def double(x):
    return 2 * x


def frozen():
    raise errors.CheckpointFrozen("CHECKPOINT_FROZEN", "phase sum seq 9 was not acked: replacement submitted")


def touch(path):
    Path(path).write_text("ran")
    return path


def exit_with(code):
    raise SystemExit(code)


def explode(message):
    raise ValueError(message)


class QuotaExceeded(Exception):
    """Its __init__ takes more than args holds: pickle alone can't rebuild it."""

    def __init__(self, message, code):
        super().__init__(message)
        self.code = code


def over_quota():
    raise QuotaExceeded("32 vCPU", code="L-1216C47A")

# A script's functions: __module__ is __main__, and the machine has no such
# script. total hands chunk to multiprocessing, which pickles it by name.
MAIN_SCRIPT = """
import multiprocessing

def chunk(n):
    return n * n

def total(n):
    with multiprocessing.get_context("fork").Pool(2) as pool:
        return sum(pool.map(chunk, range(n)))
"""


def main_function(name):
    namespace = {"__name__": "__main__"}
    exec(compile(MAIN_SCRIPT, "script.py", "exec"), namespace)
    return namespace[name]


class Squares(zenthros.Task):
    """A Task goes to zenthros.run as the call itself."""

    policy = zenthros.CheckpointPolicy(every=1)
    stop_on_notice = True

    def __init__(self, n):
        self.n = n

    def setup(self):
        self.total = 0

    def steps(self):
        return self.n

    def step(self, i):
        self.total += i * i

    def state(self):
        return {"total": self.total}

    def restore(self, state):
        self.total = state["total"]

    def result(self):
        return self.total


class RecordingStore(MemoryBlobStore):
    def __init__(self, events):
        super().__init__()
        self.events = events

    def put(self, uri, payload):
        self.events.append(("put", uri))
        return super().put(uri, payload)

    def get(self, uri):
        self.events.append(("get", uri))
        return super().get(uri)


class ExecutingClient(FakeJobClient):
    """Runs a job's `python -m zenthros.remote exec URI` at wait()."""

    def __init__(self, store, events=None):
        super().__init__()
        self.store = store
        self.machine_store = store
        self.machine_out = io.StringIO()
        self.events = events if events is not None else []
        self.exit_codes = {}
        # Pin each upstream's results in a dependent's env, as the
        # controller does from the upstream's status.
        self.pin_upstreams = True

    def submit(self, spec):
        uri = spec.spec.workload.template.command[-1]
        self.events.append(("submit", uri, uri in self.store.data))
        return super().submit(spec)

    def wait(self, job_id, timeout=None, on_reschedule=None, cancel_on_timeout=False):
        """What the controller and the machine do: upstreams first (dependsOn),
        then the command once per task."""
        if job_id in self.exit_codes:
            return self._ended(job_id)
        job = self.submitted(job_id)
        for upstream in job.spec.depends_on:
            try:
                self.wait(upstream)
            except errors.JobFailed:
                self.exit_codes[job_id] = "DEPENDENCY_FAILED"
                return self._ended(job_id)
        self.events.append(("wait", job_id))
        pins = {}
        for upstream in job.spec.depends_on if self.pin_upstreams else ():
            results = self._jobs[upstream].get("results")
            if results:
                pins[upstream] = remote.results_pin((r["uri"], r["sha256"]) for r in results)
        template = job.spec.workload.template
        tasks = template.distribution.count if template.distribution else 1
        # The digest of call.pkl the spec pins, as the machine gets it.
        pinned = {e.name: e.value for e in template.env or []}
        codes = []
        # The runner reports each result's digest to the agent, which keeps
        # it in the job's reports, where result() reads it (SEC-4).
        with tempfile.TemporaryDirectory() as work, _cwd(work), _restore_path(), ReportAgent(self, job_id) as endpoint, \
                mock.patch.dict(os.environ, {"ZENTHROS_CALL_SHA256": pinned.get("ZENTHROS_CALL_SHA256", ""),
                                             "ZENTHROS_REPORT_ENDPOINT": endpoint,
                                             "ZENTHROS_UPSTREAM_RESULTS": json.dumps(pins)}):
            if template.source and template.source.inline:
                with tarfile.open(fileobj=io.BytesIO(base64.b64decode(template.source.inline))) as tar:
                    tar.extractall(work, filter="data")
            for index in range(tasks):
                # As on a machine: TASK_INDEX only when the job is distributed.
                task = {"TASK_INDEX": str(index), "TASK_COUNT": str(tasks)} if template.distribution else {}
                with mock.patch.dict(os.environ, task):
                    codes.append(remote.execute(template.command[-1], blob_store=self.machine_store, out=self.machine_out))
        self.exit_codes[job_id] = max(codes)
        # The controller keeps each reported result digest in the job's
        # status too, past the report store.
        self._jobs[job_id]["results"] = [
            view.payload["zenthros.result"] for view in self._jobs[job_id]["reports"]
            if isinstance((view.payload or {}).get("zenthros.result"), dict)]
        return self._ended(job_id)

    def _ended(self, job_id):
        code = self.exit_codes[job_id]
        self._jobs[job_id]["phase"] = SUCCEEDED if code == 0 else FAILED
        if code == "DEPENDENCY_FAILED":
            raise errors.JobFailed(f"job {job_id}: a dependency failed", reasons=[{"code": "DEPENDENCY_FAILED"}])
        if code != 0:
            raise errors.JobFailed(f"job {job_id} failed", reasons=[{"code": "PROCESS_FAILED"}])
        return SUCCEEDED


@contextlib.contextmanager
def _cwd(path):
    before = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(before)


@contextlib.contextmanager
def _restore_path():
    import sys

    before = list(sys.path)
    try:
        yield
    finally:
        sys.path[:] = before


def call(fn, *args, store=None, client=None, ship_sdk=True, **kwargs):
    store = store if store is not None else MemoryBlobStore()
    client = client if client is not None else ExecutingClient(store)
    kwargs.setdefault("on", "demo-ml")
    kwargs.setdefault("code_dir", None)
    with remote.configure(store=STORE, client=client, blob_store=store, ship_sdk=ship_sdk):
        return remote.run(fn, *args, **kwargs), client, store


class RunTest(unittest.TestCase):
    def test_call_is_stored_before_the_job_exists(self):
        events = []
        store = RecordingStore(events)
        handle, _, _ = call(double, 21, store=store, client=ExecutingClient(store, events))
        puts = [e for e in events if e[0] == "put"]
        submit = [e for e in events if e[0] == "submit"][0]
        self.assertEqual(puts[0][1], handle.call_uri)
        self.assertTrue(submit[2], "submit ran before the call was in the store")
        self.assertLess(events.index(puts[0]), events.index(submit))

    def test_result_is_read_only_after_the_job_ended(self):
        events = []
        store = RecordingStore(events)
        handle, _, _ = call(double, 21, store=store, client=ExecutingClient(store, events))
        self.assertEqual(handle.result(), 42)
        wait = events.index(("wait", handle.name))
        result_gets = [i for i, e in enumerate(events) if e == ("get", f"{handle.prefix}/result.pkl")]
        self.assertTrue(result_gets and min(result_gets) > wait)

    def test_job_runs_the_call_on_the_pool(self):
        with mock.patch.dict(os.environ, {remote.REMOTE_ENV: json.dumps({"AWS_ENDPOINT_URL_S3": "http://h:4566"})}):
            handle, client, store = call(double, 1, env={"EXTRA": "1"}, cpu="8", gpu="A10G", spot=True)
        job = client.submitted(handle.name)
        manifest = job.to_manifest()
        spec = manifest["spec"]
        self.assertEqual(spec["poolRef"], "demo-ml")
        self.assertEqual(spec["workload"]["kind"], "Job")
        template = spec["workload"]["template"]
        self.assertEqual(template["command"],
                         ["python3", "-m", "zenthros.remote", "exec", handle.call_uri])
        self.assertEqual(template["artifacts"], {"output": f"{STORE}/{handle.name}"})
        self.assertEqual({e["name"]: e["value"] for e in template["env"]},
                         {"AWS_ENDPOINT_URL_S3": "http://h:4566", "EXTRA": "1",
                          "ZENTHROS_CHECKPOINT_PREFIX": f"{STORE}/{handle.name}",
                          "ZENTHROS_CALL_SHA256": hashlib.sha256(store.get(handle.call_uri)).hexdigest(),
                          "ZENTHROS_RUNNER_PACKAGES": " ".join(runner_packages(STORE))})
        self.assertEqual(spec["compute"], {"cpu": "8", "gpuCount": 1, "gpuModel": "A10G", "purchaseOption": "Spot"})
        self.assertEqual(spec["resilience"], {"onWorkerLost": "Reschedule", "replace": ["Reuse", "Create"]})
        self.assertEqual(manifest["metadata"]["labels"], {"zenthros.ai/remote": "true"})
        self.assertTrue(handle.name.startswith("zr-double-"))

    def test_the_call_carries_the_sdk_that_runs_it(self):
        handle, client, _ = call(double, 1)
        inline = client.submitted(handle.name).spec.workload.template.source.inline
        with tarfile.open(fileobj=io.BytesIO(base64.b64decode(inline))) as tar:
            names = set(tar.getnames())
        self.assertIn("zenthros/remote/runner.py", names)
        self.assertIn("zenthros/remote/__main__.py", names)
        self.assertIn("zenthros/jobs/spec.py", names)
        self.assertIn("zenthros/core/checkpoint.py", names)
        self.assertIn("zenthros/core/task.py", names)
        handle, client, _ = call(double, 1, ship_sdk=False)
        self.assertIsNone(client.submitted(handle.name).spec.workload.template.source)

    def test_a_project_with_its_own_zenthros_keeps_it(self):
        with tempfile.TemporaryDirectory() as project:
            Path(project, "zenthros").mkdir()
            Path(project, "zenthros", "remote.py").write_bytes(b"# the project's\n")
            with tarfile.open(fileobj=io.BytesIO(remote.pack_code(project))) as tar:
                self.assertEqual(tar.getnames(), ["zenthros/remote.py"])
                self.assertEqual(tar.extractfile("zenthros/remote.py").read(), b"# the project's\n")

    def test_resilience_follows_the_placement(self):
        spec = lambda **kw: client.submitted(call(double, 1, client=client, **kw)[0].name).to_manifest()["spec"]
        client = FakeJobClient()
        self.assertEqual(spec(on=Backend(id="b1"))["resilience"], {"onWorkerLost": "Reschedule", "replace": ["Backends"]})
        # How many machines a call may lose is the controller's default, not retries=.
        self.assertNotIn("workerLostRetryLimit", spec(retries=2, retry_on=ConnectionError)["resilience"])
        self.assertEqual(spec(resilient=False)["resilience"], {"onWorkerLost": "Fail"})

    def test_labels_python_and_env_python(self):
        client = FakeJobClient()
        handle, _, _ = call(double, 1, client=client, labels={"zenthros.ai/e2e": "true"}, python="/opt/zenthros/bin/python3")
        job = client.submitted(handle.name)
        self.assertEqual(job.labels, {"zenthros.ai/e2e": "true", "zenthros.ai/remote": "true"})
        self.assertEqual(job.spec.workload.template.command[0], "/opt/zenthros/bin/python3")
        with mock.patch.dict(os.environ, {remote.PYTHON_ENV: "/opt/py/bin/python"}):
            handle, _, _ = call(double, 1, client=client)
        self.assertEqual(client.submitted(handle.name).spec.workload.template.command[0], "/opt/py/bin/python")

    def test_resilient_false_fails_with_the_machine(self):
        # With nothing said the CRD moves the job (spec.resilience.onWorkerLost
        # defaults to Reschedule), which is what resilient=False promises not to do.
        handle, client, _ = call(double, 1, resilient=False)
        self.assertEqual(client.submitted(handle.name).spec.resilience.on_worker_lost, "Fail")

    def test_resilient_moves_it(self):
        handle, client, _ = call(double, 1)
        self.assertEqual(client.submitted(handle.name).spec.resilience.on_worker_lost, "Reschedule")

    def test_run_needs_a_place(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with remote.configure(client=FakeJobClient(), blob_store=MemoryBlobStore(), store=STORE), \
                    self.assertRaises(errors.ValidationError):
                remote.run(double, 1)

    def test_value_comes_back(self):
        handle, client, _ = call(double, 21)
        self.assertEqual(handle.result(), 42)
        self.assertEqual(client.exit_codes[handle.name], 0)
        self.assertEqual(handle.machine["python"].rsplit(".", 1)[0], remote.python_version())
        self.assertGreaterEqual(handle.overhead()["compute"], 0)

    def test_remote_exception_is_raised_here_with_its_traceback(self):
        handle, client, _ = call(explode, "boom")
        with self.assertRaises(remote.RemoteError) as caught:
            handle.result()
        err = caught.exception
        self.assertEqual(client.exit_codes[handle.name], remote.EXIT_ERROR)
        self.assertEqual(err.remote_type, "ValueError")
        self.assertIn("boom", err.remote_traceback)
        self.assertIn("explode", err.remote_traceback)
        self.assertIsInstance(err.__cause__, ValueError)

    def test_an_exception_whose_init_takes_other_arguments_comes_back_whole(self):
        handle, _, _ = call(over_quota)
        with self.assertRaises(remote.RemoteError) as caught:
            handle.result()
        cause = caught.exception.__cause__
        self.assertIsInstance(cause, QuotaExceeded)
        self.assertEqual(cause.args, ("32 vCPU",))
        self.assertEqual(cause.code, "L-1216C47A")

    @machine_side
    def test_a_script_function_can_hand_its_helpers_to_multiprocessing(self):
        total = main_function("total")
        self.assertEqual(total.__module__, "__main__")
        handle, client, _ = call(total, 10)
        self.assertEqual(handle.result(), sum(n * n for n in range(10)))
        import sys

        self.assertIsNot(sys.modules["__main__"].__dict__.get("chunk"), main_function("chunk"))

    def test_other_python_is_refused_before_the_function_runs(self):
        store = MemoryBlobStore()
        marker = Path(tempfile.mkdtemp(), "ran")
        handle, client, _ = call(touch, str(marker), store=store, client=FakeJobClient())
        uri = handle.call_uri
        envelope = pickle.loads(store.get(uri))
        envelope["python"] = "3.99"
        store.put(uri, pickle.dumps(envelope))
        out = io.StringIO()
        with reporting(client, handle.name):
            self.assertEqual(execute(uri, store, out=out), remote.EXIT_MISMATCH)
        self.assertFalse(marker.exists(), "the function ran under another Python")
        self.assertIn("Python 3.99", out.getvalue())
        with self.assertRaises(remote.RemoteError) as caught:
            handle.result(stream_logs=False)
        self.assertEqual(caught.exception.remote_type, "PythonVersionMismatch")

    def test_a_superseded_attempt_writes_no_result(self):
        store = MemoryBlobStore()
        handle, _, _ = call(frozen, store=store, client=FakeJobClient())
        out = io.StringIO()
        self.assertEqual(execute(handle.call_uri, store, out=out), remote.EXIT_SUPERSEDED)
        self.assertNotIn(f"{handle.prefix}/result.pkl", store.data)
        self.assertIn("superseded", out.getvalue())

    def test_an_attempt_that_stops_after_a_notice_leaves_the_result_to_its_replacement(self):
        with tempfile.TemporaryDirectory() as d:
            notice = Path(d, "notice.json")
            with mock.patch.dict(os.environ, {"ZENTHROS_PREEMPT_FILE": str(notice)}):
                stopped, _, store = call(exit_with, 0, client=FakeJobClient())
                # Without a notice, an exit is the call's outcome.
                out = io.StringIO()
                self.assertEqual(execute(stopped.call_uri, store, out=out), remote.EXIT_ERROR)
                self.assertIn(f"{stopped.prefix}/result.pkl", store.data)

                notice.write_text('{"source": "aws-imds", "action": "terminate"}')
                stopped, _, store = call(exit_with, 0, client=FakeJobClient())
                out = io.StringIO()
                self.assertEqual(execute(stopped.call_uri, store, out=out),
                                 remote.EXIT_SUPERSEDED)
                self.assertNotIn(f"{stopped.prefix}/result.pkl", store.data)
                self.assertIn("interruption notice", out.getvalue())

                # One that finishes anyway has the answer: it is written.
                finished, _, store = call(double, 4, client=FakeJobClient())
                self.assertEqual(execute(finished.call_uri, store, out=io.StringIO()), 0)
                self.assertIn(f"{finished.prefix}/result.pkl", store.data)

    def test_a_task_is_a_call(self):
        from support import job

        handle, _, _ = call(Squares(4))
        self.assertTrue(handle.name.startswith("zr-squares-"), handle.name)
        self.assertEqual(handle.result(), 0 + 1 + 4 + 9)

        # At the notice it saves its step and stops: no result from this
        # attempt; the replacement resumes from that save.
        with tempfile.TemporaryDirectory() as d:
            notice = Path(d, "notice.json")
            notice.write_text('{"source": "aws-imds", "action": "terminate"}')
            stopped, _, store = call(Squares(4), client=FakeJobClient())
            with job(MemoryBlobStore(), env={"ZENTHROS_PREEMPT_FILE": str(notice)}) as agent:
                out = io.StringIO()
                code = execute(stopped.call_uri, store, out=out)
        self.assertEqual(code, remote.EXIT_SUPERSEDED)
        self.assertNotIn(f"{stopped.prefix}/result.pkl", store.data)
        self.assertIn("StoppedOnNotice", out.getvalue())
        self.assertEqual([(c["phase"], c["complete"]) for c in agent.acked], [("Squares", False)])

    def test_matching_python_runs_the_function(self):
        marker = Path(tempfile.mkdtemp(), "ran")
        self.assertEqual(call(touch, str(marker))[0].result(), str(marker))
        self.assertTrue(marker.exists())

    def test_package_drift_is_reported_not_fatal(self):
        store = MemoryBlobStore()
        handle, _, _ = call(double, 5, store=store, client=FakeJobClient())
        uri = handle.call_uri
        envelope = pickle.loads(store.get(uri))
        envelope["packages"] = {"cloudpickle": "0.0.1", "no-such-dist-zr": "1.0"}
        store.put(uri, pickle.dumps(envelope))
        out = io.StringIO()
        self.assertEqual(execute(uri, store, out=out), 0)
        self.assertIn("cloudpickle 0.0.1 (here", out.getvalue())
        self.assertIn("not installed here (the call may not use them): no-such-dist-zr 1.0", out.getvalue())

    def test_failed_job_without_result_names_the_logs(self):
        store = MemoryBlobStore()
        client = FakeJobClient()
        handle, _, _ = call(double, 1, store=store, client=client)
        client.fail(handle.name)
        with self.assertRaises(remote.RemoteError) as caught:
            handle.result(stream_logs=False)
        self.assertIn("last log lines", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, errors.JobFailed)

    def test_closures_travel_by_value(self):
        base = 10

        def add(x):
            return x + base

        self.assertEqual(call(add, 5)[0].result(), 15)
        self.assertEqual(call(lambda x: x * 3, 5)[0].result(), 15)

    def test_the_package_does_not_import_the_runner(self):
        # `python -m zenthros.remote` warns, and may run twice, if importing
        # zenthros already imported it.
        import subprocess
        import sys

        code = "import sys, zenthros; print('zenthros.remote' in sys.modules)"
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                             cwd=Path(__file__).resolve().parents[1], check=True)
        self.assertEqual(out.stdout.strip(), "False")
        self.assertIs(zenthros.run, remote.run)

    def test_code_dir_travels_and_imports_on_the_machine(self):
        with tempfile.TemporaryDirectory() as project:
            Path(project, "zr_helper_mod.py").write_text("def triple(x):\n    return 3 * x\n")

            def uses_helper(x):
                import zr_helper_mod

                return zr_helper_mod.triple(x)

            handle, _, _ = call(uses_helper, 7, code_dir=project)
            self.assertEqual(handle.result(), 21)

    def test_function_of_a_project_module_travels_by_value(self):
        import importlib
        import sys

        with tempfile.TemporaryDirectory() as project:
            Path(project, "zr_local_fns.py").write_text("def quad(x):\n    return 4 * x\n")
            sys.path.insert(0, project)
            try:
                module = importlib.import_module("zr_local_fns")
                self.assertTrue(remote.is_project_module(module))
                store = MemoryBlobStore()
                handle, _, _ = call(module.quad, 3, store=store, client=ExecutingClient(store))
            finally:
                sys.path.remove(project)
                sys.modules.pop("zr_local_fns", None)
            # The module is gone here, as it is on the machine: only a
            # by-value pickle still runs.
            self.assertEqual(handle.result(), 12)
            self.assertNotIn("zr_local_fns", cloudpickle.list_registry_pickle_by_value())

    def test_a_module_the_caller_registered_by_value_stays_registered(self):
        # cloudpickle raises nothing for a module already registered: the
        # SDK's own registration for one pickle must not undo the caller's.
        import importlib
        import sys

        from zenthros.remote import bundle

        with tempfile.TemporaryDirectory() as project:
            Path(project, "zr_caller_registered.py").write_text("def quint(x):\n    return 5 * x\n")
            sys.path.insert(0, project)
            try:
                module = importlib.import_module("zr_caller_registered")
                cloudpickle.register_pickle_by_value(module)
                bundle.dumps(module.quint, module.quint)
                self.assertIn("zr_caller_registered", cloudpickle.list_registry_pickle_by_value())
            finally:
                if "zr_caller_registered" in cloudpickle.list_registry_pickle_by_value():
                    cloudpickle.unregister_pickle_by_value(module)
                sys.path.remove(project)
                sys.modules.pop("zr_caller_registered", None)

    def test_installed_modules_go_by_reference(self):
        import json as stdlib_json

        self.assertFalse(remote.is_project_module(stdlib_json))
        self.assertFalse(remote.is_project_module(cloudpickle))

    def test_cloud_calls_locally_unless_asked(self):
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        f = zenthros.cloud(code_dir=None)(double)
        self.assertEqual(f(4), 8)
        self.assertEqual(client._order, [], "calling the function submitted a job")
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            self.assertEqual(f.remote(4), 8)
            self.assertEqual(len(client._order), 1)
            pending = f.submit(5)
            self.assertEqual(f.map([1, 2]).result(), [2, 4])
        self.assertIsInstance(pending, remote.RemoteCall)
        self.assertEqual(pending.result(), 10)
        self.assertEqual(f.__name__, "double")
        self.assertEqual(f.with_options(cpu="4").options["cpu"], "4")


class FakeProc:
    def __init__(self, lines):
        self.stdout = iter(lines)

    def wait(self, timeout=None):
        return 0

    def poll(self):
        return 0


class LogFollowerTest(unittest.TestCase):
    def test_reconnects_skip_printed_lines_and_a_new_attempt_prints_whole(self):
        streams = [
            ["a1\n", "a2\n"],                 # first attempt, zctl ends early
            ["a1\n", "a2\n", "a3\n"],          # same attempt again: only a3 is new
            ["b1\n", "b2\n"],                 # the call moved machines: all of it
        ]

        class Client:
            def follow_logs(self, name):
                return FakeProc(streams.pop(0)) if streams else FakeProc([])

        out = io.StringIO()
        follower = remote._LogFollower(Client(), "zr-x", poll_seconds=0.01, out=out)
        follower.start()
        deadline = time.time() + 5
        while streams and time.time() < deadline:
            time.sleep(0.01)
        follower.stop(grace=0.2)
        self.assertEqual(out.getvalue(), "a1\na2\na3\nb1\nb2\n")

    def test_stop_waits_for_the_running_stream_not_a_fixed_second(self):
        # The last lines come while zctl still streams: stop() waits for
        # it, and with nothing streaming it has nothing to wait for.
        release, done = threading.Event(), threading.Event()

        class Streaming:
            def __init__(self):
                self.stdout = self._lines()

            def _lines(self):
                yield "a\n"
                release.wait(5)
                yield "last\n"
                done.set()

            def poll(self):
                return 0 if done.is_set() else None

            def wait(self, timeout=None):
                if not done.wait(timeout):
                    raise subprocess.TimeoutExpired("zctl", timeout)
                return 0

        procs = [Streaming()]

        class Client:
            def follow_logs(self, name):
                return procs.pop(0) if procs else FakeProc([])

        out = io.StringIO()
        follower = remote._LogFollower(Client(), "zr-x", poll_seconds=0.01, out=out)
        follower.start()
        deadline = time.time() + 5
        while out.getvalue() != "a\n" and time.time() < deadline:
            time.sleep(0.01)
        threading.Timer(0.1, release.set).start()
        follower.stop(grace=2)
        self.assertEqual(out.getvalue(), "a\nlast\n")
        started = time.monotonic()
        follower.stop(grace=2)
        self.assertLess(time.monotonic() - started, 0.5)

    def follow(self, streams):
        class Client:
            def follow_logs(self, name):
                return FakeProc(streams.pop(0)) if streams else FakeProc([])

        out = io.StringIO()
        follower = remote._LogFollower(Client(), "zr-x", poll_seconds=0.01, out=out)
        follower.start()
        deadline = time.time() + 5
        while streams and time.time() < deadline:
            time.sleep(0.01)
        follower.stop(grace=0.2)
        return out.getvalue()

    def test_a_replay_that_starts_past_the_first_line_prints_nothing_twice(self):
        # After the unit stops, zctl replays the journal without systemd's
        # "Started" line: the same attempt, not a new one.
        self.assertEqual(
            self.follow([["Started\n", "tick 0\n", "tick 1\n"], ["tick 0\n", "tick 1\n", "tick 2\n"]]),
            "Started\ntick 0\ntick 1\ntick 2\n",
        )

    def test_a_replay_from_a_line_that_repeats_earlier_prints_nothing_twice(self):
        # The replay starts on the second blank line; matching it against the
        # first one printed "y" again (and teed it to logs.txt twice).
        self.assertEqual(
            self.follow([["\n", "x\n", "\n", "y\n"], ["\n", "y\n", "z\n"]]),
            "\nx\n\ny\nz\n",
        )

    def test_a_new_attempt_prints_its_lines_even_where_they_repeat(self):
        self.assertEqual(
            self.follow([["on a\n", "tick 0\n"], ["on b\n", "tick 0\n", "tick 1\n"]]),
            "on a\ntick 0\non b\ntick 0\ntick 1\n",
        )


class PackTest(unittest.TestCase):
    def tree(self, root):
        Path(root, "pkg").mkdir()
        Path(root, "main.py").write_text("print('main')\n")
        Path(root, "pkg", "mod.py").write_text("X = 1\n")
        Path(root, "data.csv").write_text("a,b\n1,2\n")
        for skipped in (".venv", ".git", "__pycache__", ".ipynb_checkpoints"):
            Path(root, skipped).mkdir()
            Path(root, skipped, "x.py").write_text("skip\n")

    def names(self, packed):
        with tarfile.open(fileobj=io.BytesIO(packed)) as tar:
            return sorted(tar.getnames())

    def test_project_files_without_tool_directories(self):
        # Small files beside the code travel too (SDK-5); environments,
        # caches and hidden directories do not.
        with tempfile.TemporaryDirectory() as root:
            self.tree(root)
            self.assertEqual(self.names(remote.pack_code(root, ship_sdk=False)), ["data.csv", "main.py", "pkg/mod.py"])
            shipped = self.names(remote.pack_code(root))
            self.assertIn("main.py", shipped)
            self.assertIn("zenthros/remote/runner.py", shipped)

    def test_same_tree_packs_the_same_bytes(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            self.tree(a)
            self.tree(b)
            os.utime(Path(b, "main.py"), (1, 1))
            self.assertEqual(remote.pack_code(a), remote.pack_code(b))

    def test_oversize_tree_is_refused(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "big.py").write_bytes(os.urandom(remote.MAX_CODE_BYTES + 1024))
            with self.assertRaises(errors.ValidationError) as caught:
                remote.pack_code(root)
            self.assertIn("code_dir", str(caught.exception))


def names(packed):
    with tarfile.open(fileobj=io.BytesIO(packed)) as tar:
        return sorted(tar.getnames())


def tree(root, files):
    for path, content in files.items():
        Path(root, path).parent.mkdir(parents=True, exist_ok=True)
        Path(root, path).write_bytes(content if isinstance(content, bytes) else content.encode())


class SubmitTest(unittest.TestCase):
    def test_a_refused_submit_leaves_the_existing_call_intact(self):
        # Reusing a name wrote call.pkl over the live job's before the create
        # was refused: the live job then refused its own call as tampered
        # (SDK-6). Each call is stored under its own digest now.
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            first = remote.run(double, 1, name="zr-same", code_dir=None)
            with self.assertRaises(errors.ValidationError):
                remote.run(double, 2, name="zr-same", code_dir=None)
        self.assertEqual(first.result(stream_logs=False), 2)


class BundleTest(unittest.TestCase):
    def test_command_carries_its_scripts_and_configs(self):
        # Only .py files travelled: run(["bash", "run.sh"]) or --config
        # conf.yaml found nothing on the machine (SDK-5).
        with tempfile.TemporaryDirectory() as project:
            tree(project, {"prep.py": "print(1)\n", "run.sh": "python prep.py\n", "conf/train.yaml": "lr: 0.1\n",
                           "big.bin": b"\0" * (remote.bundle.MAX_FILE_BYTES + 1)})
            self.assertEqual(names(remote.pack_code(project, ship_sdk=False)),
                             ["conf/train.yaml", "prep.py", "run.sh"])

    def test_ignored_files_and_data_stay_home(self):
        # No exclusion file: data/ was walked on every run, and a .py with
        # credentials went into the job (SDK-9).
        with tempfile.TemporaryDirectory() as project:
            tree(project, {
                "train.py": "", "pkg/__init__.py": "", "pkg/data/__init__.py": "", "pkg/data/load.py": "",
                "local_settings.py": "TOKEN = 'x'\n", "secrets/key.py": "", "model.ckpt": "w",
                "data/raw/gen_cache.py": "", "notes/scratch.py": "",
                ".gitignore": "local_settings.py\n*.ckpt\n",
                ".zenthrosignore": "# not for the machine\nsecrets/\nnotes/\n",
            })
            self.assertEqual(names(remote.pack_code(project, ship_sdk=False)),
                             ["pkg/__init__.py", "pkg/data/__init__.py", "pkg/data/load.py", "train.py"])


    def test_credentials_stay_home_unless_named(self):
        # A small file travels now (SDK-5): a demo.env or a key would too.
        with tempfile.TemporaryDirectory() as project:
            tree(project, {"train.py": "", "demo.env": "TOKEN=x\n", "deploy.key": "k", "conf/kubeconfig": "k",
                           "keep.env": "PUBLIC=1\n", ".zenthrosignore": "!keep.env\n"})
            self.assertEqual(names(remote.pack_code(project, ship_sdk=False)), ["keep.env", "train.py"])

    def test_cloud_credentials_stay_home(self):
        with tempfile.TemporaryDirectory() as project:
            tree(project, {"train.py": "", "prod.tfvars": "k", "infra/terraform.tfstate": "k",
                           "gcp-credentials.json": "{}", "conf/my-service-account.json": "{}",
                           "conf/params.json": "{}"})
            self.assertEqual(names(remote.pack_code(project, ship_sdk=False)), ["conf/params.json", "train.py"])

    def test_an_ignore_file_in_a_folder_applies_to_that_folder(self):
        # As git reads them: a folder's .gitignore names that folder's files,
        # and a pattern with a slash is anchored to the folder it is in.
        with tempfile.TemporaryDirectory() as project:
            tree(project, {"train.py": "", "app/main.py": "", "app/secrets.json": "{}", "app/out/x.py": "",
                           "app/.gitignore": "secrets.json\n/out/\n", "lib/out/keep.py": "", "lib/gen/gen.py": "",
                           "lib/secrets.json": "{}", "build.py": "", "pkg/build.py": "",
                           ".gitignore": "/build.py\n**/gen.py\n"})
            self.assertEqual(names(remote.pack_code(project, ship_sdk=False)),
                             ["app/main.py", "lib/out/keep.py", "lib/secrets.json", "pkg/build.py", "train.py"])


class LoadedPackagesTest(unittest.TestCase):
    def test_the_installed_distributions_are_read_once_per_process(self):
        # Reading them took 150-400 ms, on every run() and every part of a map.
        from importlib import metadata

        remote.bundle._owners.cache_clear()
        self.addCleanup(remote.bundle._owners.cache_clear)
        with mock.patch.object(metadata, "packages_distributions",
                               wraps=metadata.packages_distributions) as scanned:
            first = remote.bundle.loaded_packages()
            self.assertEqual(remote.bundle.loaded_packages(), first)
        self.assertEqual(scanned.call_count, 1)
        self.assertIn("cloudpickle", first)


class LogTailTest(unittest.TestCase):
    def test_failed_command_shows_the_end_of_a_long_log(self):
        # The log was cut to 256 KiB from its start and then its "last 30
        # lines" taken: the real error, at the end, never showed (SDK-10).
        api = FakeAPI()
        self.addCleanup(api.close)
        log = ("noise " * 20 + "\n") * 3000 + "THE REAL ERROR\n"
        api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (200, log.encode())
        client = HTTPJobClient(api.url, "risk", "t")
        tail = client.logs("j", tail=30)
        self.assertIn("THE REAL ERROR", tail)
        query = api.requests[-1]["path"].split("?", 1)[-1]
        self.assertIn("tail=30", query)


if __name__ == "__main__":
    unittest.main()
