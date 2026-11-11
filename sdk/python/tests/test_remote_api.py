#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.run's surface, without a cluster: commands, map/gather, upstream
calls, retry_on, image, secrets, timeout, get, and what result() returns
for each way a job ends. ExecutingClient (test_remote) runs each job here."""

import contextlib
import io
import os
import pickle
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import cloudpickle

from zenthros import errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import call
from zenthros.view import Backend

from support import execute
from test_remote import STORE, ExecutingClient, double, explode


def add(a, b):
    return a + b


def total(values):
    return sum(values)


def flaky(path, fails, exc=ConnectionError):
    """Raises exc the first `fails` times it runs (counted in path)."""
    ran = int(Path(path).read_text() or 0) if Path(path).exists() else 0
    Path(path).write_text(str(ran + 1))
    if ran < fails:
        raise exc(f"attempt {ran + 1}")
    return ran + 1


def item_or_boom(x):
    if x == 1:
        raise ValueError("bad item")
    return x


class Env(unittest.TestCase):
    def setUp(self):
        self.store = MemoryBlobStore()
        self.client = ExecutingClient(self.store)
        self.configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo-ml")
        self.configured.__enter__()
        self.addCleanup(self.configured.__exit__, None, None, None)

    def manifest(self, call):
        return self.client.submitted(call.name).to_manifest()["spec"]


class CommandTest(Env):
    def test_a_list_is_argv_and_a_string_runs_in_sh(self):
        call = remote.run(["python", "prep.py", "--month", "9"], code_dir=None)
        template = self.manifest(call)["workload"]["template"]
        self.assertEqual(template["command"], ["python", "prep.py", "--month", "9"])
        self.assertTrue(call.name.startswith("zr-prep-"), call.name)
        self.assertEqual(call.kind, "command")
        shell = remote.run("echo hi > $ZENTHROS_DATA_OUTPUT/x", code_dir=None)
        self.assertEqual(self.manifest(shell)["workload"]["template"]["command"][:2], ["sh", "-c"])

    def test_the_command_runs_its_files_and_the_images_zenthros(self):
        with tempfile.TemporaryDirectory() as project:
            Path(project, "prep.py").write_text("print('prep')\n")
            call = remote.run(["python", "prep.py"], code_dir=project)
        inline = self.client.submitted(call.name).spec.workload.template.source.inline
        import base64
        import io
        import tarfile

        with tarfile.open(fileobj=io.BytesIO(base64.b64decode(inline))) as tar:
            # The SDK core is not shipped: on the machine's sys.path it would
            # hide the image's zenthros (its train, data...).
            self.assertEqual(tar.getnames(), ["prep.py"])

    def test_result_is_zero_or_the_failure_with_its_log(self):
        client = FakeJobClient()
        with remote.configure(client=client):
            ok = remote.run(["true"], code_dir=None)
            self.assertEqual(ok.result(stream_logs=False), 0)
            bug = remote.run(["false"], code_dir=None)
            client.fail(bug.name, "PROCESS_FAILED")
            with self.assertRaises(remote.RemoteError) as caught:
                bug.result(stream_logs=False)
            self.assertIn("last log lines", str(caught.exception))
            platform = remote.run(["true"], code_dir=None, image="nope:1")
            client.fail(platform.name, "IMAGE_PULL_FAILED")
            with self.assertRaises(errors.JobFailed) as caught:
                platform.result(stream_logs=False)
            self.assertEqual(caught.exception.reason, "IMAGE_PULL_FAILED")

    def test_a_command_takes_no_python_arguments_and_retries_by_rerunning(self):
        with self.assertRaises(errors.ValidationError):
            remote.run(["python", "x.py"], 1)
        with self.assertRaises(errors.ValidationError):
            remote.run(["python", "x.py"], retry_on=ConnectionError)
        call = remote.run(["python", "x.py"], code_dir=None, retries=2)
        self.assertEqual(self.manifest(call)["workload"]["template"]["restartPolicy"], {"backoffLimit": 2})


class ResultTest(Env):
    def setUp(self):
        super().setUp()
        # A refused result waits out the grace for its digest (60 s);
        # test_result_integrity pins the wait.
        grace = mock.patch("zenthros.remote.call.DIGEST_GRACE", 0.2)
        grace.start()
        self.addCleanup(grace.stop)

    def test_a_value_only_from_a_job_that_succeeded(self):
        # Found in the suite: a job Failed with CHECKPOINT_INCOMPLETE still
        # had result.pkl, and result() returned it. A failed job's value is
        # not its result.
        client = FakeJobClient()
        with remote.configure(client=client):
            call = remote.run(double, 21, code_dir=None)
            execute(call.call_uri, self.store, out=_quiet())
            client.fail(call.name, "CHECKPOINT_INCOMPLETE")
            with self.assertRaises(errors.JobFailed) as caught:
                call.result(stream_logs=False)
        self.assertEqual(caught.exception.reason, "CHECKPOINT_INCOMPLETE")

    def test_a_bug_is_a_remote_error_with_the_cause(self):
        call = remote.run(explode, "boom", code_dir=None)
        with self.assertRaises(remote.RemoteError) as caught:
            call.result(stream_logs=False)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(call.attempts, 1)


class RetryTest(Env):
    def setUp(self):
        super().setUp()
        patch = mock.patch("zenthros.remote.runner.RETRY_BASE_SECONDS", 0)
        patch.start()
        self.addCleanup(patch.stop)
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, "ran")

    def test_retry_on_retries_those_exceptions_in_place(self):
        call = remote.run(flaky, self.path, 2, retries=3, retry_on=ConnectionError, code_dir=None)
        self.assertEqual(call.result(stream_logs=False), 3)
        self.assertEqual(call.info()["attempts"], 3)
        self.assertEqual(len(self.client.exit_codes), 1, "one job: the retries ran on its machine")

    def test_past_retries_the_last_error_is_the_result(self):
        call = remote.run(flaky, self.path, 5, retries=2, retry_on=(ConnectionError, TimeoutError), code_dir=None)
        with self.assertRaises(remote.RemoteError) as caught:
            call.result(stream_logs=False)
        self.assertIn("attempt 3", str(caught.exception))
        self.assertEqual(call.attempts, 3)

    def test_a_bug_is_not_retried(self):
        call = remote.run(flaky, self.path, 1, ValueError, retries=3, retry_on=ConnectionError, code_dir=None)
        with self.assertRaises(remote.RemoteError):
            call.result(stream_logs=False)
        self.assertEqual(Path(self.path).read_text(), "1")

    def test_stopping_for_a_replacement_is_never_retried(self):
        call = remote.run(flaky, self.path, 1, errors.StoppedOnNotice, retries=3, retry_on=Exception, code_dir=None)
        with self.assertRaises(remote.RemoteError):
            call.result(stream_logs=False)
        self.assertEqual(Path(self.path).read_text(), "1")

    def test_retries_need_the_exceptions_named(self):
        for bad in (dict(retries=2), dict(retries=-1, retry_on=OSError), dict(retries=1, retry_on="OSError")):
            with self.subTest(**{k: str(v) for k, v in bad.items()}), self.assertRaises(errors.ValidationError):
                remote.run(double, 1, **bad)


class MapGatherTest(Env):
    def test_more_tasks_than_a_job_runs_is_refused_before_anything_is_stored(self):
        puts = []
        real = self.store.put
        with mock.patch.object(self.store, "put", lambda uri, data: puts.append(uri) or real(uri, data)):
            with self.assertRaises(errors.ValidationError) as caught:
                remote.map(double, range(call.MAX_TASKS + 1), code_dir=None)
        self.assertIn(str(call.MAX_TASKS), str(caught.exception))
        self.assertEqual(puts, [])
        self.assertEqual(self.client._jobs, {})

    def test_one_job_a_task_per_item_and_the_results_in_order(self):
        calls = remote.map(double, [1, 2, 3], code_dir=None)
        template = self.manifest(calls)["workload"]["template"]
        self.assertEqual(template["distribution"], {"mode": "Independent", "count": 3})
        self.assertEqual(template["command"][-2], "map")
        self.assertEqual(calls.result(stream_logs=False), [2, 4, 6])
        single = remote.map(double, [5], code_dir=None)
        self.assertNotIn("distribution", self.manifest(single)["workload"]["template"])
        self.assertEqual(single.result(stream_logs=False), [10])

    def test_a_member_takes_its_item_from_the_controllers_task_index(self):
        # distribution.count members get TASK_INDEX (operator/api/v1
        # distribution_types.go), not a ZENTHROS_-prefixed name.
        calls = remote.map(double, [1, 2, 3], code_dir=None)
        uri = calls.call_uri
        with mock.patch.dict(os.environ, {"TASK_INDEX": "2", "TASK_COUNT": "3"}):
            self.assertEqual(execute(uri, self.store, out=io.StringIO()), 0)
        record = pickle.loads(self.store.get(f"{calls.prefix}/results/2.pkl"))
        self.assertTrue(record["ok"])
        self.assertEqual(cloudpickle.loads(record["value"]), 6)

    def test_a_failed_item_names_its_index(self):
        calls = remote.map(item_or_boom, [0, 1, 2], code_dir=None)
        with self.assertRaises(remote.RemoteError) as caught:
            calls.result(stream_logs=False)
        self.assertIn("item 1", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, ValueError)

    def test_each_items_outcome_reads_its_result_once(self):
        # outcomes() read and verified every result again after result() had.
        calls = remote.map(item_or_boom, [0, 1, 2], code_dir=None)
        reads = []
        real = self.store.get
        with mock.patch.object(self.store, "get", lambda uri: reads.append(uri) or real(uri)):
            outcomes = calls.outcomes(stream_logs=False)
        self.assertEqual([ok for ok, _ in outcomes], [True, False, True])
        self.assertEqual(outcomes[0][1], 0)
        results = [uri for uri in reads if "/results/" in uri]
        self.assertEqual(sorted(results), sorted(set(results)))

    def test_a_collected_map_names_one_result_it_finds_and_reads_no_more(self):
        calls = remote.map(double, [1, 2, 3], code_dir=None)
        calls.handle.wait()
        # Collected after its TTL: the controller no longer has it.
        calls.handle.wait = mock.Mock(side_effect=errors.JobNotFound(f"zenthrosjob {calls.name} not found"))
        reads = []
        real = self.store.get
        with mock.patch.object(self.store, "get", lambda uri: reads.append(uri) or real(uri)):
            with self.assertRaises(errors.JobNotFound) as caught:
                calls.result(stream_logs=False)
        self.assertIn(f"{calls.prefix}/results/0.pkl (and maybe the other items')", str(caught.exception))
        self.assertEqual([uri for uri in reads if "/results/" in uri], [f"{calls.prefix}/results/0.pkl"])

    def test_gather(self):
        a, b = remote.run(double, 1, code_dir=None), remote.run(explode, "no", code_dir=None)
        m = remote.map(double, [2, 3], code_dir=None)
        self.assertEqual(remote.gather(a, m), [2, [4, 6]])
        self.assertEqual(remote.gather([a]), [2])
        with self.assertRaises(remote.RemoteError):
            remote.gather(a, b)
        got = remote.gather(a, b, return_exceptions=True)
        self.assertEqual(got[0], 2)
        self.assertIsInstance(got[1], remote.RemoteError)

    def test_map_takes_runs_options_and_refuses_others(self):
        calls = remote.map(double, [1, 2], gpu="A10G", spot=True, code_dir=None, name="zr-dbl")
        self.assertEqual(calls.name, "zr-dbl")
        self.assertEqual(self.manifest(calls)["compute"], {"gpuCount": 1, "gpuModel": "A10G", "purchaseOption": "Spot"})
        with self.assertRaises(errors.ValidationError):
            remote.map(double, [1], gpus=2)
        with self.assertRaises(errors.ValidationError):
            remote.map(double, [])


class ResolveTest(unittest.TestCase):
    """The machine swaps upstream calls for their values; every other
    argument reaches the function as the caller passed it."""

    def test_containers_without_an_upstream_are_the_callers_objects(self):
        import collections

        from zenthros.remote.runner import resolve

        Point = collections.namedtuple("Point", "x y")
        counts = collections.defaultdict(int, a=1)
        for value in (Point(1, 2), counts, collections.OrderedDict(b=2), collections.Counter("aab"), [1, (2, 3)]):
            self.assertIs(resolve(value, None), value)
        counts_back = resolve(counts, None)
        self.assertEqual(counts_back["missing"], 0)

    def test_containers_with_an_upstream_keep_their_type(self):
        import collections

        from zenthros.remote.call import COMMAND_KIND, Upstream
        from zenthros.remote.runner import resolve

        Point = collections.namedtuple("Point", "x y")
        out = Upstream("cmd", "printed", COMMAND_KIND)
        point = resolve(Point(1, out), None)
        self.assertEqual((type(point), point), (Point, Point(1, "printed")))
        counts = resolve(collections.defaultdict(int, a=out), None)
        self.assertEqual((counts["a"], counts["missing"]), ("printed", 0))
        self.assertEqual(resolve([out, {"k": (out,)}], None), ["printed", {"k": ("printed",)}])


class UpstreamTest(Env):
    def test_a_call_as_an_argument_waits_for_it_and_gets_its_value(self):
        a = remote.run(double, 2, code_dir=None)
        b = remote.run(add, a, 1, code_dir=None)
        self.assertEqual(self.manifest(b)["dependsOn"], [a.name])
        self.assertEqual(b.result(stream_logs=False), 5)
        self.assertLess([e for e in self.client.events if e[0] == "wait"].index(("wait", a.name)),
                        [e for e in self.client.events if e[0] == "wait"].index(("wait", b.name)))

    def test_inside_containers_and_map_items(self):
        a, b = remote.run(double, 1, code_dir=None), remote.run(double, 2, code_dir=None)
        c = remote.run(total, [a, b, 10], code_dir=None)
        self.assertEqual(sorted(self.manifest(c)["dependsOn"]), sorted([a.name, b.name]))
        self.assertEqual(c.result(stream_logs=False), 16)
        m = remote.map(double, [a, b], code_dir=None)
        self.assertEqual(m.result(stream_logs=False), [4, 8])
        s = remote.run(total, remote.map(double, [1, 2], code_dir=None), code_dir=None)
        self.assertEqual(s.result(stream_logs=False), 6)

    def test_a_commands_value_is_its_output(self):
        client = FakeJobClient()
        with remote.configure(client=client):
            prep = remote.run(["python", "prep.py"], code_dir=None)
        self.assertEqual(pickle.loads(pickle.dumps(prep)).value(self.store), prep.output)
        self.assertEqual(prep.output, f"{STORE}/{prep.name}")

    def test_after_orders_without_passing_a_value(self):
        a = remote.run(double, 1, code_dir=None)
        b = remote.run(double, 2, after=[a, "zr-other"], code_dir=None)
        self.assertEqual(self.manifest(b)["dependsOn"], [a.name, "zr-other"])

    def test_a_failed_upstream_fails_the_downstream(self):
        a = remote.run(explode, "upstream", code_dir=None)
        b = remote.run(add, a, 1, code_dir=None)
        with self.assertRaises(errors.JobFailed) as caught:
            b.result(stream_logs=False)
        self.assertEqual(caught.exception.reason, "DEPENDENCY_FAILED")


class SpecTest(Env):
    def test_the_call_records_the_python_bundle_reports_at_submit(self):
        # hello_remote.py fakes a caller on another Python by patching
        # remote.bundle.python_version; the package's re-export is a copy,
        # and patching it left the call runnable (Floci, 2026-09-30).
        with mock.patch.object(remote.bundle, "python_version", lambda: "3.99"):
            call = remote.run(double, 2, code_dir=None)
        envelope = pickle.loads(self.store.get(call.call_uri))
        self.assertEqual(envelope["python"], "3.99")
        with self.assertRaises(remote.RemoteError) as caught:
            call.result(stream_logs=False)
        self.assertEqual(caught.exception.remote_type, "PythonVersionMismatch")

    def test_image_leaves_the_runtime_to_the_pool(self):
        # The controller runs a pool job's image in containerd on a VM pool and
        # in the container on a Container pool, which refuses containerd: the
        # SDK cannot know the pool's kind, so it asks for no runtime.
        spec = self.manifest(remote.run(double, 1, image="ghcr.io/acme/train:1", code_dir=None))
        self.assertEqual(spec["workload"]["template"]["image"], "ghcr.io/acme/train:1")
        self.assertNotIn("compute", spec)
        spec = self.manifest(remote.run(double, 1, image="img:1", on=Backend(id="k8s-a"), code_dir=None))
        self.assertNotIn("compute", spec, "a cluster runs images anyway: the runtime is the machine's word")

    def test_secrets_are_references_as_env_or_file(self):
        spec = self.manifest(remote.run(double, 1, code_dir=None, secrets={
            "DB_URL": remote.Secret.aws("prod/db", key="url", region="us-east-1"),
            "/run/secrets/gcp.json": remote.Secret.k8s("gcp", "key.json"),
        }))
        self.assertEqual(spec["workload"]["template"]["secretSources"], [
            {"type": "k8s-secret", "name": "gcp", "key": "key.json",
             "materialization": {"mode": "file", "path": "/run/secrets/gcp.json"}},
            {"type": "aws-secrets-manager",
             "awsSecretsManager": {"secretId": "prod/db", "region": "us-east-1", "jsonKey": "url"},
             "materialization": {"mode": "env", "envName": "DB_URL"}},
        ])
        with self.assertRaises(errors.ValidationError):
            remote.run(double, 1, secrets={"DB_URL": "postgres://user:pw@db"})
        with self.assertRaises(errors.ValidationError):
            remote.run(double, 1, secrets={"not a name": remote.Secret.k8s("a", "b")})

    def test_secrets_on_an_untrusted_host_take_an_opt_in(self):
        secrets = {"DB_URL": remote.Secret.k8s("db", "url")}
        optin = "zenthros.ai/secrets-on-untrusted-host"
        call = remote.run(double, 1, code_dir=None, secrets=secrets)
        self.assertNotIn(optin, self.client.submitted(call.name).annotations)
        with remote.configure(untrusted_host_secrets=True):
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                call = remote.run(double, 1, code_dir=None, secrets=secrets)
        meta = self.client.submitted(call.name).to_manifest()["metadata"]
        self.assertEqual(meta["annotations"][optin], "true")
        self.assertIn("third party", stderr.getvalue())
        with remote.configure(untrusted_host_secrets=True):
            call = remote.run(double, 1, code_dir=None)
        self.assertNotIn(optin, self.client.submitted(call.name).annotations, "no secrets: nothing to consent to")

    def test_timeout(self):
        for given, want in (("2h", 7200), ("30m", 1800), ("45s", 45), (90, 90), ("1.5h", 5400)):
            spec = self.manifest(remote.run(double, 1, timeout=given, code_dir=None))
            self.assertEqual(spec["activeDeadlineSeconds"], want)
        for bad in ("soon", -1, 0):
            with self.assertRaises(errors.ValidationError):
                remote.run(double, 1, timeout=bad)


class LifecycleTest(Env):
    def test_get_finds_a_call_from_its_name(self):
        started = remote.run(double, 21, code_dir=None, name="zr-answer")
        found = remote.get("zr-answer")
        self.assertEqual((found.name, found.kind, found.prefix), (started.name, "call", started.prefix))
        self.assertEqual(found.result(stream_logs=False), 42)
        remote.map(double, [1, 2], code_dir=None, name="zr-pair")
        self.assertEqual(remote.get("zr-pair").result(stream_logs=False), [2, 4])
        with self.assertRaises(errors.JobNotFound):
            remote.get("zr-nobody")

    def test_info_and_reports(self):
        call = remote.run(double, 2, code_dir=None)
        call.result(stream_logs=False)
        info = call.info()
        self.assertEqual((info["phase"], info["kind"], info["attempts"]), ("Succeeded", "call", 1))
        self.assertEqual(info["output"], call.prefix)
        self.assertIn("python", info["machine"])
        self.assertTrue(call.reports())

    def test_configure_restores_and_knows_its_settings(self):
        with remote.configure(workspace="ws-b"):
            self.assertEqual(remote.settings().workspace, "ws-b")
        self.assertEqual(remote.settings().workspace, os.environ.get("ZENTHROS_WORKSPACE", ""))
        with self.assertRaises(errors.ValidationError):
            remote.configure(stor="s3://typo")


def _quiet():
    import io

    return io.StringIO()


if __name__ == "__main__":
    unittest.main()
