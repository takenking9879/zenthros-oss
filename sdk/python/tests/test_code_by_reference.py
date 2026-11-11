# SPDX-License-Identifier: Apache-2.0

"""A function's project goes to the store by digest (audit 2026-10-03 SDK-8).

Every job carried the project and the SDK's core inline in its spec, and
each fan-out child a copy: tens of KB rewritten with every status update.
The project is stored once at <store>/code/<sha256>.tar.gz now and the call
pins {uri, sha256}; only the SDK's core rides inline, for the runner to
start. The runner unpacks the project only if its bytes match the pin.
"""

import base64
import hashlib
import io
import os
import pickle
import tarfile
import tempfile
import unittest
from pathlib import Path

from zenthros import remote
from zenthros.core.storage import FileBlobStore, MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import runner

from support import execute
from test_remote import STORE, ExecutingClient, double
from test_remote_machine import machine


def inline_names(client, name):
    inline = client.submitted(name).spec.workload.template.source.inline
    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(inline))) as tar:
        return sorted(tar.getnames())


def write_outputs():
    os.makedirs("results", exist_ok=True)
    for path in ("out.txt", "results/metrics.csv", "conf/written.yaml"):
        with open(path, "w") as handle:
            handle.write(path)
    with open("conf/train.yaml") as handle:
        return handle.read()


def read_config():
    with open("conf/train.yaml") as handle:
        return handle.read()


class CodeByReferenceTest(unittest.TestCase):
    def setUp(self):
        self.project = tempfile.mkdtemp()
        Path(self.project, "conf").mkdir()
        Path(self.project, "conf", "train.yaml").write_text("lr: 0.1\n")
        Path(self.project, "train.py").write_text("print('train')\n")
        self.store = MemoryBlobStore()

    def submit(self, client, fn=double, *args):
        with remote.configure(store=STORE, client=client, blob_store=self.store, on="demo-ml"):
            return remote.run(fn, *(args or (1,)), code_dir=self.project)

    def test_the_spec_carries_the_sdk_core_and_a_reference_to_the_project(self):
        client = FakeJobClient()
        call = self.submit(client)
        names = inline_names(client, call.name)
        self.assertTrue(all(name.startswith("zenthros/") for name in names), names)
        self.assertIn("zenthros/remote/runner.py", names)
        for unused in ("zenthros/infra.py", "zenthros/config.py", "zenthros/jobs/fake.py"):
            self.assertNotIn(unused, names, "the machine never uses it (I.7)")
        code = pickle.loads(self.store.get(call.call_uri))["code"]
        self.assertTrue(code["uri"].startswith(f"{STORE}/code/") and code["uri"].endswith(".tar.gz"))
        self.assertEqual(hashlib.sha256(self.store.get(code["uri"])).hexdigest(), code["sha256"])

    def test_the_code_is_stored_before_the_call_and_the_call_before_the_job(self):
        # ⟂ A job never starts without its call, nor a call without its code.
        events = []
        real_put = self.store.put
        self.store.put = lambda uri, payload: (events.append(("put", uri)), real_put(uri, payload))[1]
        client = ExecutingClient(self.store, events)
        call = self.submit(client)
        code = next(i for i, e in enumerate(events) if e[0] == "put" and "/code/" in e[1])
        stored = events.index(("put", call.call_uri))
        submit = next(i for i, e in enumerate(events) if e[0] == "submit")
        self.assertLess(code, stored)
        self.assertLess(stored, submit)

    def test_the_same_project_is_stored_once(self):
        client = FakeJobClient()
        puts = []
        real_put = self.store.put
        self.store.put = lambda uri, payload: (puts.append(uri), real_put(uri, payload))[1]
        self.submit(client)
        self.submit(client)
        self.assertEqual(len([uri for uri in puts if "/code/" in uri]), 1)

    def test_each_run_makes_its_store_and_the_project_is_still_stored_once(self):
        # A run() gets a new store object for the same bucket each time
        # (store_for, a presigned store): the project went up on every run.
        with tempfile.TemporaryDirectory() as root:
            puts = []

            class Counted(FileBlobStore):
                def put(self, uri, payload):
                    puts.append(uri)
                    return super().put(uri, payload)

            for _ in range(2):
                with remote.configure(store=STORE, client=FakeJobClient(), blob_store=Counted(root), on="demo-ml"):
                    remote.run(double, 1, code_dir=self.project)
            self.assertEqual(len([uri for uri in puts if "/code/" in uri]), 1)
            # Another place is another store: sent there too.
            with remote.configure(store=STORE, client=FakeJobClient(), blob_store=Counted(root + "/other"),
                                  on="demo-ml"):
                remote.run(double, 1, code_dir=self.project)
            self.assertEqual(len([uri for uri in puts if "/code/" in uri]), 2)

    def test_the_runner_refuses_code_other_than_the_pinned(self):
        client = FakeJobClient()
        call = self.submit(client)
        code = pickle.loads(self.store.get(call.call_uri))["code"]
        self.store.put(code["uri"], b"not the project")
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as work:
            before = os.getcwd()
            os.chdir(work)
            try:
                self.assertEqual(execute(call.call_uri, self.store, out=out), runner.EXIT_TAMPERED)
            finally:
                os.chdir(before)
        self.assertIn("not the code the call was submitted with", out.getvalue())
        with self.assertRaises(FileNotFoundError):
            self.store.get(f"{call.prefix}/result.pkl")


class ProjectOnTheMachineTest(unittest.TestCase):
    def test_the_function_runs_from_its_project(self):
        # The machine unpacks the stored project and runs the function from
        # it, as it ran from the project root: relative paths still work.
        with tempfile.TemporaryDirectory() as project:
            Path(project, "conf").mkdir()
            Path(project, "conf", "train.yaml").write_text("lr: 0.1\n")
            with machine() as (client, _):
                call = remote.run(read_config, code_dir=project)
                try:
                    self.assertEqual(call.result(stream_logs=False), "lr: 0.1\n")
                except Exception:
                    print("\n".join(client.outputs))
                    raise

    def test_what_the_function_writes_stays_in_the_jobs_directory(self):
        # Files the function writes under relative paths land in the job's
        # directory, as before the project came by reference: the runner
        # deleted them with its copy of the project (post-fix review
        # 2026-10-03 R-14). The copy itself goes.
        with tempfile.TemporaryDirectory() as project:
            Path(project, "conf").mkdir()
            Path(project, "conf", "train.yaml").write_text("lr: 0.1\n")
            with machine() as (client, _):
                call = remote.run(write_outputs, code_dir=project)
                try:
                    self.assertEqual(call.result(stream_logs=False), "lr: 0.1\n")
                except Exception:
                    print("\n".join(client.outputs))
                    raise
                left = client.leftovers[-1]
        for path in ("out.txt", "results/metrics.csv", "conf/written.yaml"):
            self.assertIn(path, left)
        self.assertFalse([p for p in left if p.startswith(".zenthros-code-")], left)


if __name__ == "__main__":
    unittest.main()
