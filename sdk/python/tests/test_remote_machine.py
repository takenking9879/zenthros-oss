# SPDX-License-Identifier: Apache-2.0

"""The runner as a machine runs it: in a process of its own, from the
unpacked bundle, with only the job's environment.

ExecutingClient (test_remote.py) runs the runner in this process, where
sys.modules and sys.path already hold the caller's modules, and passes the
task index itself: both hid bugs a machine meets (audit 2026-10-03 SDK-1,
SDK-3). These tests pay a subprocess per task for that.
"""

import base64
import contextlib
import io
import os
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
import uuid
from pathlib import Path

from zenthros import errors, remote
from zenthros.core.storage import FileBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import FAILED, SUCCEEDED

from support import ReportAgent, machine_side

STORE = "s3://zr-machine/calls"


def triple(x):
    return 3 * x


class SubprocessClient(FakeJobClient):
    """Runs each task of a job as the agent does: the bundle unpacked in a
    directory of its own, the command in a new process there, with the
    template's env, TASK_INDEX and TASK_COUNT only when the job is
    distributed, and the store under a local root."""

    def __init__(self, root):
        super().__init__()
        self.root = root
        self.outputs = []
        self.exit_codes = {}
        # Per task: the files left in its directory when the command ended.
        self.leftovers = []

    def wait(self, job_id, timeout=None, on_reschedule=None, cancel_on_timeout=False):
        if job_id not in self.exit_codes:
            self.exit_codes[job_id] = self._run(self.submitted(job_id))
        code = self.exit_codes[job_id]
        self._jobs[job_id]["phase"] = SUCCEEDED if code == 0 else FAILED
        if code != 0:
            raise errors.JobFailed(f"job {job_id} failed", reasons=[{"code": "PROCESS_FAILED"}])
        return SUCCEEDED

    def _run(self, job):
        with ReportAgent(self) as endpoint:
            return self._run_tasks(job, endpoint)

    def _run_tasks(self, job, endpoint):
        template = job.spec.workload.template
        tasks = template.distribution.count if template.distribution else 1
        codes = []
        for index in range(tasks):
            with tempfile.TemporaryDirectory() as work:
                if template.source and template.source.inline:
                    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(template.source.inline))) as tar:
                        tar.extractall(work, filter="data")
                env = {k: v for k, v in os.environ.items() if not k.startswith(("ZENTHROS_", "TASK_"))}
                env.pop("PYTHONPATH", None)
                env.update({e.name: e.value for e in template.env})
                env["ZENTHROS_CHECKPOINT_LOCAL_ROOT"] = self.root
                env.update(ZENTHROS_REPORT_ENDPOINT=endpoint, ZENTHROS_JOB_ID=job.name)
                if template.distribution:
                    env.update(TASK_INDEX=str(index), TASK_COUNT=str(tasks))
                command = [sys.executable, *template.command[1:]]
                proc = subprocess.run(command, cwd=work, env=env, capture_output=True, text=True, timeout=120)
                self.outputs.append(proc.stdout + proc.stderr)
                codes.append(proc.returncode)
                self.leftovers.append(sorted(
                    os.path.relpath(os.path.join(d, f), work).replace(os.sep, "/") for d, _, files in os.walk(work) for f in files))
        return max(codes)


@contextlib.contextmanager
def machine():
    with tempfile.TemporaryDirectory() as root:
        store = FileBlobStore(root=root)
        client = SubprocessClient(root)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            yield client, store


class MachineTest(unittest.TestCase):
    def test_a_map_of_one_item_returns_its_result(self):
        # One item: no distribution, so no TASK_INDEX on the machine. The
        # runner wrote results/-1.pkl and result() looked for results/0.pkl
        # (SDK-3).
        with machine() as (client, _):
            call = remote.map(triple, [7], code_dir=None)
            try:
                self.assertEqual(call.result(stream_logs=False), [21])
            except Exception:
                print("\n".join(client.outputs))
                raise

    def test_a_map_of_two_items_returns_both_in_order(self):
        with machine() as (client, _):
            call = remote.map(triple, [1, 2], code_dir=None)
            self.assertEqual(call.result(stream_logs=False), [3, 6])

    @machine_side
    def test_project_code_under_src_is_importable_on_the_machine(self):
        # `python src/train.py` from the project root puts src/ on sys.path:
        # train imports model as a top-level module. On the machine only the
        # unpacked root was on sys.path, so unpickling train's function found
        # no module named model (SDK-1).
        tag = uuid.uuid4().hex[:8]
        model, train = f"zr_model_{tag}", f"zr_train_{tag}"
        with tempfile.TemporaryDirectory() as project, _project(project, model, train) as module:
            with machine() as (client, _):
                call = remote.run(module.fit, 4, code_dir=project)
                try:
                    self.assertEqual(call.result(stream_logs=False), 16)
                except Exception:
                    print("\n".join(client.outputs))
                    raise


@contextlib.contextmanager
def _project(project, model, train):
    """project/src/{model,train}.py, imported as `python src/train.py` would:
    src/ first on sys.path, the cwd the project root."""
    src = Path(project, "src")
    src.mkdir()
    Path(src, f"{model}.py").write_text("def square(x):\n    return x * x\n")
    Path(src, f"{train}.py").write_text(textwrap.dedent(f"""
        import {model}

        def fit(x):
            return {model}.square(x)
    """))
    before_path, before_cwd = list(sys.path), os.getcwd()
    sys.path.insert(0, str(src))
    os.chdir(project)
    try:
        import importlib

        yield importlib.import_module(train)
    finally:
        os.chdir(before_cwd)
        sys.path[:] = before_path
        sys.modules.pop(train, None)
        sys.modules.pop(model, None)


if __name__ == "__main__":
    unittest.main()
