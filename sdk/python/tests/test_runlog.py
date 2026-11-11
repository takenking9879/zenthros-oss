#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The local record of each call (.zenthros/runs/), for the user and the
assistant working beside them: what was asked, its log, its reports, and why
it failed, on disk where an assistant reads files.

Pinned: a failed call leaves error.txt (its type, message and remote
traceback) and logs.txt, and a line in index.jsonl; no secret value, env
value or token is in any file; logs.txt and the number of runs are bounded;
log_dir="off" (or ZENTHROS_LOG_DIR=off) and a job's own calls write nothing;
a disk that fails never fails the call; the folder never travels with the
code.
"""
import contextlib
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from zenthros import errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.remote import bundle, runlog


def adds(a, b):
    print(f"adding {a} and {b}")
    return a + b


def boom(x):
    print("about to fail")
    raise ValueError(f"no good: {x}")


class RunLogTest(unittest.TestCase):
    def setUp(self):
        from test_remote import STORE, ExecutingClient

        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = Path(self.dir.name) / "runs"
        self.store = MemoryBlobStore()
        self.client = ExecutingClient(self.store)
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo",
                                      log_dir=str(self.root), token="tok-not-in-any-file")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        patcher = mock.patch.dict(os.environ, {"ZENTHROS_JOB_ID": ""})
        patcher.start()
        self.addCleanup(patcher.stop)

    def index(self):
        return [json.loads(line) for line in (self.root / "index.jsonl").read_text().splitlines()]

    def every_file(self):
        return "\n".join(p.read_text(errors="replace") for p in self.root.rglob("*") if p.is_file())

    def test_a_call_says_what_was_asked_and_how_it_ended(self):
        call = remote.run(adds, 2, 3, cpu=2, memory="4Gi", code_dir=None)
        self.assertEqual(call.result(stream_logs=False), 5)
        here = self.root / call.name
        self.assertEqual(call.log_dir, str(here))
        asked = json.loads((here / "call.json").read_text())
        self.assertEqual((asked["job"], asked["function"], asked["pool"], asked["cpu"]), (call.name, "adds", "demo", 2))
        self.assertEqual(asked["status"], "Succeeded")
        self.assertIn("2", asked["args"])
        self.assertFalse((here / "error.txt").exists())
        events = [(e["job"], e["event"]) for e in self.index()]
        self.assertEqual(events, [(call.name, "submitted"), (call.name, "Succeeded")])
        self.assertTrue((self.root / "README.md").exists())
        self.assertEqual((self.root / ".gitignore").read_text().strip(), "*")

    def test_a_failure_leaves_its_error_and_its_log(self):
        call = remote.run(boom, 7, code_dir=None)
        with self.assertRaises(remote.RemoteError):
            call.result(stream_logs=False)
        here = self.root / call.name
        error = (here / "error.txt").read_text()
        for part in ("ValueError", "no good: 7", "Traceback"):
            self.assertIn(part, error)
        self.assertEqual(error.count("Traceback (most recent call last)"), 1, error)
        # Its log was not followed (stream_logs=False): its end is fetched.
        self.assertIn(f"fake job {call.name}", (here / "logs.txt").read_text())
        last = self.index()[-1]
        self.assertEqual((last["event"], last["error"].splitlines()[0][:10]), ("Failed", "ValueError"))
        self.assertEqual(remote.last_failure(), str(here))

    def test_reports_are_one_json_object_each_and_only_the_codes(self):
        from zenthros.jobs.handle import ReportView
        from zenthros.remote.call import RESULT_PRODUCER

        call = remote.run(adds, 2, 3, code_dir=None)
        mine = ReportView(job_id="j", producer_id="host-a", payload={"epoca": 3, "loss": 0.33}, sequence=3,
                          produced_at="1792128932")
        internal = ReportView(job_id="j", producer_id=f"{RESULT_PRODUCER}-g1", payload={"zenthros.result": {}})
        with mock.patch.object(type(call), "reports", lambda self: [mine, internal]):
            call.result(stream_logs=False)
        rows = [json.loads(line) for line in (self.root / call.name / "reports.jsonl").read_text().splitlines()]
        self.assertEqual(rows, [{"producer_id": "host-a", "sequence": 3, "produced_at": "1792128932", "final": False,
                                 "payload": {"epoca": 3, "loss": 0.33}}])

    def test_no_secret_env_value_or_token_is_written(self):
        secret = remote.Secret.aws("prod/db", key="url")
        call = remote.run(adds, 1, 1, env={"PLAIN": "env-value-not-in-any-file"}, secrets={"DB_URL": secret},
                          code_dir=None)
        call.result(stream_logs=False)
        written = self.every_file()
        for value in ("env-value-not-in-any-file", "tok-not-in-any-file"):
            self.assertNotIn(value, written)
        asked = json.loads((self.root / call.name / "call.json").read_text())
        self.assertEqual(asked["env"], ["PLAIN"])
        self.assertEqual(asked["secrets"], {"DB_URL": "aws-secrets-manager"})

    def test_arguments_are_cut_to_their_bound(self):
        call = remote.run(adds, "x" * 50_000, "y", code_dir=None)
        asked = json.loads((self.root / call.name / "call.json").read_text())
        self.assertLessEqual(len(asked["args"]), runlog.MAX_REPR)

    def test_the_log_and_the_runs_are_bounded(self):
        with mock.patch.object(runlog, "MAX_LOG_BYTES", 200), mock.patch.object(runlog, "MAX_RUNS", 3):
            record = runlog.open("zr-long-1", remote.settings())
            for i in range(100):
                record.line(f"line {i:03d}\n")
            text = (self.root / "zr-long-1" / "logs.txt").read_text()
            self.assertLessEqual(len(text.encode()), 200)
            self.assertTrue(text.endswith("line 099\n"))
            for i in range(5):
                runlog.open(f"zr-many-{i}", remote.settings()).started({"job": f"zr-many-{i}"})
        runs = sorted(p.name for p in self.root.iterdir() if p.is_dir())
        self.assertEqual(runs, ["zr-many-2", "zr-many-3", "zr-many-4"])

    def test_a_folder_of_the_users_keeps_its_files(self):
        # log_dir naming a project folder: its README.md and folders stay,
        # and no "*" .gitignore hides it from git (audit 2026-10-16 NEW-009).
        project = Path(self.dir.name) / "project"
        (project / "data").mkdir(parents=True)
        (project / "data" / "train.csv").write_text("a,b\n")
        (project / "README.md").write_text("# mine\n")
        with remote.configure(log_dir=str(project)), mock.patch.object(runlog, "MAX_RUNS", 2):
            for i in range(3):
                runlog.open(f"zr-proj-{i}", remote.settings()).started({"job": f"zr-proj-{i}"})
        self.assertEqual((project / "README.md").read_text(), "# mine\n")
        self.assertFalse((project / ".gitignore").exists())
        self.assertTrue((project / "data" / "train.csv").exists())
        self.assertEqual(sorted(p.name for p in project.iterdir() if p.name.startswith("zr-")), ["zr-proj-1", "zr-proj-2"])
        # The default folder, or a new one, still ignores itself, and never
        # replaces a file already there.
        fresh = Path(self.dir.name) / "fresh"
        with remote.configure(log_dir=str(fresh)):
            runlog.open("zr-fresh-0", remote.settings()).started({"job": "zr-fresh-0"})
        self.assertEqual((fresh / ".gitignore").read_text(), "*\n")
        (fresh / "README.md").write_text("edited\n")
        (fresh / ".gitignore").unlink()
        with remote.configure(log_dir=str(fresh)):
            runlog.open("zr-fresh-1", remote.settings()).started({"job": "zr-fresh-1"})
        self.assertEqual((fresh / "README.md").read_text(), "edited\n")

    def test_off_and_a_jobs_own_calls_write_nothing(self):
        with remote.configure(log_dir="off"):
            remote.run(adds, 1, 2, code_dir=None).result(stream_logs=False)
        self.assertFalse(self.root.exists())
        with remote.configure(log_dir=""), mock.patch.dict(os.environ, {"ZENTHROS_LOG_DIR": "off"}):
            self.assertIsNone(runlog.open("zr-x", remote.settings()))
        with mock.patch.dict(os.environ, {"ZENTHROS_JOB_ID": "job-1"}):
            self.assertIsNone(runlog.open("zr-x", remote.settings()))

    def test_a_client_given_by_configure_writes_only_where_log_dir_says(self):
        # configure(client=) is how tests run calls: no record in the folder
        # they run from unless they name one.
        with remote.configure(log_dir=""), mock.patch.dict(os.environ, {"ZENTHROS_LOG_DIR": ""}):
            self.assertIsNone(runlog.open("zr-x", remote.settings()))
        with remote.configure(log_dir="", client=None), mock.patch.dict(os.environ, {"ZENTHROS_LOG_DIR": ""}):
            self.assertEqual(runlog.open("zr-x", remote.settings()).root, os.path.abspath(runlog.DEFAULT_DIR))

    def test_a_failing_disk_never_fails_the_call(self):
        blocked = Path(self.dir.name) / "a-file"
        blocked.write_text("not a folder")
        err = io.StringIO()
        with remote.configure(log_dir=str(blocked)), contextlib.redirect_stderr(err):
            first = remote.run(adds, 1, 2, code_dir=None)
            second = remote.run(adds, 3, 4, code_dir=None)
            self.assertEqual((first.result(stream_logs=False), second.result(stream_logs=False)), (3, 7))
        self.assertEqual(err.getvalue().count("local log"), 1, err.getvalue())

    def test_an_interrupted_call_says_so(self):
        call = remote.run(adds, 1, 2, code_dir=None)
        with mock.patch.object(call.handle, "wait", side_effect=KeyboardInterrupt), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(KeyboardInterrupt):
            call.result(stream_logs=False)
        self.assertEqual(self.index()[-1]["event"], "Interrupted")

    def test_the_folder_never_travels(self):
        project = Path(self.dir.name) / "project"
        (project / ".zenthros" / "runs" / "zr-a").mkdir(parents=True)
        (project / ".zenthros" / "runs" / "zr-a" / "logs.txt").write_text("a log")
        (project / "train.py").write_text("print(1)\n")
        packed = bundle.pack_code(str(project), ship_sdk=False)
        with tarfile.open(fileobj=io.BytesIO(packed)) as tar:
            names = tar.getnames()
        self.assertIn("train.py", names)
        self.assertFalse([n for n in names if ".zenthros" in n], names)


class ErrorTextTest(unittest.TestCase):
    """error.txt's text: the type and message first, then what the machine
    printed, once."""

    def test_a_traceback_not_in_the_message_is_added(self):
        from zenthros.remote.call import RemoteError, _error_text

        text = _error_text(RemoteError("zr-x: exited 2", remote_type="CommandFailed", remote_traceback="TB-LINE"))
        self.assertTrue(text.startswith("CommandFailed: zr-x: exited 2"))
        self.assertIn("TB-LINE", text)

    def test_a_platform_failure_says_its_reason(self):
        from zenthros.remote.call import _error_text

        failed = errors.JobFailed("the image could not be pulled", reasons=[{"code": "IMAGE_PULL_FAILED"}])
        self.assertEqual(_error_text(failed), "JobFailed IMAGE_PULL_FAILED: the image could not be pulled\n")


class CutTest(unittest.TestCase):
    def test_large_bytes_are_cut_before_their_repr_is_built(self):
        # An argument of 200 MB of bytes became an 800 MB repr, then was cut.
        class Big(bytes):
            def __repr__(self):
                raise AssertionError("the whole value was put in text")

        text = runlog.cut(Big(b"x" * (runlog.MAX_REPR * 4)))
        self.assertLessEqual(len(text), runlog.MAX_REPR)
        self.assertTrue(text.startswith("b'xxx") and "..." in text, text[:20])


if __name__ == "__main__":
    unittest.main()
