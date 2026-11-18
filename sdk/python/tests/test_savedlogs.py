# SPDX-License-Identifier: Apache-2.0

"""A call's whole log in its bucket (logs=): what the spec says, how it is
read back, checked and completed from the machine, and the wait for its
END.json before the machine goes."""

import hashlib
import io
import json
import os
import struct
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import console, errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs import logrecords
from zenthros.jobs import spec as S
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import savedlogs

STORE = "s3://zr-test/calls"
BUCKET = "s3://logs-bucket/runs"


def double(x):
    return 2 * x


def lines(first, n):
    return b"".join(b"%08d %s\n" % (i, b"x" * 40) for i in range(first, first + n))


def sealed(text, before, cursor, gap=0):
    """An object as the agent seals it (agent/logpersist): zstd frames of up
    to 128 KiB of the log, then the trailer, which counts what never came
    through it (gap)."""
    zstd = logrecords.zstd_module()
    frames = b"".join(zstd.compress(text[i:i + savedlogs.MAX_FRAME]) for i in range(0, len(text), savedlogs.MAX_FRAME))
    said = {"cursor": cursor, "bytes": before + len(text), "sha256": hashlib.sha256(frames).hexdigest()}
    if gap:
        said["gap"] = gap
    said = json.dumps(said).encode()
    return frames + struct.pack("<II", savedlogs.TRAILER_MAGIC, len(said)) + said


class Machine(FakeJobClient):
    """The fake, whose machine keeps a log and answers after a cursor."""

    def __init__(self):
        super().__init__()
        self.rest, self.asked, self.gone = "", [], False

    def whole_log(self, job_id, after=None):
        self.asked.append(after)
        if self.gone:
            raise errors.JobNotFound(f"{job_id}: its machine is gone")
        return self.rest

    def logs(self, job_id, tail=None):
        if self.gone:
            raise errors.TransientError(f"{job_id}: its machine is gone")
        return super().logs(job_id, tail)


class SavedLogTest(unittest.TestCase):
    def setUp(self):
        self.bucket = MemoryBlobStore()
        self.client = Machine()
        self.configured = remote.configure(store=STORE, client=self.client, blob_store=self.bucket, ship_sdk=True)
        self.configured.__enter__()
        self.addCleanup(self.configured.__exit__, None, None, None)
        self.call = remote.run(double, 1, on="demo-ml", code_dir=None, logs=zenthros.Logs(BUCKET))
        # The handle as the agent escapes it into one segment (url.PathEscape).
        self.folder = f"{BUCKET}/{self.call.name}/uid-{self.call.name}/g1-a0/job:ns%2Fpod/"
        self.dir = tempfile.mkdtemp()

    def keep(self, *texts, end=None, folder=None, gaps=(), end_gap=0):
        """The bucket holds texts as objects (gaps: what never came through
        each), and END.json if end."""
        folder, before, log = folder or self.folder, 0, b""
        for i, text in enumerate(texts):
            gap = gaps[i] if i < len(gaps) else 0
            self.bucket.put(f"{folder}{i:08d}.log.zst", sealed(text, before, str(before + len(text)), gap))
            before += len(text)
            log += text
        if end:
            said = {"status": end, "objects": len(texts), "bytes": before}
            if end_gap:
                said["gap"] = end_gap
            self.bucket.put(folder + "END.json", json.dumps(said).encode())
        return log

    def saved(self):
        path = os.path.join(self.dir, "x.log")
        self.assertEqual(self.call.logs(to=path), path)
        return Path(path).read_bytes()

    def test_the_jobs_spec_says_where_its_log_goes_and_the_runner_keeps_no_copy(self):
        template = self.client.submitted(self.call.name).spec.workload.template
        self.assertEqual(template.logs.destination, BUCKET)
        self.assertIn((savedlogs.LOGS_SAVED_ENV, "1"), [(e.name, e.value) for e in template.env])
        # Without logs=, nothing is said.
        plain = remote.run(double, 1, on="demo-ml", code_dir=None)
        template = self.client.submitted(plain.name).spec.workload.template
        self.assertIsNone(template.logs)
        self.assertNotIn(savedlogs.LOGS_SAVED_ENV, [e.name for e in template.env])
        # configure(logs=) is every call's, and logs=False takes it back.
        with remote.configure(logs=True):
            stored = remote.run(double, 1, on="demo-ml", code_dir=None)
            unsaved = remote.run(double, 1, on="demo-ml", code_dir=None, logs=False)
        self.assertEqual(self.client.submitted(stored.name).spec.workload.template.logs.destination, STORE + "/logs")
        self.assertIsNone(self.client.submitted(unsaved.name).spec.workload.template.logs)

    def test_the_whole_log_comes_from_its_bucket(self):
        log = self.keep(lines(0, 3000), lines(3000, 3000), lines(6000, 10), end="complete")
        self.client.gone = True
        self.assertEqual(self.saved(), log)

    def test_a_log_not_all_saved_goes_on_from_the_machine_after_the_last_object(self):
        log = self.keep(lines(0, 3000), lines(3000, 3000))
        self.client.rest = lines(6000, 50).decode()
        self.assertEqual(self.saved(), log + lines(6000, 50))
        after = self.client.asked[-1]
        # The handle as the agent named it, and the last object's cursor.
        self.assertEqual((after.handle, after.cursor), ("job:ns/pod", str(len(log))))

    def test_a_machine_gone_before_all_was_saved_leaves_it_said_incomplete(self):
        log = self.keep(lines(0, 3000))
        self.client.gone = True
        text = self.saved()
        self.assertTrue(text.startswith(log))
        self.assertIn(b"incomplete", text[len(log):])

    def test_a_part_never_saved_is_said_where_it_is_and_a_cut_where_it_was_cut(self):
        self.keep(lines(0, 100), end="truncated")
        self.assertIn(b"cut here", self.saved()[-100:])
        # 2 KB missing before the second object, and a part no one counted
        # after the last.
        first, second = lines(0, 100), lines(100, 100)
        self.keep(first, second, gaps=(0, 2048), end="gap", end_gap=-1)
        text = self.saved()
        note = b"[zenthros: %s of this log were never saved here" % logrecords.size(2048).encode()
        self.assertTrue(text.startswith(first + note), text[len(first) - 20:len(first) + 120])
        self.assertIn(b"]\n" + second + b"[zenthros: a part of this log was never saved here", text)
        self.assertTrue(text.endswith(b"it is missing]\n"))

    def test_a_corrupt_object_is_refused_and_no_file_is_left(self):
        self.keep(lines(0, 3000), lines(3000, 3000), end="complete")
        uri = self.folder + "00000001.log.zst"
        broken = bytearray(self.bucket.get(uri))
        broken[10] ^= 0xFF
        self.bucket.put(uri, bytes(broken))
        path = os.path.join(self.dir, "x.log")
        with self.assertRaises(logrecords.Corrupt):
            self.call.logs(to=path)
        self.assertEqual(os.listdir(self.dir), [])

    def test_an_object_that_unpacks_to_more_than_it_says_is_refused(self):
        frames = logrecords.zstd_module().compress(b"y" * (64 << 10))
        said = json.dumps({"cursor": "1", "bytes": 10, "sha256": hashlib.sha256(frames).hexdigest()}).encode()
        self.bucket.put(self.folder + "00000000.log.zst", frames + struct.pack("<II", savedlogs.TRAILER_MAGIC, len(said)) + said)
        with self.assertRaises(logrecords.Corrupt):
            self.saved()
        # A frame past the 128 KiB an agent packs is refused whatever it says.
        frames = logrecords.zstd_module().compress(b"y" * (savedlogs.MAX_FRAME + 1))
        said = json.dumps({"bytes": savedlogs.MAX_FRAME + 1, "sha256": hashlib.sha256(frames).hexdigest()}).encode()
        self.bucket.put(self.folder + "00000000.log.zst", frames + struct.pack("<II", savedlogs.TRAILER_MAGIC, len(said)) + said)
        with self.assertRaises(logrecords.Corrupt):
            self.saved()

    def test_the_run_the_job_is_on_is_the_one_read(self):
        # One that saved nothing yet is read from its machine, never an
        # earlier attempt's copy, whose END.json would pass for the whole log.
        root = f"{BUCKET}/{self.call.name}/uid-{self.call.name}/"
        self.keep(b"first\n", end="complete", folder=root + "g1-a0/job:ns%2Fpod-a/")
        self.client.place(self.call.name, "job:ns/pod-b")
        self.client.rest = "second, from its machine\n"
        self.assertEqual(self.saved(), b"second, from its machine\n")
        self.keep(b"second\n", end="complete", folder=root + "g1-a1/job:ns%2Fpod-b/")
        self.assertEqual(self.saved(), b"second\n")
        self.client.place(self.call.name, "job:ns/pod-a")
        self.assertEqual(self.saved(), b"first\n")

    def test_the_last_attempt_is_the_one_read(self):
        root = f"{BUCKET}/{self.call.name}/uid-{self.call.name}/"
        self.keep(b"first\n", end="complete", folder=root + "g1-a0/h/")
        self.keep(b"second\n", end="complete", folder=root + "g2-a0/h/")
        self.keep(b"third\n", end="complete", folder=root + "g2-a1/h/")
        self.assertEqual(self.saved(), b"third\n")

    def test_without_logs_it_is_all_the_machine_keeps(self):
        plain = remote.run(double, 1, on="demo-ml", code_dir=None)
        self.client.rest = "[zenthros: the first 1.0 MB of this log are not shown]\nend\n"
        path = os.path.join(self.dir, "plain.log")
        plain.logs(to=path)
        self.assertEqual(Path(path).read_text(), self.client.rest)
        self.assertIsNone(self.client.asked[-1])

    def test_the_end_of_the_log_comes_from_its_bucket_once_the_machine_is_gone(self):
        first = lines(0, 20000)  # 980 KB
        log = self.keep(first, lines(20000, 100), end="complete")
        self.client.gone = True
        text = self.call.logs()
        note, kept = text.split("\n", 1)
        self.assertLessEqual(len(kept), logrecords.HISTORY_BYTES)
        self.assertTrue(log.endswith(kept.encode()))
        self.assertEqual(note + "\n", logrecords.omitted_note(len(log) - len(kept)))
        self.assertEqual(self.call.logs(tail=3), lines(20097, 3).decode())


class SpecTest(unittest.TestCase):
    def test_logs_true_without_a_store_is_refused(self):
        with self.assertRaises(errors.ValidationError) as caught:
            savedlogs.spec_for(zenthros.Logs(), "", {}, [])
        self.assertIn("zenthros store", str(caught.exception))

    def test_logs_true_is_the_stores_with_the_credentials_the_call_got(self):
        key = S.SecretSource(type="encrypted-payload", materialization=S.Materialization(mode="env", env_name="AWS_ACCESS_KEY_ID"))
        other = S.SecretSource(type="encrypted-payload", materialization=S.Materialization(mode="env", env_name="HF_TOKEN"))
        logs = savedlogs.spec_for(zenthros.Logs(), STORE, {"AWS_REGION": "eu-west-3"}, [key, other])
        self.assertEqual((logs.destination, logs.region, logs.credentials), (STORE + "/logs", "eu-west-3", [key]))
        # A bucket of its own is written with the machine's role.
        mine = savedlogs.spec_for(zenthros.Logs("s3://mine/logs"), STORE, {"AWS_REGION": "eu-west-3"}, [key])
        self.assertEqual((mine.destination, mine.region, mine.credentials), ("s3://mine/logs", "", []))

    def test_a_store_reached_over_http_and_bad_options_are_refused(self):
        with self.assertRaises(errors.ValidationError):
            savedlogs.spec_for(zenthros.Logs(), STORE, {"AWS_ENDPOINT_URL": "http://10.0.0.1:4566"}, [])
        for bad in (dict(destination="https://x"), dict(endpoint="http://x"), dict(credentials={"HOME": None}),
                    dict(max_bytes=10)):
            with self.subTest(bad), self.assertRaises(errors.ValidationError):
                zenthros.Logs(**{"destination": "s3://bkt/p", **bad})
        with self.assertRaises(errors.ValidationError):
            savedlogs.chosen(3)

    def test_credentials_are_secrets_the_agent_alone_resolves(self):
        logs = savedlogs.spec_for(zenthros.Logs("s3://bkt/p", credentials={
            "AWS_ACCESS_KEY_ID": zenthros.Secret.workspace("logs-id"),
            "AWS_SECRET_ACCESS_KEY": zenthros.Secret.workspace("logs-key")}), STORE, {}, [])
        self.assertEqual([(c.type, c.name, c.materialization.env_name) for c in logs.credentials],
                         [("workspace-secret", "logs-id", "AWS_ACCESS_KEY_ID"), ("workspace-secret", "logs-key", "AWS_SECRET_ACCESS_KEY")])
        with self.assertRaises(errors.ValidationError):
            savedlogs.spec_for(zenthros.Logs("s3://bkt/p", credentials={"AWS_ACCESS_KEY_ID": "AKIA-in-clear"}), STORE, {}, [])


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.bucket, self.client = MemoryBlobStore(), Machine()
        self.configured = remote.configure(store=STORE, client=self.client, blob_store=self.bucket, ship_sdk=True)
        self.configured.__enter__()
        self.addCleanup(self.configured.__exit__, None, None, None)

    def made(self, **kw):
        """A call on a machine made for it: released once its result is in."""
        call = remote.run(double, 1, on="demo-ml", code_dir=None, logs=zenthros.Logs(BUCKET), **kw)
        call.released = []
        call._release = lambda: call.released.append(time.monotonic())
        return call

    def test_the_machine_waits_for_the_logs_end_then_goes(self):
        call = self.made()
        folder = f"{BUCKET}/{call.name}/uid-{call.name}/g1-a0/h/"
        threading.Timer(1.5, lambda: self.bucket.put(folder + "END.json", b'{"status": "complete", "objects": 0, "bytes": 0}')).start()
        began, out = time.monotonic(), io.StringIO()
        with mock.patch.object(sys, "stderr", out):
            call.release()
        self.assertGreaterEqual(call.released[0] - began, 1.4)
        self.assertEqual(out.getvalue(), "")

    def test_a_slow_bucket_is_waited_for_then_the_log_said_incomplete(self):
        call, out = self.made(), io.StringIO()
        with mock.patch.object(savedlogs, "END_WAIT", 0.5), mock.patch.object(sys, "stderr", out):
            began = time.monotonic()
            call.release()
        self.assertTrue(call.released and call.released[0] - began >= 0.5)
        self.assertIn("incomplete", out.getvalue())

    def test_a_bucket_that_refused_the_log_is_not_waited_for(self):
        call = self.made()
        job = self.client._jobs[call.name]
        original = self.client.job_object

        def refused(job_id):
            obj = original(job_id)
            obj["status"]["reasons"] = [{"code": savedlogs.LOGS_NOT_SAVED, "message": "AccessDenied"}]
            return obj

        self.assertTrue(job)
        with mock.patch.object(self.client, "job_object", refused):
            began = time.monotonic()
            call.release()
        self.assertLess(call.released[0] - began, 0.5)

    def test_ctrl_c_in_the_wait_still_lets_the_machine_go(self):
        call = self.made()
        with mock.patch.object(savedlogs.time, "sleep", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            call.release()
        self.assertEqual(len(call.released), 1)

    def test_gather_waits_for_each_calls_log(self):
        from test_remote import ExecutingClient

        client = ExecutingClient(self.bucket)
        with remote.configure(client=client):
            calls = [self.made() for _ in range(2)]
        for call in calls:
            self.bucket.put(f"{BUCKET}/{call.name}/uid-{call.name}/g1-a0/h/END.json",
                            b'{"status": "complete", "objects": 0, "bytes": 0}')
        with remote.configure(client=client), mock.patch.object(sys, "stdout", io.StringIO()):
            self.assertEqual(remote.gather(*calls), [2, 2])
        self.assertEqual([len(c.released) for c in calls], [1, 1])


class TranscriptTest(unittest.TestCase):
    def test_what_shows_goes_to_the_file_too_each_line_whole_in_order(self):
        path = os.path.join(tempfile.mkdtemp(), "session.log")
        shown = io.StringIO()
        with mock.patch.object(sys, "stdout", shown):
            with zenthros.transcript(path):
                print("before")

                def follow():
                    for i in range(200):
                        console.write(f"remote {i:03d} " + "r" * 200 + "\n")

                thread = threading.Thread(target=follow)
                thread.start()
                for i in range(200):
                    console.write(f"local {i:03d} " + "l" * 200 + "\n")
                thread.join()
                print("after", file=sys.stderr)
            print("not kept")
        kept = Path(path).read_text().splitlines()
        self.assertEqual(kept[0], "before")
        self.assertEqual(kept[-1], "after")
        self.assertEqual(len(kept), 402)
        for line in kept[1:-1]:
            self.assertRegex(line, r"^(remote \d{3} r{200}|local \d{3} l{200})$")
        self.assertEqual([x for x in kept if x.startswith("remote")], [f"remote {i:03d} " + "r" * 200 for i in range(200)])
        self.assertIn("not kept", shown.getvalue())
        self.assertNotIn("not kept", Path(path).read_text())

    def test_in_jupyter_a_threads_lines_reach_its_cell_and_the_file(self):
        # H3: in a notebook, a call's log is followed on a thread of its own
        # and written to the kernel's stdout, which the transcript wraps.
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpec

        path = os.path.join(tempfile.mkdtemp(), "nb.log")
        manager = KernelManager()
        manager._kernel_spec = KernelSpec(argv=[sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                                          display_name="sdk", language="python")
        manager.start_kernel(env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])})
        self.addCleanup(manager.shutdown_kernel, now=True)
        kernel = manager.client()
        kernel.start_channels()
        self.addCleanup(kernel.stop_channels)
        kernel.wait_for_ready(timeout=60)
        cell = kernel.execute("import threading, zenthros\n"
                              "from zenthros import console\n"
                              f"with zenthros.transcript({path!r}):\n"
                              "    t = threading.Thread(target=lambda: [console.write(f'line {i}\\n') for i in range(50)])\n"
                              "    t.start(); t.join()\n")
        shown = ""
        while True:
            msg = kernel.get_iopub_msg(timeout=30)
            if msg["parent_header"].get("msg_id") != cell:
                continue
            if msg["msg_type"] == "stream":
                shown += msg["content"]["text"]
            elif msg["msg_type"] == "error":
                self.fail("\n".join(msg["content"]["traceback"]))
            elif msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle":
                break
        want = "".join(f"line {i}\n" for i in range(50))
        self.assertEqual(shown, want)
        self.assertEqual(Path(path).read_text(), want)

if __name__ == "__main__":
    unittest.main()
