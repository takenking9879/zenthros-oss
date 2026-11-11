#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""A snapshot: the code a command runs, as the worktree holds it at that moment.
A real git repository in a temporary folder; the store is in memory."""
import hashlib
import io
import os
import stat
import subprocess
import tarfile
import tempfile
import time
import unittest
from unittest import mock

from zenthros import errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import bundle, runner, snapshot
from zenthros.remote import call as remote_call

from support import machine_side

STORE = "s3://bucket/calls"


def git(root, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=root, check=True, capture_output=True)


class Repo(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = folder.name
        git(self.root, "init", "-q")
        self.write(".gitignore", "*.log\nbuilt/\n")
        self.write("src/a.py", "print('a')\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "base")

    def write(self, path, text="x", mode=None):
        full = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as handle:
            handle.write(text)
        if mode:
            os.chmod(full, mode)
        return full


class WhatTravelsTest(Repo):
    def test_what_a_python_project_leaves_behind_travels_here(self):
        """The project rules leave out what a repository's tests need: hidden
        files, large data files, data/ and build/, and every +x."""
        self.write(".github/workflow.yml")
        self.write("testdata/large.yaml", "x" * (bundle.MAX_FILE_BYTES + 1))
        self.write("data/seed.csv")
        self.write("scripts/run.sh", "#!/bin/sh\n", 0o755)
        taken = snapshot.take(self.root)
        for path in (".github/workflow.yml", "testdata/large.yaml", "data/seed.csv", "scripts/run.sh", "src/a.py", ".gitignore"):
            self.assertIn(path, taken.files)

    def test_what_git_ignores_stays(self):
        self.write("debug.log")
        self.write("built/out.bin")
        taken = snapshot.take(self.root)
        self.assertNotIn("debug.log", taken.files)
        self.assertNotIn("built/out.bin", taken.files)

    def test_what_ran_here_before_stays(self):
        """Local runs keep their results in .zenthros/: no part of the code."""
        self.write(".zenthros/local/store/old/result.pkl")
        self.assertEqual([path for path in snapshot.take(self.root).files if path.startswith(".zenthros")], [])

    def test_credentials_stay_even_if_committed_and_it_says_so(self):
        self.write(".env", "SECRET=1")
        self.write("deploy/id_rsa")
        self.write("gcp/service-account-prod.json")
        git(self.root, "add", "-f", ".env", "deploy/id_rsa", "gcp/service-account-prod.json")
        git(self.root, "commit", "-qm", "oops")
        taken = snapshot.take(self.root)
        self.assertEqual(sorted(taken.excluded), [".env", "deploy/id_rsa", "gcp/service-account-prod.json"])
        for path in taken.excluded:
            self.assertNotIn(path, taken.files)

    def test_a_tracked_file_removed_is_not_there(self):
        os.remove(os.path.join(self.root, "src/a.py"))
        self.assertNotIn("src/a.py", snapshot.take(self.root).files)

    def test_a_link_to_a_folder_does_not_stop_it(self):
        try:
            os.symlink("src", os.path.join(self.root, "link"))
        except OSError as err:  # Windows without developer mode: no links for its user
            self.skipTest(f"this system lets its user make no links: {err}")
        self.assertNotIn("link", snapshot.take(self.root).files)

    def test_outside_a_repository_says_so(self):
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(errors.ValidationError) as caught:
            snapshot.take(folder)
        self.assertIn("git", str(caught.exception))


class IdentityTest(Repo):
    def test_the_same_tree_is_the_same_bytes(self):
        one = snapshot.take(self.root).pack()
        os.utime(os.path.join(self.root, "src/a.py"), (1, 1))
        self.assertEqual(one, snapshot.take(self.root).pack(), "no dates, owners or order in them")

    def test_uncommitted_changes_are_what_goes_and_it_says_so(self):
        clean = snapshot.take(self.root)
        self.assertFalse(clean.dirty)
        self.assertEqual(len(clean.commit), 40)
        before = clean.pack()
        self.write("src/a.py", "print('edited')\n")
        dirty = snapshot.take(self.root)
        self.assertTrue(dirty.dirty)
        self.assertEqual(dirty.commit, clean.commit)
        self.assertNotEqual(dirty.pack(), before)

    def test_a_new_file_not_ignored_goes(self):
        self.write("new.py")
        self.assertIn("new.py", snapshot.take(self.root).files)

    def test_x_is_packed_from_what_git_keeps_on_every_system(self):
        # Windows keeps no executable bit: there git's index says it (a file
        # it tracks); elsewhere the file's own bit, as git takes it.
        self.write("scripts/run.sh", "#!/bin/sh\necho hi\n", 0o755)
        git(self.root, "add", "--chmod=+x", "scripts/run.sh")
        with tarfile.open(fileobj=io.BytesIO(snapshot.take(self.root).pack())) as tar:
            modes = {member.name: member.mode for member in tar.getmembers()}
        self.assertEqual((modes["scripts/run.sh"], modes["src/a.py"]), (0o755, 0o644))

    @machine_side
    def test_x_survives_the_trip(self):
        self.write("scripts/run.sh", "#!/bin/sh\necho hi\n", 0o755)
        packed = snapshot.take(self.root).pack()
        with tempfile.TemporaryDirectory() as into:
            bundle.extract(packed, into)
            self.assertTrue(os.stat(os.path.join(into, "scripts/run.sh")).st_mode & stat.S_IXUSR)
            self.assertFalse(os.stat(os.path.join(into, "src/a.py")).st_mode & stat.S_IXUSR)


class WhileItIsEditedTest(Repo):
    def edits(self, times):
        """A file another agent edits while this packs, times times."""
        taken = snapshot.take(self.root)
        real, left = bundle._pack, [times]

        def pack(*args, **kwargs):
            packed = real(*args, **kwargs)
            if left[0]:
                left[0] -= 1
                self.write("src/a.py", f"edit {left[0]}\n" * (left[0] + 2))
            return packed
        return taken, mock.patch.object(bundle, "_pack", pack)

    def test_a_file_edited_while_packing_is_packed_again(self):
        taken, patch = self.edits(1)
        with patch:
            packed = taken.pack()
        with tempfile.TemporaryDirectory() as into:
            bundle.extract(packed, into)
            with open(os.path.join(into, "src/a.py")) as handle:
                self.assertTrue(handle.read().startswith("edit 0"), "never a mix of before and after")

    def test_one_that_keeps_changing_stops_it(self):
        taken, patch = self.edits(2)
        with patch, self.assertRaises(errors.ValidationError) as caught:
            taken.pack()
        self.assertIn("src/a.py", str(caught.exception))


class SubmitTest(Repo):
    def submit(self, target=("make", "e2e"), **options):
        self.client, self.store = FakeJobClient(), MemoryBlobStore()
        with remote.configure(store=STORE, client=self.client, blob_store=self.store):
            call = remote.run(list(target), on="p", code_dir=self.root, snapshot=True, **options)
        return call, self.client.job_object(call.name)

    def test_the_job_pins_the_snapshot_and_the_store_holds_it(self):
        self.write("src/a.py", "edited\n")
        call, obj = self.submit()
        template = obj["spec"]["workload"]["template"]
        command = template["command"]
        self.assertEqual(command[1:4], ["-m", "zenthros.remote", "snapshot"])
        self.assertEqual(command[5:], ["--", "make", "e2e"])
        env = {e["name"]: e.get("value") for e in template["env"]}
        digest = env[remote_call.CALL_SHA256_ENV]
        self.assertEqual(hashlib.sha256(self.store.get(command[4])).hexdigest(), digest)
        self.assertEqual(command[4], f"{STORE}/code/{digest}.tar.gz")
        notes = obj["metadata"]["annotations"]
        self.assertEqual(notes[snapshot.CODE_SHA256], digest)
        self.assertEqual(len(notes[snapshot.CODE_BASE]), 40)
        self.assertEqual(notes[snapshot.CODE_DIRTY], "true")
        self.assertEqual(call.code, digest)

    def test_only_the_sdk_core_rides_in_the_job(self):
        _, obj = self.submit()
        inline = obj["spec"]["workload"]["template"]["source"]["inline"]
        self.assertLess(len(inline), bundle.MAX_CODE_BYTES * 2)

    def test_editing_afterwards_changes_nothing_that_runs(self):
        _, first = self.submit()
        uri = first["spec"]["workload"]["template"]["command"][4]
        before = self.store.get(uri)
        self.write("src/a.py", "later\n")
        self.assertEqual(self.store.get(uri), before)

    def test_a_function_cannot_ask_for_one(self):
        with remote.configure(store=STORE, client=FakeJobClient(), blob_store=MemoryBlobStore()), \
                self.assertRaises(errors.ValidationError) as caught:
            remote.run(len, [1], on="p", code_dir=self.root, snapshot=True)
        self.assertIn("command", str(caught.exception))

    def test_the_command_may_be_a_string(self):
        self.client, self.store = FakeJobClient(), MemoryBlobStore()
        with remote.configure(store=STORE, client=self.client, blob_store=self.store):
            call = remote.run("make e2e | tee out", on="p", code_dir=self.root, snapshot=True)
        command = self.client.job_object(call.name)["spec"]["workload"]["template"]["command"]
        self.assertEqual(command[5:], ["--", "sh", "-c", "make e2e | tee out"])


@machine_side
class RunTest(Repo):
    def setUp(self):
        super().setUp()
        self.write("job.sh", "#!/bin/sh\necho ran in $(basename $PWD) > \"$OUT\"\nexit 7\n", 0o755)
        self.store = MemoryBlobStore()
        taken = snapshot.take(self.root)
        self.packed = taken.pack()
        self.digest = hashlib.sha256(self.packed).hexdigest()
        self.uri = f"{STORE}/code/{self.digest}.tar.gz"
        self.store.put(self.uri, self.packed)
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.work = folder.name
        previous = os.getcwd()
        os.chdir(self.work)
        self.addCleanup(os.chdir, previous)
        patcher = mock.patch.dict(os.environ, {remote_call.CALL_SHA256_ENV: self.digest, "OUT": self.work + "/out"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_it_runs_in_the_snapshot_and_its_exit_code_is_the_jobs(self):
        self.assertEqual(runner.run_snapshot(self.uri, ["./job.sh"], self.store), 7)
        with open(self.work + "/out") as handle:
            self.assertIn("ran in", handle.read())

    def test_other_bytes_are_never_run(self):
        self.store.put(self.uri, self.packed + b"x")
        err = mock.Mock()
        self.assertEqual(runner.run_snapshot(self.uri, ["./job.sh"], self.store, out=err), runner.EXIT_TAMPERED)
        self.assertFalse(os.path.exists(self.work + "/out"))

    def test_a_command_not_there_says_which(self):
        self.assertEqual(runner.run_snapshot(self.uri, ["./nope"], self.store), 127)

    def test_main_takes_the_form_the_job_sends(self):
        with mock.patch.object(runner, "ensure_runner"), mock.patch.object(runner, "run_snapshot", return_value=3) as ran, \
                mock.patch.object(runner, "store_for", return_value=self.store):
            self.assertEqual(runner.main(["snapshot", self.uri, "--", "make", "x"]), 3)
        self.assertEqual(ran.call_args.args[:2], (self.uri, ["make", "x"]))


class SavedLogTest(RunTest):
    """The log channel ends with the machine: the command's output is kept
    among the job's outputs too, while it runs and at its end."""
    OUTPUT = f"{STORE}/job-1"

    def setUp(self):
        super().setUp()
        patcher = mock.patch.dict(os.environ, {"ZENTHROS_DATA_OUTPUT": self.OUTPUT})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.saved = f"{self.OUTPUT}/{runner.OUTPUT_LOG}"

    def run_it(self, script, **options):
        self.write_and_pack(script)
        self.sink = bytearray()
        return runner.run_snapshot(self.uri, ["./go.sh"], self.store, log_sink=self.sink.extend, **options)

    def write_and_pack(self, script):
        self.write("go.sh", "#!/bin/sh\n" + script, 0o755)
        packed = snapshot.take(self.root).pack()
        digest = hashlib.sha256(packed).hexdigest()
        self.uri = f"{STORE}/code/{digest}.tar.gz"
        self.store.put(self.uri, packed)
        os.environ[remote_call.CALL_SHA256_ENV] = digest

    def test_both_streams_reach_the_job_log_and_the_store(self):
        self.assertEqual(self.run_it("echo out\necho err >&2\nexit 7\n"), 7)
        self.assertEqual(sorted(bytes(self.sink).split()), [b"err", b"out"])
        self.assertEqual(sorted(self.store.get(self.saved).split()), [b"err", b"out"])

    def test_it_is_saved_while_it_runs(self):
        """A machine lost in the middle still leaves what came before."""
        saves = []
        real = self.store.put
        self.store.put = lambda uri, payload: (saves.append(payload) if uri == self.saved else None, real(uri, payload))[1]
        self.run_it("echo early\nsleep 1\necho late\n", log_every=0.2)
        self.assertTrue(any(b"early" in saved and b"late" not in saved for saved in saves), saves)
        self.assertIn(b"late", self.store.get(self.saved))

    def test_what_it_left_running_does_not_hold_the_job(self):
        """A service it started keeps the pipe open, and the job still ends."""
        started = time.monotonic()
        self.assertEqual(self.run_it("sleep 30 &\necho done\nexit 3\n"), 3)
        self.assertLess(time.monotonic() - started, 10)
        self.assertIn(b"done", self.store.get(self.saved))

    def test_a_long_log_keeps_its_end(self):
        with mock.patch.object(runner, "LOG_MAX_BYTES", 1000):
            self.run_it("i=0; while [ $i -lt 2000 ]; do echo line-$i; i=$((i+1)); done\n")
        saved = self.store.get(self.saved)
        self.assertLessEqual(len(saved), 1000)
        self.assertIn(b"line-1999", saved)
        self.assertIn(b"line-1", bytes(self.sink)[:40], "the job's own log is whole")

    def test_a_store_that_refuses_does_not_end_the_job(self):
        real = self.store.put

        def put(uri, payload):
            if uri == self.saved:
                raise OSError("no permission")
            return real(uri, payload)

        self.store.put = put
        err = io.StringIO()
        self.assertEqual(self.run_it("echo out\nexit 4\n", out=err), 4)
        self.assertIn(b"out", bytes(self.sink))
        self.assertIn("no permission", err.getvalue())


if __name__ == "__main__":
    unittest.main()
