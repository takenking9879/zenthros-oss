# SPDX-License-Identifier: Apache-2.0

"""rerun and upload=: the same call elsewhere, and what it reads going with it."""

import contextlib
import hashlib
import io
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from zenthros import errors, remote
from zenthros.artifact import Sent
from zenthros.core.storage import MemoryBlobStore
from zenthros.remote import runner
from zenthros.remote.call import RESUME, get

from test_remote import STORE, ExecutingClient


def twice(value):
    return value * 2


def read_it(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.project = self.root / "project"
        (self.project / ".zenthros").mkdir(parents=True)
        (self.project / "main.py").write_text("x = 1\n")
        cwd = os.getcwd()
        os.chdir(self.project)
        self.addCleanup(os.chdir, cwd)
        env = mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": str(self.root / "config" / "config.yaml"),
                                           "XDG_CACHE_HOME": str(self.root / "c")})
        env.start()
        self.addCleanup(env.stop)
        self.store = MemoryBlobStore()
        self.client = ExecutingClient(self.store)
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo-ml")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        self.places = []

        def placed(on, options, **_):
            self.places.append(on)
            where = mock.Mock(made=None)
            where.name = str(on)
            where.options = {"pool": str(on), "backend": "", "acquire": None, "client": None}
            return where

        place = mock.patch("zenthros.machines.place", side_effect=placed)
        place.start()
        self.addCleanup(place.stop)
        self.stderr = io.StringIO()
        out = contextlib.redirect_stderr(self.stderr)
        out.__enter__()
        self.addCleanup(out.__exit__, None, None, None)

    def job(self, call):
        return self.client.submitted(call.name)

    def ended_with_a_checkpoint(self, call, seq=14):
        record = self.client._jobs[call.name]
        record["phase"] = "Failed"
        record["progress"] = {"uri": "s3://b/ck", "seq": seq, "sha256": "aa"}


class Rerun(Base):
    def test_the_same_call_in_a_new_job(self):
        first = remote.run(twice, 21, on="gpu")
        self.assertEqual(first.result(), 42)
        again = get(first.name).rerun()
        self.assertNotEqual(again.name, first.name)
        self.assertEqual(again.result(), 42)
        self.assertEqual(self.places, ["gpu", "gpu"])

    def test_it_keeps_what_was_asked(self):
        first = remote.run(twice, 1, on="gpu", cpu="4", memory="8Gi", gpu=1, image="img:1", env={"MODE": "x"}, timeout="1h")
        spec = self.job(get(first.name).rerun()).spec
        self.assertEqual((spec.compute.cpu, spec.compute.memory, spec.compute.gpu_count), ("4", "8Gi", 1))
        self.assertEqual(spec.workload.template.image, "img:1")
        self.assertEqual(spec.active_deadline_seconds, 3600)
        self.assertIn("MODE", {e.name for e in spec.workload.template.env})

    def test_it_waits_for_what_it_waited_for(self):
        # The digest a call checks its upstream against comes with dependsOn.
        before = remote.run(twice, 21, on="gpu")
        first = remote.run(twice, before, on="gpu")
        again = first.rerun()
        self.assertEqual(self.job(again).spec.depends_on, [before.name])
        self.assertEqual(again.result(), 84)
        told = remote.run(twice, 1, on="gpu", after=[before])
        self.assertEqual(self.job(told.rerun()).spec.depends_on, [before.name])

    def test_what_you_change_is_changed(self):
        first = remote.run(twice, 1, on="gpu", cpu="4")
        spec = self.job(first.rerun(on="other", cpu="8")).spec
        self.assertEqual(spec.compute.cpu, "8")
        self.assertEqual(spec.pool_ref, "other")

    def test_it_runs_the_code_it_ran_not_what_is_on_disk_now(self):
        first = remote.run(twice, 1, on="gpu", code_dir=str(self.project))
        before = self.client.submitted(first.name).annotations["zenthros.ai/code-sha256"]
        (self.project / "main.py").write_text("x = 2\n")
        again = get(first.name).rerun()
        self.assertEqual(self.job(again).annotations["zenthros.ai/code-sha256"], before)

    def test_a_command_runs_again_with_its_snapshot(self):
        for args in (["init", "-q"], ["add", "."], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base"]):
            subprocess.run(["git", *args], cwd=self.project, check=True)
        first = remote.run(["make", "test"], on="gpu", snapshot=True, code_dir=str(self.project))
        before = self.job(first).spec.workload.template.command
        (self.project / "main.py").write_text("x = 2\n")
        again = self.job(get(first.name).rerun()).spec.workload.template.command
        self.assertEqual(again, before)

    def test_a_plain_command_runs_again(self):
        first = remote.run(["echo", "hola"], on="gpu")
        self.assertEqual(self.job(get(first.name).rerun()).spec.workload.template.command, ["echo", "hola"])

    def test_secrets_are_asked_again_for_they_were_sealed_for_a_machine(self):
        first = remote.run(twice, 1, on="gpu", secrets={"TOKEN": remote.options.Secret("kubernetes", "s", "k")})
        before = len(self.client._jobs)
        with self.assertRaisesRegex(errors.ValidationError, "TOKEN"):
            get(first.name).rerun()
        self.assertEqual(len(self.client._jobs), before)
        self.assertIn("TOKEN", {s.materialization.env_name for s in self.job(first.rerun(secrets={"TOKEN": remote.options.Secret("kubernetes", "s", "k")})).spec.workload.template.secret_sources})

    def test_a_map_or_a_service_is_not_one_call(self):
        fan = remote.map(twice, [1, 2], on="gpu")
        with self.assertRaisesRegex(errors.ValidationError, "one call"):
            get(fan.name).rerun()

    def test_a_job_that_is_gone_says_so(self):
        first = remote.run(twice, 1, on="gpu")
        self.client._jobs.pop(first.name)
        self.client._order.remove(first.name)
        with self.assertRaises(errors.JobNotFound):
            first.rerun()


class RerunResume(Base):
    def test_it_goes_on_from_the_last_confirmed_checkpoint(self):
        first = remote.run(twice, 1, on="gpu")
        self.ended_with_a_checkpoint(first)
        again = get(first.name).rerun(resume=True)
        self.assertEqual(again.action, RESUME)
        self.assertIn(f"checkpoint 14 of {first.name}", again.why)
        self.assertEqual(self.job(again).spec.resume_from.job, first.name)

    def test_without_resume_it_starts_from_the_beginning(self):
        first = remote.run(twice, 1, on="gpu")
        self.ended_with_a_checkpoint(first)
        self.assertIsNone(self.job(get(first.name).rerun()).spec.resume_from)

    def test_another_image_starts_over_and_says_so(self):
        first = remote.run(twice, 1, on="gpu", image="img:1")
        self.ended_with_a_checkpoint(first)
        again = get(first.name).rerun(resume=True, image="img:2")
        self.assertIsNone(self.job(again).spec.resume_from)
        self.assertIn("image", again.why)
        self.assertIn("starts over", again.why)

    def test_a_job_still_running_starts_over_and_says_so(self):
        first = remote.run(twice, 1, on="gpu")
        self.client._jobs[first.name].update(phase="Running", progress={"uri": "s3://b/ck", "seq": 1})
        again = get(first.name).rerun(resume=True)
        self.assertIsNone(self.job(again).spec.resume_from)
        self.assertIn("has not ended", again.why)


class Paths(Base):
    def data(self, name="datos.csv", text="a\n"):
        path = self.root / name
        path.write_text(text, newline="\n")
        return str(path)

    def test_a_path_only_here_stops_a_call_elsewhere_before_anything_is_made(self):
        path = self.data()
        with self.assertRaisesRegex(errors.ValidationError, "only on this computer.*upload=True"):
            remote.run(read_it, path, on="gpu")
        self.assertEqual(self.client._jobs, {})
        self.assertEqual(self.places, [])

    def test_a_string_that_is_no_path_goes_as_it_is(self):
        self.assertEqual(remote.run(twice, "no/such/thing", on="gpu").result(), "no/such/thingno/such/thing")

    def test_upload_puts_it_in_the_store_by_its_content(self):
        path = self.data()
        call = remote.run(read_it, path, on="gpu", upload=True)
        digest = hashlib.sha256(b"a\n").hexdigest()
        self.assertEqual(self.store.data[f"{STORE}/data/{digest}"], b"a\n")
        self.assertEqual(call.result(), "a\n")

    def test_what_is_there_is_not_sent_again(self):
        path = self.data()
        remote.run(read_it, path, on="gpu", upload=True)
        with mock.patch.object(self.store, "put_file", side_effect=AssertionError("sent twice")):
            with mock.patch.object(self.store, "put", wraps=self.store.put) as put:
                remote.run(read_it, path, on="gpu", upload=True)
        self.assertFalse([args for args, _ in put.call_args_list if "/data/" in args[0]])

    def test_a_folder_goes_whole_and_its_list_last(self):
        folder = self.root / "tabla"
        (folder / "a").mkdir(parents=True)
        (folder / "a" / "uno.txt").write_text("1")
        (folder / "dos.txt").write_text("2")
        def names(path):
            return sorted(os.path.relpath(os.path.join(top, name), path).replace(os.sep, "/")
                          for top, _, files in os.walk(path) for name in files)
        self.assertEqual(sorted(remote.run(names, str(folder), on="gpu", upload=True).result()), ["a/uno.txt", "dos.txt"])

    def test_a_credential_is_refused_before_anything_is_sent(self):
        path = self.data(".env", "KEY=1\n")
        with self.assertRaises(errors.ValidationError):
            remote.run(read_it, path, on="gpu", upload=True)
        self.assertFalse([uri for uri in self.store.data if "/data/" in uri])
        self.assertEqual(self.client._jobs, {})

    def test_the_key_of_a_call_does_not_change_with_upload(self):
        path = self.data()
        first = remote.run(read_it, path, on="gpu", upload=True, cache=True)
        first.result()
        self.assertTrue(remote.run(read_it, path, on="gpu", upload=True, cache=True).cached)

    def test_upload_does_not_copy_what_is_in_the_store_already(self):
        uri = f"{STORE}/ya/x.txt"
        self.store.data[uri] = b"z"
        call = remote.run(twice, uri, on="gpu", upload=True)
        self.assertEqual(call.result(), uri * 2)


class WhereItRuns(Base):
    def test_bytes_that_changed_in_the_store_are_never_handed_over(self):
        self.store.data[f"{STORE}/data/x"] = b"changed"
        sent = Sent(f"{STORE}/data/x", hashlib.sha256(b"original").hexdigest(), 8, name="d.csv")
        with self.assertRaisesRegex(errors.ZenthrosError, "hashes to"):
            runner.resolve((sent,), self.store)

    def test_the_function_gets_a_path_of_the_same_name(self):
        self.store.data[f"{STORE}/data/x"] = b"original"
        sent = Sent(f"{STORE}/data/x", hashlib.sha256(b"original").hexdigest(), 8, name="d.csv")
        (path,) = runner.resolve((sent,), self.store)
        self.addCleanup(os.unlink, path)
        self.assertEqual(os.path.basename(path), "d.csv")
        self.assertEqual(Path(path).read_bytes(), b"original")


if __name__ == "__main__":
    unittest.main()
