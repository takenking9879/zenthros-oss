# SPDX-License-Identifier: Apache-2.0

"""The cache: a signed manifest per entry, and what may never be opened."""

import contextlib
import hashlib
import io
import json
import os
import pickle
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import cache as entries
from zenthros import errors, identity, remote
from zenthros.artifact import FOLDER_LIST, Artifact, publish_folder, read_folder
from zenthros.core.storage import FileBlobStore, MemoryBlobStore

from support import assert_mode
from test_remote import STORE, ExecutingClient


def add(a, b=1):
    return a + b


RUNS = []


def counted(a):
    return a * 2


class Boom:
    """Unpickling this is what a forged manifest would do on your computer."""

    opened = []

    def __reduce__(self):
        return (Boom.opened.append, ("opened",))


class CacheBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.project = self.root / "project"
        self.project.mkdir()
        (self.project / "main.py").write_text("x = 1\n")
        patches = [mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": str(self.root / "config" / "config.yaml"),
                                                "XDG_CACHE_HOME": str(self.root / "cache-home")}),
                   mock.patch("os.getcwd", return_value=str(self.project))]
        patches[0].start()
        self.addCleanup(patches[0].stop)
        self.store = MemoryBlobStore()
        self.client = ExecutingClient(self.store)
        self.configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo-ml")
        self.configured.__enter__()
        self.addCleanup(self.configured.__exit__, None, None, None)
        self.stderr = io.StringIO()
        redirected = contextlib.redirect_stderr(self.stderr)
        redirected.__enter__()
        self.addCleanup(redirected.__exit__, None, None, None)

    def call(self, fn, *args, **kwargs):
        options = dict(code_dir=str(self.project), pool="demo", cache=True)
        pool = options.pop("pool")
        options.update(kwargs)
        # These calls name a file only this test has; where a path reaches is test_rerun's.
        with mock.patch("zenthros.machines.place", return_value=_placed(pool)), \
                mock.patch("zenthros.remote.call._in_reach", lambda value, *_: value):
            return remote.run(fn, *args, **options)

    def submits(self):
        return len(self.client._jobs)

    def only_manifest(self):
        names = [uri for uri in self.store.data if "/cache/" in uri and "/last/" not in uri and uri.endswith(".json")]
        self.assertEqual(len(names), 1, names)
        return names[0]


def _placed(pool):
    placed = mock.Mock()
    placed.options = {"pool": pool, "backend": "", "acquire": None, "client": None}
    placed.made = None
    placed.name = pool
    return placed


class Sizes(CacheBase):
    def test_memory_goes_as_kubernetes_reads_it_in_any_form_the_sdk_takes(self):
        for asked, sent in (("1g", "1Gi"), ("12g", "12Gi"), ("512m", "512Mi"), ("8Gi", "8Gi"), ("1.5Gi", "1536Mi"), ("2Ti", "2048Gi")):
            with self.subTest(asked):
                self.assertEqual(remote.call._compute(None, asked, None, "", None).memory, sent)

    def test_a_size_it_cannot_read_stops_before_a_job(self):
        with self.assertRaisesRegex(errors.ValidationError, "memory='lots'"):
            self.call(add, 1, memory="lots", cache=False)
        self.assertEqual(self.submits(), 0)


class Hits(CacheBase):
    def test_a_second_call_does_not_run_and_returns_the_same(self):
        first = self.call(add, 2, b=3)
        self.assertEqual(first.result(), 5)
        again = self.call(add, 2, b=3)
        self.assertTrue(again.cached)
        self.assertEqual(again.result(), 5)
        self.assertEqual(self.submits(), 1)
        self.assertEqual(again.cache_note.key, first.cache_note.key)

    def test_it_says_why_it_runs_naming_the_argument(self):
        self.call(add, 2, b=3).result()
        again = self.call(add, 2, b=4)
        self.assertEqual(again.why, "b changed")
        self.assertEqual(again.result(), 6)
        self.assertEqual(self.submits(), 2)

    def test_why_compares_with_what_you_last_used_even_from_the_cache(self):
        """Back to an earlier call and on from there: only what changed since is
        named, not the detour before the hit."""
        self.call(add, 1, b=1).result()
        self.call(add, 1, b=2).result()
        self.assertTrue(self.call(add, 1, b=1).cached)
        again = self.call(add, 2, b=1)
        self.assertEqual(again.why, "a changed")

    def test_a_plan_does_not_count_as_a_use(self):
        self.call(add, 1, b=1).result()
        self.call(add, 1, b=2).result()
        with mock.patch("zenthros.machines.place", return_value=_placed("demo")):
            plan = remote.call._plan_cache(add, (1,), {"b": 1}, {"code_dir": str(self.project)}, None, True, None, None)
        self.assertTrue(plan["hit"])
        self.assertEqual(self.call(add, 2, b=2).why, "a changed")

    def test_where_it_ran_and_its_name_are_not_in_the_key(self):
        self.call(add, 1, name="uno").result()
        self.assertTrue(self.call(add, 1, name="dos").cached)

    def test_a_file_argument_goes_by_its_content(self):
        data = self.root / "datos.csv"
        data.write_text("a\n")
        self.call(add, str(data), b="x").result()
        self.assertTrue(self.call(add, str(data), b="x").cached)
        data.write_text("b\n")
        again = self.call(add, str(data), b="x")
        self.assertFalse(again.cached)
        self.assertEqual(again.why, "datos.csv changed")

    def test_deps_invalidate_and_are_named(self):
        data = self.root / "tabla.csv"
        data.write_text("1\n")
        self.call(add, 1, deps=[str(data)]).result()
        self.assertTrue(self.call(add, 1, deps=[str(data)]).cached)
        data.write_text("2\n")
        self.assertEqual(self.call(add, 1, deps=[str(data)]).why, "tabla.csv changed")

    def test_a_version_given_as_text_is_a_dep(self):
        self.call(add, 1, deps={"ventas": "2026-10-31"}).result()
        self.assertTrue(self.call(add, 1, deps={"ventas": "2026-10-31"}).cached)
        self.assertEqual(self.call(add, 1, deps={"ventas": "2026-11-01"}).why, "ventas changed")

    def test_a_dep_that_is_not_there_is_refused(self):
        with self.assertRaisesRegex(errors.ValidationError, "not here"):
            self.call(add, 1, deps=["no-existe.csv"])

    def test_a_project_file_changed_is_named(self):
        self.call(add, 1).result()
        (self.project / "main.py").write_text("x = 2\n")
        self.assertIn("main.py", self.call(add, 1).why)

    def test_publishing_an_environment_does_not_change_the_key(self):
        """env push rewrites its lock, and the image's content says all of it."""
        lock = self.project / "envs" / "train" / "env.lock.json"
        lock.parent.mkdir(parents=True)
        lock.write_text('{"published": []}')
        first = entries.note_for(add, (1,), {}, code_dir=str(self.project))
        lock.write_text('{"published": ["somewhere"]}')
        (lock.parent / "Dockerfile").write_text("FROM x")
        self.assertEqual(entries.note_for(add, (1,), {}, code_dir=str(self.project)).key, first.key)
        (self.project / "envs" / "code.py").write_text("x = 1")
        self.assertNotEqual(entries.note_for(add, (1,), {}, code_dir=str(self.project)).key, first.key)

    def test_the_code_of_the_function_changed_is_said(self):
        def step(a):
            return a

        first = step
        self.call(first, 1, name="a").result()

        def step(a):  # noqa: F811 - the same name, other code
            return a + 1

        self.assertIn("the code of", self.call(step, 1, name="b").why)

    def test_a_command_takes_no_cache(self):
        with self.assertRaisesRegex(errors.ValidationError, "a function's"):
            remote.run(["true"], cache=True, on="demo-ml")

    def test_a_failed_call_is_not_kept(self):
        def fails(a):
            raise ValueError("no")

        with self.assertRaises(remote.RemoteError):
            self.call(fails, 1).result()
        self.assertFalse([uri for uri in self.store.data if "/cache/" in uri])

    def test_the_label_and_the_sealed_note_are_on_the_job(self):
        call = self.call(add, 1)
        job = self.client.submitted(call.name)
        self.assertEqual(job.labels[entries.KEY_LABEL], call.cache_key[:63])
        self.assertIn(entries.PARTS_ANNOTATION, job.annotations)


SAME_IN_EACH_PROCESS = """
import json, os, sys, tempfile
os.environ["ZENTHROS_CONFIG"] = os.path.join(tempfile.mkdtemp(), "config.yaml")
sys.path.insert(0, os.getcwd())
import steps
from zenthros import cache
note = cache.note_for(steps.work, ("x",), {"depth": 4, "options": {"a": {1, 2, 3}, "b": ("s", "s")}}, code_dir=".")
print(json.dumps({"key": note.key, "parts": note.parts}))
"""

STEPS = """
import os
from dataclasses import dataclass, field

import zenthros

RATES = {"one": 1, "two": frozenset({"a", "b", "c", "d"})}


@dataclass(frozen=True)
class Shape:
    name: str = "shape"
    columns: dict = field(default_factory=lambda: {"a": "int64", "b": "string"})


SHAPE = Shape()


def helper(value):
    return {"got": value, "names": {"x", "y", "z"}, "rates": RATES}


@zenthros.cloud(env="tables", outputs=zenthros.parquet(columns={"a": "int64"}))
def wrapped(value):
    return helper(value)


def work(path, depth=1, options=None):
    import json

    def inside(item):
        return helper(item), SHAPE, os.path.basename(path), wrapped

    return [inside(i) for i in range(depth)], json.dumps(options is None)
"""


class SameAcrossProcesses(unittest.TestCase):
    """What the key is made of must not depend on the process that makes it: each
    run of a script is a new one, with its own hash seed and interned strings."""

    def _key(self, folder, seed):
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": str(Path(__file__).parents[1])}
        env.pop("ZENTHROS_CONFIG", None)
        done = subprocess.run([sys.executable, "-c", SAME_IN_EACH_PROCESS], cwd=folder, env=env, capture_output=True,
                              text=True, check=False)
        self.assertEqual(done.returncode, 0, done.stderr)
        return json.loads(done.stdout.splitlines()[-1])

    def test_the_same_call_has_the_same_key(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "steps.py").write_text(STEPS)
            seen = [self._key(folder, seed) for seed in ("1", "2", "3", "4")]
        self.assertEqual([one["parts"]["code"] for one in seen], [seen[0]["parts"]["code"]] * 4)
        self.assertEqual([one["key"] for one in seen], [seen[0]["key"]] * 4)

    def test_other_code_or_other_values_a_function_uses_have_another_key(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "steps.py").write_text(STEPS)
            first = self._key(folder, "1")["parts"]["code"]
            for changed in (STEPS.replace('"got": value', '"got": value + 1'),
                            STEPS.replace('"one": 1', '"one": 2'),
                            STEPS.replace('name: str = "shape"', 'name: str = "other"'),
                            STEPS.replace('columns={"a": "int64"}', 'columns={"a": "string"}'),
                            STEPS.replace("range(depth)", "range(depth + 1)")):
                self.assertNotEqual(changed, STEPS)
                (Path(folder) / "steps.py").write_text(changed)
                self.assertNotEqual(self._key(folder, "1")["parts"]["code"], first, changed)
                (Path(folder) / "steps.py").write_text(STEPS)

    def test_a_comment_above_it_is_not_its_code(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "steps.py").write_text(STEPS)
            first = self._key(folder, "1")["parts"]["code"]
            (Path(folder) / "steps.py").write_text("# a comment\n" + STEPS)
            self.assertEqual(self._key(folder, "1")["parts"]["code"], first)

    def test_a_class_annotation_is_not_its_code(self):
        # Python 3.14 keeps them in a function of the class (__annotate_func__),
        # whose code names each type: its key changed with them and, through the
        # class namespace it closes over, with the line the class is on.
        def made(kind, value=1):
            class Point:
                x: kind = value

            return Point

        self.assertEqual(entries.code_digest(made(int)), entries.code_digest(made(str)))
        self.assertNotEqual(entries.code_digest(made(int)), entries.code_digest(made(int, 2)), "the class is its code")

    def test_a_function_that_refers_to_itself_ends(self):
        def again(n):
            return again(n - 1) if n else 0

        self.assertTrue(entries.code_digest(again))

    def test_what_cannot_be_pickled_is_told_by_its_type(self):
        lock = threading.Lock()

        def uses_it():
            return lock

        self.assertTrue(entries.code_digest(uses_it))


class WhatIsNeverOpened(CacheBase):
    def _forge(self, key, signature="hmac-sha256:" + "0" * 64):
        raw = pickle.dumps({"ok": True, "value": pickle.dumps(Boom())})
        uri = f"{STORE}/evil/result.pkl"
        self.store.put(uri, raw)
        body = {"key": key, "fn": "add", "parts": {}, "result": Artifact(uri, hashlib.sha256(raw).hexdigest(), len(raw)).to_dict(),
                "outs": [], "produced_by": {"job": "evil"}, "signature": signature}
        self.store.put(f"{STORE}/cache/{key}.json", json.dumps(body).encode())

    def test_a_forged_manifest_is_never_opened(self):
        Boom.opened.clear()
        note = entries.note_for(add, (1,), {}, code_dir=str(self.project))
        self._forge(note.key)
        call = self.call(add, 1)
        self.assertFalse(call.cached)
        self.assertIn("not signed by you", call.why)
        self.assertEqual(Boom.opened, [])
        self.assertEqual(call.result(), 2)

    def test_its_signature_with_another_key_is_no_signature(self):
        Boom.opened.clear()
        note = entries.note_for(add, (1,), {}, code_dir=str(self.project))
        other = entries.signed({"key": note.key})["signature"]
        with mock.patch.object(entries, "signing_key", return_value=b"another"):
            self.assertFalse(entries.genuine({"key": note.key, "signature": other}))

    def test_changed_bytes_make_the_entry_invalid_and_say_which_digest(self):
        first = self.call(add, 1)
        first.result()
        self.store.put(f"{first.prefix}/result.pkl", b"written over")
        again = self.call(add, 1)
        self.assertFalse(again.cached)
        self.assertIn("hashes to", again.why)
        self.assertIn("not the one verified", again.why)

    def test_missing_bytes_make_the_entry_invalid(self):
        first = self.call(add, 1)
        first.result()
        del self.store.data[f"{first.prefix}/result.pkl"]
        again = self.call(add, 1)
        self.assertFalse(again.cached)
        self.assertIn("gone", again.why)

    def test_a_manifest_changed_after_signing_is_not_believed(self):
        self.call(add, 1).result()
        uri = self.only_manifest()
        body = json.loads(self.store.get(uri))
        body["produced_by"]["where"] = "elsewhere"
        self.store.put(uri, json.dumps(body).encode())
        self.assertFalse(self.call(add, 1).cached)

    def test_an_entry_of_another_key_is_not_this_one(self):
        self.call(add, 1).result()
        uri = self.only_manifest()
        note = entries.note_for(add, (2,), {}, code_dir=str(self.project))
        self.store.put(f"{STORE}/cache/{note.key}.json", self.store.get(uri))
        self.assertFalse(self.call(add, 2).cached)

    def test_the_key_file_is_for_this_person_alone(self):
        entries.signing_key()
        assert_mode(self, entries.key_path(), 0o600)

    def test_a_key_never_reaches_a_job(self):
        call = self.call(add, 1)
        key = entries.signing_key()
        self.assertNotIn(key.hex(), json.dumps(self.client.submitted(call.name).annotations))
        self.assertFalse(any(key in value for value in self.store.data.values()))

    def test_losing_the_key_only_loses_entries(self):
        self.call(add, 1).result()
        os.remove(entries.key_path())
        again = self.call(add, 1)
        self.assertFalse(again.cached)
        self.assertEqual(again.result(), 2)

    def test_key_export_and_import_share_entries(self):
        self.call(add, 1).result()
        text = entries.export_key()
        os.remove(entries.key_path())
        entries.import_key(text)
        self.assertTrue(self.call(add, 1).cached)
        assert_mode(self, entries.key_path(), 0o600)

    def test_a_job_sent_by_someone_else_cannot_publish(self):
        call = self.call(add, 1)
        obj = {"metadata": {"name": "other-name", "annotations": dict(self.client.submitted(call.name).annotations)}}
        self.assertIsNone(entries.note_of_job(obj))
        obj["metadata"]["name"] = call.name
        self.assertEqual(entries.note_of_job(obj).key, call.cache_key)


class Seeing(CacheBase):
    def test_a_job_nobody_looked_at_enters_when_it_is_seen(self):
        call = self.call(add, 1)
        self.client.wait(call.name)
        self.assertFalse([uri for uri in self.store.data if "/cache/" in uri])
        again = remote.get(call.name)
        self.assertEqual(again.result(stream_logs=False), 2)
        self.assertTrue(self.call(add, 1).cached)

    def test_a_file_that_changed_while_it_ran_is_not_published(self):
        data = self.root / "datos.csv"
        data.write_text("a\n")
        call = self.call(add, str(data), b="x")
        time.sleep(0.01)
        data.write_text("changed\n")
        call.result()
        self.assertFalse([uri for uri in self.store.data if "/cache/" in uri])
        self.assertIn("changed while it ran", self.stderr.getvalue())

    def test_ls_show_rm(self):
        call = self.call(add, 1)
        call.result()
        home = entries.Home(STORE, self.store)
        rows = entries.ls([home])
        self.assertEqual([(row["key"], row["valid"], row["job"]) for row in rows], [(call.cache_key, True, call.name)])
        self.assertEqual(entries.show(call.cache_key[:8], [home])["fn"], "add")
        self.assertEqual(entries.rm(call.cache_key[:8], [home]), [call.cache_key])
        self.assertEqual(entries.ls([home]), [])
        self.assertFalse(self.call(add, 1).cached)

    def test_a_forged_manifest_shows_as_not_valid(self):
        WhatIsNeverOpened._forge(self, "f" * 64)
        rows = entries.ls([entries.Home(STORE, self.store)])
        self.assertEqual([row["valid"] for row in rows], [False])
        self.assertNotIn("result", rows[0])

    def test_find_gives_the_entry_of_a_call(self):
        self.call(add, 1).result()
        with mock.patch("zenthros.machines.default_on", return_value="demo-ml"):
            found = entries.find(add, 1, code_dir=str(self.project))
        self.assertEqual(found.result(stream_logs=False), 2)
        with mock.patch("zenthros.machines.default_on", return_value="demo-ml"):
            self.assertIsNone(entries.find(add, 99, code_dir=str(self.project)))

    def test_a_reused_call_passes_as_its_value(self):
        self.call(add, 1).result()
        hit = self.call(add, 1)
        next_call = self.call(add, hit, b=10)
        self.assertEqual(next_call.result(), 12)
        self.assertEqual(self.client.submitted(next_call.name).spec.depends_on, [])

    def test_a_second_store_keeps_the_result_and_outlives_the_first(self):
        shared_store = MemoryBlobStore()
        home = entries.Home("s3://other/z", shared_store, shared=True)
        first = self.call(add, 1)
        first.cache_note.homes = [entries.Home(STORE, self.store), home]
        first.result()
        for uri in list(self.store.data):
            if "/cache/" in uri or uri.endswith("result.pkl"):
                del self.store.data[uri]
        note = entries.note_for(add, (1,), {}, code_dir=str(self.project))
        found, why = entries.lookup(note, [entries.Home(STORE, self.store), home])
        self.assertEqual(why, "")
        self.assertTrue(found.manifest["result"]["uri"].startswith("s3://other/z/cache/blobs/"))


class Files(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        env = mock.patch.dict(os.environ, {"XDG_CACHE_HOME": str(self.root / "c")})
        env.start()
        self.addCleanup(env.stop)
        self.big = self.root / "big.bin"
        size = mock.patch.object(identity, "ALWAYS_READ_BYTES", 4)
        size.start()
        self.addCleanup(size.stop)

    def _old(self, content):
        self.big.write_bytes(content)
        old = time.time() - 60
        os.utime(self.big, (old, old))

    def test_a_small_file_is_always_read(self):
        with mock.patch.object(identity, "ALWAYS_READ_BYTES", 1 << 20):
            self._old(b"abcdefgh")
            self.assertEqual(identity.file_digest(self.big)[1], identity.CONTENT)
            self.assertEqual(identity.file_digest(self.big)[1], identity.CONTENT)

    def _settled(self):
        """The file's dates, far enough from reading it to be believed."""
        recent = mock.patch.object(identity, "RECENT_NS", 0)
        recent.start()
        self.addCleanup(recent.stop)

    def test_a_large_old_file_is_read_once(self):
        self._settled()
        self._old(b"abcdefgh")
        digest, how = identity.file_digest(self.big)
        self.assertEqual(how, identity.CONTENT)
        with mock.patch("zenthros.identity.sha256_of", side_effect=AssertionError("read again")):
            self.assertEqual(identity.file_digest(self.big), (digest, identity.METADATA))

    def test_strict_reads_it_again(self):
        self._old(b"abcdefgh")
        identity.file_digest(self.big)
        self.assertEqual(identity.file_digest(self.big, strict=True)[1], identity.CONTENT)

    @unittest.skipIf(os.name == "nt", "Windows keeps no change time: its st_ctime is when the file was made")
    def test_same_size_and_mtime_with_other_bytes_is_caught_by_ctime(self):
        self._settled()
        self._old(b"abcdefgh")
        seen = os.stat(self.big)
        before, _ = identity.file_digest(self.big)
        # Past the tick of the clock the first write's ctime fell in.
        time.sleep(0.05)
        self.big.write_bytes(b"ABCDEFGH")
        os.utime(self.big, ns=(seen.st_atime_ns, seen.st_mtime_ns))
        self.assertEqual(os.stat(self.big).st_mtime_ns, seen.st_mtime_ns)
        after, how = identity.file_digest(self.big)
        self.assertNotEqual(before, after)
        self.assertEqual(how, identity.CONTENT)

    def test_a_file_written_right_before_reading_is_not_believed(self):
        self.big.write_bytes(b"abcdefgh")
        identity.file_digest(self.big)
        self.assertEqual(identity.file_digest(self.big)[1], identity.CONTENT)

    def test_a_write_in_the_same_tick_as_the_last_is_read_again(self):
        """Without the guard on ctime, this keeps the digest of the old bytes."""
        self._old(b"abcdefgh")
        seen = os.stat(self.big)
        before, _ = identity.file_digest(self.big)
        self.big.write_bytes(b"ABCDEFGH")
        os.utime(self.big, ns=(seen.st_atime_ns, seen.st_mtime_ns))
        self.assertNotEqual(identity.file_digest(self.big)[0], before)

    def test_a_folder_goes_by_names_and_content(self):
        folder = self.root / "d"
        folder.mkdir()
        (folder / "a").write_text("1")
        first = identity.path_digest(folder)[0]
        (folder / "b").write_text("1")
        second = identity.path_digest(folder)[0]
        self.assertNotEqual(first, second)
        (folder / "b").rename(folder / "c")
        self.assertNotEqual(second, identity.path_digest(folder)[0])

    def test_changed_says_which(self):
        self.big.write_bytes(b"abc")
        _, _, stats = identity.path_digest(self.big)
        self.assertEqual(identity.changed(stats), [])
        self.big.write_bytes(b"abcd")
        self.assertEqual(identity.changed(stats), [str(self.big)])

    def test_an_object_is_known_by_its_version(self):
        store = MemoryBlobStore()
        store.put("s3://b/x", b"1")
        before = identity.object_identity("s3://b/x", store)
        store.put("s3://b/x", b"2")
        self.assertNotEqual(before, identity.object_identity("s3://b/x", store))
        self.assertEqual(identity.object_identity("s3://b/none", store), "")

    def test_a_store_that_does_not_say_a_version_is_not_believed(self):
        store = mock.Mock()
        store.ls.return_value = [zenthros.core.storage.StoredObject("s3://b/x", 1)]
        self.assertEqual(identity.object_identity("s3://b/x", store), "")


class Stores(unittest.TestCase):
    def test_a_file_put_appears_whole(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "a" / "b"
            store = FileBlobStore()
            with mock.patch("pathlib.Path.write_bytes", side_effect=OSError("disk")), self.assertRaises(OSError):
                store.put(str(path), b"x")
            self.assertFalse(path.exists())
            self.assertEqual(os.listdir(path.parent), [])
            store.put(str(path), b"x")
            self.assertEqual(path.read_bytes(), b"x")

    def test_a_published_folder_needs_its_list(self):
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root) / "f"
            folder.mkdir()
            (folder / "a.txt").write_text("hola")
            store = MemoryBlobStore()
            publish_folder(store, "s3://b/out", folder)
            self.assertEqual([a.uri for a in read_folder(store, "s3://b/out")], ["s3://b/out/a.txt"])
            del store.data[f"s3://b/out/{FOLDER_LIST}"]
            with self.assertRaisesRegex(errors.ValidationError, "not a published folder"):
                read_folder(store, "s3://b/out")

    def test_a_published_folder_says_when_a_file_is_gone(self):
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root) / "f"
            folder.mkdir()
            (folder / "a.txt").write_text("hola")
            store = MemoryBlobStore()
            publish_folder(store, "s3://b/out", folder)
            del store.data["s3://b/out/a.txt"]
            with self.assertRaisesRegex(errors.ValidationError, "gone"):
                read_folder(store, "s3://b/out")

    def test_the_tail_of_a_file(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "a"
            path.write_bytes(b"abcdef")
            self.assertEqual(FileBlobStore().tail(str(path), 2), b"ef")
            self.assertEqual(FileBlobStore().tail(str(path), 20), b"abcdef")

    def test_the_form_is_the_one_go_reads(self):
        sample = json.loads((Path(__file__).parents[3] / "operator/pkg/resultdigest/testdata/artifact.json").read_text())
        artifact = Artifact.from_dict(sample["zenthros.result"])
        self.assertEqual(artifact.to_dict(), sample["zenthros.result"])


if __name__ == "__main__":
    unittest.main()
