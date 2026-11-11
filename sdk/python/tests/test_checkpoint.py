#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Resume from every acked phase, S3 checkpoints, and a frozen cursor that
is raised rather than swallowed.

Files of a checkpoint: what any library saves (a booster, weights, a
pickle) goes to the store next to the checkpoint's JSON, and the replacement
reads it back checked. Zenthros is acked the JSON's URI only.

GCS namespace + File/Memory stores. Zero cloud credentials.
"""
import builtins
import io
import json
import os
import tempfile
import unittest
from unittest import mock

from zenthros import errors
from zenthros.core.checkpoint import (
    Checkpointer,
    checkpoint_uri,
    forget_acked,
    load_state,
    object_key,
    phase,
    write_blob,
)
from zenthros.core.storage import FileBlobStore, GCSBlobStore, MemoryBlobStore, S3BlobStore, parse_gs_uri, store_for
from zenthros.core.task import Task

from support import AckAgent, job


def _acked(store, uri, state):
    _, digest = write_blob(uri, state, store=store)
    return {"seq": 1, "uri": uri, "sha256": digest, "complete": True}


class CursorsTest(unittest.TestCase):
    def test_skip_all_completed_phases(self):
        """A job resumed after two completed phases runs neither again: the
        cursors carry both, not only the latest."""
        store = MemoryBlobStore()
        cursors = {
            "load": _acked(store, "memory://out/jobs/u/gen/1/load/1", {"rows": 10}),
            "train": _acked(store, "memory://out/jobs/u/gen/1/train/1", {"model": "m"}),
        }
        ran = []
        env = {
            "ZENTHROS_CHECKPOINT_CURSORS": json.dumps(cursors),
            # The latest single cursor is train; load is only in the cursors.
            "ZENTHROS_CHECKPOINT_PHASE": "train",
            "ZENTHROS_CHECKPOINT_URI": cursors["train"]["uri"],
            "ZENTHROS_CHECKPOINT_SEQ": "1",
            "ZENTHROS_CHECKPOINT_SHA256": cursors["train"]["sha256"],
            "ZENTHROS_CHECKPOINT_COMPLETE": "true",
        }
        with mock.patch.dict(os.environ, env, clear=False), \
                mock.patch("zenthros.core.checkpoint.store_for", return_value=store):

            @phase("load")
            def load(ctx):
                ran.append("load")
                return {"rows": 0}

            @phase("train")
            def train(ctx):
                ran.append("train")
                return {"model": "new"}

            self.assertEqual(load(), {"rows": 10})
            self.assertEqual(train(), {"model": "m"})
        self.assertEqual(ran, [])

    def test_incomplete_phase_in_cursors_resumes_at_its_seq(self):
        store = MemoryBlobStore()
        cursor = _acked(store, "memory://out/jobs/u/gen/1/load/1", {"part": 3})
        cursor.update(seq=4, complete=False)
        with mock.patch.dict(os.environ, {"ZENTHROS_CHECKPOINT_CURSORS": json.dumps({"load": cursor})}, clear=False):
            ctx = Checkpointer("load", store=store)
        self.assertEqual((ctx.seq, ctx.completed, ctx.state), (4, False, {"part": 3}))


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            err = Exception("NoSuchKey")
            err.response = {"Error": {"Code": "NoSuchKey"}}
            raise err
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


class S3StoreTest(unittest.TestCase):
    def test_s3_store_put_get(self):
        fake = FakeS3()
        store = S3BlobStore(client=fake)
        store.put("s3://bucket/out/jobs/u/gen/1/train/1", b"payload")
        self.assertEqual(fake.objects[("bucket", "out/jobs/u/gen/1/train/1")], b"payload")
        self.assertEqual(store.get("s3://bucket/out/jobs/u/gen/1/train/1"), b"payload")
        with self.assertRaises(FileNotFoundError):
            store.get("s3://bucket/missing")

    def test_s3_missing_boto3_message(self):
        real_import = builtins.__import__

        def no_boto3(name, *args, **kwargs):
            if name == "boto3":
                raise ImportError("No module named 'boto3'")
            return real_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=no_boto3):
            with self.assertRaises(errors.CheckpointError) as caught:
                S3BlobStore().put("s3://bucket/key", b"x")
        self.assertIn("pip install 'zenthros[s3]'", str(caught.exception))

    def test_store_for_s3_uri(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ZENTHROS_CHECKPOINT_LOCAL_ROOT", None)
            self.assertIsInstance(store_for("s3://bucket/out"), S3BlobStore)


class FrozenTest(unittest.TestCase):
    def test_frozen_raises(self):
        """A commit the controller keeps refusing because the lease ended is
        raised as CheckpointFrozen; the caller never trains on believing the
        work is saved."""
        store = MemoryBlobStore()
        calls = []

        def ack(commit):
            calls.append(commit)
            raise errors.CheckpointError("LEASE_NOT_LIVE", "lease is not live; cursor frozen")

        env = {"ZENTHROS_JOB_ID": "u", "ZENTHROS_GENERATION": "1", "ZENTHROS_CHECKPOINT_PREFIX": "memory://out"}
        with mock.patch.dict(os.environ, env, clear=False), mock.patch("zenthros.core.checkpoint.time.sleep"):
            ctx = Checkpointer("train", store=store, agent=AckAgent(ack))
            with self.assertRaises(errors.CheckpointFrozen) as caught:
                ctx.save({"epoch": 1})
        self.assertGreater(len(calls), 1)
        self.assertIn("cursor frozen", str(caught.exception))

    def test_a_refusal_goes_by_its_code_not_its_words(self):
        """A plain refusal that reads like a lease being handed over is
        raised at once as what it is: the words are for people."""
        calls = []

        def ack(commit):
            calls.append(commit)
            raise errors.CheckpointError("REJECTED", "lease is not live; cursor frozen: replacement submitted")

        env = {"ZENTHROS_JOB_ID": "u", "ZENTHROS_CHECKPOINT_PREFIX": "memory://out"}
        with mock.patch.dict(os.environ, env, clear=False), mock.patch("zenthros.core.checkpoint.time.sleep"):
            with self.assertRaises(errors.CheckpointError) as caught:
                Checkpointer("train", store=MemoryBlobStore(), agent=AckAgent(ack)).save({"epoch": 1})
        self.assertNotIsInstance(caught.exception, errors.CheckpointFrozen)
        self.assertEqual(len(calls), 1)

    def test_frozen_code_raises_at_once(self):
        def ack(commit):
            raise errors.from_code("CHECKPOINT_FROZEN", "replacement submitted")

        env = {"ZENTHROS_JOB_ID": "u", "ZENTHROS_CHECKPOINT_PREFIX": "memory://out"}
        with mock.patch.dict(os.environ, env, clear=False):
            with self.assertRaises(errors.CheckpointFrozen):
                Checkpointer("train", store=MemoryBlobStore(), agent=AckAgent(ack)).save({"epoch": 1})


class PhaseCheckpointTest(unittest.TestCase):
    def test_object_key_is_generation_scoped(self):
        self.assertEqual(object_key("uid-1", 3, "train", 5), "jobs/uid-1/gen/3/train/5")
        uri = checkpoint_uri("s3://bucket/out", "uid-1", 3, "train", 5)
        self.assertEqual(uri, "s3://bucket/out/jobs/uid-1/gen/3/train/5")

    def test_resume_at_seq_plus_one(self):
        store = MemoryBlobStore()
        agent = AckAgent()
        prefix = "memory://out"
        env = {
            "ZENTHROS_JOB_ID": "uid-1",
            "ZENTHROS_GENERATION": "1",
            "ZENTHROS_CHECKPOINT_PREFIX": prefix,
        }
        with mock.patch.dict(os.environ, env, clear=False):
            ckpt = Checkpointer("train", store=store, agent=agent)
            ckpt.save({"epoch": 1})
            ckpt.save({"epoch": 2})
        self.assertEqual(ckpt.seq, 2)
        self.assertEqual(agent.acked[-1]["uri"], "memory://out/jobs/uid-1/gen/1/train/2")

        resume_env = {
            "ZENTHROS_JOB_ID": "uid-1",
            "ZENTHROS_GENERATION": "2",
            "ZENTHROS_CHECKPOINT_PREFIX": prefix,
            "ZENTHROS_CHECKPOINT_URI": "memory://out/jobs/uid-1/gen/1/train/2",
            "ZENTHROS_CHECKPOINT_PHASE": "train",
            "ZENTHROS_CHECKPOINT_SEQ": "2",
            "ZENTHROS_CHECKPOINT_SHA256": agent.acked[-1]["sha256"],
            "ZENTHROS_CHECKPOINT_COMPLETE": "false",
        }
        with mock.patch.dict(os.environ, resume_env, clear=False):
            replacement = Checkpointer("train", store=store, agent=agent)
            self.assertEqual(replacement.seq, 2)
            self.assertEqual(replacement.state, {"epoch": 2})
            replacement.save({"epoch": 3})
        self.assertEqual(replacement.seq, 3)
        self.assertEqual(agent.acked[-1]["generation"], 2)
        self.assertIn("/gen/2/train/3", agent.acked[-1]["uri"])

    def test_partition_resumes_frozen_ack_not_later_object(self):
        store = MemoryBlobStore()
        frozen_uri = "memory://out/jobs/uid-1/gen/1/train/2"
        later_uri = "memory://out/jobs/uid-1/gen/1/train/3"
        store.put(frozen_uri, b'{"kind":"Checkpoint","state":{"epoch":2}}')
        store.put(later_uri, b'{"kind":"Checkpoint","state":{"epoch":3}}')
        env = {
            "ZENTHROS_JOB_ID": "uid-1",
            "ZENTHROS_GENERATION": "2",
            "ZENTHROS_CHECKPOINT_PREFIX": "memory://out",
            "ZENTHROS_CHECKPOINT_URI": frozen_uri,
            "ZENTHROS_CHECKPOINT_PHASE": "train",
            "ZENTHROS_CHECKPOINT_SEQ": "2",
            "ZENTHROS_CHECKPOINT_COMPLETE": "false",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            ckpt = Checkpointer("train", store=store, agent=AckAgent())
        self.assertEqual(ckpt.state, {"epoch": 2})

    def test_phase_decorator_writes_complete(self):
        store = MemoryBlobStore()

        @phase("train")
        def train(ckpt):
            ckpt.save({"n": 1})
            return ckpt.state

        with job(store) as agent:
            train()
        self.assertEqual(agent.acked[0]["seq"], 1)
        self.assertTrue(agent.acked[-1]["complete"])

    def test_completed_phase_is_skipped(self):
        store = MemoryBlobStore()
        uri = "memory://out/jobs/uid-1/gen/1/train/1"
        store.put(uri, b'{"kind":"Checkpoint","state":{"done":true}}')
        env = {
            "ZENTHROS_CHECKPOINT_URI": uri,
            "ZENTHROS_CHECKPOINT_PHASE": "train",
            "ZENTHROS_CHECKPOINT_SEQ": "1",
            "ZENTHROS_CHECKPOINT_COMPLETE": "true",
        }
        ran = {"n": 0}

        @phase("train")
        def train(ckpt):
            ran["n"] += 1
            return "ran"

        with job(store, env=env, generation="2"):
            result = train()
        self.assertEqual(ran["n"], 0)
        self.assertEqual(result, {"done": True})


class RecordingStore(MemoryBlobStore):
    """A MemoryBlobStore that remembers each write, in order, and how."""

    def __init__(self):
        super().__init__()
        self.writes = []

    def put(self, uri, payload):
        self.writes.append(("put", uri))
        return super().put(uri, payload)

    def put_file(self, uri, path):
        self.writes.append(("put_file", uri))
        with open(path, "rb") as fh:
            return super().put(uri, fh.read())


def replacement_env(saved):
    """The env the controller gives the replacement: the acked cursor."""
    return {"ZENTHROS_GENERATION": "2", "ZENTHROS_CHECKPOINT_PHASE": "train",
            "ZENTHROS_CHECKPOINT_URI": saved["uri"], "ZENTHROS_CHECKPOINT_SEQ": str(saved["seq"]),
            "ZENTHROS_CHECKPOINT_SHA256": saved["sha256"]}


class CheckpointFilesTest(unittest.TestCase):
    def test_files_are_stored_before_the_json_and_the_json_before_the_ack(self):
        store = RecordingStore()
        seen = []

        def ack(commit):
            # What the store holds when the controller is told: everything
            # the cursor names is already there.
            seen.append(dict(store.data))
            return {"accepted": True, "seq": commit["seq"]}

        with job(store, agent=AckAgent(ack)):
            saved = Checkpointer("train").save({"round": 3}, files={"model.ubj": b"booster"})
        uri = saved["uri"]
        self.assertEqual(store.writes, [("put", f"{uri}.files/model.ubj"), ("put", uri)])
        self.assertIn(uri, seen[0])
        self.assertIn(f"{uri}.files/model.ubj", seen[0])
        body = json.loads(store.data[uri])
        self.assertEqual(body["state"], {"round": 3})
        self.assertEqual(set(body["files"]), {"model.ubj"})

    def test_the_replacement_reads_the_files_back(self):
        store = MemoryBlobStore()
        with job(store) as agent:
            saved = Checkpointer("train").save({"round": 3}, files={"model.ubj": b"booster"})
        forget_acked()
        with job(store, agent=agent, env=replacement_env(saved)):
            ckpt = Checkpointer("train")
            self.assertEqual(ckpt.state, {"round": 3})
            self.assertEqual(ckpt.file("model.ubj"), b"booster")
            with open(ckpt.file_path("model.ubj"), "rb") as fh:
                self.assertEqual(fh.read(), b"booster")
        # The ack names the JSON alone: no file goes through Zenthros.
        self.assertEqual(set(agent.acked[0]), {"job_id", "generation", "phase", "seq", "uri", "sha256", "complete"})

    def test_a_local_file_is_sent_as_a_file(self):
        store = RecordingStore()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "weights.pt")
            with open(path, "wb") as fh:
                fh.write(b"w" * 3000)
            with job(store):
                saved = Checkpointer("train").save({"epoch": 1}, files={"weights.pt": path})
                self.assertEqual(Checkpointer("train").file("weights.pt"), b"w" * 3000)
        self.assertEqual(store.writes[0], ("put_file", f"{saved['uri']}.files/weights.pt"))

    def test_a_changed_file_is_refused(self):
        store = MemoryBlobStore()
        with job(store) as agent:
            saved = Checkpointer("train").save({"round": 3}, files={"model.ubj": b"booster"})
        store.data[f"{saved['uri']}.files/model.ubj"] = b"someone else's"
        forget_acked()
        with job(store, agent=agent, env=replacement_env(saved)), tempfile.TemporaryDirectory() as tmp:
            ckpt = Checkpointer("train")
            with self.assertRaises(errors.CheckpointError):
                ckpt.file("model.ubj")
            path = os.path.join(tmp, "model.ubj")
            with self.assertRaises(errors.CheckpointError):
                ckpt.file_path("model.ubj", path)
            self.assertFalse(os.path.exists(path))

    def test_a_name_is_one_file_name(self):
        for name in ("", ".", "..", "a/b", "../model.ubj"):
            store = RecordingStore()
            with job(store) as agent, self.subTest(name=name):
                with self.assertRaises(errors.ValidationError):
                    Checkpointer("train").save({"round": 1}, files={name: b"x"})
                self.assertEqual(store.writes, [])
                self.assertEqual(agent.acked, [])

    def test_state_that_is_not_json_is_not_turned_into_text(self):
        store = MemoryBlobStore()
        with job(store) as agent:
            with self.assertRaises(TypeError):
                Checkpointer("train").save({"model": object()})
        self.assertEqual(agent.acked, [])

    def test_a_file_stays_until_its_name_is_saved_again(self):
        store = MemoryBlobStore()
        with job(store):
            ckpt = Checkpointer("train")
            first = ckpt.save({"round": 1}, files={"model.ubj": b"one", "vocab.json": b"v"})
            ckpt.save({"round": 2}, files={"model.ubj": b"two"})
            done = ckpt.complete()
        files = json.loads(store.data[done["uri"]])["files"]
        self.assertEqual(files["vocab.json"]["uri"], f"{first['uri']}.files/vocab.json")
        self.assertEqual(store.data[files["model.ubj"]["uri"]], b"two")

    def test_outside_a_job_nothing_is_written(self):
        store = RecordingStore()
        with mock.patch.dict(os.environ, {}, clear=False):
            for name in ("ZENTHROS_JOB_ID", "ZENTHROS_CHECKPOINT_PREFIX", "ZENTHROS_DATA_OUTPUT"):
                os.environ.pop(name, None)
            Checkpointer("train", store=store).save({"round": 1}, files={"model.ubj": b"x"})
        self.assertEqual(store.writes, [])


class Counted(Task):
    """A model that is only bytes: it is saved as a file, not in state()."""

    def setup(self):
        self.model = b""

    def steps(self):
        return 4

    def step(self, i):
        self.model += str(i).encode()

    def files(self):
        return {"model.bin": self.model}

    def restore(self, state):
        self.model = self.checkpointer.file("model.bin")

    def result(self):
        return self.model.decode()


class TaskFilesTest(unittest.TestCase):
    def test_a_task_saves_its_files_and_restores_from_them(self):
        store = MemoryBlobStore()

        class Lost(Counted):
            policy = Counted.policy.__class__(every=2)

            def step(self, i):
                if i == 3:
                    raise KeyboardInterrupt("the machine went")
                super().step(i)

        with job(store) as agent:
            with self.assertRaises(KeyboardInterrupt):
                Lost().run()
        cursor = agent.acked[-1]
        forget_acked()
        env = {"ZENTHROS_GENERATION": "2", "ZENTHROS_CHECKPOINT_PHASE": "Lost", "ZENTHROS_CHECKPOINT_URI": cursor["uri"],
               "ZENTHROS_CHECKPOINT_SEQ": str(cursor["seq"]), "ZENTHROS_CHECKPOINT_SHA256": cursor["sha256"]}
        with job(store, agent=agent, env=env):
            resumed = type("Lost", (Counted,), {})()
            self.assertEqual(resumed.run(), "0123")


class FileStoreTest(unittest.TestCase):
    def test_a_store_without_its_own_moves_files_through_put_and_get(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = os.path.join(tmp, "in.bin"), os.path.join(tmp, "out.bin")
            with open(src, "wb") as fh:
                fh.write(b"payload")
            for store in (MemoryBlobStore(), FileBlobStore(root=tmp)):
                with self.subTest(store=type(store).__name__):
                    store.put_file("s3://b/k", src)
                    store.get_file("s3://b/k", dst)
                    with open(dst, "rb") as fh:
                        self.assertEqual(fh.read(), b"payload")
                    self.assertFalse(os.path.exists(dst + ".part"))


class FakeGCSBlob:
    def __init__(self, store, bucket, key):
        self.store = store
        self.bucket_name = bucket
        self.key = key

    def upload_from_string(self, payload):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        self.store[(self.bucket_name, self.key)] = payload

    def download_as_bytes(self):
        return self.store.get((self.bucket_name, self.key))


class FakeGCSBucket:
    def __init__(self, store, name):
        self.store = store
        self.name = name

    def blob(self, key):
        return FakeGCSBlob(self.store, self.name, key)


class FakeGCSClient:
    def __init__(self):
        self.objects = {}
        self.listed = False

    def bucket(self, name):
        return FakeGCSBucket(self.objects, name)

    def list_blobs(self, *args, **kwargs):
        self.listed = True
        raise AssertionError("resume must not List the bucket")


class CheckpointGCSTest(unittest.TestCase):
    def test_a_checkpoint_uri_is_its_prefix_and_its_object_key(self):
        prefix = "gs://bucket/users/user-a/runs/run_01"
        key = object_key("uid-1", 2, "xgboost", 3)
        self.assertEqual(checkpoint_uri(prefix, "uid-1", 2, "xgboost", 3), prefix + "/" + key)

    def test_parse_gs_uri(self):
        bucket, key = parse_gs_uri("gs://art/users/user-a/runs/run_01/meta.json")
        self.assertEqual(bucket, "art")
        self.assertEqual(key, "users/user-a/runs/run_01/meta.json")

    def test_gcs_store_put_get_no_list(self):
        client = FakeGCSClient()
        store = GCSBlobStore(client=client)
        uri = "gs://art/users/user-a/runs/run_01/jobs/uid/gen/1/xgboost/1"
        write_blob(uri, {"round": 19}, store=store)
        state = load_state(uri, store=store)
        self.assertEqual(state, {"round": 19})
        self.assertFalse(client.listed)
        self.assertFalse(hasattr(store, "list") and callable(store.list))

    def test_gcs_missing_blob_is_file_not_found(self):
        class NotFound(Exception):
            pass

        class RaisingBlob:
            def download_as_bytes(self):
                raise NotFound("404 GET")

        class RaisingBucket:
            def blob(self, key):
                return RaisingBlob()

        class RaisingClient:
            def bucket(self, name):
                return RaisingBucket()

        store = GCSBlobStore(client=RaisingClient())
        with self.assertRaises(FileNotFoundError):
            store.get("gs://art/users/user-a/runs/run_01/meta.json")


    def test_file_store_maps_gs_when_root_injected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileBlobStore(root=tmp)
            uri = "gs://art/users/user-a/runs/run_01/checkpoints/xgboost/1"
            write_blob(uri, {"round": 19, "model_b64": "YQ=="}, store=store)
            state = load_state(uri, store=store)
            self.assertEqual(state["round"], 19)
            later = "gs://art/users/user-a/runs/run_01/checkpoints/xgboost/2"
            write_blob(later, {"round": 39}, store=store)
            self.assertEqual(load_state(uri, store=store)["round"], 19)


    def test_store_for_gs_without_local_root(self):
        with mock.patch.dict(os.environ, {"ZENTHROS_CHECKPOINT_LOCAL_ROOT": ""}, clear=False):
            store = store_for("gs://art/x")
        self.assertIsInstance(store, GCSBlobStore)

    def test_local_root_maps_every_scheme(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {"ZENTHROS_CHECKPOINT_LOCAL_ROOT": tmp}, clear=False):
                for uri in ("gs://art/x", "s3://b/x", "/tmp/x"):
                    self.assertIsInstance(store_for(uri), FileBlobStore)
                    self.assertEqual(str(store_for(uri).root), tmp)


if __name__ == "__main__":
    unittest.main()
