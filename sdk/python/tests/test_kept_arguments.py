# SPDX-License-Identifier: Apache-2.0

"""An argument that pickles to more than 1 MiB goes to the store apart, by
its digest, at <root>/data/<sha256>: the same DataFrame is sent once, however
many calls pass it (observaciones P20: with a bucket, each call uploaded it
again, inside a call whose digest has its time).

A bucket's own rule deletes <root>/data/ after 30 days (zenthros store says
how to set it): what is there for 15 days or more is sent again, which renews
it, so a job that starts within 15 days of its call finds its arguments."""

import contextlib
import datetime
import io
import unittest
from unittest import mock

import pandas as pd

from zenthros import errors, remote, upload, userstore
from zenthros.core.storage import MemoryBlobStore
from zenthros.remote import payload

from test_remote import STORE, ExecutingClient

DATA = f"{STORE}/data/"


def rows(frame, scale=1):
    return len(frame) * scale


def total(values):
    return sum(len(v) for v in values)


def a_frame(seed):
    """~5 MB that do not pack away: a DataFrame as a user passes one."""
    rng = __import__("numpy").random.default_rng(seed)
    return pd.DataFrame({"x": rng.random(300_000), "y": rng.random(300_000)})


class Counting(MemoryBlobStore):
    def __init__(self):
        super().__init__()
        self.puts = []

    def put(self, uri, payload):
        self.puts.append(uri)
        return super().put(uri, payload)

    def data_puts(self):
        return [uri for uri in self.puts if uri.startswith(DATA)]


class KeptArguments(unittest.TestCase):
    def setUp(self):
        self.store = Counting()
        self.client = ExecutingClient(self.store)
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo-ml")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        quiet = contextlib.redirect_stderr(io.StringIO())
        quiet.__enter__()
        self.addCleanup(quiet.__exit__, None, None, None)

    def age(self, days):
        for uri in self.store.data_puts():
            self.store.written[uri] = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)

    def test_the_same_frame_is_sent_once(self):
        frame = a_frame(1)
        first = remote.run(rows, frame, code_dir=None)
        second = remote.run(rows, frame, scale=2, code_dir=None)
        self.assertEqual((first.result(stream_logs=False), second.result(stream_logs=False)), (300_000, 600_000))
        self.assertEqual(len(self.store.data_puts()), 1, self.store.data_puts())

    def test_another_frame_is_sent_again(self):
        remote.run(rows, a_frame(1), code_dir=None).result(stream_logs=False)
        remote.run(rows, a_frame(2), code_dir=None).result(stream_logs=False)
        self.assertEqual(len(set(self.store.data_puts())), 2)

    def test_a_small_argument_stays_in_the_call(self):
        remote.run(rows, a_frame(1).head(100), code_dir=None).result(stream_logs=False)
        self.assertEqual(self.store.data_puts(), [])

    def test_one_there_for_15_days_or_more_is_sent_again_and_a_newer_one_is_not(self):
        frame = a_frame(1)
        remote.run(rows, frame, code_dir=None).result(stream_logs=False)
        self.age(3)
        remote.run(rows, frame, code_dir=None).result(stream_logs=False)
        self.assertEqual(len(self.store.data_puts()), 1, "3 days: it is there")
        self.age(20)
        remote.run(rows, frame, code_dir=None).result(stream_logs=False)
        self.assertEqual(len(self.store.data_puts()), 2, "20 days: the bucket's rule may take it before the job starts")

    def test_a_map_sends_its_items_and_its_shared_arguments_once(self):
        frame = a_frame(1)
        mapped = remote.map(rows, [frame, frame, frame], code_dir=None)
        self.assertEqual(mapped.result(stream_logs=False), [300_000] * 3)
        self.assertEqual(len(self.store.data_puts()), 1)
        shared = remote.cloud(rows, code_dir=None).map([{"scale": 1}, {"scale": 3}], shared={"frame": frame})
        self.assertEqual(shared.result(stream_logs=False), [300_000, 900_000])
        self.assertEqual(len(self.store.data_puts()), 1, "the shared frame is the same bytes")

    def test_bytes_other_than_the_calls_are_refused_where_it_runs(self):
        call = remote.run(rows, a_frame(1), code_dir=None)
        [uri] = self.store.data_puts()
        self.store.data[uri] = payload.dump({"value": b"forged"})
        with self.assertRaisesRegex(remote.RemoteError, "SHA-256"):
            call.result(stream_logs=False)

    def test_a_rerun_renews_an_old_argument_and_says_when_it_is_gone(self):
        first = remote.run(rows, a_frame(1), code_dir=None)
        first.result(stream_logs=False)
        [uri] = self.store.data_puts()
        self.age(20)
        self.assertEqual(remote.get(first.name).rerun().result(stream_logs=False), 300_000)
        self.assertEqual(self.store.data_puts(), [uri, uri], "renewed in place")
        del self.store.data[uri]
        with self.assertRaisesRegex(errors.ValidationError, "gone from the store"):
            remote.get(first.name).rerun()


    def test_a_rerun_to_a_place_of_another_store_sends_the_values(self):
        frame = a_frame(1)
        first = remote.run(rows, frame, code_dir=None)
        first.result(stream_logs=False)
        sent = []

        def run(fn, *args, **kwargs):
            sent.append(args)
            return mock.Mock()

        # The place's store first, then the job's: another bucket.
        with mock.patch("zenthros.remote.call._store_or_carried", side_effect=["gs://other/zenthros", STORE]), \
                mock.patch("zenthros.remote.call.run", side_effect=run):
            remote.get(first.name).rerun(on="elsewhere")
        [args] = sent
        self.assertIsInstance(args[0], pd.DataFrame, "a reference into a store the new place does not read")
        self.assertTrue(args[0].equals(frame))


class LocalDataLifecycle(unittest.TestCase):
    """A local store has no bucket rule: zenthros clean --results takes what
    data/ holds for LIFECYCLE_DAYS or more (a newer one is renewed by a call
    that sends it again, as in a bucket)."""

    def test_clean_results_takes_old_data_and_leaves_new(self):
        import os
        import tempfile
        import time

        from zenthros.jobs import local as localjobs

        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, {localjobs.ROOT_ENV: root}):
            client = localjobs.LocalJobClient(root=root)
            data = client.store_dir / "data"
            data.mkdir(parents=True)
            old, new = data / ("a" * 64), data / ("b" * 64)
            old.write_bytes(b"old"), new.write_bytes(b"new")
            past = time.time() - (upload.LIFECYCLE_DAYS + 1) * 86400
            os.utime(old, (past, past))
            self.assertIn("data", client.clean_results())
            self.assertEqual(sorted(p.name for p in data.iterdir()), ["b" * 64])


class LifecycleAdvice(unittest.TestCase):
    def test_it_names_the_prefix_and_the_days_for_each_cloud(self):
        google = userstore.lifecycle_advice("gs://my-bucket/zenthros")
        self.assertIn("gcloud storage buckets update gs://my-bucket", google)
        self.assertIn('"matchesPrefix": ["zenthros/data/"]', google)
        self.assertIn(f'"age": {upload.LIFECYCLE_DAYS}', google)
        aws = userstore.lifecycle_advice("s3://my-bucket/zenthros")
        self.assertIn("aws s3api put-bucket-lifecycle-configuration --bucket my-bucket", aws)
        self.assertIn('"Prefix": "zenthros/data/"', aws)
        self.assertIn(f'"Days": {upload.LIFECYCLE_DAYS}', aws)
        for said in (google, aws):
            self.assertIn("replaces", said, "the rule replaces the bucket's own: say so")


if __name__ == "__main__":
    unittest.main()
