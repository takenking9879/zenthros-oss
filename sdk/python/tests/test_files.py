#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.files: a function reads and writes the Workspace's store by URI,
the same here and on the machine that runs it, and a local path too.

Here (a zenthros-api connection, no cloud credentials) through URLs the API
signs; in a job (ZENTHROS_JOB_ID) with the machine's own store access, never
the API; a path that is no s3:// or gs:// URI is a local file.
"""
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from zenthros import files
from zenthros.core.storage import S3BlobStore
from zenthros.remote import bundle

from support import FakeAPI
from test_view import listing


class LocalTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, clean, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_path_is_a_local_file(self):
        path = os.path.join(self.dir, "out", "model.pkl")
        self.assertEqual(files.write_bytes(path, b"m"), path)
        self.assertEqual(files.read_bytes(path), b"m")
        with files.open(path) as fh:
            self.assertEqual(fh.read(), b"m")
        with files.open(os.path.join(self.dir, "out", "notes.txt"), "w") as fh:
            fh.write("hola")
        self.assertEqual(files.read_bytes(os.path.join(self.dir, "out", "notes.txt")), b"hola")
        self.assertEqual([(os.path.basename(o.uri), o.size) for o in files.ls(os.path.join(self.dir, "out"))],
                         [("model.pkl", 1), ("notes.txt", 4)])
        # A prefix that ends mid-name, as on a store.
        self.assertEqual([os.path.basename(o.uri) for o in files.ls(os.path.join(self.dir, "out", "mo"))], ["model.pkl"])
        with self.assertRaises(FileNotFoundError):
            files.read_bytes(os.path.join(self.dir, "nope"))

    def test_only_read_and_write_modes(self):
        for mode in ("a", "r+", "x", "rb+"):
            with self.assertRaises(ValueError, msg=mode):
                files.open(os.path.join(self.dir, "x"), mode)

    def test_it_travels_with_every_call(self):
        self.assertIn("zenthros/files.py", bundle.sdk_files())


class HereTest(unittest.TestCase):
    """A notebook with a key: the store through signed URLs."""

    def setUp(self):
        self.api, self.s3 = FakeAPI(), FakeAPI()
        self.addCleanup(self.api.close)
        self.addCleanup(self.s3.close)
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://calls/zenthros/risk"})
        self.objects = {"zenthros/risk/in.csv": b"a,b"}

        def sign(body):
            asked = json.loads(body)
            if asked["method"] == "LIST":
                self.s3.routes[("GET", "/list")] = (200, listing([("zenthros/risk/in.csv", 3)]))
                return 200, {"url": self.s3.url + "/list", "method": "GET", "headers": {}}
            key = asked["uri"][len("s3://calls/"):]
            if asked["method"] == "GET":
                self.s3.routes[("GET", "/" + key)] = (200, self.objects[key])
            else:
                self.s3.routes[("PUT", "/" + key)] = (200, b"")
            return 200, {"url": f"{self.s3.url}/{key}", "method": asked["method"], "headers": {}}

        self.api.routes[("POST", "/v1/workspaces/risk/objects/presign")] = sign
        clean = {k: v for k, v in os.environ.items() if not k.startswith(("ZENTHROS_", "AWS_"))}
        patcher = mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.devnull + ".none",
                                               "ZENTHROS_SERVER": self.api.url, "ZENTHROS_TOKEN": "bob-token",
                                               "ZENTHROS_WORKSPACE": "risk"}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def puts(self):
        return {r["path"]: r["body"] for r in self.s3.requests if r["method"] == "PUT"}

    def test_reads_writes_and_lists_through_signed_urls(self):
        self.assertEqual(files.read_bytes("s3://calls/zenthros/risk/in.csv"), b"a,b")
        with files.open("s3://calls/zenthros/risk/in.csv") as fh:
            self.assertEqual(fh.read(), b"a,b")
        with files.open("s3://calls/zenthros/risk/in.csv", "r") as fh:
            self.assertEqual(fh.read(), "a,b")
        files.write_bytes("s3://calls/zenthros/risk/out/m.pkl", b"m")
        self.assertEqual([o.uri for o in files.ls("s3://calls/zenthros/risk/")], ["s3://calls/zenthros/risk/in.csv"])
        self.assertEqual(self.puts(), {"/zenthros/risk/out/m.pkl": b"m"})
        self.assertTrue(all(r["auth"] is None for r in self.s3.requests), "the token reached the store")

    def test_a_file_opened_to_write_goes_when_closed_and_not_after_an_error(self):
        with files.open("s3://calls/zenthros/risk/out/a.txt", "w") as fh:
            fh.write("uno ")
            fh.write("dos")
            self.assertEqual(self.puts(), {}, "sent before it was closed")
        self.assertEqual(self.puts(), {"/zenthros/risk/out/a.txt": b"uno dos"})
        with self.assertRaises(RuntimeError):
            with files.open("s3://calls/zenthros/risk/out/b.bin", "wb") as fh:
                fh.write(b"half")
                raise RuntimeError("the function failed")
        self.assertNotIn("/zenthros/risk/out/b.bin", self.puts())
        with self.assertRaises(RuntimeError):
            with files.open("s3://calls/zenthros/risk/out/c.txt", "w") as fh:
                fh.write("half")
                raise RuntimeError("the function failed")
        self.assertNotIn("/zenthros/risk/out/c.txt", self.puts())


class UploadTest(unittest.TestCase):
    def test_a_failed_send_closes_the_file_and_is_not_tried_again(self):
        # Left open, the garbage collector closed it later and sent it again.
        sent = []

        def fail(uri, raw):
            sent.append(raw)
            raise OSError("the store is down")

        with mock.patch.object(files, "write_bytes", fail):
            upload = files.open("s3://b/out/m.bin", "wb")
            upload.write(b"m")
            with self.assertRaises(OSError):
                upload.close()
            self.assertTrue(upload.closed)
            upload.close()
        self.assertEqual(sent, [b"m"])


class InAJobTest(unittest.TestCase):
    """On the machine: the store as the job reaches it (boto3, the endpoint
    the hub set), never zenthros-api, even with a connection configured."""

    def test_a_job_uses_its_own_store_access(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        env = {"ZENTHROS_JOB_ID": "zr-1", "ZENTHROS_CHECKPOINT_LOCAL_ROOT": root,
               "ZENTHROS_SERVER": "http://127.0.0.1:9", "ZENTHROS_TOKEN": "t", "ZENTHROS_WORKSPACE": "risk"}
        with mock.patch.dict(os.environ, env), \
                mock.patch("zenthros.remote.options.blob_store", side_effect=AssertionError("asked the API")):
            files.write_bytes("s3://calls/zenthros/risk/out/m.pkl", b"m")
            self.assertEqual(files.read_bytes("s3://calls/zenthros/risk/out/m.pkl"), b"m")
            self.assertEqual([(o.uri, o.size) for o in files.ls("s3://calls/zenthros/risk/out/")],
                             [("s3://calls/zenthros/risk/out/m.pkl", 1)])
        self.assertTrue(os.path.exists(os.path.join(root, "calls", "zenthros", "risk", "out", "m.pkl")))

    def test_s3_lists_every_page_of_the_prefix(self):
        import datetime

        when = datetime.datetime(2026, 10, 15, tzinfo=datetime.timezone.utc)

        class Paginator:
            def paginate(self, **kwargs):
                self.asked = kwargs
                return [{"Contents": [{"Key": "p/a", "Size": 1, "LastModified": when}]},
                        {"Contents": [{"Key": "p/b/c", "Size": 2, "LastModified": when}]}, {}]

        paginator = Paginator()
        client = mock.Mock()
        client.get_paginator.return_value = paginator
        found = S3BlobStore(client).ls("s3://calls/p/")
        client.get_paginator.assert_called_once_with("list_objects_v2")
        self.assertEqual(paginator.asked, {"Bucket": "calls", "Prefix": "p/"})
        self.assertEqual([(o.uri, o.size, o.modified) for o in found], [("s3://calls/p/a", 1, when), ("s3://calls/p/b/c", 2, when)])


if __name__ == "__main__":
    unittest.main()
