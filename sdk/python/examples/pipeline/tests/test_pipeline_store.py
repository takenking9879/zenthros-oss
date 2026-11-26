#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The example pipeline's store (examples/pipeline/store.py) against an S3
that grants what an hour of STS credentials from zenthros.aws grants: the
objects under one prefix, and listing the bucket only under it.

Found on real AWS (Floci enforces no IAM): pyarrow's create_dir asks
HeadBucket first, which S3 authorizes as ListBucket on the whole bucket, and
the job failed with ACCESS_DENIED before writing a byte. S3 has no
directories: a write needs no parent made.
"""
import json
import os
import re
import sys
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import store
except ImportError:  # pragma: no cover - the example's requirements are not installed
    store = None

BUCKET = "data"
PREFIX = "pipeline/run/"


class PrefixOnlyS3(BaseHTTPRequestHandler):
    """Objects under BUCKET/PREFIX, kept in memory; 403 for anything else,
    as S3 answers a request its policy does not allow."""

    protocol_version = "HTTP/1.1"  # answers Expect: 100-continue at once
    objects = {}
    parts = {}
    requests = []

    def log_message(self, *args):
        pass

    def _place(self):
        url = urllib.parse.urlsplit(self.path)
        bucket, _, key = url.path.lstrip("/").partition("/")
        return bucket, urllib.parse.unquote(key), urllib.parse.parse_qs(url.query, keep_blank_values=True)

    def _allowed(self, bucket, key, query):
        if bucket != BUCKET:
            return False
        if self.command == "POST" and "delete" in query:
            return True  # DeleteObjects: S3 authorizes each key it names, below
        if key:
            return key.startswith(PREFIX)
        # ListBucket, only with a prefix inside PREFIX.
        return "list-type" in query and query.get("prefix", [""])[0].startswith(PREFIX)

    def _body(self):
        """The request's bytes: pyarrow sends parts chunked, and each chunk
        in aws-chunked framing (size;chunk-signature=..., then trailers)."""
        if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
            return self.rfile.read(int(self.headers.get("Content-Length") or 0))
        raw = b""
        while True:
            size = int(self.rfile.readline().split(b";")[0], 16)
            raw += self.rfile.read(size)
            self.rfile.readline()
            if size == 0:
                break
        if "aws-chunked" not in self.headers.get("Content-Encoding", ""):
            return raw
        data = b""
        while raw:
            line, _, raw = raw.partition(b"\r\n")
            size = int(line.split(b";")[0], 16)
            if size == 0:
                break
            data, raw = data + raw[:size], raw[size + 2:]
        return data

    def _answer(self, code, body=b"", headers=None):
        self.send_response(code)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        if "Content-Length" not in (headers or {}):
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _handle(self):
        bucket, key, query = self._place()
        body = self._body()
        allowed = self._allowed(bucket, key, query)
        type(self).requests.append((self.command, bucket, key, 200 if allowed else 403))
        if not allowed:
            return self._answer(403, b"<Error><Code>AccessDenied</Code></Error>")
        if not key and "delete" in query:
            keys = re.findall(r"<Key>([^<]*)</Key>", body.decode())
            if not all(k.startswith(PREFIX) for k in keys):
                return self._answer(403, b"<Error><Code>AccessDenied</Code></Error>")
            for k in keys:
                self.objects.pop(k, None)
            done = "".join(f"<Deleted><Key>{k}</Key></Deleted>" for k in keys)
            return self._answer(200, f"<DeleteResult>{done}</DeleteResult>".encode())
        if not key:  # ListObjectsV2 under the prefix
            prefix = query.get("prefix", [""])[0]
            keys = [(k, v) for k, v in sorted(self.objects.items()) if k.startswith(prefix)]
            found = "".join(f"<Contents><Key>{k}</Key><Size>{len(v)}</Size></Contents>" for k, v in keys)
            return self._answer(200, f"<ListBucketResult><Name>{bucket}</Name><KeyCount>{len(keys)}</KeyCount>{found}"
                                     "<IsTruncated>false</IsTruncated></ListBucketResult>".encode())
        if self.command == "POST" and "uploads" in query:  # a multipart upload starts
            self.parts[key] = {}
            return self._answer(200, f"<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key>"
                                     "<UploadId>1</UploadId></InitiateMultipartUploadResult>".encode())
        if self.command == "PUT" and "partNumber" in query:
            self.parts[key][int(query["partNumber"][0])] = body
            return self._answer(200, headers={"ETag": '"0"'})
        if self.command == "POST" and "uploadId" in query:  # ... and completes
            self.objects[key] = b"".join(part for _, part in sorted(self.parts.pop(key).items()))
            return self._answer(200, f"<CompleteMultipartUploadResult><Key>{key}</Key><ETag>\"0\"</ETag>"
                                     "</CompleteMultipartUploadResult>".encode())
        if self.command == "DELETE":  # S3 answers 204 for a key that is not there too
            self.objects.pop(key, None)
            return self._answer(204)
        if self.command == "PUT":
            self.objects[key] = body
            return self._answer(200, headers={"ETag": '"0"'})
        if key not in self.objects:
            return self._answer(404)
        data = self.objects[key]
        return self._answer(200, data if self.command == "GET" else b"",
                            {"Content-Type": "application/octet-stream", "Content-Length": str(len(data)),
                             "ETag": '"0"', "Last-Modified": "Wed, 14 Oct 2026 00:00:00 GMT"})

    do_GET = do_PUT = do_HEAD = do_POST = do_DELETE = _handle


@unittest.skipIf(store is None, "pip install -r examples/pipeline/requirements.txt")
class StoreOnAPrefixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), PrefixOnlyS3)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.addClassCleanup(cls.server.shutdown)

    def setUp(self):
        PrefixOnlyS3.objects, PrefixOnlyS3.parts, PrefixOnlyS3.requests = {}, {}, []
        env = {"AWS_ENDPOINT_URL": f"http://127.0.0.1:{self.server.server_port}", "AWS_REGION": "us-east-2",
               "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_EC2_METADATA_DISABLED": "true"}
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def outside(self):
        """Requests the policy refused: what AWS would answer 403."""
        return [r for r in PrefixOnlyS3.requests if r[3] == 403]

    def test_a_table_is_written_with_no_request_outside_its_prefix(self):
        uri = f"s3://{BUCKET}/{PREFIX}raw/orders/part-0000.parquet"
        store.write_parquet(pa.table({"x": [1, 2, 3]}), uri)
        self.assertEqual(self.outside(), [], PrefixOnlyS3.requests)
        written = pq.read_table(pa.BufferReader(PrefixOnlyS3.objects[f"{PREFIX}raw/orders/part-0000.parquet"]))
        self.assertEqual(written.column("x").to_pylist(), [1, 2, 3])

    def test_json_is_written_with_no_request_outside_its_prefix(self):
        uri = f"s3://{BUCKET}/{PREFIX}result.json"
        store.write_json(uri, {"rows": 3})
        self.assertEqual(self.outside(), [], PrefixOnlyS3.requests)
        self.assertEqual(json.loads(PrefixOnlyS3.objects[f"{PREFIX}result.json"]), {"rows": 3})

    def test_what_the_pipeline_reads_and_removes_stays_inside_its_prefix(self):
        # gen_data and run.py: a manifest read before it exists, a table
        # written, counted and listed, then the folder removed for a rerun.
        base = f"s3://{BUCKET}/{PREFIX}raw"
        self.assertIsNone(store.read_json(store.join(base, "manifest.json")))
        for index in range(2):
            store.write_parquet(pa.table({"x": list(range(index + 2))}), store.join(base, "orders", f"part-{index}.parquet"))
        store.write_json(store.join(base, "manifest.json"), {"orders": 5})
        self.assertEqual(store.read_json(store.join(base, "manifest.json")), {"orders": 5})
        self.assertEqual(len(store.parquet_files(store.join(base, "orders"))), 2)
        self.assertEqual(store.parquet_rows(store.join(base, "orders")), 5)
        store.remove_dir(store.join(base, "orders"))
        self.assertEqual(store.parquet_files(store.join(base, "orders")), [])
        self.assertEqual(self.outside(), [], PrefixOnlyS3.requests)

    def test_a_local_table_still_gets_its_directory(self):
        import tempfile

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "raw", "orders", "part-0000.parquet")
            store.write_parquet(pa.table({"x": [1]}), path)
            self.assertTrue(os.path.isfile(path))


if __name__ == "__main__":
    unittest.main()
