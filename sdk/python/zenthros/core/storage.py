# SPDX-License-Identifier: Apache-2.0

"""Blob stores: put and get by URI, and ls of a prefix (zenthros.files).

store_for(uri) picks one by scheme: s3:// (boto3), gs:// (google-cloud-
storage), else the local filesystem. ZENTHROS_CHECKPOINT_LOCAL_ROOT maps
every scheme onto a local directory (tests, Floci without S3).
"""

import datetime
import glob
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from zenthros import errors
from zenthros.core.env import JobEnv


@dataclass(frozen=True)
class StoredObject:
    """One object of a listing: its URI, its size in bytes, when it was
    last written (UTC), and what changes whenever it is written again (a
    generation, an ETag), which its size and date do not promise."""

    uri: str
    size: int
    modified: datetime.datetime = None
    version: str = ""


class BlobStore:
    """A store by URI. Every store puts and gets bytes; put_file and get_file
    move a local file, here through memory, streamed where the store can."""

    def put(self, uri, payload):
        raise NotImplementedError

    def get(self, uri):
        raise NotImplementedError

    def where(self, uri):
        """What names uri's object in this process, whichever store object
        reaches it: the URI itself for a bucket or a path."""
        return uri

    def put_file(self, uri, path):
        return self.put(uri, Path(path).read_bytes())

    def tail(self, uri, size):
        """The object's last size bytes (all of it, when shorter)."""
        return self.get(uri)[-size:]

    def get_file(self, uri, path):
        """The object into a local file, which appears only once complete."""
        partial = f"{path}.part"
        try:
            Path(partial).write_bytes(self.get(uri))
            os.replace(partial, path)
        finally:
            if os.path.exists(partial):
                os.remove(partial)
        return path


class MemoryBlobStore(BlobStore):
    def __init__(self):
        self.data = {}

    def put(self, uri, payload):
        self.data[uri] = payload
        return uri

    def get(self, uri):
        if uri not in self.data:
            raise FileNotFoundError(uri)
        return self.data[uri]

    def ls(self, prefix):
        return [StoredObject(uri, len(payload), version=hashlib.sha256(payload).hexdigest())
                for uri, payload in sorted(self.data.items()) if uri.startswith(str(prefix))]

    def delete(self, uri):
        self.data.pop(uri, None)

    def where(self, uri):
        return (self, uri)


class FileBlobStore(BlobStore):
    """file://, local paths, and s3:// or gs:// under an injected local root."""

    def __init__(self, root=None):
        self.root = Path(root) if root else None

    def put(self, uri, payload):
        """All of payload or none of it: whoever reads the path never sees a
        part, as with a bucket's PUT."""
        path = self._path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.part")
        try:
            partial.write_bytes(payload)
            os.replace(partial, path)
        finally:
            partial.unlink(missing_ok=True)
        return uri

    def get(self, uri):
        return self._path(uri).read_bytes()

    def tail(self, uri, size):
        with open(self._path(uri), "rb") as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - size))
            return handle.read()

    def delete(self, uri):
        self._path(uri).unlink(missing_ok=True)

    def where(self, uri):
        return str(self._path(uri))

    def ls(self, prefix):
        """The files whose path starts with prefix's, as URIs of prefix's
        kind: a folder lists its whole tree."""
        prefix = str(prefix)
        base = self._path(prefix)
        start = str(base) + (os.sep if base.is_dir() else "")
        # Only what starts with prefix: a store's folder can hold many jobs.
        tops = [base] if base.is_dir() else base.parent.glob(glob.escape(base.name) + "*")
        files = (found for top in tops for found in ([top] if top.is_file() else top.rglob("*")))
        found = []
        for path in sorted(p for p in files if p.is_file() and str(p).startswith(start)):
            stat = path.stat()
            rel = os.path.relpath(path, base).replace(os.sep, "/")
            if prefix.startswith(("s3://", "gs://")):
                uri = prefix.rstrip("/") + "/" + rel if base.is_dir() else prefix + str(path)[len(str(base)):]
            else:
                uri = str(path)
            found.append(StoredObject(uri, stat.st_size,
                                      datetime.datetime.fromtimestamp(stat.st_mtime, datetime.timezone.utc),
                                      f"{stat.st_mtime_ns}-{stat.st_ctime_ns}-{stat.st_ino}"))
        return found

    def _path(self, uri):
        if uri.startswith("file://"):
            return Path(urlparse(uri).path)
        if uri.startswith(("s3://", "gs://")) or is_carried(uri):
            if self.root is None:
                raise errors.CheckpointError(
                    "CHECKPOINT_REJECTED", "an object-store URI needs an object store or a local root")
            parsed = urlparse(uri)
            return self.root / parsed.netloc / parsed.path.lstrip("/")
        return Path(uri)


# What a machine with no role of its own reads a gs:// store with: the key of an
# account that reaches only that prefix (userstore.job_key), sealed to it.
GCS_KEY_ENV = "GOOGLE_STORE_KEY"


class GCSBlobStore(BlobStore):
    """gs:// put/get (pip install 'zenthros[gcs]')."""

    def __init__(self, client=None):
        self._client = client

    def _gcs(self):
        if self._client is None:
            from google.cloud import storage

            key = os.environ.get(GCS_KEY_ENV)
            self._client = storage.Client.from_service_account_info(json.loads(key)) if key else storage.Client()
        return self._client

    def put(self, uri, payload):
        bucket, key = parse_gs_uri(uri)
        self._gcs().bucket(bucket).blob(key).upload_from_string(payload)
        return uri

    def get(self, uri):
        bucket, key = parse_gs_uri(uri)
        blob = self._gcs().bucket(bucket).blob(key)
        try:
            data = blob.download_as_bytes()
        except Exception as exc:
            if exc.__class__.__name__ == "NotFound" or getattr(exc, "code", None) == 404:
                raise FileNotFoundError(uri) from exc
            raise
        if data is None:
            raise FileNotFoundError(uri)
        return data

    def tail(self, uri, size):
        bucket, key = parse_gs_uri(uri)
        return self._gcs().bucket(bucket).blob(key).download_as_bytes(start=-size)

    def put_file(self, uri, path):
        bucket, key = parse_gs_uri(uri)
        self._gcs().bucket(bucket).blob(key).upload_from_filename(os.fspath(path))
        return uri

    def get_file(self, uri, path):
        bucket, key = parse_gs_uri(uri)
        partial = f"{path}.part"
        try:
            self._gcs().bucket(bucket).blob(key).download_to_filename(partial)
            os.replace(partial, path)
        except Exception as exc:
            if exc.__class__.__name__ == "NotFound":
                raise FileNotFoundError(uri) from exc
            raise
        finally:
            if os.path.exists(partial):
                os.remove(partial)
        return path

    def delete(self, uri):
        """Delete one object; one already gone is no error."""
        bucket, key = parse_gs_uri(uri)
        try:
            self._gcs().bucket(bucket).blob(key).delete()
        except Exception as exc:
            if exc.__class__.__name__ != "NotFound":
                raise

    def ls(self, prefix):
        parsed = urlparse(str(prefix))
        return [StoredObject(f"gs://{parsed.netloc}/{blob.name}", blob.size or 0, blob.updated, str(blob.generation or ""))
                for blob in self._gcs().list_blobs(parsed.netloc, prefix=parsed.path.lstrip("/"))]


class S3BlobStore(BlobStore):
    """s3:// put/get with boto3 (pip install 'zenthros[s3]'). Credentials come
    from boto3's own chain: an instance role, env, or a profile."""

    def __init__(self, client=None):
        self._client = client

    def _s3(self):
        if self._client is None:
            try:
                import boto3
            except ImportError as exc:
                raise errors.CheckpointError(
                    "CHECKPOINT_REJECTED", "s3:// needs boto3: pip install 'zenthros[s3]'") from exc
            self._client = boto3.client("s3")
        return self._client

    def put(self, uri, payload):
        bucket, key = parse_s3_uri(uri)
        self._s3().put_object(Bucket=bucket, Key=key, Body=payload)
        return uri

    def get(self, uri):
        bucket, key = parse_s3_uri(uri)
        try:
            response = self._s3().get_object(Bucket=bucket, Key=key)
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(uri) from exc
            raise
        return response["Body"].read()

    def tail(self, uri, size):
        bucket, key = parse_s3_uri(uri)
        return self._s3().get_object(Bucket=bucket, Key=key, Range=f"bytes=-{size}")["Body"].read()

    def put_file(self, uri, path):
        """Upload a local file, read as it is sent (multipart when large)."""
        bucket, key = parse_s3_uri(uri)
        self._s3().upload_file(os.fspath(path), bucket, key)
        return uri

    def get_file(self, uri, path):
        """Download an object into a local file, which appears only once
        complete."""
        bucket, key = parse_s3_uri(uri)
        partial = f"{path}.part"
        try:
            self._s3().download_file(bucket, key, partial)
            os.replace(partial, path)
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(uri) from exc
            raise
        finally:
            if os.path.exists(partial):
                os.remove(partial)
        return path

    def delete(self, uri):
        """Delete one object; one already gone is no error."""
        bucket, key = parse_s3_uri(uri)
        self._s3().delete_object(Bucket=bucket, Key=key)

    def ls(self, prefix):
        """Every object under prefix, page by page."""
        parsed = urlparse(str(prefix))
        if parsed.scheme != "s3" or not parsed.netloc:
            raise errors.ValidationError(f"invalid s3 prefix: {prefix}")
        pages = self._s3().get_paginator("list_objects_v2").paginate(Bucket=parsed.netloc, Prefix=parsed.path.lstrip("/"))
        return [StoredObject(f"s3://{parsed.netloc}/{item['Key']}", item.get("Size", 0), item.get("LastModified"), item.get("ETag", ""))
                for page in pages for item in page.get("Contents") or []]


# A job with no bucket carries what it needs in its own files (remote/carried.py):
# URIs of this root, unpacked in this folder of the job's directory.
CARRIED_ROOT = "inline://job"
CARRIED_FOLDER = ".zenthros-carried"


def is_carried(uri):
    return str(uri).startswith(CARRIED_ROOT + "/")


def carried_on_this_machine():
    return os.path.join(os.getcwd(), CARRIED_FOLDER)


def store_for(uri="", env=None):
    """The store for uri. With a local root set, every URI maps onto it."""
    env = env or JobEnv.current()
    if env.local_root:
        return FileBlobStore(root=env.local_root)
    uri = str(uri or "")
    if is_carried(uri):
        return FileBlobStore(root=carried_on_this_machine())
    if uri.startswith("s3://"):
        return S3BlobStore()
    if uri.startswith("gs://"):
        return GCSBlobStore()
    return FileBlobStore()


def object_counts(uris):
    """{uri: how many objects its store lists under it}. Reads no data. A
    place runs it to say what it sees (a pipeline's plan with probe=True):
    it is here, in what every call ships, so that it is there by name."""
    return {uri: len(store_for(uri).ls(uri)) for uri in uris}


def parse_s3_uri(uri):
    parsed = urlparse(str(uri or ""))
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise errors.CheckpointError("CHECKPOINT_REJECTED", f"invalid s3 uri: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def parse_gs_uri(uri):
    parsed = urlparse(str(uri or ""))
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise errors.CheckpointError("CHECKPOINT_REJECTED", f"invalid gs uri: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")
