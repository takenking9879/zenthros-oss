# SPDX-License-Identifier: Apache-2.0

"""One reference to stored bytes: where they are and their SHA-256. A result, a
step's output and a cache entry all say it this way, the same form a job
reports its result in (operator/pkg/resultdigest). The bytes live in a store or
a path; only this travels.

A folder is published as its files and, last, a list of each one's digest: with
no list, the folder is not published."""

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass

from zenthros import errors

# The last object of a published folder.
FOLDER_LIST = ".zenthros-artifact-list.json"
_CHUNK = 1 << 20


def sha256_of(path):
    """The SHA-256 of a file, read as a stream."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class Artifact:
    """Stored bytes by reference. sha256 is what they must hash to; version,
    what the store said it was when last seen (StoredObject.version), for what
    is too large to read each time."""

    uri: str
    sha256: str = ""
    size: int = None
    version: str = ""

    def to_dict(self):
        out = {"uri": self.uri, "sha256": self.sha256}
        if self.size is not None:
            out["size"] = self.size
        if self.version:
            out["version"] = self.version
        return out

    @classmethod
    def from_dict(cls, raw):
        return cls(raw["uri"], raw.get("sha256", ""), raw.get("size"), raw.get("version", ""))

    def __str__(self):
        return self.uri

    def __fspath__(self):
        """A file:// or plain path's own path, to hand to open(); a bucket's
        URI has none."""
        if self.uri.startswith("file://"):
            return self.uri[len("file://"):]
        if "://" in self.uri:
            raise TypeError(f"{self.uri} is not on this file system; read it with zenthros.files")
        return self.uri


def of_file(path, uri=None):
    """The Artifact of a local file, by its content."""
    return Artifact(uri or os.path.abspath(path), sha256_of(path), os.path.getsize(path))


@dataclass(frozen=True)
class Sent(Artifact):
    """What upload= put in the store for a call. Where it runs, the function gets
    a file or a folder of this name, each byte checked against its digest."""

    name: str = "data"
    folder: bool = False

    def fetch(self, store):
        """The path it was put at, or ValidationError when bytes differ."""
        into = os.path.join(tempfile.mkdtemp(prefix="zenthros-input-"), self.name)
        if not self.folder:
            _fetch_one(store, self.uri, self.sha256, into)
            return into
        listing = store.get(self.uri)
        _same(self.uri, hashlib.sha256(listing).hexdigest(), self.sha256)
        for entry in json.loads(listing)["files"]:
            target = os.path.join(into, *entry["path"].split("/"))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            _fetch_one(store, entry["uri"], entry["sha256"], target)
        return into


def _same(uri, seen, expected):
    if seen != expected:
        raise errors.ValidationError(f"{uri} hashes to {seen[:12]}…, not to the {expected[:12]}… that was sent: "
                                     "it changed in the store")


def _fetch_one(store, uri, expected, path):
    store.get_file(uri, path)
    _same(uri, sha256_of(path), expected)


def publish_folder(store, prefix, folder):
    """folder's files under prefix, then the list that says they are all
    there. Returns the list's Artifact."""
    prefix = str(prefix).rstrip("/")
    entries = []
    for directory, _, names in os.walk(folder):
        for name in sorted(names):
            path = os.path.join(directory, name)
            rel = os.path.relpath(path, folder).replace(os.sep, "/")
            entries.append(of_file(path, f"{prefix}/{rel}").to_dict())
            store.put_file(entries[-1]["uri"], path)
    listing = json.dumps({"files": sorted(entries, key=lambda e: e["uri"])}, sort_keys=True).encode()
    uri = f"{prefix}/{FOLDER_LIST}"
    store.put(uri, listing)
    return Artifact(uri, hashlib.sha256(listing).hexdigest(), len(listing))


def read_folder(store, prefix):
    """The files of the folder published at prefix ([Artifact]), each checked
    against the store by its size; ValidationError when no list says it is
    complete, or a file is gone."""
    prefix = str(prefix).rstrip("/")
    try:
        listing = json.loads(store.get(f"{prefix}/{FOLDER_LIST}"))
    except FileNotFoundError:
        raise errors.ValidationError(f"{prefix} is not a published folder: it has no {FOLDER_LIST}, which is "
                                     "written last") from None
    files = [Artifact.from_dict(entry) for entry in listing["files"]]
    found = {item.uri: item.size for item in store.ls(prefix)}
    for artifact in files:
        if found.get(artifact.uri) != artifact.size:
            raise errors.ValidationError(f"{artifact.uri} of the folder {prefix} is gone or changed size")
    return files
