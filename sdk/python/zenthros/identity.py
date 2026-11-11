# SPDX-License-Identifier: Apache-2.0

"""What a file or stored object is, without reading it each time. A digest of a
large file is kept with its size, mtime, ctime, inode and device, git's way:
tools can restore a size and an mtime, not a ctime, so `cp -p` and `touch -d`
give themselves away. Someone who controls this disk can still lie to it;
cache="strict" reads everything again.

An object in a bucket is known by what the bucket changes on every write (a
generation, an ETag), never by its size and date."""

import hashlib
import json
import os
import tempfile
import time
from pathlib import Path

from zenthros.artifact import sha256_of

# Files below this are read every time: it costs little, and nothing can lie.
ALWAYS_READ_BYTES = 64 << 20
# A file written this close to the moment it was read may have changed in the
# same instant: its entry is not believed, and it is read again next time.
RECENT_NS = 2_000_000_000
CONTENT = "by content"
METADATA = "by the file's metadata"
URI_SCHEMES = ("s3://", "gs://", "file://")


def _memo_path():
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "zenthros" / "file-digests.json"


def stat_of(path):
    """What a change of path's content changes."""
    seen = os.stat(path)
    return [seen.st_size, seen.st_mtime_ns, seen.st_ctime_ns, seen.st_ino, seen.st_dev]


def _load():
    try:
        return json.loads(_memo_path().read_text())
    except (OSError, ValueError):
        return {}


def _save(memo):
    path = _memo_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, partial = tempfile.mkstemp(dir=path.parent, prefix=".file-digests.")
    with os.fdopen(handle, "w") as out:
        json.dump(memo, out)
    os.replace(partial, path)


def _believed(entry, seen):
    """A memo's digest may stand for the file: nothing about it moved since,
    and the file was already old when it was read. Two writes in one tick of the
    clock leave one ctime, so neither date alone would show the second."""
    return bool(entry) and entry["stat"] == seen and entry["read_at"] - max(seen[1], seen[2]) >= RECENT_NS


def file_digest(path, strict=False, _memo=None):
    """(the file's SHA-256, how it is known: CONTENT or METADATA)."""
    path = os.path.abspath(path)
    seen = stat_of(path)
    if seen[0] < ALWAYS_READ_BYTES:
        return sha256_of(path), CONTENT
    memo = _load() if _memo is None else _memo
    if not strict and _believed(memo.get(path), seen):
        return memo[path]["sha256"], METADATA
    read_at = time.time_ns()
    digest = sha256_of(path)
    if stat_of(path) == seen:
        memo[path] = {"stat": seen, "sha256": digest, "read_at": read_at}
    else:
        memo.pop(path, None)
    if _memo is None:
        _save(memo)
    return digest, CONTENT


def _files_under(path):
    for directory, dirs, names in os.walk(path):
        dirs.sort()
        for name in sorted(names):
            yield os.path.join(directory, name)


def path_digest(path, strict=False):
    """(digest, how, {file: stat}) of a file or a whole folder, by content and
    the names under it. How is METADATA if any file was known that way."""
    memo = _load()
    before = json.dumps(memo, sort_keys=True)
    if os.path.isdir(path):
        total, how, stats = hashlib.sha256(), CONTENT, {}
        for found in _files_under(path):
            digest, this = file_digest(found, strict, memo)
            total.update(f"{os.path.relpath(found, path).replace(os.sep, '/')}\0{digest}\n".encode())
            how = METADATA if this == METADATA else how
            stats[found] = stat_of(found)
        answer = (total.hexdigest(), how, stats)
    else:
        digest, how = file_digest(path, strict, memo)
        answer = (digest, how, {os.path.abspath(path): stat_of(path)})
    if json.dumps(memo, sort_keys=True) != before:
        _save(memo)
    return answer


def changed(stats):
    """The files of path_digest's stats that are not what they were."""
    moved = []
    for path, was in stats.items():
        try:
            if stat_of(path) != was:
                moved.append(path)
        except OSError:
            moved.append(path)
    return moved


def object_identity(uri, store):
    """What the store says this object or prefix is now: a text that changes on
    any write. "" when it holds nothing, or does not say."""
    prefix = str(uri).rstrip("/").removeprefix("file://")
    found = [item for item in store.ls(prefix) if item.uri == prefix or item.uri.startswith(prefix + "/")]
    if not found or not all(item.version for item in found):
        return ""
    if len(found) == 1 and found[0].uri == prefix:
        return found[0].version
    listed = "".join(f"{item.uri[len(prefix):]}\0{item.version}\n" for item in sorted(found, key=lambda i: i.uri))
    return hashlib.sha256(listed.encode()).hexdigest()
