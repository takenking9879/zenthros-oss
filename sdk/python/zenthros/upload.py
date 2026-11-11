# SPDX-License-Identifier: Apache-2.0

"""upload=True: what a call names on this computer goes to the store its job
reads, by content, so the same file is never sent twice. It happens only when
you ask, and plan says what it would send. Nothing is deleted, and nothing comes
back without being asked for.

The function receives a path of the same name where it runs, so the same code
runs here and there (Sent, in artifact.py)."""

import hashlib
import json
import os

from zenthros import errors, ignore
from zenthros.artifact import FOLDER_LIST, Sent, of_file


def _size(path):
    if os.path.isdir(path):
        return sum(os.path.getsize(os.path.join(top, name)) for top, _, names in os.walk(path) for name in names)
    return os.path.getsize(path)


def describe(path):
    """What it would send, in words."""
    size = _size(path)
    shown = f"{size / (1 << 30):.1f} GiB" if size >= 1 << 30 else f"{size / (1 << 20):.1f} MiB" if size >= 1 << 20 else f"{size} bytes"
    return f"it would send {path} ({shown}) to your store"


def _is_there(store, uri, size):
    try:
        return any(item.uri == uri and item.size == size for item in store.ls(uri))
    except FileNotFoundError:
        return False


def _send_file(path, root, store):
    made = of_file(path, "")
    uri = f"{root}/data/{made.sha256}"
    if not _is_there(store, uri, made.size):
        store.put_file(uri, path)
    return Sent(uri, made.sha256, made.size, name=os.path.basename(path))


def _send_folder(path, root, store):
    path = path.rstrip(os.sep)
    entries = []
    for top, dirs, names in os.walk(path):
        dirs.sort()
        for name in sorted(names):
            full = os.path.join(top, name)
            ignore.refuse_path(full)
            entries.append((os.path.relpath(full, path).replace(os.sep, "/"), of_file(full, "")))
    digest = hashlib.sha256(json.dumps([[rel, made.sha256] for rel, made in entries]).encode()).hexdigest()
    prefix = f"{root}/data/{digest}"
    listing = json.dumps({"files": [{"path": rel, "uri": f"{root}/data/{made.sha256}", "sha256": made.sha256, "size": made.size}
                                    for rel, made in entries]}, sort_keys=True).encode()
    uri = f"{prefix}/{FOLDER_LIST}"
    if not _is_there(store, uri, len(listing)):
        for rel, made in entries:
            if not _is_there(store, f"{root}/data/{made.sha256}", made.size):
                store.put_file(f"{root}/data/{made.sha256}", os.path.join(path, *rel.split("/")))
        # Last: with the list there, everything it names is.
        store.put(uri, listing)
    return Sent(uri, hashlib.sha256(listing).hexdigest(), len(listing), name=os.path.basename(path), folder=True)


def send(value, paths, root, store):
    """value with each of paths swapped for what was sent, checked for credentials
    first: nothing goes if one is refused."""
    root = root.rstrip("/")
    wanted = {os.path.abspath(path) for path in paths}
    for path in wanted:
        if not os.path.exists(path):
            raise errors.ValidationError(f"upload=True: {path} is not on this computer")
        ignore.refuse_path(path)

    def swap(item):
        if isinstance(item, str) and item and os.path.abspath(item) in wanted and os.path.exists(item):
            full = os.path.abspath(item)
            return _send_folder(full, root, store) if os.path.isdir(full) else _send_file(full, root, store)
        if isinstance(item, (list, tuple)) and type(item) in (list, tuple):
            return type(item)(swap(one) for one in item)
        if type(item) is dict:
            return {key: swap(one) for key, one in item.items()}
        return item

    return swap(value)
