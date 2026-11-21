# SPDX-License-Identifier: Apache-2.0

"""upload=True: what a call names on this computer goes to the store its job
reads, by content, so the same file is never sent twice. It happens only when
you ask, and plan says what it would send. Nothing is deleted, and nothing comes
back without being asked for.

The function receives a path of the same name where it runs, so the same code
runs here and there (Sent, in artifact.py).

A call's argument that pickles to more than KEEP_FROM goes to the same place
the same way, with a bucket (keep_arguments): the function gets its value
(Kept, in artifact.py).

What is under <root>/data/ is deleted by the bucket's own rule, after
LIFECYCLE_DAYS (zenthros store says how to set it); Zenthros deletes none of
it, so nothing it does removes what a job still reads. What has been there
FRESH_DAYS or more is sent again, which renews it: a job that starts within
FRESH_DAYS of its call finds what the call named."""

import datetime
import hashlib
import json
import os
import sys

from zenthros import errors, ignore
from zenthros.artifact import FOLDER_LIST, Kept, Sent, of_file
from zenthros.remote import payload

LIFECYCLE_DAYS = 30
FRESH_DAYS = 15
# An argument whose pickle weighs more goes apart, by its digest: the same
# one is sent once, however many calls pass it (observaciones P20).
KEEP_FROM = 1 << 20
# A file this big takes a while to send: it is said as it goes (the taxi
# train, 2.2 GB, sent for 3 minutes in silence; wave 7).
SAY_FROM = 64 << 20


def _size(path):
    if os.path.isdir(path):
        return sum(os.path.getsize(os.path.join(top, name)) for top, _, names in os.walk(path) for name in names)
    return os.path.getsize(path)


def _shown(size):
    return f"{size / (1 << 30):.1f} GiB" if size >= 1 << 30 else f"{size / (1 << 20):.1f} MiB" if size >= 1 << 20 else f"{size} bytes"


def describe(path):
    """What it would send, in words."""
    return f"it would send {path} ({_shown(_size(path))}) to your store"


def _sending(name, size):
    if size >= SAY_FROM:
        print(f"zenthros: sending {name} ({_shown(size)}) to your store", file=sys.stderr, flush=True)


def _is_there(store, uri, size):
    """Whether uri holds size bytes written less than FRESH_DAYS ago."""
    since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=FRESH_DAYS)
    try:
        return any(item.uri == uri and item.size == size and item.modified > since for item in store.ls(uri))
    except FileNotFoundError:
        return False


def _send_file(path, root, store):
    made = of_file(path, "")
    uri = f"{root}/data/{made.sha256}"
    if not _is_there(store, uri, made.size):
        _sending(os.path.basename(path), made.size)
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
        missing = [(rel, made) for rel, made in entries if not _is_there(store, f"{root}/data/{made.sha256}", made.size)]
        _sending(os.path.basename(path) + "/", sum(made.size for _, made in missing))
        for rel, made in missing:
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


def keep_arguments(body, dumps, root, store):
    """A call's body with each argument whose pickle (dumps) weighs more than
    KEEP_FROM swapped for a Kept: its args and kwargs, a map's items, and a
    config's values and shared ones (spread.arguments). Its bytes go to
    <root>/data/<sha256> unless they are there, fresh."""
    root = root.rstrip("/")

    def kept(value):
        raw = dumps(value)
        if len(raw) <= KEEP_FROM:
            return value
        blob = payload.dump({"value": raw})
        digest = hashlib.sha256(blob).hexdigest()
        made = Kept(f"{root}/data/{digest}", digest, len(blob))
        if not _is_there(store, made.uri, made.size):
            store.put(made.uri, blob)
        return made

    out = dict(body)
    if "args" in body:
        out["args"] = tuple(kept(value) for value in body["args"])
        out["kwargs"] = {key: kept(value) for key, value in body["kwargs"].items()}
    if "items" in body:
        configs = "shared" in body
        out["items"] = [{key: kept(value) for key, value in item.items()} if configs and isinstance(item, dict)
                        else kept(item) for item in body["items"]]
    if "shared" in body:
        out["shared"] = {key: kept(value) for key, value in body["shared"].items()}
    return out


def opened(value, store):
    """value (a call's args and kwargs, again) with each Kept's value read
    from store: for a place that does not read that store."""
    if isinstance(value, Kept):
        return value.value(store)
    if isinstance(value, tuple):
        return tuple(opened(one, store) for one in value)
    if type(value) is dict:
        return {key: opened(one, store) for key, one in value.items()}
    return value


def renewed(value, store):
    """value (a call's args and kwargs, again) with each Kept's bytes sent
    again in place when they are FRESH_DAYS old or more; ValidationError
    when the bucket's rule already took them."""
    if isinstance(value, Kept):
        if _is_there(store, value.uri, value.size):
            return value
        try:
            blob = store.get(value.uri)
        except FileNotFoundError:
            raise errors.ValidationError(
                f"an argument of this call is gone from the store ({value.uri}; the bucket's rule deletes "
                f"<store>/data/ after {LIFECYCLE_DAYS} days): call the function again with its value") from None
        if hashlib.sha256(blob).hexdigest() != value.sha256:
            raise errors.ValidationError(f"{value.uri} is not the argument the call stored: it changed in the store")
        store.put(value.uri, blob)
        return value
    if isinstance(value, tuple):
        return tuple(renewed(one, store) for one in value)
    if type(value) is dict:
        return {key: renewed(one, store) for key, one in value.items()}
    return value
