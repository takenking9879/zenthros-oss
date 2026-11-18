# SPDX-License-Identifier: Apache-2.0

"""A call's whole log, kept in a bucket of yours (logs=), and how this side
reads it back.

The machine's agent saves it as the job writes it, from the first line
(operator agent/logpersist): under
<destination>/<job>/<uid>/g<generation>-a<attempt>/<handle>/ the objects
00000000.log.zst, 00000001.log.zst ..., each zstd frames of the log ending
with a skippable frame (its trailer) that says where the log goes on after
it, how much of the log the objects hold through it and the SHA-256 of its
frames; then END.json, once every object is in, which says complete only
when nothing was lost. The job never waits for the bucket, and a bucket that
refuses is one of the job's reasons (LOGS_NOT_SAVED), never its failure.

Read back, each object is checked by its trailer and unpacked bounded; a log
without END.json is completed from its machine after the last object's
cursor, or said to be incomplete.
"""

import hashlib
import json
import os
import re
import struct
import sys
import tempfile
import time
import urllib.parse

from zenthros import console, errors
from zenthros.jobs import logrecords
from zenthros.jobs import spec as S

# Set on a job whose agent saves its log: its runner keeps no copy of its own
# among the outputs (runner._SavedLog).
LOGS_SAVED_ENV = "ZENTHROS_LOGS_SAVED"
END = "END.json"
OBJECT_SUFFIX = ".log.zst"
TRAILER_MAGIC = 0x184D2A5E
# The trailer is a cursor, a number and a hash: never this long.
MAX_TRAILER = 4096
# An agent packs at most 128 KiB of the log in a frame.
MAX_FRAME = 128 << 10
CREDENTIALS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "GOOGLE_STORE_KEY")
LOGS_NOT_SAVED = "LOGS_NOT_SAVED"
# How long a call's machine is kept for its log's END.json before it goes.
END_WAIT = 60.0
_DESTINATION = re.compile(r"(s3|gs)://[a-z0-9][a-z0-9._-]{1,61}[a-z0-9](/[^?#]*)?")


class Logs:
    """Where a call's whole log goes: run(..., logs=zenthros.Logs("s3://bucket/prefix")).

    The machine's agent saves it as it is written, from the first line, in
    objects of about 1 MiB (or every 30 s), and END.json last; the job never
    waits for the bucket, and a bucket that refuses shows in the job's
    status (LOGS_NOT_SAVED), never as a failure. call.logs(to="x.log")
    reads it back whole, also after the machine is gone.

    logs=True (or Logs() with no destination) is your store's <store>/logs,
    written with the credentials your calls already get for it; a
    destination of your own is written with the machine's own role, or with
    credentials={"AWS_ACCESS_KEY_ID": Secret.workspace("logs-id"),
    "AWS_SECRET_ACCESS_KEY": Secret.workspace("logs-key")} (or
    GOOGLE_STORE_KEY for gs://): zenthros.Secret values the agent alone
    resolves, never the job's environment. endpoint (https) and ca (a PEM,
    or a file of one) reach a store that speaks S3 elsewhere; region its
    region; max_bytes cuts a longer log (1 GiB, at least 1 MiB).
    """

    def __init__(self, destination="", *, endpoint="", region="", ca="", credentials=None, max_bytes=None):
        self.destination = str(destination or "").rstrip("/")
        self.endpoint, self.region = str(endpoint or ""), str(region or "")
        self.ca = _pem(ca)
        self.credentials = dict(credentials or {})
        self.max_bytes = max_bytes
        if self.destination and not _DESTINATION.fullmatch(self.destination):
            raise errors.ValidationError(f"logs: {destination!r} is not s3://bucket/prefix or gs://bucket/prefix")
        if self.endpoint and not self.endpoint.startswith("https://"):
            raise errors.ValidationError(f"logs: endpoint {endpoint!r}: a log's bucket is reached over https only")
        for name in self.credentials:
            if name not in CREDENTIALS:
                raise errors.ValidationError(f"logs: credentials[{name!r}]: one of {', '.join(CREDENTIALS)}")
        if max_bytes is not None and (not isinstance(max_bytes, int) or max_bytes < 1 << 20):
            raise errors.ValidationError(f"logs: max_bytes={max_bytes!r}: a whole number of bytes, at least 1 MiB")

    def __repr__(self):
        return f"Logs({self.destination!r})" if self.destination else "Logs(your store)"


def _pem(ca):
    ca = str(ca or "")
    if not ca or ca.lstrip().startswith("-----BEGIN"):
        return ca
    with open(ca, encoding="ascii") as fh:
        return fh.read()


def chosen(logs):
    """logs= as a Logs, or None: True is the store's, a URI a bucket."""
    if logs is None or logs is False:
        return None
    if logs is True:
        return Logs()
    if isinstance(logs, str):
        return Logs(logs)
    if isinstance(logs, Logs):
        return logs
    raise errors.ValidationError(f"logs={logs!r}: True, an s3:// or gs:// URI, or zenthros.Logs(...)")


def spec_for(logs, root, access_env, access_sources):
    """The job's spec.workload.template.logs for logs (a Logs), under the
    call's store root: its own destination and credentials, or, for the
    store's <root>/logs, the credentials the call got for its store (its
    region, and the endpoint jobs reach it at)."""
    from zenthros.remote import carried
    from zenthros.remote.options import secret_sources

    destination, endpoint, region = logs.destination, logs.endpoint, logs.region
    credentials = secret_sources(logs.credentials) if logs.credentials else []
    in_store = root and root != carried.ROOT and (not destination or destination == root or
                                                   destination.startswith(root.rstrip("/") + "/"))
    if not destination:
        if not root or root == carried.ROOT:
            raise errors.ValidationError(
                "logs=True keeps the whole log in your store, and there is none here: give one "
                "(`zenthros store s3://your-bucket/zenthros`) or a bucket, logs=\"s3://your-bucket/logs\"")
        destination = root.rstrip("/") + "/logs"
    if in_store:
        if not credentials:
            credentials = [s for s in access_sources if s.materialization and s.materialization.env_name in CREDENTIALS]
        region = region or (access_env or {}).get("AWS_REGION", "")
        reached = (access_env or {}).get("AWS_ENDPOINT_URL", "")
        if not endpoint and reached:
            if not reached.startswith("https://"):
                raise errors.ValidationError(
                    f"logs=: your store is reached at {reached}, and a log's bucket only over https: "
                    "give logs=zenthros.Logs(...) a bucket of its own")
            endpoint = reached
    return S.Logs(destination=destination, endpoint=endpoint, region=region, ca_data=logs.ca,
                  credentials=credentials, max_bytes=logs.max_bytes)


# --- Reading it back -------------------------------------------------------


class SavedLog:
    """A run's log in the bucket: its folder, its handle, its objects in
    order and END.json, if written."""

    def __init__(self, folder, handle, objects, ended):
        self.folder, self.handle, self.objects, self.ended = folder, handle, objects, ended

    def end(self, blobs):
        """END.json as the agent wrote it, or None before it is."""
        if not self.ended:
            return None
        try:
            end = json.loads(blobs.get(self.folder + END))
        except (ValueError, FileNotFoundError):
            raise logrecords.Corrupt(f"{self.folder}{END} is not one an agent wrote") from None
        if not isinstance(end, dict) or end.get("status") not in ("complete", "truncated", "gap"):
            raise logrecords.Corrupt(f"{self.folder}{END} is not one an agent wrote: {end!r}")
        return end


def logs_of(job):
    """A job object's spec.workload.template.logs (camelCase), or None."""
    return ((((job or {}).get("spec") or {}).get("workload") or {}).get("template") or {}).get("logs") or None


def handle_of(job):
    """The run a job object is on (its status.backendJobName), "" between runs."""
    return ((job or {}).get("status") or {}).get("backendJobName") or ""


def find(blobs, destination, job, uid, handle=""):
    """The bucket's log of job's run on handle (its status.backendJobName),
    or None when it holds none of it yet: an earlier attempt's is not this
    run's, whose machine has it all. Without a handle, the last run's: the
    highest generation, then attempt."""
    under = f"{destination.rstrip('/')}/{job}/{uid}/"
    attempts = {}
    for item in blobs.ls(under):
        parts = item.uri[len(under):].split("/") if item.uri.startswith(under) else []
        number = re.fullmatch(r"g(\d+)-a(\d+)", parts[0]) if len(parts) == 3 else None
        # The agent escapes a handle into one segment of the key (Go's PathEscape).
        if number and (not handle or urllib.parse.unquote(parts[1]) == handle):
            folder = attempts.setdefault((int(number[1]), int(number[2])), {}).setdefault(parts[1], [])
            folder.append(item)
    if not attempts:
        return None
    attempt = max(attempts)
    handles = attempts[attempt]
    # One handle an attempt; a second is a job sent twice to its machine:
    # the newer one is the attempt's.
    handle = max(handles, key=lambda h: max(_stamp(i) for i in handles[h]))
    names = sorted(i.uri.rsplit("/", 1)[1] for i in handles[handle])
    objects = [n for n in names if n.endswith(OBJECT_SUFFIX)]
    for i, name in enumerate(objects):
        if name != f"{i:08d}{OBJECT_SUFFIX}":
            raise logrecords.Corrupt(f"{under}g{attempt[0]}-a{attempt[1]}/{handle}/: object {i} is {name}")
    return SavedLog(f"{under}g{attempt[0]}-a{attempt[1]}/{handle}/", urllib.parse.unquote(handle), objects, END in names)


def _stamp(item):
    modified = getattr(item, "modified", None)
    return modified.timestamp() if modified is not None and hasattr(modified, "timestamp") else 0.0


def trailer(data):
    """(frames, trailer) of an object, checked: its last frame is the
    agent's trailer and its frames the bytes the trailer vouches for."""
    for at in range(len(data) - 9, max(len(data) - 8 - MAX_TRAILER, 0) - 1, -1):
        magic, size = struct.unpack_from("<II", data, at)
        if magic != TRAILER_MAGIC or size != len(data) - at - 8:
            continue
        try:
            said = json.loads(data[at + 8:])
        except ValueError:
            break
        frames = data[:at]
        if not isinstance(said, dict) or hashlib.sha256(frames).hexdigest() != said.get("sha256") \
                or not isinstance(said.get("bytes"), int):
            raise logrecords.Corrupt("an object of the log is not the bytes its trailer vouches for")
        return frames, said
    raise logrecords.Corrupt("an object of the log has no trailer: not one its agent sealed whole")


def unpack(frames, n):
    """The n bytes of log in an object's frames, unpacked bounded: frame by
    frame, none past 128 KiB or a 1 MiB window, all of them n exactly."""
    if n < 1:
        raise logrecords.Corrupt(f"an object of the log says it holds {n} bytes")
    zstd = logrecords.zstd_module()
    out, data = bytearray(), frames
    while data:
        frame = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: logrecords.MAX_WINDOW_LOG})
        try:
            piece = frame.decompress(data, max_length=min(MAX_FRAME, n - len(out)) + 1)
        except zstd.ZstdError as err:
            raise logrecords.Corrupt(f"an object of the log: {err}") from None
        out += piece
        if len(piece) > MAX_FRAME or len(out) > n or not frame.eof:
            raise logrecords.Corrupt(f"an object of the log unpacks to more than the {n} bytes it says, or is cut short")
        data = frame.unused_data
    if len(out) != n:
        raise logrecords.Corrupt(f"an object of the log unpacks to {len(out)} bytes, not the {n} it says")
    return bytes(out)


def missing(before, after):
    """What never came between two counts of it (a trailer's or END.json's
    gap): -1 once a part could not be counted."""
    before, after = before or 0, after or 0
    if after == before:
        return 0
    return -1 if after < 0 else after - before


def missing_note(n):
    """The line a saved log says where n bytes of it never came."""
    why = "the machine's queue was full, or its file freed it first"
    if n < 0:
        return f"a part of this log was never saved here ({why}): it is missing"
    return f"{logrecords.size(n)} of this log were never saved here ({why}): they are missing"


def read_objects(blobs, saved, first=0, before=0):
    """Each object's log from first on, in order, with its trailer: before
    is how much the objects before first hold."""
    for name in saved.objects[first:]:
        try:
            data = blobs.get(saved.folder + name)
        except FileNotFoundError:
            raise logrecords.Corrupt(f"{saved.folder}{name} went missing while it was read") from None
        frames, said = trailer(data)
        yield unpack(frames, said["bytes"] - before), said
        before = said["bytes"]


def blobs_for(logs, current=None, client=None):
    """Where this side reads the bucket of a job's logs (its spec, camelCase):
    the blob store configure() set, the Workspace's store through URLs
    zenthros-api signs, your own store with your credentials, a server of
    its own (endpoint, ca) or any other bucket with this computer's
    credentials."""
    from zenthros.core.storage import store_for
    from zenthros.jobs.http import HTTPJobClient, PresignedBlobStore
    from zenthros.remote.options import _own_bucket, job_client, settings

    destination = logs["destination"]
    current = current or settings()
    if current.blob_store is not None:
        return current.blob_store
    if logs.get("endpoint") or logs.get("caData"):
        return _s3_at(logs.get("endpoint", ""), logs.get("region", ""), logs.get("caData", ""))
    client = client or job_client(current)
    if isinstance(client, HTTPJobClient):
        hub = client.store.rstrip("/")
        if hub and (destination == hub or destination.startswith(hub + "/")):
            return PresignedBlobStore(client)
        if _own_bucket(destination, current, client):
            from zenthros import userstore

            return userstore.blobs()
    return store_for(destination)


_cas = {}


def _s3_at(endpoint, region, ca):
    from zenthros.core.storage import S3BlobStore

    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise errors.ValidationError('reading a log from its bucket needs boto3 here: pip install "zenthros[s3]"') from None
    verify = None
    if ca:
        if ca not in _cas:
            with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as fh:
                fh.write(ca)
            _cas[ca] = fh.name
        verify = _cas[ca]
    return S3BlobStore(boto3.client("s3", endpoint_url=endpoint or None, region_name=region or None, verify=verify,
                                    config=Config(s3={"addressing_style": "path"}) if endpoint else None))


def save(client, job_id, path, current=None):
    """The job's whole log into path, returned: with logs=, its bucket and
    what its machine has after the last object; without, all its machine
    keeps (up to 50 MiB), the first line saying what is left out. A log
    with a part missing says so on a line where the part is missing."""
    job = client.job_object(job_id)
    logs = logs_of(job)
    partial = f"{path}.part"
    try:
        with open(partial, "wb") as out:
            if not (logs and _from_bucket(client, job_id, job, logs, out, current)):
                out.write(client.whole_log(job_id).encode())
        os.replace(partial, path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)
    return path


def _from_bucket(client, job_id, job, logs, out, current):
    """Write the bucket's log of job into out, completed from the machine;
    False when the bucket holds none yet."""
    blobs = blobs_for(logs, current, client)
    saved = find(blobs, logs["destination"], job_id, (job.get("metadata") or {}).get("uid", ""), handle_of(job))
    if saved is None or not saved.objects:
        return False
    end = saved.end(blobs)
    out = _Text(out)
    held, gap, cursor = 0, 0, ""
    for raw, said in read_objects(blobs, saved):
        # A part missing is said where it is: before the object it starts.
        if n := missing(gap, said.get("gap")):
            out.note(missing_note(n))
        out.write(raw)
        held, gap, cursor = said["bytes"], said.get("gap") or 0, said.get("cursor", "")
    if end is not None:
        if end["bytes"] != held:
            raise logrecords.Corrupt(f"{saved.folder}{END} says {end['bytes']} bytes; its objects hold {held}")
        if n := missing(gap, end.get("gap")):
            out.note(missing_note(n))
        if end["status"] == "truncated":
            out.note(f"the log was cut here, at its logs.maxBytes ({logrecords.size(held)})")
        return True
    # Not all saved yet, or never will be: the rest from the machine.
    try:
        rest = client.whole_log(job_id, after=logrecords.Position(handle=saved.handle, cursor=cursor)) if cursor else None
    except errors.ZenthrosError as err:
        rest, why = None, str(err)
    else:
        why = "its machine has no more of it"
    if rest is None:
        out.note(f"the log stops here: the rest was not saved ({why}); it is incomplete")
    else:
        out.write(rest.encode())
    return True


class _Text:
    """A log written to out: a note takes a line of its own, after the line
    the log was on (zctl's logstream.Text writes the same)."""

    def __init__(self, out):
        self._out, self._ended = out, True

    def write(self, data):
        if data:
            self._out.write(data)
            self._ended = data.endswith(b"\n")

    def note(self, text):
        self._out.write(("" if self._ended else "\n").encode() + f"[zenthros: {text}]\n".encode())
        self._ended = True


def tail(client, job_id, tail_lines=None, current=None):
    """The end of the job's log from its bucket, as call.logs() gives it
    from the machine: its last 512 KiB from the start of a line (or its
    last tail lines), the first line saying what is left out. None when the
    job keeps no log in a bucket, or it holds none yet."""
    job = client.job_object(job_id)
    logs = logs_of(job)
    if not logs:
        return None
    blobs = blobs_for(logs, current, client)
    saved = find(blobs, logs["destination"], job_id, (job.get("metadata") or {}).get("uid", ""), handle_of(job))
    if saved is None or not saved.objects:
        return None
    # From the end back until the objects hold 512 KiB; how much comes
    # before them, the trailer of the object before them says.
    data = {}

    def said(i):
        if i not in data:
            data[i] = trailer(blobs.get(saved.folder + saved.objects[i]))
        return data[i]

    raws, held, i = [], 0, len(saved.objects) - 1
    while i >= 0 and held < logrecords.HISTORY_BYTES:
        frames, this = said(i)
        before = said(i - 1)[1]["bytes"] if i else 0
        raws.insert(0, unpack(frames, this["bytes"] - before))
        held += len(raws[0])
        i -= 1
    raw = b"".join(raws)
    if tail_lines is not None:
        return logrecords.clip(raw, tail_lines)
    start = max(len(raw) - logrecords.HISTORY_BYTES, 0)
    if start and raw[start - 1:start] != b"\n":
        newline = raw.find(b"\n", start)
        start = len(raw) if newline < 0 else newline + 1
    omitted = (said(i)[1]["bytes"] if i >= 0 else 0) + start
    return (logrecords.omitted_note(omitted) if omitted else "") + raw[start:].decode("utf-8", "replace")


def wait_end(client, job_id, timeout=None, current=None, out=None):
    """Wait, up to timeout, for the job's log to be all in its bucket
    (END.json), before its machine goes: at once when the job keeps none or
    its bucket refused it (LOGS_NOT_SAVED). Says when the log is left
    incomplete or with a part missing; never raises."""
    out = out or sys.stderr
    timeout = END_WAIT if timeout is None else timeout
    try:
        job = client.job_object(job_id)
        logs = logs_of(job)
        if not logs:
            return True
        reasons = ((job.get("status") or {}).get("reasons") or [])
        if any(isinstance(r, dict) and r.get("code") == LOGS_NOT_SAVED for r in reasons):
            return False
        blobs = blobs_for(logs, current, client)
        uid = (job.get("metadata") or {}).get("uid", "")
        deadline = time.monotonic() + timeout
        while True:
            saved = find(blobs, logs["destination"], job_id, uid, handle_of(job))
            end = saved.end(blobs) if saved is not None else None
            if end is not None:
                if end["status"] != "complete":
                    console.show(f"zenthros: the log of {job_id} in {logs['destination']} is {end['status']}: "
                                 + ("a part of it is missing" if end["status"] == "gap" else "cut at its maxBytes"), out)
                return end["status"] == "complete"
            if time.monotonic() >= deadline:
                console.show(f"zenthros: the log of {job_id} in {logs['destination']} is incomplete: its machine goes "
                             f"before the bucket took it all ({int(timeout)} s)", out)
                return False
            time.sleep(1.0)
    except Exception as err:  # noqa: BLE001 - the machine goes whatever happens to its log
        console.show(f"zenthros: could not see whether the log of {job_id} is all in its bucket: {err}", out)
        return False
