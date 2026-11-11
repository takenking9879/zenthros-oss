# SPDX-License-Identifier: Apache-2.0

"""The job client for zenthros-api: HTTPS and a bearer token, no zctl, no
kubeconfig. Standard library only.

Every call carries the caller's token and zenthros-api forwards it: what the
client may do is what Kubernetes RBAC grants that token in the Workspace's
namespace. A refusal is Kubernetes' own words.
"""

import gzip
import http.client
import json
import os
import shutil
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib

from zenthros import errors
from zenthros.core.storage import BlobStore
from zenthros.jobs import spec as S
from zenthros.jobs.client import JobClient, clip_log, parse_report_body, require_job
from zenthros.jobs.handle import JobHandle, status_from_object
from zenthros.jobs.hubconn import Hub
from zenthros.jobs.wire import HEADERS
from zenthros.logging import create_logger

logger = create_logger("zenthros.jobs", user=True)

# LIST_PAGE is how many jobs one GET /jobs asks for (zenthros-api caps a page
# at 500).
LIST_PAGE = 200

# A submit the network lost is sent again, by the same name, this many times
# in all, SUBMIT_BACKOFF seconds apart. SUBMISSION, a nonce on the job, tells
# the job a lost answer created from another one by that name.
SUBMIT_ATTEMPTS = 3
# The API's rate limit answers before doing anything, so any verb is safe to
# send again: this many tries, each waiting what Retry-After says, at most this.
RATE_LIMIT_ATTEMPTS = 4
RATE_LIMIT_MAX_WAIT = 30
SUBMIT_BACKOFF = 2.0
# A wait's answer comes in its seconds; this much more is the network's.
WAIT_MARGIN = 15
SUBMISSION = "zenthros.ai/submission"
_sleep = time.sleep


def _status(raw, fallback):
    """The message of a metav1.Status body, else the body itself, and the
    Status's details ({} for another body)."""
    try:
        body = json.loads(raw)
        if isinstance(body, dict) and body.get("message"):
            return str(body["message"]), body.get("details") or {}
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        pass
    text = raw.decode("utf-8", "replace").strip() if isinstance(raw, bytes) else str(raw or "")
    return text or fallback, {}


def _retry_after(answer):
    """The seconds a 429 says to wait, within the ceiling; None if it says none."""
    value = answer.headers.get("Retry-After") if answer.headers else None
    return min(int(value), RATE_LIMIT_MAX_WAIT) if value and value.isdigit() else None


def _refusal(code, message, details):
    if code == 401:
        return errors.login_expired(message)
    if code == 403:
        return errors.WorkspaceForbidden(message)
    if code == 404:
        # A job gone only when the Status names a ZenthrosJob, as zctl reads
        # a 404 (operator/pkg/zctl exit.go aboutAJob): a Workspace the hub
        # does not route, or a page that is not the API's, is another thing
        # not there, never a job collected after its TTL.
        if details.get("group") == S.API_VERSION.split("/")[0] and details.get("kind") == S.RESOURCE:
            return errors.JobNotFound(message)
        return errors.NotFound(message)
    if code == 409:
        return errors.Conflict(f"zenthros-api 409: {message}")
    if code in (400, 413, 422):
        return errors.ValidationError(message)
    if code == 426:
        return errors.WireMismatch(message)
    # Asked again or not by the one table (errors.transient_status).
    cls = errors.TransientError if errors.transient_status(code) else errors.Refused
    err = cls(f"zenthros-api {code}: {message}")
    err.status = code
    return err


# The process's clients (HTTPJobClient.for_connection): one per server,
# Workspace and CA, with the credential it was made for.
_shared, _shared_lock = {}, threading.Lock()


def forget_shared():
    """Drop the process's clients, so the next call makes one from the
    zenconfig as it is now: login() and logout() change who a context's
    session is. A client still held (a Workspace view) keeps working."""
    with _shared_lock:
        _shared.clear()


def _maybe_created(name, cause):
    """A submit with no answer: the job may be there."""
    lost = errors.TransientError(f"job {name}: {cause}; it may have been created: "
                                 f"zenthros.get({name!r}) finds it if it was")
    lost.job = name
    return lost


class HTTPJobClient(JobClient):
    """Jobs of one Workspace through zenthros-api."""

    transport = "zenthros-api"

    def __init__(self, server, workspace, token, ca_data=None, poll_seconds=2.0, timeout=60.0):
        self.server = server.rstrip("/")
        self.workspace = workspace
        self._token = token if callable(token) else (lambda: token)
        self.poll_seconds = poll_seconds
        self.timeout = timeout
        self._ssl = None
        if self.server.startswith("https://"):
            self._ssl = ssl.create_default_context(cadata=ca_data.decode() if isinstance(ca_data, bytes) else ca_data) \
                if ca_data else ssl.create_default_context()
        self._hub = Hub(self.server, self._ssl)

    @classmethod
    def from_connection(cls, conn, **kwargs):
        return cls(conn.server, conn.workspace, conn.token, ca_data=conn.ca_data, **kwargs)

    def close(self):
        """Close the connections it keeps to the API; a call after opens one."""
        self._hub.close()

    @classmethod
    def for_connection(cls, conn):
        """The process's one client of conn's server, Workspace and CA, for
        its credential: another credential (a new key, a token refreshed)
        replaces it, which goes when nothing holds it. Every call through it
        reuses its connections (hubconn), where a client per call opened a
        TLS connection each."""
        key = (conn.server, conn.workspace, conn.ca_data)
        with _shared_lock:
            identity, client = _shared.get(key, (None, None))
            if client is None or identity != conn.token.identity:
                client = cls.from_connection(conn)
                _shared[key] = (conn.token.identity, client)
            return client

    def _url(self, path, query=None):
        url = f"{self.server}/v1/workspaces/{urllib.parse.quote(self.workspace, safe='')}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        return url

    def _open(self, method, url, body=None, timeout=None, idempotent=None, lane="calls", gzip=False):
        """The answer (2xx) to one request, its body to read; a refusal
        raised. On this thread's kept connection of lane (hubconn): None, one
        of its own. idempotent: whether sending it twice does what once does,
        so a kept connection the server had closed sends it again; by
        default a GET, PUT or DELETE. gzip: it may come compressed (the job
        list and the logs, which zenthros-api compresses and nothing else)."""
        data = None if body is None else json.dumps(body).encode()
        headers = {**HEADERS, "Authorization": "Bearer " + self._token()}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if gzip:
            headers["Accept-Encoding"] = "gzip"
        if idempotent is None:
            idempotent = method in ("GET", "PUT", "DELETE")
        for attempt in range(1, RATE_LIMIT_ATTEMPTS + 1):
            try:
                answer = self._hub.request(method, url, data, headers, timeout or self.timeout, idempotent, lane)
            except (OSError, http.client.HTTPException) as err:
                raise errors.TransientError(f"zenthros-api at {self.server}: {err}") from err
            if answer.status < 300:
                return answer
            try:
                raw = answer.read()
            except (OSError, http.client.HTTPException) as err:
                self._hub.discard(lane)
                raise errors.TransientError(f"zenthros-api at {self.server}: the answer broke off: {err}") from err
            if answer.status < 400:
                # A redirect is not followed: the token would go along.
                raise errors.ValidationError(f"zenthros-api at {self.server} answered {answer.status} to "
                                             f"{answer.getheader('Location') or 'elsewhere'}: set the server to it")
            wait = _retry_after(answer) if answer.status == 429 else None
            if wait is None or attempt == RATE_LIMIT_ATTEMPTS:
                raise _refusal(answer.status, *_status(raw, answer.reason)) from None
            _sleep(wait)

    def _call(self, method, path, body=None, query=None, idempotent=None, timeout=None, lane="calls", gzip=False):
        with self._open(method, self._url(path, query), body, timeout, idempotent, lane, gzip) as resp:
            # The network's errors are this client's to name, while the
            # answer is read as when it was asked.
            try:
                raw = resp.read()
                return _gunzip(raw) if _compressed(resp) else raw
            except (OSError, EOFError, zlib.error, http.client.HTTPException) as err:
                # Closed here, it would look read: what is left of it is on
                # the connection.
                self._hub.discard(lane)
                raise errors.TransientError(f"zenthros-api at {self.server}: the answer broke off: {err}") from err

    def _json(self, method, path, body=None, query=None, idempotent=None, timeout=None, lane="calls", gzip=False):
        raw = self._call(method, path, body, query, idempotent, timeout, lane, gzip)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as err:
            raise errors.TransientError(f"zenthros-api {method} {path}: not JSON: {err}") from err

    def submit(self, job):
        """Create the job in the Workspace. The Workspace decides the
        namespace; a Job that names none gets it."""
        job = require_job(job)
        manifest = job.to_manifest()
        if not job.namespace:
            manifest["metadata"].pop("namespace", None)
        # A POST whose answer is lost may have created the job: it is sent
        # again by the same name, and a name taken by this very submission
        # is the job. A rerun named another job and ran the work twice, and
        # the error did not name the first (audit 2026-10-16 AUD-075).
        nonce = uuid.uuid4().hex
        manifest["metadata"].setdefault("annotations", {})[SUBMISSION] = nonce
        for attempt in range(1, SUBMIT_ATTEMPTS + 1):
            try:
                # Sent twice, by its nonce, it is one job.
                self._json("POST", "/jobs", manifest, idempotent=True)
                break
            except errors.Conflict:
                if attempt == 1:
                    raise
                try:
                    mine = self._submitted(job.name, nonce)
                except errors.TransientError as exc:
                    raise _maybe_created(job.name, exc) from exc
                if not mine:
                    raise
                break
            except errors.TransientError as exc:
                if attempt == SUBMIT_ATTEMPTS:
                    raise _maybe_created(job.name, exc) from exc
                logger.warning(f"Submitting job {job.name}: {exc}; sending it again by the same name")
                _sleep(SUBMIT_BACKOFF)
        logger.info(f"Submitted job {job.name} workspace={self.workspace} via {self.server}")
        return JobHandle(job.name, self)

    def _submitted(self, name, nonce):
        """Whether the job named name is the one a submission with nonce
        created. A read that did not answer raises: whose the name is is
        not known."""
        try:
            found = self.job_object(name)
        except errors.JobNotFound:
            return False
        return ((found.get("metadata") or {}).get("annotations") or {}).get(SUBMISSION) == nonce

    def job_status(self, job_id):
        return status_from_object(self.job_object(job_id))

    def job_object(self, job_id):
        return self._json("GET", "/jobs/" + urllib.parse.quote(job_id, safe=""))

    waits_for_change = True

    def _status_since(self, job_id, seen, wait):
        """zenthros-api's wait (GET .../wait): its answer comes when the job
        is other than seen names (at once for seen ""), or after wait
        seconds. On a connection of its own lane, so a call of this thread
        is not held behind it; asked again with the same seen after a cut,
        nothing is missed."""
        seconds = max(1, int(wait))
        answer = self._json("GET", "/jobs/" + urllib.parse.quote(job_id, safe="") + "/wait",
                            query={"seen": seen or "", "timeoutSeconds": str(seconds)},
                            timeout=seconds + WAIT_MARGIN, lane="waits")
        return status_from_object(answer["job"]), answer["seen"]

    def source_of(self, job_id):
        """Asked apart: zenthros-api keeps a large one out of the job, by reference."""
        return self._json("GET", "/jobs/" + urllib.parse.quote(job_id, safe="") + "/source")["inline"]

    def list_jobs(self, selector=""):
        """The Workspace's jobs as stored, optionally those a label selector
        matches. Read a page at a time (limit, continue): one list of a busy
        Workspace held the whole namespace in zenthros-api's memory at once.
        A zenthros-api that does not page answers everything in one."""
        items, query = [], {"limit": str(LIST_PAGE)}
        if selector:
            query["labelSelector"] = selector
        while True:
            listed = self._json("GET", "/jobs", query=dict(query), gzip=True)
            items.extend(listed.get("items") or [])
            following = (listed.get("metadata") or {}).get("continue") or ""
            if not following:
                return items
            query["continue"] = following

    def backends(self):
        """The backends the Workspace may use, as the Gateway describes them
        (the proto's JSON: unset fields are left out)."""
        return list(self._json("GET", "/backends").get("backends") or [])

    def join_backend(self, name="", kind="", runtime="", python=""):
        """A one-use join (POST /backends): its backend ID and the two steps
        to run where it joins. The second holds the token. name is what the
        Workspace will call it, once; kind is "container" (empty) or "vm",
        a machine the user has, as root; runtime, for a vm, is where its
        jobs run: systemd (empty) or containerd; python, the caller's minor,
        which a vm running its jobs directly installs and a container's
        agent completes its Python to."""
        body = {k: v for k, v in (("name", name), ("type", kind), ("runtime", runtime), ("python", python)) if v}
        # Never sent twice: a second join opens a second token, of the
        # Workspace's 10.
        return self._json("POST", "/backends", body or None, idempotent=False)

    def seal(self, backend, env):
        """env's values as encrypted-payload secrets (the API's JSON) sealed
        to the backend's age keys: only its agent opens them."""
        return list(self._json("POST", "/seal", {"backend": backend, "env": dict(env)}, idempotent=True).get("secrets") or [])

    def retire_backend(self, backend_id):
        """Retire one of the Workspace's Private backends: revoked, then its
        access removed."""
        return self._json("DELETE", "/backends/" + urllib.parse.quote(backend_id, safe=""))

    def machines(self):
        """The templates the Workspace may make machines from, with their
        limit and what it has of each, and its machines (GET /machines)."""
        return self._json("GET", "/machines")

    def create_machine(self, template, name, idle="", boot=False, max_lifetime="", image="", python="", packages=()):
        """A machine of the Workspace from a template: a pool of one; with
        idle ("30m") one that ends after that long with no job, and with
        boot too one that starts now rather than at its first job. With
        max_lifetime ("24h") it ends that long after it was made, job or no
        job. image, python and packages: on a Container template, the image it
        runs, and what completes it before it takes work."""
        body = {"template": template, "name": name}
        if idle:
            body["idle"] = idle
            if boot:
                body["boot"] = True
        if max_lifetime:
            body["maxLifetime"] = max_lifetime
        if image:
            body["image"] = image
        if python:
            body["python"] = python
        if packages:
            body["packages"] = list(packages)
        # Never sent twice: the second is a 409 for the name the first made.
        return self._json("POST", "/machines", body, idempotent=False)

    def delete_machine(self, name):
        return self._json("DELETE", "/machines/" + urllib.parse.quote(name, safe=""))

    def targets(self):
        """Where the Workspace can run (GET /targets): its backends, pools,
        templates and machines, each with its status, how many are
        available, one machine's CPU and memory and its price; and what
        the caller may make (can)."""
        return self._json("GET", "/targets")

    def pools(self):
        """The pools the Workspace may name in a job, by name."""
        return list(self._json("GET", "/pools").get("pools") or [])

    def cancel(self, job_id):
        self._json("POST", "/jobs/" + urllib.parse.quote(job_id, safe="") + "/cancel", idempotent=True)

    def delete(self, job_id):
        self._json("DELETE", "/jobs/" + urllib.parse.quote(job_id, safe=""))

    def logs(self, job_id, tail=None):
        query = {"tail": str(int(tail))} if tail is not None else None
        return clip_log(self._call("GET", "/jobs/" + urllib.parse.quote(job_id, safe="") + "/logs", query=query, gzip=True), tail)

    def follow_logs(self, job_id):
        """The followed log as a process would give it (zctl logs --watch):
        iterate .stdout for lines; wait() when it ends; terminate() to stop."""
        url = self._url("/jobs/" + urllib.parse.quote(job_id, safe="") + "/logs", {"follow": "1"})
        # A followed log stays open as long as the job runs, on a connection
        # of its own.
        return _LogStream(self._open("GET", url, timeout=3600, lane=None, gzip=True))

    def reports(self, job_id):
        return parse_report_body(job_id, self._call("GET", "/jobs/" + urllib.parse.quote(job_id, safe="") + "/reports").decode())


    @property
    def info(self):
        """The Workspace as zenthros-api describes it: its store, the store's
        region, where jobs reach it. Asked once."""
        if not hasattr(self, "_info"):
            self._info = json.loads(self._call("GET", ""))
        return self._info

    @property
    def store(self):
        """The Workspace's call store (<root>/<workspace>), "" if the API has
        none."""
        return self.info.get("store") or ""

    def store_access(self, backend=""):
        """(env, secrets) a job needs for the Workspace's store: its region
        and the endpoint jobs reach it at, and for a backend an hour of
        credentials sealed to that backend's key, which only it opens. A
        pool's machines read the store with their own role. An API with no
        role to mint from leaves the backend its own credentials."""
        info = self.info
        env = {}
        if str(info.get("store", "")).startswith("s3://"):
            if info.get("storeRegion"):
                env["AWS_REGION"] = info["storeRegion"]
            if info.get("storeJobEndpoint"):
                env["AWS_ENDPOINT_URL"] = info["storeJobEndpoint"]
        if not backend:
            return env, []
        try:
            sealed = self._json("POST", "/store/credentials", {"backend": backend}, idempotent=True)
        except errors.TransientError as err:
            # 501: the API has no role to mint from, for good. A 503 is an
            # outage: a job sent without its credentials could not read its
            # call, so it is raised.
            if getattr(err, "status", None) != 501:
                raise
            print(f"zenthros: {err}; {backend} reads the store with its own credentials", file=sys.stderr)
            return env, []
        env.update(sealed.get("env") or {})
        return env, [_sealed_source(s) for s in sealed.get("secrets") or []]

    def presign(self, uri, method, after="", size=None):
        """A URL the API signed for one object of the Workspace's store, or
        with method LIST for a page of the keys under the prefix uri, from
        the store's continuation token after. A PUT says its size: the
        signature holds the store to it, and the API signs none over its
        limit."""
        body = {"uri": uri, "method": method}
        if after:
            body["after"] = after
        if size is not None:
            body["size"] = size
        return self._json("POST", "/objects/presign", body, idempotent=True)

    def secrets(self):
        """The Workspace's secrets: [{"name", "updatedAt"}], never a value."""
        return list(self._json("GET", "/secrets").get("secrets") or [])

    def set_secret(self, name, value):
        """Set (or replace) a Workspace secret. The value goes to the API in
        the request's body, never its URL, and does not come back."""
        return self._json("PUT", "/secrets/" + urllib.parse.quote(name, safe=""), {"value": value})

    def delete_secret(self, name):
        return self._json("DELETE", "/secrets/" + urllib.parse.quote(name, safe=""))

    def job_cost(self, name):
        """What the job cost so far, as the API counts it (see call.cost())."""
        return self._json("GET", f"/jobs/{urllib.parse.quote(name, safe='')}/cost")

    def whoami(self):
        """Who Kubernetes says this token is: username, groups."""
        with self._open("GET", f"{self.server}/v1/whoami") as resp:
            return json.loads(resp.read())


def _sealed_source(ref):
    payload = ref.get("encryptedPayload") or {}
    where = ref.get("materialization") or {}
    return S.SecretSource(
        type=ref.get("type", ""),
        encrypted_payload=S.EncryptedPayload(ciphertext=payload.get("ciphertext", ""), key_id=payload.get("keyId", ""),
                                             recipients=list(payload.get("recipients") or [])),
        materialization=S.Materialization(mode=where.get("mode", ""), env_name=where.get("envName", ""),
                                          path=where.get("path", "")))


def _compressed(response):
    return (response.getheader("Content-Encoding") or "").lower() == "gzip"


def _gunzip(raw):
    # gzip.decompress, out of reach of the calls whose argument gzip= is
    # named like the module.
    return gzip.decompress(raw)


def _gunzipped_lines(response):
    """A gzip answer's lines as each piece comes: zenthros-api flushes the
    stream after each one, so each decompresses whole."""
    unzip = zlib.decompressobj(16 + zlib.MAX_WBITS)
    pending = b""
    while chunk := response.read1(1 << 16):
        pending += unzip.decompress(chunk)
        *lines, pending = pending.split(b"\n")
        for line in lines:
            yield line + b"\n"
    *lines, rest = (pending + unzip.flush()).split(b"\n")
    for line in lines:
        yield line + b"\n"
    if rest:
        yield rest


class _LogStream:
    def __init__(self, response):
        self._response = response
        self._ended = threading.Event()
        self._closed = False
        self.stdout = self._lines()

    def _lines(self):
        try:
            for raw in _gunzipped_lines(self._response) if _compressed(self._response) else self._response:
                yield raw.decode("utf-8", "replace")
        except (OSError, ValueError, zlib.error, http.client.HTTPException):
            # The stream broke off (a cut mid-chunk, a piece that does not
            # decompress): the log ends here, as a closed one does.
            return
        except AttributeError:
            # http.client, closed by terminate() from another thread mid-read,
            # reads from a file it already dropped. That is the end we asked
            # for; anything else is a bug and propagates.
            if not self._closed:
                raise
        finally:
            self._ended.set()
            # Closed by the thread that reads it: it holds the buffer's lock.
            self._response.close()

    def poll(self):
        return 0 if self._ended.is_set() else None

    def wait(self, timeout=None):
        """Until the log ends, for at most timeout; then it is stopped. A
        followed log may outlive its job (an attempt on a machine that is
        gone), so this never waits on the server."""
        if not self._ended.wait(timeout or 0):
            self.terminate()
        return 0

    def terminate(self):
        self._closed = True
        # close() would wait for the buffer's lock, which the reading thread
        # holds while it waits on the server: shutting the socket down ends
        # that read instead, and the reader closes the response. Windows does
        # not end a read already waiting on a socket shut down: there its
        # descriptor is closed too, taken off the socket first, so the
        # reader's next read is refused, never made on a number reused.
        sock = getattr(getattr(getattr(self._response, "fp", None), "raw", None), "_sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
                if os.name == "nt":
                    socket.close(sock.detach())
                return
            except OSError:
                pass
        self._response.close()


class PresignedBlobStore(BlobStore):
    """put/get on the Workspace's call store with URLs zenthros-api signs: no
    cloud credentials on this side. The bearer token goes to the API only;
    the store sees the signed URL and nothing else."""

    def __init__(self, client):
        self._client = client

    def _object(self, uri, method, payload=None, length=None, into=None, after=""):
        if method == "PUT" and length is None:
            length = len(payload)
        signed = self._client.presign(uri, method, after, size=length)
        request = urllib.request.Request(signed["url"], data=payload, method=signed.get("method") or method)
        if length is not None:
            # A file is streamed: urllib sends no length of its own for it.
            request.add_header("Content-Length", str(length))
        for name, values in (signed.get("headers") or {}).items():
            for value in values if isinstance(values, list) else [values]:
                request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=self._client.timeout) as resp:
                if into is not None:
                    shutil.copyfileobj(resp, into, 1 << 20)
                    return None
                return resp.read()
        except urllib.error.HTTPError as err:
            if method == "GET" and err.code == 404:
                raise FileNotFoundError(uri) from None
            if method == "DELETE" and err.code == 404:
                return None
            raise errors.TransientError(f"store {method} {uri}: {err.code} {err.reason}") from None
        except (urllib.error.URLError, OSError) as err:
            raise errors.TransientError(f"store {method} {uri}: {getattr(err, 'reason', err)}") from err

    def put(self, uri, payload):
        self._object(uri, "PUT", bytes(payload))
        return uri

    def get(self, uri):
        return self._object(uri, "GET")

    def put_file(self, uri, path):
        """PUT a local file, read as it is sent rather than held whole."""
        with open(path, "rb") as fh:
            self._object(uri, "PUT", fh, length=os.fstat(fh.fileno()).st_size)
        return uri

    def get_file(self, uri, path):
        """GET an object into a local file, written as it arrives rather than
        held whole; the file appears only once complete."""
        partial = f"{path}.part"
        try:
            with open(partial, "wb") as fh:
                self._object(uri, "GET", into=fh)
            os.replace(partial, path)
        finally:
            if os.path.exists(partial):
                os.remove(partial)
        return path

    def delete(self, uri):
        """DELETE one object; one already gone is no error."""
        self._object(uri, "DELETE")

    def ls(self, prefix):
        """Every object under prefix: signed ListObjectsV2 pages, each signed
        for the same prefix, until the store says there is no more."""
        found, after = [], ""
        while True:
            objects, after = _listing_page(self._object(prefix, "LIST", after=after), prefix)
            found.extend(objects)
            if not after:
                return found


def _listing_page(raw, prefix):
    """(objects, next token) of a ListObjectsV2 answer; token "" at the end."""
    import datetime
    import xml.etree.ElementTree as ET

    from zenthros.core.storage import StoredObject

    scheme_bucket = "/".join(prefix.split("/", 3)[:3])
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as err:
        raise errors.TransientError(f"store listing of {prefix}: not a listing: {err}") from None

    def child(node, name):
        for item in node:
            if item.tag.rsplit("}", 1)[-1] == name:
                return item.text or ""
        return ""

    objects = []
    for item in root:
        if item.tag.rsplit("}", 1)[-1] != "Contents":
            continue
        stamp = child(item, "LastModified")
        modified = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")) if stamp else None
        objects.append(StoredObject(f"{scheme_bucket}/{child(item, 'Key')}", int(child(item, "Size") or 0), modified, child(item, "ETag")))
    more = child(root, "IsTruncated") == "true"
    return objects, child(root, "NextContinuationToken") if more else ""
