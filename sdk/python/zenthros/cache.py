# SPDX-License-Identifier: Apache-2.0

"""Not doing again what already ran. An entry is one signed manifest in the
store (<store>/cache/<key>.json), next to the bytes it pins: it lives as long
as they do, whatever happens to the job or the cluster that made them.

Only the SDK writes one, once it has checked a result against the digests its
job reported: "the cache holds what you verified". A job can write the whole
store, so a manifest with no valid signature is nothing: its pickle is never
opened, and it protects nothing from a clean. The key that signs
(<config>/cache.key) never goes to a job. Losing it only loses the entries.

The signature answers who says this came from a job of mine; the SHA-256, that
these are the same bytes. Neither says the machine that ran it was honest."""

import base64
import datetime
import hashlib
import hmac
import inspect
import json
import os
import pickle
import re
import secrets
import sys
import time
import types
from dataclasses import dataclass, field

from zenthros import config, errors, identity
from zenthros.artifact import Artifact
from zenthros.remote import bundle, carried

KEY_LABEL = "zenthros.ai/cache-key"
PARTS_ANNOTATION = "zenthros.ai/cache-parts"
SIGNATURE = "hmac-sha256:"
FOLDER = "cache"
# Up to this many project files are named one by one when one of them changes.
NAMED_FILES = 300


def key_path():
    return os.path.join(os.path.dirname(config.config_path()), "cache.key")


def signing_key():
    """This person's key, made at the first use."""
    path = key_path()
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        pass
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        with open(path, "rb") as existing:
            return existing.read()
    made = secrets.token_bytes(32)
    with os.fdopen(handle, "wb") as out:
        out.write(made)
    return made


def export_key():
    """The key as text, for another computer of the same person."""
    return base64.b64encode(signing_key()).decode()


def import_key(text):
    made = base64.b64decode(text.strip(), validate=True)
    if len(made) != 32:
        raise errors.ValidationError("that is not a cache key: it is 32 bytes, as `zenthros cache key export` prints it")
    path = key_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(handle, "wb") as out:
        out.write(made)
    os.chmod(path, 0o600)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _signature(body):
    unsigned = {name: value for name, value in body.items() if name != "signature"}
    return SIGNATURE + hmac.new(signing_key(), _canonical(unsigned), hashlib.sha256).hexdigest()


def signed(body):
    return {**body, "signature": _signature(body)}


def genuine(body):
    return isinstance(body, dict) and hmac.compare_digest(str(body.get("signature", "")), _signature(body))


def _seal(name, key):
    return hmac.new(signing_key(), f"{name}\0{key}".encode(), hashlib.sha256).hexdigest()


class CannotKey(Exception):
    """What a call reads cannot be told apart from what it read before."""


@dataclass
class Home:
    """A store that holds entries. shared: not the one the call's job writes to,
    so a result is copied into it."""

    root: str
    store: object
    shared: bool = False

    @property
    def folder(self):
        return f"{self.root.rstrip('/')}/{FOLDER}"

    def manifest_uri(self, key):
        return f"{self.folder}/{key}.json"

    def last_uri(self, fn):
        return f"{self.folder}/last/{hashlib.sha256(fn.encode()).hexdigest()[:32]}.json"


@dataclass
class Note:
    """What a call is, as the cache sees it."""

    key: str
    parts: dict
    mode: object = True
    # Argument -> how a large file was known (identity.CONTENT, METADATA).
    checks: dict = field(default_factory=dict)
    # File -> its stat when the key was made: a file that moved meanwhile is
    # not published.
    stats: dict = field(default_factory=dict)
    # Argument -> the path it named, to say it by its name.
    paths: dict = field(default_factory=dict)
    files: dict = field(default_factory=dict)
    homes: list = field(default_factory=list)
    # Why it runs, for whoever asks.
    why: str = ""
    changed: list = field(default_factory=list)

    def label(self):
        return self.key[:63]

    def annotation(self, name):
        """The note on the job, for a process that sees it finished later. The
        seal says it was this person who sent this job with this key: an editor
        of the Workspace can make a job, not make it publish into this cache."""
        return json.dumps({"key": self.key, "parts": self.parts, "mode": self.mode, "seal": _seal(name, self.key)},
                          sort_keys=True, separators=(",", ":"))


def note_of_job(obj):
    """The Note on a job this person sent, or None."""
    meta = obj.get("metadata") or {}
    raw = (meta.get("annotations") or {}).get(PARTS_ANNOTATION)
    if not raw:
        return None
    try:
        body = json.loads(raw)
        key, parts, mode, seal = body["key"], body["parts"], body["mode"], body["seal"]
    except (ValueError, KeyError, TypeError):
        return None
    if not hmac.compare_digest(str(seal), _seal(meta.get("name", ""), str(key))):
        return None
    return Note(key, parts, mode)


def _is_uri(value):
    return isinstance(value, str) and value.startswith(identity.URI_SCHEMES)


def _stores_for(uri):
    from zenthros.core.storage import store_for

    return store_for(uri)


def _reference(name, value, note, strict):
    """What the path or URI value is now, or None when it is neither."""
    if _is_uri(value) and not value.startswith("file://"):
        version = identity.object_identity(value, _stores_for(value))
        if not version:
            raise CannotKey(f"{name}: {value} holds nothing, or its store does not say what version it has")
        return f"object:{version}"
    path = value[len("file://"):] if _is_uri(value) else value
    if isinstance(path, str) and path and not path.startswith("-") and os.path.exists(path):
        digest, how, stats = identity.path_digest(path, strict)
        note.checks[name] = how
        note.stats.update(stats)
        note.paths[name] = os.path.abspath(path)
        return f"path:{digest}"
    return None


# A class's annotations are not its code, nor are where it is and what Python
# keeps about it. Since 3.14 they are a function, __annotate_func__, that
# closes over the whole class namespace (its __firstlineno__, its dataclass
# Fields), and their cache.
_NOT_CODE = {"__dict__", "__weakref__", "__module__", "__qualname__", "__firstlineno__", "__static_attributes__",
             "__dataclass_fields__", "__dataclass_params__", "__match_args__", "__doc__", "__annotations__",
             "__annotate_func__", "__annotations_cache__"}


class _Stable:
    """A digest of a function or a value made of what it is, not of its pickle: a
    pickle's bytes change from one process to the next (which strings are the
    same object, the order of a set), and each run of a script is a new process.
    What is pickled by reference on its way to a job goes by its name here too."""

    def __init__(self, seen=None):
        self.hash = hashlib.sha256()
        self.seen = {} if seen is None else seen

    def _add(self, tag, *parts):
        self.hash.update(tag.encode() + b"\0")
        for part in parts:
            self.hash.update(part if isinstance(part, bytes) else str(part).encode())
            self.hash.update(b"\0")

    def _inner(self, value):
        one = _Stable(self.seen)
        one.walk(value)
        return one.hash.hexdigest()

    def walk(self, value):
        if value is None or isinstance(value, (bool, int, float, complex, str, bytes)):
            self._add(type(value).__name__, repr(value))
            return
        if id(value) in self.seen:
            self._add("again", self.seen[id(value)])
            return
        self.seen[id(value)] = len(self.seen)
        if isinstance(value, (list, tuple)):
            self._add(type(value).__name__, len(value))
            for item in value:
                self.walk(item)
        elif isinstance(value, (set, frozenset)):
            self._add("set", *sorted(self._inner(item) for item in value))
        elif isinstance(value, dict):
            self._add("dict", *sorted(self._inner(key) + self._inner(item) for key, item in value.items()))
        elif isinstance(value, types.CodeType):
            self._code(value)
        elif isinstance(value, types.FunctionType):
            self._function(value)
        elif isinstance(value, types.ModuleType):
            self._add("module", value.__name__)
        elif isinstance(value, type):
            self._class(value)
        elif isinstance(value, (types.BuiltinFunctionType, types.MethodWrapperType, types.WrapperDescriptorType)):
            self._add("builtin", getattr(value, "__qualname__", repr(type(value))))
        elif isinstance(value, types.MethodType):
            self.walk(value.__func__)
            self.walk(value.__self__)
        else:
            self._object(value)

    def _code(self, code):
        self._add("code", code.co_code, code.co_argcount, code.co_kwonlyargcount, code.co_flags, *code.co_names,
                  *code.co_varnames, *code.co_freevars, *code.co_cellvars)
        for constant in code.co_consts:
            self.walk(constant)

    def _function(self, fn):
        if _by_reference(fn):
            self._add("function", fn.__module__, fn.__qualname__)
            return
        self._add("function", fn.__qualname__)
        self.walk(fn.__code__)
        self.walk(fn.__defaults__)
        self.walk(fn.__kwdefaults__)
        for cell in fn.__closure__ or ():
            try:
                self.walk(cell.cell_contents)
            except ValueError:
                self._add("empty cell")
        for name in sorted(_globals_of(fn.__code__)):
            if name in fn.__globals__:
                self._add("global", name)
                self.walk(fn.__globals__[name])

    def _class(self, cls):
        self._add("class", cls.__module__, cls.__qualname__)
        if _by_reference(cls):
            return
        for base in cls.__bases__:
            self.walk(base)
        for name in sorted(vars(cls)):
            if name not in _NOT_CODE:
                self._add("attribute", name)
                self.walk(vars(cls)[name])

    def _object(self, value):
        kind = type(value)
        self.walk(kind)
        if not _by_reference(kind) or kind.__module__.startswith("zenthros."):
            if hasattr(value, "__dict__"):
                self.walk(vars(value))
                return
        try:
            self._add("pickle", bundle.cloudpickle().dumps(value))
        except Exception:  # noqa: BLE001 - a lock, a connection: no data of its own to tell apart
            self._add("no bytes")


def _by_reference(thing):
    """Whether thing goes to a job by its name: it comes from an installed package.
    The Python it runs on says what it is then."""
    if "<locals>" in getattr(thing, "__qualname__", ""):
        return False
    module = sys.modules.get(getattr(thing, "__module__", None) or "")
    return module is None or not bundle.is_project_module(module)


def _globals_of(code):
    names = set(code.co_names)
    for constant in code.co_consts:
        if isinstance(constant, types.CodeType):
            names |= _globals_of(constant)
    return names


def code_digest(fn):
    """What fn and everything of the project it uses are, in any process. Neither
    where it is in its file nor the file's name count."""
    stable = _Stable()
    stable.walk(fn)
    return stable.hash.hexdigest()


def _digest_of(name, value, note, strict, pickler):
    """value, as what makes this call another: a path by its content, a URI by
    the object's version, a call by what it produced, the rest by its bytes."""
    if isinstance(value, Artifact):
        if value.sha256:
            return f"artifact:{value.sha256}"
        value = value.uri
    if isinstance(value, str):
        known = _reference(name, value, note, strict)
        if known:
            return known
    elif isinstance(value, (list, tuple)):
        inner = [_digest_of(f"{name}[{i}]", item, note, strict, pickler) for i, item in enumerate(value)]
        return hashlib.sha256(_canonical(inner)).hexdigest()
    elif isinstance(value, dict) and all(isinstance(k, str) for k in value):
        inner = {k: _digest_of(f"{name}.{k}", item, note, strict, pickler) for k, item in value.items()}
        return hashlib.sha256(_canonical(inner)).hexdigest()
    elif getattr(value, "cache_key", None):
        return f"step:{value.cache_key}"
    return code_digest(value)


def named_arguments(fn, args, kwargs):
    """A call's arguments by the names fn gives them."""
    try:
        return dict(inspect.signature(fn).bind(*args, **kwargs).arguments)
    except (TypeError, ValueError):
        return {**{f"argument {i + 1}": value for i, value in enumerate(args)}, **kwargs}


def note_for(fn, args, kwargs, *, code_dir=".", image="", env_key="", variables=None, secret_names=(), gpu=None,
             deps=None, contracts=None, mode=True):
    """The key of this call and the parts that make it. Neither where it runs,
    nor when, nor its job's name are among them."""
    strict = mode == "strict"
    note = Note("", {}, mode=mode)
    pickler = bundle.cloudpickle()
    code = code_digest(fn)
    # An environment's folder is the image's: its content counts already, and
    # publishing it rewrites its lock.
    files = _without_environments(bundle.project_files(code_dir))
    project = bundle.project_digest(code_dir, files)
    if len(files) <= NAMED_FILES:
        note.files = {name: identity.file_digest(path)[0][:16] for name, path in files.items()}
    parts = {"kind": "call", "fn": getattr(fn, "__qualname__", type(fn).__qualname__), "code": code, "project": project,
             "image": image or "", "env": env_key or "", "python": bundle.python_version(), "gpu": "" if gpu is None else str(gpu),
             "args": {}, "deps": {}}
    if variables:
        parts["vars"] = hashlib.sha256(_canonical({str(k): str(v) for k, v in variables.items()})).hexdigest()
    if secret_names:
        parts["secrets"] = sorted(str(name) for name in secret_names)
    if contracts:
        parts["contracts"] = hashlib.sha256(_canonical(contracts)).hexdigest()
    for name, value in named_arguments(fn, args, kwargs).items():
        parts["args"][name] = _digest_of(name, value, note, strict, pickler)
    for name, value in _deps(deps).items():
        known = _reference(name, value, note, strict) if isinstance(value, str) else None
        if known is None and isinstance(deps, (list, tuple)):
            raise errors.ValidationError(f"deps= names {value!r}, which is not here; deps={{'name': 'version'}} gives "
                                         "a version of something else (a table) as text")
        parts["deps"][name] = known or str(value)
    note.parts = parts
    note.key = hashlib.sha256(_canonical(parts)).hexdigest()
    return note


def _without_environments(files):
    made = {name.rsplit("/", 1)[0] + "/" for name in files if re.fullmatch(r"envs/[^/]+/env\.lock\.json", name)}
    return {name: path for name, path in files.items() if not any(name.startswith(folder) for folder in made)}


def _deps(deps):
    if not deps:
        return {}
    if isinstance(deps, dict):
        return dict(deps)
    if isinstance(deps, str):
        deps = [deps]
    return {os.path.basename(str(item).rstrip("/")) or str(item): str(item) for item in deps}


def dep_paths(deps):
    """The local paths deps= names, to go where the job runs."""
    found = []
    for value in _deps(deps).values():
        path = value[len("file://"):] if isinstance(value, str) and value.startswith("file://") else value
        if isinstance(path, str) and not _is_uri(path) and os.path.exists(path):
            found.append(os.path.abspath(path))
    return found


def _read_manifest(home, uri):
    """(the manifest, or None, why it is not good): a signature checked before
    anything else of it is believed."""
    try:
        raw = home.store.get(uri)
    except FileNotFoundError:
        return None, ""
    try:
        body = json.loads(raw)
    except ValueError:
        return None, "its entry is not a manifest"
    if not genuine(body):
        return None, "its entry is not signed by you"
    return body, ""


def _check_bytes(body, home, full):
    """(the result's bytes or None, why they are not good). They are always
    read and checked; a large output by the store's word for it, unless full."""
    result = Artifact.from_dict(body["result"])
    try:
        raw = _store_of(home, result.uri).get(result.uri)
    except FileNotFoundError:
        return None, f"the bytes of its result are gone ({result.uri})"
    seen = hashlib.sha256(raw).hexdigest()
    if seen != result.sha256:
        return None, f"its result is not the one verified: {result.uri} hashes to {seen[:12]}…, expected {result.sha256[:12]}…"
    for raw_out in body.get("outs") or []:
        why = _check_out(Artifact.from_dict(raw_out), home, full)
        if why:
            return None, why
    return raw, ""


def _store_of(home, uri):
    return home.store if uri.startswith(home.root.rstrip("/")) else _stores_for(uri)


def _check_out(out, home, full):
    store = _store_of(home, out.uri)
    if full and out.sha256:
        try:
            raw = store.get(out.uri)
        except FileNotFoundError:
            return f"its output {out.uri} is gone"
        return "" if hashlib.sha256(raw).hexdigest() == out.sha256 else f"its output {out.uri} is not what it left"
    if out.version and identity.object_identity(out.uri, store) != out.version:
        return f"its output {out.uri} is gone or was written again"
    return ""


@dataclass
class Found:
    """An entry whose signature and bytes are good."""

    manifest: dict
    raw: bytes
    home: Home


def lookup(note, homes=None):
    """(Found or None, why not). The key decides the entry, and its signature,
    then its bytes, decide whether it is good; only then may it be opened."""
    why = ""
    for home in homes if homes is not None else note.homes:
        body, problem = _read_manifest(home, home.manifest_uri(note.key))
        why = problem or why
        if body is None:
            continue
        if body.get("key") != note.key:
            why = "its entry is another call's"
            continue
        raw, problem = _check_bytes(body, home, note.mode == "strict")
        if raw is None:
            why = problem
            continue
        return Found(body, raw, home), ""
    return None, why


def used(found, note):
    """found is what this function gave last, so the next change is told from it.
    Only an explanation depends on this: a store that refuses it costs nothing."""
    fn = note.parts["fn"]
    for home in note.homes:
        last, _ = _read_manifest(home, home.last_uri(fn))
        if last is not None and last.get("key") == note.key:
            continue
        try:
            home.store.put(home.last_uri(fn), json.dumps(found.manifest, sort_keys=True).encode())
        except Exception as err:  # noqa: BLE001 - the result is good and is returned
            print(f"zenthros: could not note the use of {fn} at {home.root}: {err}", file=sys.stderr)


def _described(name, note):
    path = note.paths.get(name)
    return os.path.basename(path) if path else name


def compare(before, after, note=None, files=None):
    """What differs between two calls' parts, in words."""
    note = note or Note("", {})
    said = []
    fn = after.get("fn", "")
    before_files, files = before.get("files") or {}, files or {}
    moved = sorted(name for name in set(before_files) | set(files) if before_files.get(name) != files.get(name))
    named = f" ({', '.join(moved[:3])})" if moved and before_files else ""
    if before.get("code") != after.get("code"):
        said.append(f"the code of {fn} changed{named}")
    elif before.get("project") != after.get("project"):
        said.append(f"a file of the project changed{named}")
    labels = {"image": "the image changed", "env": "its packages changed", "python": "its Python changed",
              "gpu": "its gpu changed", "vars": "its environment variables changed", "secrets": "its secrets changed",
              "contracts": "its contracts changed"}
    said += [text for part, text in labels.items() if before.get(part) != after.get(part)]
    for group in ("args", "deps"):
        old, new = before.get(group) or {}, after.get(group) or {}
        said += [f"{_described(name, note)} changed" for name in sorted(set(old) | set(new)) if old.get(name) != new.get(name)]
    return said


def explain(note):
    """Why this call runs: note.why and note.changed, from the last time this
    function ran, and its entry's own trouble if it has one."""
    found, problem = lookup(note)
    if found is not None:
        return found
    if problem:
        note.why = f"entry not valid: {problem}"
        return None
    for home in note.homes:
        body, _ = _read_manifest(home, home.last_uri(note.parts["fn"]))
        if body is None:
            continue
        note.changed = compare({**body["parts"], "files": body.get("files")}, note.parts, note, note.files)
        note.why = ", ".join(note.changed) or "its entry was removed"
        return None
    note.why = "it has not run before"
    return None


class CachedCall:
    """A call that did not need to run: it answers as a RemoteCall does, with
    what the job that did run left."""

    cached = True
    action = "reuse"

    def __init__(self, found, note):
        self.found, self.cache_note = found, note
        made = found.manifest.get("produced_by") or {}
        self.name, self.where = made.get("job", ""), made.get("where", "")
        self.cache_key = note.key
        self.why = f"same as {self.name}: same bytes, signed by you"
        self._said = False

    @property
    def artifact(self):
        return Artifact.from_dict(self.found.manifest["result"])

    def __repr__(self):
        return f"CachedCall({self.name!r})"

    def __reduce__(self):
        # As another call's argument, its value: no job exists to wait for.
        return _value_of_call, (self.result(stream_logs=False),)

    def result(self, timeout=None, stream_logs=True, cancel_on_interrupt=True):  # noqa: ARG002 - RemoteCall's
        if stream_logs and not self._said:
            self._said = True
            made = self.found.manifest.get("produced_by") or {}
            print(f"✓ {self.cache_note.parts['fn']} reused from {self.name} ({made.get('at', '')}, on {self.where}, it took "
                  f"{made.get('took_s', 0):.0f}s): same bytes, signed by you", file=sys.stderr)
        # Its signature and its bytes were checked in lookup(); only now is it opened.
        record = pickle.loads(self.found.raw)
        return pickle.loads(record["value"])

    def status(self):
        return "reused"

    def logs(self, tail=None):  # noqa: ARG002 - RemoteCall's
        return ""

    def reports(self):
        return []

    @property
    def output(self):
        return self.artifact.uri.rsplit("/", 1)[0]

    def outs(self):
        return [Artifact.from_dict(raw) for raw in self.found.manifest.get("outs") or []]

    def cancel(self):
        return None

    def delete(self):
        return None

    def release(self):
        return None

    def cost(self):
        return None

    def to_dict(self):
        return {"job": self.name, "state": "reused", "key": self.cache_note.key, "where": self.where,
                "result": self.found.manifest["result"], "outs": self.found.manifest.get("outs") or [],
                "produced_by": self.found.manifest.get("produced_by") or {}}


def _value_of_call(value):
    return value


def publish(note, call, sha256, size, record):
    """A result this process just checked against its job's digests, as an
    entry of each home. Failing here costs a run later, never this result."""
    moved = identity.changed(note.stats)
    if moved:
        print(f"zenthros: {call.name} not kept in the cache: {', '.join(moved[:3])} changed while it ran",
              file=sys.stderr)
        return False
    made = {"job": call.name, "code": getattr(call, "code", ""), "where": call.where or (record.get("machine") or {}).get("host", ""),
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "took_s": max(0.0, (record.get("timings") or {}).get("finished", 0) - (record.get("timings") or {}).get("started", 0))}
    outs = []
    for raw in record.get("outs") or []:
        out = Artifact.from_dict(raw)
        try:
            version = identity.object_identity(out.uri, _stores_for(out.uri))
        except Exception:  # noqa: BLE001 - an output nobody can vouch for does not enter
            return False
        outs.append(Artifact(out.uri, out.sha256, out.size, version).to_dict())
    uri = f"{call.prefix}/result.pkl"
    done = False
    for home in note.homes:
        try:
            result_uri = uri
            if home.shared or carried.carries(uri):
                result_uri = f"{home.folder}/blobs/{sha256}"
                home.store.put(result_uri, call._store.get(uri))
            body = signed({"key": note.key, "fn": note.parts["fn"], "parts": note.parts, "files": note.files,
                           "result": Artifact(result_uri, sha256, size).to_dict(), "outs": outs, "produced_by": made})
            home.store.put(home.manifest_uri(note.key), json.dumps(body, sort_keys=True).encode())
            home.store.put(home.last_uri(note.parts["fn"]), json.dumps(body, sort_keys=True).encode())
            done = True
        except Exception as err:  # noqa: BLE001 - the result itself is good and is returned
            print(f"zenthros: could not keep {call.name} in the cache at {home.root}: {err}", file=sys.stderr)
    return done


@dataclass
class Entry:
    """One manifest of a store, good or not."""

    key: str
    uri: str
    home: Home
    manifest: dict = None
    modified: object = None
    problem: str = ""

    def to_dict(self):
        body = self.manifest or {}
        made = body.get("produced_by") or {}
        out = {"key": self.key, "uri": self.uri, "valid": not self.problem, "fn": body.get("fn"), "job": made.get("job"),
               "where": made.get("where"), "at": made.get("at"), "took_s": made.get("took_s")}
        if self.problem:
            out["problem"] = self.problem
        else:
            out.update(result=body.get("result"), outs=body.get("outs"), parts=body.get("parts"))
        return out


def entries(home):
    """Every manifest of a store. A listing that fails raises: an incomplete
    answer must not look like an empty cache."""
    found = []
    prefix = home.folder + "/"
    for item in home.store.ls(prefix):
        rest = item.uri[len(prefix):] if item.uri.startswith(prefix) else item.uri.rsplit("/", 1)[-1]
        if "/" in rest or not rest.endswith(".json"):
            continue
        key = rest[:-len(".json")]
        body, problem = _read_manifest(home, item.uri)
        if body is None and not problem:
            continue
        found.append(Entry(key, item.uri, home, body, item.modified, problem))
    return found


def referenced(home):
    """(the URIs valid manifests of this store pin, the project digests they
    hold). What a forged one names is not here."""
    uris, projects = set(), set()
    for entry in entries(home):
        if entry.manifest is None:
            continue
        uris.add(entry.manifest["result"]["uri"])
        uris.update(out["uri"] for out in entry.manifest.get("outs") or [])
        projects.add((entry.manifest.get("produced_by") or {}).get("code"))
    return uris, projects


def homes_here():
    """Every store of this project and this person that holds entries."""
    from zenthros.remote.options import local_client, local_k8s_client, settings

    found = [_home_of(local_client().store)]
    try:
        found.append(_home_of(local_k8s_client().store))
    except errors.ZenthrosError:
        pass
    current = settings()
    if current.store:
        found.append(_home_of(current.store, shared=True))
    return found


def _home_of(root, shared=False):
    from zenthros.core.storage import FileBlobStore
    from zenthros.remote.options import blob_store

    store = FileBlobStore() if root.startswith("file://") else blob_store(root)
    return Home(root, store, shared)


def _named(key, homes):
    found = [entry for home in homes for entry in entries(home) if entry.key.startswith(key)]
    if not found:
        raise errors.NotFound(f"no cache entry starts with {key!r} (zenthros cache ls)")
    if len({entry.key for entry in found}) > 1:
        raise errors.ValidationError(f"{key!r} names several entries: {sorted({e.key[:12] for e in found})}")
    return found


def ls(homes=None):
    return [entry.to_dict() for home in (homes if homes is not None else homes_here()) for entry in entries(home)]


def show(key, homes=None):
    return _named(key, homes if homes is not None else homes_here())[0].to_dict()


def rm(key, homes=None):
    """Remove an entry: its manifest. Its bytes go with the next clean."""
    removed = []
    for entry in _named(key, homes if homes is not None else homes_here()):
        entry.home.store.delete(entry.uri)
        removed.append(entry.key)
    return removed


def clean(older_than, homes=None, dry_run=False, now=None):
    """The manifests older than older_than go, with the copies of results only
    they held; their results then go with `zenthros clean --results`."""
    from zenthros.remote.options import seconds

    cutoff = (now or datetime.datetime.now(datetime.timezone.utc)) - datetime.timedelta(seconds=seconds(older_than, "older_than"))
    removed = []
    for home in homes if homes is not None else homes_here():
        kept = set()
        for entry in entries(home):
            if entry.modified is not None and entry.modified < cutoff:
                removed.append(entry.key)
                if not dry_run:
                    home.store.delete(entry.uri)
            elif entry.manifest is not None:
                kept.add(entry.manifest["result"]["uri"])
        for item in home.store.ls(f"{home.folder}/blobs/"):
            if item.uri not in kept and item.modified is not None and item.modified < cutoff and not dry_run:
                home.store.delete(item.uri)
    return removed


def notice_finished(jobs, homes):
    """Enter the jobs this person sent with cache= and nobody looked at since
    they finished. Each costs one read when it is already in."""
    from zenthros.remote.call import get

    entered = []
    for obj in jobs:
        note = note_of_job(obj)
        name = (obj.get("metadata") or {}).get("name", "")
        if note is None or not note.mode or (obj.get("status") or {}).get("phase") != "Succeeded":
            continue
        if any(_read_manifest(home, home.manifest_uri(note.key))[0] is not None for home in homes):
            continue
        try:
            get(name).result(stream_logs=False)
            entered.append(name)
        except Exception as err:  # noqa: BLE001 - one job that does not check out stays out
            print(f"zenthros: {name} stays out of the cache: {err}", file=sys.stderr)
    return entered


def sync():
    """`zenthros cache sync`: the jobs of this project and your Workspace."""
    from zenthros.remote.options import job_client, local_clients

    clients = [job_client()]
    clients += [here for here in local_clients() if here is not clients[0]]
    entered = []
    for client in clients:
        entered += notice_finished(client.list_jobs(), homes_here())
    return entered


def find(fn, *args, on=None, code_dir=".", deps=None, **kwargs):
    """The good entry of fn(*args, **kwargs), as a call that gives its value, or
    None. fn is what a call would be, with its options: for upstream=."""
    from zenthros.remote.call import CloudFunction, _cache_homes, _cache_image, _contracts, _environment, envs

    options = {}
    if isinstance(fn, CloudFunction):
        options, fn = dict(fn.options), fn.fn
    environment = _environment(options.get("requirements"), options.get("packages"), options.get("tools"))
    note = note_for(fn, args, kwargs, code_dir=options.get("code_dir", code_dir), image=_cache_image(options, on),
                    env_key=envs.key(environment) if environment else "",
                    variables=options.get("env") if isinstance(options.get("env"), dict) else None,
                    secret_names=list(options.get("secrets") or ()), gpu=options.get("gpu"),
                    deps=deps if deps is not None else options.get("deps"),
                    contracts=_contracts(options.get("inputs"), options.get("outputs")))
    found, _ = lookup(note, _cache_homes(on))
    return CachedCall(found, note) if found is not None else None
