# SPDX-License-Identifier: Apache-2.0

"""The machine's side: `python -m zenthros.remote exec|map <call uri>`.

It checks the Python version before unpickling anything of the caller's,
swaps upstream calls in the arguments for their results, runs the code, and
writes result.pkl (map: results/<TASK_INDEX>.pkl) beside the call: the value,
or the exception and its traceback. It then reports the result's SHA-256
through its agent, which only this job can do: the caller unpickles only
bytes that match.

Three failures, three owners:
  - retry_on: the listed exceptions are retried here, in this process, up to
    retries times; a Task or phase continues from its last save.
  - a bug (anything else) is written as the result and ends the job Failed.
  - the machine going away is the controller's: an attempt that stops at the
    interruption notice, or whose lease ended, writes nothing and leaves the
    result to its replacement.
"""

import base64
import contextlib
import copy
import hashlib
import inspect
import os
import pickle
import platform
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import types

from zenthros import artifact, contracts, errors
from zenthros.core import preemption
from zenthros.core.env import JobEnv
from zenthros.core.report import Reporter
from zenthros.core.storage import store_for
from zenthros.remote import bundle, carried, envs
from zenthros.remote.call import (
    CALL_SHA256_ENV,
    MAP_KIND,
    OUTPUT_LOG,
    RESULT,
    RESULT_PRODUCER,
    RESULT_REPORT,
    RESULTS,
    RUNNER_PACKAGES_ENV,
    Upstream,
)

EXIT_ERROR = 1
EXIT_MISMATCH = 3
EXIT_SUPERSEDED = 4
EXIT_TAMPERED = 5
# RESULT_REPORT_SECONDS is how long a result's digest is sent again while
# the agent answers only transiently: a controller restart (2-3 min measured)
# with the agent's spool full.
RESULT_REPORT_SECONDS = 600
# How often a command's output is saved among the job's outputs, and how much
# of its end. Each save writes it all: a store cannot append.
LOG_EVERY = 30
LOG_MAX_BYTES = 8 << 20
# retry_on waits 1, 2, 4... seconds between attempts, at most 30.
RETRY_BASE_SECONDS = 1.0
RETRY_MAX_SECONDS = 30.0
# Runs the call again inside its environment (replaceable in tests).
_reexec = os.execve
# Stopping for a replacement is never retried here.
_HANDOVER = (errors.CheckpointFrozen, errors.StoppedOnNotice)


def pack_exception(exc):
    """(class, args, attributes) of exc. Pickling the exception itself
    rebuilds it as cls(*args), which fails for any class whose __init__
    takes other arguments (class QuotaExceeded(Exception): def __init__(self,
    message, code))."""
    return type(exc), exc.args, dict(getattr(exc, "__dict__", {}))


def rebuild_exception(packed):
    cls, args, attributes = packed
    exc = cls.__new__(cls)
    exc.args = args
    exc.__dict__.update(attributes)
    return exc


def _caller_main(fn):
    """A __main__ module holding the caller's __main__ functions and classes
    fn uses. Standard pickle names a __main__ function by its name, so code
    that hands one to multiprocessing (Pool.map(helper, ...)) finds it in the
    forked workers, as it would have in the caller's script."""
    main = types.ModuleType("__main__")
    if getattr(fn, "__module__", None) == "__main__":
        main.__dict__.update({k: v for k, v in getattr(fn, "__globals__", {}).items() if not k.startswith("__")})
    return main


def _package_drift(wanted):
    """(differ, missing): the caller's packages this machine has at another
    version, and those it lacks. The caller lists every package it had
    loaded, so a missing one is often not the function's."""
    from importlib import metadata

    differ, missing = [], []
    for dist, version in sorted(wanted.items()):
        # The SDK's core travels with the call, whatever this machine has.
        if dist == "zenthros":
            continue
        try:
            have = metadata.version(dist)
        except metadata.PackageNotFoundError:
            missing.append(f"{dist} {version}")
            continue
        if have != version:
            differ.append(f"{dist} {version} (here {have})")
    return differ, missing


def resolve(value, store):
    """value with every upstream call swapped for its result. A container
    holding none is the caller's object itself; one holding some is rebuilt
    as its own type (a namedtuple, a defaultdict, an OrderedDict)."""
    if isinstance(value, Upstream):
        return value.value(store or store_for(value.prefix))
    if isinstance(value, artifact.Sent):
        return value.fetch(store or store_for(value.uri))
    if isinstance(value, dict):
        items = {k: resolve(v, store) for k, v in value.items()}
        if all(items[k] is v for k, v in value.items()):
            return value
        out = copy.copy(value)
        out.update(items)
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [resolve(v, store) for v in value]
        if all(a is b for a, b in zip(items, value)):
            return value
        if isinstance(value, list):
            out = copy.copy(value)
            out[:] = items
            return out
        return type(value)(*items) if hasattr(value, "_fields") else type(value)(items)
    return value


def call_with_retries(fn, args, kwargs, retries=0, retry_on=(), out=None):
    """(value, attempts). An exception in retry_on runs fn again after a
    backoff, up to retries times, unless the machine is being taken back."""
    attempts = 0
    while True:
        attempts += 1
        try:
            return fn(*args, **kwargs), attempts
        except _HANDOVER:
            raise
        except tuple(retry_on) as exc:
            if attempts > retries or preemption.requested():
                exc.zenthros_attempts = attempts
                raise
            wait = min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * 2 ** (attempts - 1))
            print(f"zenthros: {type(exc).__name__}: {exc}; retry {attempts} of {retries} in {wait:g}s",
                  file=out or sys.stderr, flush=True)
            time.sleep(wait)


def store_result(store, uri, record, producer, out):
    """Write the result, then report its SHA-256: a reported digest always
    names bytes that are in the store. Each attempt and item reports from a
    producer of its own, so a replacement's digest never waits behind the
    lost attempt's sequence."""
    raw = pickle.dumps(record, protocol=4)
    extra = {}
    if carried.carries(uri):
        # No bucket: the result comes back in the report, if it fits one.
        if len(raw) > carried.MAX_RESULT_BYTES:
            message = carried.too_large(len(raw))
            print(f"zenthros: {message}", file=out, flush=True)
            raw = pickle.dumps({**record, "ok": False, "value": None, "type": "ResultTooLarge", "message": message,
                                "traceback": ""}, protocol=4)
        extra = {carried.BYTES_KEY: base64.b64encode(raw).decode("ascii")}
    store.put(uri, raw)
    reporter = Reporter(producer_id=producer, strict=False).set(
        RESULT_REPORT, {**artifact.Artifact(uri, hashlib.sha256(raw).hexdigest(), len(raw)).to_dict(), **extra})
    # Without its digest the result is refused: an agent that only answers
    # transiently (its spool full while the controller is away) is asked for
    # longer than one call's 45 s.
    deadline = time.monotonic() + RESULT_REPORT_SECONDS
    while True:
        try:
            reporter.publish()
            return
        except errors.AgentUnanswered as exc:
            if time.monotonic() < deadline:
                continue
            failure = exc
        except (errors.ReportError, OSError, ValueError) as exc:
            failure = exc
        print(f"zenthros: could not report the SHA-256 of {uri} ({failure}); the caller will refuse to read it",
              file=out, flush=True)
        return


def fetch_code(code, store):
    """The project the call pins, unpacked in a directory of its own: under
    the job's directory, which is the job's alone, or the temporary one when
    that is a read-only mount. None when the bytes in the store are not the
    ones the call pinned: they are not run."""
    packed = store.get(code["uri"])
    if hashlib.sha256(packed).hexdigest() != code["sha256"]:
        return None
    try:
        root = tempfile.mkdtemp(prefix=".zenthros-code-", dir=os.getcwd())
    except OSError:
        root = tempfile.mkdtemp(prefix="zenthros-code-")
    bundle.extract(packed, root)
    return root


def execute(call_uri, blob_store=None, out=None, index=None):
    """Run the call at call_uri and store its result beside it. Returns the
    process exit code."""
    out = out or sys.stderr
    store = blob_store or store_for(call_uri)
    base = call_uri.rsplit("/", 1)[0]
    raw = store.get(call_uri)
    job = JobEnv.current()
    expected = os.environ.get(CALL_SHA256_ENV, "")
    if hashlib.sha256(raw).hexdigest() != expected:
        # Unpickling runs code: a call.pkl other than the caller's (written
        # over it in the store after submit) is refused before it is read,
        # and so is one whose spec pins no digest: submit always pins it.
        message = (f"{call_uri} is not the call that was submitted (its SHA-256 differs from "
                   f"{CALL_SHA256_ENV}); refusing to unpickle it")
        print(f"zenthros: {message}", file=out, flush=True)
        return EXIT_TAMPERED
    envelope = pickle.loads(raw)
    is_map = envelope.get("kind") == MAP_KIND
    if is_map:
        # A map of one item is not distributed, so its machine has no
        # TASK_INDEX: its one task is item 0. Reading -1 wrote results/-1.pkl,
        # which result() never found (audit 2026-10-03 SDK-3).
        if index is None:
            index = job.task_index if job.task_index >= 0 else 0
        result_uri = f"{base}/{RESULTS}/{index}.pkl"
    else:
        result_uri = f"{base}/{RESULT}"
    producer = f"{RESULT_PRODUCER}-g{job.generation or 1}" + (f"-{index}" if is_map else "")
    machine = {"host": socket.gethostname(), "cpus": os.cpu_count(), "python": platform.python_version(),
               "generation": job.generation or 1}
    try:
        machine["affinity"] = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        pass
    quota = cpu_quota()
    if quota is not None:
        machine["cpu_quota"] = quota
    what = envelope.get("function") + (f"[{index}]" if is_map else "")
    print(f"zenthros: {what} on {machine['host']} ({cpus_text(machine['cpus'], quota)}, Python {machine['python']})",
          file=out, flush=True)
    if envelope.get("python") != bundle.python_version():
        message = (f"the call was pickled by Python {envelope.get('python')} and this machine runs "
                   f"{bundle.python_version()}; cloudpickle needs the same minor version on both ends")
        print(f"zenthros: {message}", file=out, flush=True)
        store_result(store, result_uri, {
            "ok": False, "type": "PythonVersionMismatch", "message": message, "traceback": "",
            "machine": machine, "timings": {}}, producer, out)
        return EXIT_MISMATCH
    # The function's own packages and tools, before anything of the caller's
    # is unpacked or unpickled: the call then runs again inside them.
    needs = envelope.get("environment")
    if needs and os.environ.get(envs.READY_ENV) != envs.key(needs):
        try:
            python, extra = envs.prepare(needs, out=out)
        except envs.EnvironmentFailed as exc:
            print(f"zenthros: {exc}", file=out, flush=True)
            store_result(store, result_uri, {"ok": False, "type": "EnvironmentFailed", "message": str(exc),
                                             "traceback": "", "machine": machine, "timings": {}}, producer, out)
            return EXIT_ERROR
        os.environ.update(extra)
        if python != sys.executable:
            os.environ[envs.READY_ENV] = envs.key(needs)
            for stream in (out, sys.stdout, sys.stderr):
                stream.flush()
            _reexec(python, [python, "-m", "zenthros.remote", MAP_KIND if is_map else "exec", call_uri],
                    dict(os.environ))
    # The machine's base (zenthros install), after the call was read: the
    # runner's own packages are loaded, so the base cannot break them, and
    # it comes before the machine's for what the function imports.
    envs.use_base()
    differ, missing = _package_drift(envelope.get("packages", {}))
    if differ:
        print("zenthros: packages differ from the caller's: " + ", ".join(differ), file=out, flush=True)
    if missing:
        shown = ", ".join(missing[:5]) + (f" and {len(missing) - 5} more" if len(missing) > 5 else "")
        print(f"zenthros: {len(missing)} packages the caller had loaded are not installed here "
              f"(the call may not use them): {shown}", file=out, flush=True)
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    # The project comes from the store, checked against the digest the call
    # pins, and is unpacked before anything of the caller's is unpickled
    # (SDK-8). The code runs from it, as it ran from the project root.
    code, project = envelope.get("code"), None
    if code:
        try:
            project = fetch_code(code, store)
        except FileNotFoundError:
            message = f"the call's code is not at {code['uri']}"
            print(f"zenthros: {message}", file=out, flush=True)
            store_result(store, result_uri, {"ok": False, "type": "CodeMissing", "message": message,
                                             "traceback": "", "machine": machine, "timings": {}}, producer, out)
            return EXIT_ERROR
        if project is None:
            print(f"zenthros: {code['uri']} is not the code the call was submitted with (its SHA-256 differs); "
                  "refusing to run it", file=out, flush=True)
            return EXIT_TAMPERED
        sys.path.insert(0, project)
    here = project or os.path.realpath(os.getcwd())
    # The caller's import roots inside code_dir (src/), first as they were
    # there; only paths under the project (SDK-1).
    for relative in reversed(envelope.get("sys_path") or []):
        path = os.path.realpath(os.path.join(here, relative))
        if path.startswith(here + os.sep) and path not in sys.path:
            sys.path.insert(0, path)
    started_in = os.getcwd()
    shipped = set()
    if project:
        shipped = _tree(project)
        os.chdir(project)
    try:
        return _run(envelope, is_map, index, blob_store, store, result_uri, producer, machine, out)
    finally:
        if project:
            os.chdir(started_in)
            _keep_outputs(project, shipped, started_in, out)


def _tree(root):
    """Every path under root, relative to it."""
    found = set()
    for directory, dirs, files in os.walk(root):
        for name in dirs + files:
            found.add(os.path.relpath(os.path.join(directory, name), root))
    return found


def _keep_outputs(project, shipped, into, out):
    """Drop the project's copy, moving what the call wrote under relative
    paths to into, the job's directory, where it landed before the project
    came by reference (post-fix review 2026-10-03 R-14). A path that is
    there already stays in the copy, which is then kept."""
    left = []
    if os.path.dirname(project) == os.path.realpath(into):
        for directory, dirs, files in os.walk(project):
            for name in list(dirs) + files:
                rel = os.path.relpath(os.path.join(directory, name), project)
                if rel in shipped:
                    continue
                if name in dirs:
                    dirs.remove(name)
                if name == "__pycache__":
                    continue
                target = os.path.join(into, rel)
                if os.path.lexists(target):
                    left.append(rel)
                    continue
                try:
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    shutil.move(os.path.join(directory, name), target)
                except OSError:
                    left.append(rel)
    if left:
        print(f"zenthros: kept {project}: the call wrote {', '.join(sorted(left)[:5])} there, which the job's "
              "directory already has", file=out, flush=True)
        return
    shutil.rmtree(project, ignore_errors=True)


PRODUCER_ENV = "ZENTHROS_PRODUCER_ID"
# A map task's zenthros.report is producer task-<i>: the tasks of one job on
# one machine are told apart (MapCalls.reports()).
TASK_PRODUCER = "task-"


@contextlib.contextmanager
def _task_producer(index):
    if index is None:
        yield
        return
    before = os.environ.get(PRODUCER_ENV)
    os.environ[PRODUCER_ENV] = f"{TASK_PRODUCER}{index}"
    try:
        yield
    finally:
        if before is None:
            os.environ.pop(PRODUCER_ENV, None)
        else:
            os.environ[PRODUCER_ENV] = before


# An output below this is read for its SHA-256; a larger one the store knows by
# its version.
OUTS_READ_BYTES = 64 << 20


def _report_contract(producer, reason, message, out):
    """The code a job's status cannot say. The result says it as well, so
    failing to say it here loses nothing."""
    try:
        Reporter(producer_id=f"{producer}-contract", strict=False).set(
            contracts.REPORT, {"reason": reason, "message": message}).publish()
    except (errors.ReportError, errors.AgentUnanswered, OSError, ValueError) as exc:
        print(f"zenthros: could not report {reason}: {exc}", file=out, flush=True)


def _check_inputs(declared, fn, args, kwargs, store):
    """A call's arguments against its inputs=, before it runs on them."""
    if not declared:
        return
    try:
        arguments = dict(inspect.signature(fn).bind(*args, **kwargs).arguments)
    except (TypeError, ValueError):
        arguments = dict(kwargs)
    contracts.check_inputs(declared, arguments, store)


def _outs(output, store):
    """What the function left in its output, each with its SHA-256 where it is
    here to read and not too large to; the rest the store knows by its version."""
    from zenthros.remote.call import is_output

    if not output:
        return []
    found = []
    base = output.rstrip("/")
    for item in store.ls(base + "/"):
        rel = item.uri[len(base) + 1:] if item.uri.startswith(base + "/") else os.path.basename(item.uri)
        # The project's copy is unpacked here while it runs, and goes after.
        if not is_output(rel) or rel.startswith(".zenthros-"):
            continue
        digest = ""
        path = item.uri[len("file://"):] if item.uri.startswith("file://") else item.uri
        if "://" not in path and item.size < OUTS_READ_BYTES:
            digest = artifact.sha256_of(path)
        found.append(artifact.Artifact(item.uri, digest, item.size).to_dict())
    return found


def _run(envelope, is_map, index, blob_store, store, result_uri, producer, machine, out):
    pickler = bundle.cloudpickle()
    started = time.time()
    runner_main = sys.modules.get("__main__")
    attempts = 1
    try:
        # The call gets a __main__ of its own: the runner's (this module,
        # under python -m) is not the caller's.
        sys.modules["__main__"] = types.ModuleType("__main__")
        body = pickler.loads(envelope["call"])
        fn = body["fn"]
        sys.modules["__main__"] = _caller_main(fn)
        if is_map and "shared" in body:
            # f.map: a config is keyword arguments over the shared ones.
            from zenthros.remote import spread

            args, kwargs = spread.arguments(body["items"][index], body["shared"])
        elif is_map:
            args, kwargs = (body["items"][index],), {}
        else:
            args, kwargs = body["args"], body["kwargs"]
        args, kwargs = resolve(args, blob_store), resolve(kwargs, blob_store)
        declared = envelope.get("contracts") or {}
        _check_inputs(declared.get("inputs"), fn, args, kwargs, store)
        with _task_producer(index if is_map else None):
            value, attempts = call_with_retries(fn, args, kwargs, body.get("retries", 0), body.get("retry_on", ()),
                                                out)
        output = JobEnv.current().data_output
        # Before it is reported: what does not hold its contract goes neither
        # to the next step nor to the cache.
        if declared.get("outputs"):
            contracts.check_output(declared["outputs"], value, output, store)
        record = {"ok": True, "value": pickler.dumps(value)}
        if envelope.get("outs"):
            record["outs"] = _outs(output, store)
        code = 0
    except errors.CheckpointFrozen as exc:
        # This attempt lost its lease (its machine is being reclaimed) and a
        # replacement resumed the call from the last acked checkpoint. The
        # replacement writes the result; one from here could overwrite it.
        print(f"zenthros: superseded by a replacement, leaving the result to it: {exc}", file=out, flush=True)
        return EXIT_SUPERSEDED
    except BaseException as exc:  # noqa: BLE001 - the caller gets whatever fn raised
        if preemption.requested():
            # The machine is being taken back and the code stopped (at its
            # notice, or the process was told to): the replacement resumes
            # from the last checkpoint and writes the result.
            print(f"zenthros: stopped after an interruption notice ({type(exc).__name__}); "
                  "a replacement resumes from the last checkpoint", file=out, flush=True)
            return EXIT_SUPERSEDED
        attempts = getattr(exc, "zenthros_attempts", attempts)
        tb = traceback.format_exc()
        print(tb, file=out, flush=True)
        try:
            pickled = pickler.dumps(pack_exception(exc))
        except Exception:  # noqa: BLE001 - the user's exception may not pickle: its traceback text still travels
            pickled = b""
        record = {"ok": False, "type": type(exc).__name__, "message": str(exc), "traceback": tb,
                  "error": pickled}
        if isinstance(exc, contracts.ContractError):
            record["reason"] = exc.reason
            record["message"] = f"{exc.reason}: {exc}. {exc.hint}"
            _report_contract(producer, exc.reason, record["message"], out)
        code = EXIT_ERROR
    finally:
        if runner_main is not None:
            sys.modules["__main__"] = runner_main
    record["attempts"] = attempts
    record["timings"] = {"started": started, "finished": time.time()}
    record["machine"] = machine
    store_result(store, result_uri, record, producer, out)
    # No "done" line here: the caller's receipt says it, with where and the
    # cost (R9: the user read it twice, and 0.0s for a quick function).
    return code


# Set in the runner started again inside the runner's own environment.
RUNNER_READY_ENV = "ZENTHROS_RUNNER_ENV"
# The module each runner package is imported as.
_MODULES = {"cloudpickle": "cloudpickle", "boto3": "boto3", "google-cloud-storage": "google.cloud.storage"}


def _missing(packages):
    """The packages of packages this Python cannot import."""
    import importlib.util

    missing = []
    for package in packages:
        name = package.split("==")[0].split(">=")[0].strip().lower()
        module = _MODULES.get(name, name.replace("-", "_"))
        try:
            found = importlib.util.find_spec(module) is not None
        except (ImportError, ValueError):
            found = False
        if not found:
            missing.append(package)
    return missing


def ensure_runner(argv, out=None):
    """Before anything is read from the store: what the runner needs
    (RUNNER_PACKAGES_ENV) that this Python lacks is installed in a venv of
    its own, kept for the next call, and the runner starts again inside it.
    A machine that has them (an image with cloudpickle) is left as it is."""
    out = out or sys.stderr
    wanted = os.environ.get(RUNNER_PACKAGES_ENV, "").split()
    if not wanted or os.environ.get(RUNNER_READY_ENV):
        return
    missing = _missing(wanted)
    if not missing:
        return
    python = envs.runner_env(wanted, out=out)
    os.environ[RUNNER_READY_ENV] = "1"
    for stream in (out, sys.stdout, sys.stderr):
        stream.flush()
    _reexec(python, [python, "-m", "zenthros.remote", *argv], dict(os.environ))


class _SavedLog:
    """A command's output, on to the job's own log and, every so often, among
    the job's outputs: the log channel ends with the machine, which may go any
    time. The store may refuse: the job goes on, and says so once."""

    def __init__(self, pipe, sink, uri, store, out, every):
        self.pipe, self.sink, self.uri, self.store, self.out, self.every = pipe, sink, uri, store, out, every
        self.kept = bytearray()
        self.unsaved = self.warned = False
        self.saved_at = time.monotonic()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._copy, daemon=True)
        self.thread.start()

    def _read(self, wait):
        """One chunk, if there is any within wait. False once the pipe ended."""
        if not select.select([self.pipe], [], [], wait)[0]:
            return True
        chunk = os.read(self.pipe, 1 << 16)
        if not chunk:
            return False
        self.sink(chunk)
        self.kept += chunk
        del self.kept[:-LOG_MAX_BYTES]
        self.unsaved = True
        return True

    def _copy(self):
        while not self.stop.is_set() and self._read(min(self.every, 0.5)):
            if self.unsaved and time.monotonic() - self.saved_at >= self.every:
                self._save()
        # The command ended. What it left running may hold the pipe open and
        # even keep writing: what is there now is the log.
        ending = time.monotonic() + 1
        while time.monotonic() < ending and select.select([self.pipe], [], [], 0)[0] and self._read(0):
            pass

    def _save(self):
        self.unsaved, self.saved_at = False, time.monotonic()
        try:
            self.store.put(self.uri, bytes(self.kept))
        # A store's client raises its own errors; none of them ends the job.
        except Exception as exc:  # pylint: disable=broad-except
            if not self.warned:
                print(f"zenthros: could not save the log to {self.uri}: {exc}", file=self.out, flush=True)
            self.warned = True

    def finish(self):
        self.stop.set()
        self.thread.join()
        if self.unsaved:
            self._save()


def _job_log_sink(chunk):
    stream = sys.__stdout__.buffer
    stream.write(chunk)
    stream.flush()


def run_snapshot(uri, command, blob_store=None, out=None, log_sink=_job_log_sink, log_every=None):
    """Run command in the snapshot at uri, once its bytes are the ones the job
    pins. Its exit code is the job's; a stop reaches it, and what it started.
    What it prints also stays among the job's outputs (OUTPUT_LOG)."""
    out = out or sys.stderr
    store = blob_store or store_for(uri)
    root = fetch_code({"uri": uri, "sha256": os.environ.get(CALL_SHA256_ENV, "")}, store)
    if root is None:
        print(f"zenthros: {uri} is not the snapshot this job pins (its SHA-256 differs from {CALL_SHA256_ENV}); "
              "it is not run", file=out, flush=True)
        return EXIT_TAMPERED
    output = JobEnv.current().data_output
    try:
        child = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE if output else None,
                                 stderr=subprocess.STDOUT if output else None)
    except (FileNotFoundError, PermissionError) as exc:
        print(f"zenthros: {command[0]}: {exc.strerror}", file=out, flush=True)
        return 127 if isinstance(exc, FileNotFoundError) else 126
    saved = None
    if output:
        saved = _SavedLog(child.stdout.fileno(), log_sink, f"{output.rstrip('/')}/{OUTPUT_LOG}", store, out,
                          LOG_EVERY if log_every is None else log_every)
    stop = lambda number, _frame: child.send_signal(number)  # noqa: E731
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, stop)
    code = child.wait()
    if saved:
        saved.finish()
    # The shell's own way of saying a signal ended it.
    return 128 - code if code < 0 else code


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) >= 4 and argv[0] == "snapshot" and argv[2] == "--":
        ensure = argv[:2]
    elif len(argv) == 2 and argv[0] in ("exec", "map"):
        ensure = argv
    else:
        print("usage: python -m zenthros.remote exec|map CALL_URI | snapshot URI -- COMMAND...", file=sys.stderr)
        return 2
    try:
        ensure_runner(ensure)
    except envs.EnvironmentFailed as exc:
        print(f"zenthros: the runner's packages: {exc}", file=sys.stderr, flush=True)
        return EXIT_ERROR
    if argv[0] == "snapshot":
        return run_snapshot(argv[1], argv[3:])
    return execute(argv[1])


def cpu_quota(root="/sys/fs/cgroup", self_cgroup="/proc/self/cgroup"):
    """The CPUs this process may use by its cgroup's quota (cgroup v2): the
    smallest of its cgroup's and its parents'; None without one."""
    try:
        with open(self_cgroup) as f:
            own = next((line.split("::", 1)[1].strip() for line in f if line.startswith("0::")), "/")
    except OSError:
        return None
    found = []
    path = own
    while True:
        try:
            with open(os.path.join(root, path.strip("/"), "cpu.max")) as f:
                limit, period = (f.read().split() + ["100000"])[:2]
            # "max": no quota here (int raises).
            if int(period) > 0:
                found.append(int(limit) / int(period))
        except (OSError, ValueError):
            pass
        if path in ("", "/"):
            break
        path = os.path.dirname(path.rstrip("/")) or "/"
    return min(found) if found else None


def cpus_text(cpus, quota):
    """What the runner says of its CPUs: the quota when it is less."""
    if quota is not None and cpus and quota < cpus:
        return f"{quota:g} of {cpus} CPUs"
    return f"{cpus} CPUs"
