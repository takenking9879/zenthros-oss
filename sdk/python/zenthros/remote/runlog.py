# SPDX-License-Identifier: Apache-2.0

"""A local record of each call, for the user and the assistant beside them
(Claude Code, Cursor...): what was asked, its log, its reports, and why it
failed, on disk where an assistant reads files, after the job is gone.

    .zenthros/runs/index.jsonl          one line per event: submitted, then how it ended
    .zenthros/runs/<job>/call.json      what was asked (no secret or env values) and how it ended
    .zenthros/runs/<job>/logs.txt       its log, as result() followed it
    .zenthros/runs/<job>/reports.jsonl  what its code published with zenthros.report
    .zenthros/runs/<job>/error.txt      a failure: its type, message and remote traceback

Where: configure(log_dir=...) or ZENTHROS_LOG_DIR; "off" writes nothing;
the default is ./.zenthros/runs (not with a client given by
configure(client=), a test's). A job's own calls (zenthros.map inside a
job) write nothing: their log is the job's. A hidden folder never travels
with the code (zenthros.remote.bundle), and the folder ignores itself in git.

Bounded: logs.txt keeps its last MAX_LOG_BYTES, the folder its last MAX_RUNS
calls, arguments MAX_REPR characters. A disk that fails never fails a call:
it is said once, and the record stops.
"""

import builtins
import json
import os
import re
import reprlib
import shutil
import sys
import time

from zenthros.core.env import JobEnv

# The module has its own open(); files are opened with this one.
_open_file = builtins.open

LOG_DIR_ENV = "ZENTHROS_LOG_DIR"
DEFAULT_DIR = os.path.join(".zenthros", "runs")
OFF = "off"
INDEX = "index.jsonl"
MAX_LOG_BYTES = 10 << 20
MAX_INDEX_BYTES = 1 << 20
MAX_RUNS = 200
MAX_REPR = 2048
_NAME = re.compile(r"^[a-z0-9][-a-z0-9.]*$")
_README = """# Zenthros calls made from here

One folder per call (its job's name), newest last in index.jsonl:

- call.json: what was asked (function or command, where, size, options; secret and env names, never values) and how it ended (status, seconds)
- logs.txt: the job's log as the call followed it (its last 10 MiB)
- reports.jsonl: what the code published with zenthros.report, oldest first
- error.txt: a failed call's error type, message and remote traceback

The SDK writes this folder (zenthros.remote.runlog); delete it freely.
"""

_said = False


def root(current):
    """The folder the records go in, or None when they are off. A client
    given by configure(client=) is a test's: it writes only where log_dir
    or ZENTHROS_LOG_DIR says, never into the folder it runs from."""
    value = (current.log_dir or os.environ.get(LOG_DIR_ENV, "")).strip()
    if value.lower() == OFF or JobEnv.current().job_id:
        return None
    if not value and current.client is not None:
        return None
    return os.path.abspath(value or DEFAULT_DIR)


def open(name, current):  # noqa: A001 - runlog.open
    """The record of job name, or None when records are off."""
    where = root(current)
    if where is None or not _NAME.match(name or ""):
        return None
    return RunLog(where, name)


def cut(value):
    """value as text, at most MAX_REPR characters, without building a
    large repr first."""
    if isinstance(value, (bytes, bytearray)) and len(value) > MAX_REPR:
        # reprlib has no rule for bytes: it would build the whole repr.
        value = bytes(value[:MAX_REPR])
    short = reprlib.Repr()
    short.maxstring = short.maxother = MAX_REPR
    short.maxlist = short.maxtuple = short.maxdict = short.maxset = 50
    text = short.repr(value)
    return text if len(text) <= MAX_REPR else text[:MAX_REPR - 3] + "..."


def last_failure(current=None):
    """The folder of the last call made from here that failed, or None."""
    from zenthros.remote.options import settings

    where = root(current or settings())
    if where is None:
        return None
    try:
        with _open_file(os.path.join(where, INDEX)) as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("event") in ("Failed", "Cancelled", "Interrupted"):
            return os.path.join(where, event.get("job", ""))
    return None




class RunLog:
    """One call's record. Every method is quiet: a failing disk turns the
    record off and is said once on stderr."""

    def __init__(self, where, name):
        self.root, self.name = where, name
        self.dir = os.path.join(where, name)
        self.off = False
        self._asked = {}
        self._logged = 0

    def _guard(self, step):
        global _said
        if self.off:
            return None
        try:
            return step()
        except (OSError, ValueError, TypeError) as exc:
            self.off = True
            if not _said:
                _said = True
                print(f"zenthros: the local log of calls ({self.root}) is off: {exc}", file=sys.stderr)
            return None

    def started(self, asked):
        def step():
            # The folder ignores itself in git and says what it is only when
            # it is the SDK's: new, empty, or the default one. log_dir can
            # name a project's folder, whose README.md it overwrote and whose
            # files a "*" .gitignore hid from git (audit 2026-10-16 NEW-009).
            # An existing file is never replaced.
            ours = not os.path.isdir(self.root) or not os.listdir(self.root) \
                or self.root.endswith(os.sep + DEFAULT_DIR)
            os.makedirs(self.dir, exist_ok=True)
            if ours:
                _create(os.path.join(self.root, ".gitignore"), "*\n")
                _create(os.path.join(self.root, "README.md"), _README)
            self._asked = dict(asked)
            self._save()
            self._index("submitted", asked)
            _prune(self.root, keep=self.name)
        self._guard(step)

    def note(self, **more):
        """More of what was asked, known after it was sent (where a map
        put each config, the machine made for it)."""
        def step():
            self._asked.update(more)
            self._save()
        self._guard(step)

    def line(self, text):
        def step():
            os.makedirs(self.dir, exist_ok=True)
            path = os.path.join(self.dir, "logs.txt")
            with _open_file(path, "a", encoding="utf-8", errors="replace") as fh:
                fh.write(text)
            self._logged += len(text.encode("utf-8", errors="replace"))
            if self._logged > MAX_LOG_BYTES:
                self._logged = _keep_tail(path, MAX_LOG_BYTES // 2)
        self._guard(step)

    def logged(self):
        return self._logged > 0

    def reports(self, snapshots):
        def step():
            _write(os.path.join(self.dir, "reports.jsonl"),
                   "".join(json.dumps(s, default=str) + "\n" for s in snapshots))
        self._guard(step)

    def ended(self, status, error="", **more):
        def step():
            self._asked.update(status=status, **more)
            self._save()
            if error:
                _write(os.path.join(self.dir, "error.txt"), error)
            self._index(status, {"error": error.strip()[:500]} if error else {})
        self._guard(step)

    def _save(self):
        _write(os.path.join(self.dir, "call.json"), json.dumps(self._asked, indent=1, default=str) + "\n")

    def _index(self, event, extra):
        line = {"at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "job": self.name, "event": event,
                "function": self._asked.get("function", ""), "pool": self._asked.get("pool", ""),
                "backend": self._asked.get("backend", "")}
        if "seconds" in self._asked:
            line["seconds"] = self._asked["seconds"]
        line.update({k: v for k, v in extra.items() if k in ("error",)})
        path = os.path.join(self.root, INDEX)
        with _open_file(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(line, default=str) + "\n")
        if os.path.getsize(path) > MAX_INDEX_BYTES:
            _keep_tail(path, MAX_INDEX_BYTES // 2)


def _write(path, text):
    with _open_file(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def _create(path, text):
    """Write path only if nothing is there."""
    try:
        with _open_file(path, "x", encoding="utf-8") as fh:
            fh.write(text)
    except FileExistsError:
        pass


def _keep_tail(path, size):
    """Keep the last size bytes of path, from a line's start; its new size."""
    with _open_file(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        end = fh.tell()
        fh.seek(max(0, end - size))
        tail = fh.read()
    if end > size and b"\n" in tail:
        tail = tail[tail.index(b"\n") + 1:]
    with _open_file(path, "wb") as fh:
        fh.write(tail)
    return len(tail)


# What a call's folder holds; one holding anything else is not a call's.
_RUN_FILES = frozenset({"call.json", "logs.txt", "reports.jsonl", "error.txt"})


def _is_run(path):
    try:
        names = os.listdir(path)
    except OSError:
        return False
    return bool(names) and _RUN_FILES.issuperset(names)


def _prune(where, keep):
    """The folder keeps its last MAX_RUNS calls (by when each was written).
    Only a call's folder is ever removed: any lowercase folder of a log_dir
    the user shares with other files was (audit 2026-10-16 NEW-009)."""
    runs = []
    for entry in os.scandir(where):
        if entry.is_dir(follow_symlinks=False) and _NAME.match(entry.name) and entry.name != keep \
                and _is_run(entry.path):
            runs.append((entry.stat(follow_symlinks=False).st_mtime, entry.name))
    extra = len(runs) + 1 - MAX_RUNS
    for _, name in sorted(runs)[:max(0, extra)]:
        shutil.rmtree(os.path.join(where, name), ignore_errors=True)
