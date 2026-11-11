# SPDX-License-Identifier: Apache-2.0

"""Fan-out: a running job spawns child jobs (docs/fan-out.md).

The job is head and worker at once. Each child is a ZenthrosJob of its own,
named <job>-<group>-<index>, that inherits the job's image, runtime,
compute, target and placement, and sees TASK_INDEX of TASK_COUNT,
ZENTHROS_PARENT_JOB_ID and its own output prefix
<output>/children/<group>/<index>. Results travel through object storage;
children never talk to their parent or to each other.

The wire is POST /v1/children on the agent's socket, the same bytes
`zenthros-agent job spawn` sends (the shared wire fixture, pkg/jobcli/testdata).
The controller decides: a refusal (a quota, a lease that is not live, a
child spawning) comes back in its words as SpawnRefused.
"""

import hashlib
import importlib
import json
import sys
import time
from dataclasses import dataclass

from zenthros import errors
from zenthros.core.agent import AgentClient
from zenthros.core.env import JobEnv
from zenthros.core.storage import store_for
from zenthros.errors import SpawnRefused
from zenthros.jobs.handle import TERMINAL_PHASES

RESULT = "result.json"
ITEMS = "items.json"

__all__ = ["ChildFailed", "Group", "SpawnRefused", "children", "map", "spawn"]

# zenthros.map is zenthros.remote.map: inside a job it comes here.


class ChildFailed(errors.ZenthrosError, RuntimeError):
    """A child ended other than Succeeded. str() is one line per such
    child: its name, its phase and its own message, whole."""

    def __init__(self, failed):
        self.failed = list(failed)
        super().__init__("\n".join(f"{c.name} ended {c.phase}: {c.message}" for c in self.failed))


@dataclass
class Child:
    group: str
    index: int
    name: str
    phase: str = ""
    message: str = ""

    @property
    def done(self):
        return self.phase in TERMINAL_PHASES


def _post(body, agent=None):
    data = (agent or AgentClient()).children(body)
    return [Child(c.get("group", ""), int(c.get("index", 0)), c.get("name", ""), c.get("phase", ""), c.get("message", ""))
            for c in data.get("children") or []]


def _body(group, count=0, cmd=None, same=False, env=None, list_only=False):
    job = JobEnv.current()
    body = {"job_id": job.job_id, "generation": job.generation, "group": group, "count": count}
    if cmd:
        body["command"] = list(cmd)
    if same:
        body["same"] = True
    if env:
        body["env"] = dict(env)
    if list_only:
        body["list"] = True
    return body


def children(name=""):
    """The children of group name, or of every group, as the controller
    last saw them."""
    return _post(_body(name, list_only=True))


class Group:
    """A spawned group. wait() until every child has ended; results() reads
    what each child wrote under its output prefix."""

    def __init__(self, name, count, children_, blob_store=None, strict=False):
        self.name = name
        self.count = count
        self.children = children_
        self._store = blob_store
        self._strict = strict

    def result(self, timeout=None, poll_seconds=5):
        """wait(), then results(): what zenthros.gather reads."""
        self.wait(timeout=timeout, poll_seconds=poll_seconds)
        return self.results(blob_store=self._store, strict=self._strict)

    def refresh(self):
        self.children = children(self.name)
        return self.children

    def wait(self, timeout=None, poll_seconds=5):
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # One call gives up after 45 s, the wait at its timeout: an agent
            # or controller away for longer is waited out, on what was seen.
            try:
                current = self.refresh()
            except errors.AgentUnanswered:
                current = self.children
            if current and all(c.done for c in current):
                failed = [c for c in current if c.phase != "Succeeded"]
                if failed:
                    raise ChildFailed(failed)
                return current
            if deadline is not None and time.monotonic() > deadline:
                pending = ", ".join(f"{c.name} {c.phase or '-'}" for c in current if not c.done)
                raise errors.WaitTimeout(f"children of {self.name} still running after {timeout}s: {pending}")
            time.sleep(poll_seconds)

    def results(self, blob_store=None, strict=False):
        """Each child's result.json under <output>/children/<group>/<i>, in
        index order; None where a child wrote none, or ValidationError
        naming them when strict."""
        prefix = child_prefix(self.name)
        store = blob_store or store_for(prefix)
        out, missing = [], []
        for i in range(self.count):
            uri = f"{prefix}/{i}/{RESULT}"
            try:
                out.append(json.loads(store.get(uri).decode("utf-8")))
            except FileNotFoundError:
                out.append(None)
                missing.append(uri)
        if strict and missing:
            raise errors.ValidationError(f"{len(missing)} of {self.count} children of {self.name} wrote no result: "
                                         + ", ".join(missing))
        return out


def child_prefix(group):
    output = JobEnv.current().data_output.rstrip("/")
    if not output:
        raise errors.ValidationError(
            "fan-out results travel through object storage: set workload.template.artifacts.output "
            "(ZENTHROS_DATA_OUTPUT) on the job")
    return f"{output}/children/{group}"


def spawn(name, count, cmd=None, same=False, env=None, wait=False, timeout=None, poll_seconds=5):
    """Create count children named <job>-<name>-<i>. cmd is their argv, or
    same=True runs this job's own command. Spawning name again (this job's
    replacement after its machine was lost) creates nothing twice: it
    returns the children that are there."""
    if bool(cmd) == bool(same):
        raise errors.ValidationError("spawn needs exactly one of cmd or same=True")
    if int(count) < 1:
        raise errors.ValidationError(f"spawn needs at least one child, not {count}")
    group = Group(name, int(count), _post(_body(name, int(count), cmd=cmd, same=same, env=env)))
    if wait:
        group.wait(timeout=timeout, poll_seconds=poll_seconds)
    return group


def _fn_ref(fn):
    if isinstance(fn, str):
        if ":" not in fn:
            raise errors.ValidationError(f"{fn!r} is not module:function")
        return fn
    module, qual = getattr(fn, "__module__", ""), getattr(fn, "__qualname__", "")
    if not module or not qual or "<" in qual or module == "__main__":
        raise errors.ValidationError(
            "map needs a function its children can import: a module-level function in an importable module, "
            "or 'module:function'")
    return f"{module}:{qual}"


def map(fn, items, name=None, blob_store=None, env=None):  # noqa: A001 - zenthros.map
    """Run fn(item) for each item in a child of its own. Returns the Group;
    its result() waits and returns the results in order. fn is
    'module:function', an importable function, or an argv list: a command
    reads its item (ZENTHROS_MAP_ITEMS[TASK_INDEX]) and may write
    result.json under its ZENTHROS_DATA_OUTPUT. Items and results travel
    as JSON: anything else is refused, not turned into its str().

    The group's name defaults to a hash of fn and items, so this job's
    replacement maps the same work onto the same children."""
    items = list(items)
    if not items:
        raise errors.ValidationError("map needs at least one item")
    encoded = json.dumps(items, sort_keys=True).encode("utf-8")
    target = list(fn) if isinstance(fn, (list, tuple)) else _fn_ref(fn)
    if name is None:
        digest = hashlib.sha256(json.dumps(target).encode("utf-8") + b"\0" + encoded).hexdigest()[:10]
        name = f"map-{digest}"
    prefix = child_prefix(name)
    store = blob_store or store_for(prefix)
    items_uri = f"{prefix}/{ITEMS}"
    store.put(items_uri, encoded)
    child_env = dict(env or {})
    child_env["ZENTHROS_MAP_ITEMS"] = items_uri
    if isinstance(target, list):
        cmd = target
    else:
        cmd = ["python3", "-m", "zenthros.fanout", "run", target]
    group = spawn(name, len(items), cmd=cmd, env=child_env)
    group._store, group._strict = store, not isinstance(target, list)
    return group


def run_child(fn_ref, blob_store=None):
    """A map child: read item TASK_INDEX, call fn, write result.json."""
    job = JobEnv.current()
    store = blob_store or store_for(job.map_items)
    items = json.loads(store.get(job.map_items).decode("utf-8"))
    item = items[job.task_index]
    module, _, qual = fn_ref.partition(":")
    target = importlib.import_module(module)
    for part in qual.split("."):
        target = getattr(target, part)
    result = target(item)
    output = job.data_output.rstrip("/")
    store.put(f"{output}/{RESULT}", json.dumps(result).encode("utf-8"))
    return result


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2 or argv[0] != "run":
        print("usage: python -m zenthros.fanout run module:function", file=sys.stderr)
        return 2
    run_child(argv[1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
