# SPDX-License-Identifier: Apache-2.0

"""The caller's side: run, map, gather, get, and RemoteCall.

    call = zenthros.run(fn, *args, on="my-vm", gpu="A10G")      # a function or a zenthros.Task
    call = zenthros.run(["python", "train.py"], on="my-vm")     # a command
    calls = zenthros.map(fn, items, on="aws-spot")               # one machine per item
    zenthros.gather(call, calls)                                 # their results, in order

on= is where; without it, configure(on=) or ZENTHROS_ON, else this
computer ("local").

A call is stored at <store>/<name>/call-<sha16>.pkl before its job exists
(named by its digest: a create refused for a taken name leaves the live
job's call alone); the job runs `python -m zenthros.remote exec <call uri>`
(map: `map`) and writes result.pkl (map: results/<i>.pkl) beside it. Only
URIs travel through the control plane; data goes by reference (s3://...),
never inside the call.

Unpickling runs code, and more than the caller can write the store. The
spec pins the call's SHA-256 (the runner and get() refuse another call),
and the runner reports each result's SHA-256 through its agent, which only
the job can do: result() unpickles only bytes that match. A result is as
trustworthy as the machine that produced it.

A RemoteCall passed as an argument (or inside a list, tuple or dict
argument, or as a map item) makes the new call wait for it (spec.dependsOn)
and receive its result: a function's value, a map's list, a command's output
URI.
"""

import base64
import hashlib
import json
import os
import pickle
import re
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from zenthros import console, errors
from zenthros.core import checkpoint
from zenthros.core.storage import GCS_KEY_ENV
from zenthros.jobs import spec as S
from zenthros.jobs.handle import CANCELLED, FAILED, TERMINAL_PHASES, JobHandle, seconds_at
from zenthros.remote import bundle, carried, envs, runlog
from zenthros.remote import snapshot as code_snapshot
from zenthros.remote.options import (
    _own_bucket,
    blob_store,
    job_client,
    local_client,
    local_client_of,
    local_k8s_client,
    seconds,
    secret_sources,
    settings,
    split_local_secrets,
)

FORMAT = 2
CALL_OBJECT = re.compile(r"call-[0-9a-f]{16}\.pkl")
RESULT = "result.pkl"
RESULTS = "results"
_MAP_RESULT = re.compile(rf"{RESULTS}/\d+\.pkl")
CALL_KIND, MAP_KIND, COMMAND_KIND = "call", "map", "command"
PROCESS_FAILED = "PROCESS_FAILED"
# The SHA-256 of the call as the caller wrote it, in the job's spec: only who
# may edit the job's spec can change it.
CALL_SHA256_ENV = "ZENTHROS_CALL_SHA256"
# A snapshot's command leaves its output here among its outputs: the log channel
# ends with the machine.
OUTPUT_LOG = "zenthros-output.log"
# What the runner needs to read the call and unpickle it, as requirements
# (cloudpickle at the caller's version, the store's client): installed where
# it runs when the machine lacks them (a slim image, a bare VM's Python).
RUNNER_PACKAGES_ENV = "ZENTHROS_RUNNER_PACKAGES"
# The report payload key under which the runner gives a result's {uri,
# sha256}, from a producer of its own per attempt and item.
RESULT_REPORT = "zenthros.result"
RESULT_PRODUCER = "zenthros-result"
# The agent spools a report before the controller has it: a job can end
# Succeeded a moment before its result's digest is readable.
DIGEST_GRACE = 60.0
# On a zenthros.run job: how many results it writes. The controller holds a
# job that depends on it until that many digests are in its status.
RESULT_COUNT = "zenthros.ai/result-count"
# A map spread over several destinations: its jobs carry the map's name (a
# label, for zenthros.get to list them), which configs each took and where.
MAP_GROUP = "zenthros.ai/map"
MAP_INDICES = "zenthros.ai/map-indices"
MAP_ON = "zenthros.ai/map-on"
MAP_SIZE = "zenthros.ai/map-size"
# A label on every job this module submits (run, map, serve).
REMOTE_LABEL = "zenthros.ai/remote"
# distribution.count's Maximum in the CRD (operator/api/v1
# distribution_types.go): a map of more is refused before it uploads.
MAX_TASKS = 256
# Upstream job name -> results_pin of its results, set by the controller from
# the upstreams' status when the job's dependencies were met.
UPSTREAM_RESULTS_ENV = "ZENTHROS_UPSTREAM_RESULTS"


def results_pin(digests):
    """One digest for a job's results, from (uri, sha256) pairs: SHA-256 over
    the latest digest of each URI, sorted by URI, as "uri sha256\\n" lines.
    The controller pins the same (resultdigest.Pin)."""
    latest = dict(digests)
    lines = "".join(f"{uri} {latest[uri]}\n" for uri in sorted(latest))
    return hashlib.sha256(lines.encode()).hexdigest()


def _upstream_pins():
    try:
        pins = json.loads(os.environ.get(UPSTREAM_RESULTS_ENV) or "{}")
    except ValueError:
        return {}
    return pins if isinstance(pins, dict) else {}


class RemoteError(errors.ZenthrosError, RuntimeError):
    """Your code failed on the machine: the function raised (__cause__ is its
    exception, remote_traceback what the machine printed), or the command
    exited non-zero (the message ends with its last log lines).

    A failure of the platform instead (the image, a quota, the machine lost
    past its retries) is errors.JobFailed, with the controller's reason."""

    def __init__(self, message, remote_type="", remote_traceback="", job="", reason=""):
        super().__init__(message)
        self.remote_type = remote_type
        self.remote_traceback = remote_traceback
        self.job = job
        # A code the runner gave it (OUTPUT_CONTRACT), or "".
        self.reason = reason


def _target_backends(call):
    """The backends a call's job is pinned to (spec.target.backends)."""
    try:
        return list(((call._client.job_object(call.name) or {}).get("spec") or {}).get("target", {}).get("backends") or [])
    except (AttributeError, errors.JobNotFound):
        return []


class Upstream:
    """A RemoteCall inside another call's arguments. The machine swaps it for
    the call's result before the function runs."""

    def __init__(self, name, prefix, kind, count=0):
        self.name, self.prefix, self.kind, self.count = name, prefix, kind, count

    def __repr__(self):
        return f"Upstream({self.name!r})"

    def value(self, store):
        if self.kind == COMMAND_KIND:
            return self.prefix
        if self.kind == MAP_KIND:
            uris = [f"{self.prefix}/{RESULTS}/{i}.pkl" for i in range(self.count)]
        else:
            uris = [f"{self.prefix}/{RESULT}"]
        raws = self._verified(store, uris)
        values = [self._read(uri, raws[uri]) for uri in uris]
        return values if self.kind == MAP_KIND else values[0]

    def _verified(self, store, uris):
        """The bytes at each uri, once they hash to the pin the controller
        set from the upstream's status. The store is not the upstream's to
        vouch for: anyone who may write there could run code here (audit
        2026-10-03 SEC-4, post-fix review R-1)."""
        raws = {}
        for uri in uris:
            try:
                raws[uri] = store.get(uri)
            except FileNotFoundError:
                raise RemoteError(f"upstream {self.name} has no result at {uri}", job=self.name) from None
        pinned = _upstream_pins().get(self.name)
        if not pinned:
            raise RemoteError(f"refusing to unpickle upstream {self.name}'s result: the controller pinned no "
                              f"SHA-256 for it in {UPSTREAM_RESULTS_ENV} (the upstream reported none, or the "
                              "control plane predates the check)", job=self.name)
        if results_pin((uri, hashlib.sha256(raw).hexdigest()) for uri, raw in raws.items()) != pinned:
            raise RemoteError(f"refusing to unpickle upstream {self.name}'s result: it is not what the upstream "
                              "reported (written over in the store after it ran); whoever wrote it could run "
                              "code here", job=self.name)
        return raws

    def _read(self, uri, raw):
        record = pickle.loads(raw)
        if not record.get("ok"):
            raise RemoteError(f"upstream {self.name} failed: {record.get('type')}: {record.get('message')}",
                              remote_type=record.get("type", ""), job=self.name)
        return pickle.loads(record["value"])


def run(target, *args, on=None, cpu=None, memory=None, gpu=None, instance_type="", spot=None,
        image="", python=None, code_dir=".", env=None, secrets=None, retries=0, retry_on=(), timeout=None,
        after=(), resilient=True, name=None, labels=None, requirements=None, packages=None, tools=(), params=None,
        keep=False, dry_run=False, keep_for=None, snapshot=False, cache=False, deps=None, inputs=None, outputs=None,
        upload=False, fresh=False, _where=None, _note=None, _keyed=False, _resume=None, _same=None, **kwargs):
    """Run target on a Zenthros machine. Returns a RemoteCall at once;
    .result() waits for it.

    target is a function (or a zenthros.Task, or any callable), called as
    target(*args, **kwargs); or a command: a list (argv) or a string (run by
    sh -c) in code_dir's files, whose data goes to $ZENTHROS_DATA_OUTPUT
    (call.output). snapshot=True takes code_dir's whole git worktree for a
    command, as it is now and by its digest: editing it later changes nothing
    that runs (a command with snapshot needs Python on the machine).

    Where: on= a machine, its name, or a template (zenthros.targets()); with
    no on=, configure(on=) or ZENTHROS_ON, else this computer. A template
    gives a machine of its with nothing to do, or a new one; fresh=True always
    a new one, for a run nothing else may have touched.
    cpu, memory, gpu ("A10G" or a count), instance_type and spot constrain
    the machine; image runs the job in that container image, on a machine
    that runs images (containerd, a cluster).
    Environment: env for plain values, or the name of an environment
    (zenthros.env) to run in; secrets={NAME or /path: Secret} for
    secrets. Failure: retry_on=(ConnectionError, ...) retries those
    exceptions of a function in place, up to retries times; a command's
    retries re-run it. resilient moves the call to another machine when its
    machine is lost (Spot), where zenthros.Task and zenthros.phase resume
    from their checkpoints. timeout ends the job ("30m", "2h", seconds).
    after waits for other calls to succeed first. The function's own
    packages: requirements= a requirements file (relative to where this
    runs) and packages=["polars==1.44.2", ...], installed on the machine in a
    venv cached by their hash; tools=["java"] adds those tools.
    params={...} are target's keyword arguments, whatever their names
    (memory=, timeout=...).
    A function's result, once checked, is kept with cache=True: a later call
    with the same code, arguments, data and environment returns it without
    running, wherever it was to run, and says why not when it does run
    (cache="strict" also reads every large file and output again). deps= names
    what it reads without taking as an argument: paths, URIs, or {"sales":
    "2026-10-31"}, a version you give. inputs= and outputs= are its contracts
    (zenthros.contracts), checked where it runs.
    """
    if snapshot and not isinstance(target, (str, list, tuple)):
        raise errors.ValidationError("snapshot=True is a command's: a function's project already goes by its digest")
    if cache not in (False, True, "strict"):
        raise errors.ValidationError(f'cache={cache!r}: True or "strict"')
    if isinstance(target, (str, list, tuple)) and (cache or deps or inputs or outputs):
        raise errors.ValidationError("cache=, deps=, inputs= and outputs= are a function's: a command leaves no value")
    options = dict(cpu=cpu, memory=memory, gpu=gpu, instance_type=instance_type,
                   spot=spot, image=image, python=python, code_dir=code_dir, env=env, secrets=secrets,
                   timeout=timeout, after=after, resilient=resilient, labels=labels, requirements=requirements,
                   packages=packages, tools=tools, keep_for=keep_for)
    if _where is None:
        if dry_run:
            return _plan_of_call(target, args, {**kwargs, **(params or {})}, on, options, keep, cache, deps, inputs, outputs)
        args, kwargs = _settled(args, bool(cache)), _settled(kwargs, bool(cache))
        params = _settled(params, bool(cache))
        note = (_same or {}).get("note")
        if cache or _keyed:
            note, hit = _consult_cache(target, args, {**kwargs, **(params or {})}, options, on, cache, deps,
                                       _contracts(inputs, outputs))
            if hit is not None:
                return hit
        if isinstance(_resume, str):
            _resume = _resume_ref(_client_for(on), _resume, note)
        args, kwargs, params = _in_reach((args, kwargs, params), on, code_dir, upload)
        if inputs:
            _check_inputs_here(target, args, {**kwargs, **(params or {})}, inputs)
        return _placed_call(on, options, keep, lambda where, placed: run(
            target, *args, retries=retries, retry_on=retry_on, name=name, params=params, snapshot=snapshot, _where=where,
            deps=deps, inputs=inputs, outputs=outputs, _note=note, _resume=_resume, _same=_same, **placed, **kwargs),
            python=_pickled(target), fresh=fresh)
    options.update(_WHERE, **_where)
    if isinstance(target, (str, list, tuple)):
        if args or kwargs or params:
            raise errors.ValidationError("a command takes its arguments inside it: run(['python', 'x.py', '--n', '3'])")
        if retry_on:
            raise errors.ValidationError("retry_on names a function's exceptions; a command's retries= re-runs it")
        argv = ["sh", "-c", target] if isinstance(target, str) else [str(part) for part in target]
        if not argv or not argv[-1]:
            raise errors.ValidationError("run: the command is empty")
        return _submit(COMMAND_KIND, name or _job_name(argv), None, argv=argv, backoff=retries, snapshot=snapshot,
                       resume_from=_resume, same=_same, **options)
    if not callable(target):
        raise errors.ValidationError(f"run takes a function, a Task or a command, not {type(target).__name__}")
    kwargs = {**kwargs, **(params or {})}
    body = {"fn": target, "args": args, "kwargs": kwargs, **_retry(retries, retry_on)}
    return _submit(CALL_KIND, name or _job_name(target), target, body=body, upstream=(args, kwargs), note=_note, deps=deps,
                   contracts=_contracts(inputs, outputs), resume_from=_resume, same=_same, **options)


def _in_reach(value, on, code_dir, upload):
    """value, if the place it runs can reach what it names. A path only here stops
    it before anything is made; with upload= it goes to the store first."""
    from zenthros import machines, reach
    from zenthros import upload as uploads

    found = reach.answers(value, on, code_dir)
    blocked = [(ref, answer) for ref, answer in found if answer.answer == reach.IMPOSSIBLE]
    if not blocked:
        return value
    if not upload or (machines.default_on() if on is None else on) == machines.LOCAL:
        raise errors.ValidationError(blocked[0][1].why)
    current = settings()
    client = _client_for(on)
    root = _store(current, client)
    return uploads.send(value, [ref for ref, _ in blocked], root, blob_store(root, current, client))


def _contracts(inputs, outputs):
    """inputs= and outputs= as the runner reads them, or None."""
    if inputs is None and outputs is None:
        return None
    from zenthros import contracts

    return {"inputs": contracts.declared(inputs), "outputs": contracts.declared(outputs)}


def _check_inputs_here(fn, args, kwargs, inputs):
    """What a call's contract can already see here is refused before a job
    exists; a path only its machine has is checked there."""
    from zenthros import cache as entries
    from zenthros import contracts

    named = entries.named_arguments(fn, args, kwargs)
    here = {name: value for name, value in named.items() if isinstance(value, str) and
            (os.path.exists(value) or value.startswith("file://"))}
    declared = contracts.declared(inputs)
    if here and declared is not None and "format" in declared and len(named) == 1:
        contracts.check_inputs(declared, here)
    elif here and declared is not None and "format" not in declared:
        contracts.check_inputs(declared, here)


def _plan_of_call(target, args, kwargs, on, options, keep, cache, deps, inputs, outputs):
    from zenthros.targetlist import plan_call

    plan = plan_call(on, options, keep=keep, python=_pickled(target))
    if not callable(target):
        return plan
    return plan_details(plan, target, args, kwargs, on, options, cache, deps, _contracts(inputs, outputs))


def plan_details(plan, fn, args, kwargs, on, options, mode, deps, contracts):
    """A Plan with what this call would use, and every reason it could not.
    Nothing is created, built, pulled or sent."""
    from zenthros import env as environments
    from zenthros import machines, reach

    where = machines.default_on() if on is None else on
    name = str(getattr(where, "name", "") or where)
    env_name = options.get("env") if isinstance(options.get("env"), str) else ""
    if not env_name and name in (machines.LOCAL, machines.LOCAL_K8S):
        env_name = environments.using()
    details = {"env": env_name, "image": str(options.get("image") or ""), "warnings": [],
               "resources": {what: str(options[what]) for what in ("cpu", "memory", "gpu") if options.get(what) not in (None, "")},
               "compatible": {"python": bundle.python_version(), "gpu": True}}
    if env_name and name in (machines.LOCAL, machines.LOCAL_K8S):
        details["image"] = details["image"] or environments.tag_of(env_name)
    elif env_name and not details["image"]:
        try:
            details["image"] = environments.image_for(env_name, name, None)
            if environments.packages_for(env_name, name):
                details["warnings"].append(f"{env_name} is not published: its base and its packages at the same versions, "
                                           "not the same image")
        except errors.ValidationError as err:
            plan.problems.append(str(err))
    if details["image"] and name not in (machines.LOCAL, machines.LOCAL_K8S):
        answer = machines.who_reads(name, details["image"])
        details["image_access"] = answer.to_dict()
        if answer.answer == reach.IMPOSSIBLE:
            plan.problems.append(answer.why)
    environment = _environment(options.get("requirements"), options.get("packages"), options.get("tools"))
    details["packages"] = len((environment.get("requirements") or "").split("\n")) if environment else 0
    details["code_bytes"] = len(bundle._packed_project(options.get("code_dir")) or b"")
    data = []
    for ref, answer in reach.answers((args, kwargs), where):
        data.append({"path": ref, **answer.to_dict(), **({"mount": "ro"} if answer.answer == reach.POSSIBLE and "://" not in ref else {})})
        if answer.answer == reach.IMPOSSIBLE:
            plan.problems.append(answer.why)
    details["data"] = data
    details["cache"] = _plan_cache(fn, args, kwargs, options, on, mode, deps, contracts)
    if name == machines.LOCAL:
        try:
            missing = local_client().missing_offline(options.get("image") or (environments.tag_of(env_name) if env_name else None))
        except errors.EngineMissing as err:
            # Said like any other problem of the plan: what to install.
            plan.problems.append(str(err))
            missing = ["a container engine"]
        details["offline_ready"], details["offline_missing"] = not missing, missing
    if name == machines.LOCAL_K8S:
        details["store"] = {"shared": local_k8s_client().shared_now(), "where": str(local_k8s_client().store_dir)}
    elif name == machines.LOCAL:
        details["store"] = {"shared": False, "where": local_client().store}
    plan.call = details
    return plan


def _plan_cache(fn, args, kwargs, options, on, mode, deps, contracts):
    if not mode:
        return {"enabled": False, "hit": False, "changed": [], "why": "cache is off"}
    note, hit = _consult_cache(fn, _settled(args, True), _settled(kwargs, True), options, on, mode, deps, contracts, say=False)
    if note is None:
        return {"enabled": True, "hit": False, "changed": [], "why": "what it reads cannot be told apart"}
    return {"enabled": True, "hit": hit is not None, "changed": note.changed, "why": note.why, "key": note.key,
            "how": sorted(set(note.checks.values()))}


def _settled(value, wait):
    """value with a reused call as its value: it has no job to wait for. With
    wait, a running call too: the cache keys on what it produced."""
    if getattr(value, "cached", False) or (wait and isinstance(value, RemoteCall)):
        return value.result(stream_logs=False)
    if isinstance(value, (list, tuple)) and type(value) in (list, tuple):
        return type(value)(_settled(item, wait) for item in value)
    if type(value) is dict:
        return {key: _settled(item, wait) for key, item in value.items()}
    return value


def _cache_image(options, on):
    """What the call's image is, for the cache: an environment's content, which
    no recompressing changes, else the reference as it is written."""
    from zenthros import env as environments
    from zenthros import machines

    if options.get("image"):
        return str(options["image"])
    where = machines.default_on() if on is None else on
    name = options.get("env") if isinstance(options.get("env"), str) else ""
    if not name and where in (machines.LOCAL, machines.LOCAL_K8S):
        name = environments.using()
    return environments.content_of(name) if name else ""


def _client_for(on):
    """The client a call sent to on= goes through."""
    from zenthros import machines

    where = machines.default_on() if on is None else on
    return (local_client() if where == machines.LOCAL else
            local_k8s_client() if where == machines.LOCAL_K8S else job_client(settings()))


def _cache_homes(on):
    """Where entries of a call sent to on= live: the store its job writes to and,
    for a local one, your bucket as well, so they outlive this cluster."""
    from zenthros import cache as entries

    return _homes_of_client(_client_for(on), settings(), entries)


def _resume_ref(client, source, note):
    """What this call starts from: the job source's last checkpoint, when that job
    ended and did this same call. note.why says what it does, and why it starts
    over if it does. A checkpoint made with other code or other inputs is not used."""
    from zenthros import cache as entries

    try:
        obj = client.job_object(source)
    except errors.JobNotFound:
        note.why = f"{source} is gone: it starts over"
        return None
    theirs = _note_of_job(obj)
    if theirs is None:
        note.why = f"{source} cannot be told to be the same call: it starts over"
    elif theirs.key != note.key:
        note.why = f"{', '.join(entries.compare(theirs.parts, note.parts, note)) or 'its key changed'}: it starts over"
    else:
        ref, note.why = _cursor_ref(client, source, obj)
        return ref
    return None


def _cursor_ref(client, source, obj):
    """(what a new job names to start from an ended job's checkpoint, or None; in words)."""
    if (obj.get("status") or {}).get("phase") not in TERMINAL_PHASES:
        return None, f"{source} has not ended: it starts over"
    cursor = client.cursor_of(source) or {}
    if not cursor.get("uri"):
        return None, f"{source} has no confirmed checkpoint: it starts over"
    return S.ResumeFrom(job=source, uid=obj["metadata"]["uid"]), f"from checkpoint {cursor.get('seq')} of {source}"


def _homes_of_client(client, current, entries):
    if client.transport != "local" and carries_itself(current, client):
        # With no bucket, the cache keeps here what this computer checked.
        here = local_client()
        return [entries.Home(here.store, blob_store(here.store, current, here))]
    root = _store(current, client)
    homes = [entries.Home(root, blob_store(root, current, client))]
    if client.transport == "local" and current.store:
        homes.append(entries.Home(current.store, blob_store(current.store, current), shared=True))
    return homes


def _consult_cache(fn, args, kwargs, options, on, mode, deps, contracts, say=True):
    """(the call's Note or None, a CachedCall when it need not run). What it reads
    that cannot be told apart runs it, and says so."""
    from zenthros import cache as entries
    from zenthros.core.env import JobEnv

    if JobEnv.current().job_id:
        if mode:
            raise errors.ValidationError("cache= is for your computer: the key that signs entries never goes to a job")
        return None, None
    plain = options.get("env") if isinstance(options.get("env"), dict) else None
    environment = _environment(options.get("requirements"), options.get("packages"), options.get("tools"))
    try:
        note = entries.note_for(fn, args, kwargs, code_dir=options.get("code_dir"), image=_cache_image(options, on),
                                env_key=envs.key(environment) if environment else "", variables=plain,
                                secret_names=list(options.get("secrets") or ()), gpu=options.get("gpu"), deps=deps,
                                contracts=contracts, mode=mode)
    except entries.CannotKey as err:
        print(f"zenthros: {_fn_name(fn)} runs without the cache: {err}", file=sys.stderr)
        return None, None
    if not mode:
        return note, None
    note.homes = _cache_homes(on)
    found = entries.explain(note)
    if found is not None:
        if say:  # a plan writes nothing
            entries.used(found, note)
        return note, entries.CachedCall(found, note)
    if say:
        print(f"zenthros: {_fn_name(fn)} runs: {note.why}", file=sys.stderr)
    return note, None


def map(fn, items, _shared=None, _annotations=None, on=None, _where=None, **options):  # noqa: A001 - zenthros.map
    """fn(item) for each item, each on a machine of its own: one job with a
    task per item. Returns a RemoteCall whose result() is the list of
    results, in order. on= and the options are run()'s.

    Inside a Zenthros job, map spawns the job's children instead (they run
    where the job runs, and its replacement maps onto the same children):
    fn is then an importable function or 'module:function', and only name=
    and env= apply."""
    from zenthros.core.env import JobEnv

    job = JobEnv.current()
    if job.job_id and job.endpoint and _where is None:
        extra = set(options) - {"name", "env"} | ({"on"} if on is not None else set())
        if extra:
            raise errors.ValidationError(
                f"inside a job, map's children run where the job runs; {sorted(extra)} do not apply")
        from zenthros import fanout

        return fanout.map(fn, items, name=options.get("name"), env=options.get("env"))
    items = list(items)
    if not items:
        raise errors.ValidationError("map needs at least one item")
    if not callable(fn):
        raise errors.ValidationError(f"map takes a function or a Task class, not {type(fn).__name__}")
    unknown = set(options) - set(_OPTIONS) - {"name", "retries", "retry_on"}
    if unknown:
        raise errors.ValidationError(f"map: unknown options {sorted(unknown)}")
    if _where is None:
        return _placed_call(on, options, False, lambda where, placed: map(
            fn, items, _shared=_shared, _annotations=_annotations, _where=where, **placed),
            python=bundle.python_version())
    retries, retry_on = options.pop("retries", 0), options.pop("retry_on", ())
    name = options.pop("name", None) or _job_name(fn)
    body = {"fn": fn, "items": items, **_retry(retries, retry_on)}
    if _shared is not None:
        # f.map: each dict item is fn's keyword arguments over these.
        from zenthros.remote import spread

        spread.check(items, _shared)
        body["shared"] = dict(_shared)
    return _submit(MAP_KIND, name, fn, body=body, count=len(items), upstream=(items,), annotations=_annotations,
                   **{**_OPTIONS, **_WHERE, **options, **_where})


def serve(target, *args, port=None, ports=None, retries=2, on=None, keep=False, stop_after=None, _where=None,
          **options):
    """Run target as a Service: it runs until cancelled, and .url() says
    where it answers. Returns a RemoteCall at once.

        llm = zenthros.serve("vllm serve Qwen/Qwen3-8B --port 8000", on="gpu-box", port=8000)
        client = OpenAI(base_url=llm.url() + "/v1")
        ...
        llm.cancel()

    target is run()'s: a command, or a function that serves (uvicorn.run).
    port=8000 publishes one port, named "http"; ports={"api": 8000,
    "metrics": (9090, "TCP")} or [8000, 9090] several. The machine must
    publish them. A process that exits is run
    again, up to retries times; a lost machine is replaced (resilient). Each
    run publishes its own URL: read it again with .url() after a restart.

    on= is where, as run's: a machine, its name, or a template. A machine
    made for it ("local") goes when it is cancelled, unless keep=True.

    stop_after="4h": the controller stops it (Cancelled, STOP_AFTER) once it
    has served that long, with this notebook closed or not;
    zenthros.services() lists what is serving now.

    The URL may be public: protect the service (an API key from secrets=)."""
    service = _service(port, ports)
    service.stop_after_seconds = seconds(stop_after, "stop_after")
    if options.get("timeout") is not None:
        raise errors.ValidationError("a Service runs until it is cancelled: timeout does not apply")
    if options.get("retry_on"):
        raise errors.ValidationError("a Service's retries= re-runs the process; retry_on does not apply")
    unknown = set(options) - set(_OPTIONS) - {"name"}
    if unknown:
        raise errors.ValidationError(f"serve: unknown options {sorted(unknown)}")
    retries = int(retries or 0)
    if retries < 0:
        raise errors.ValidationError("retries must be 0 or more")
    if _where is None:
        return _placed_call(on, {**_OPTIONS, **options}, keep,
                            lambda where, placed: serve(target, *args, port=port, ports=ports, retries=retries,
                                                        stop_after=stop_after, _where=where, **placed),
                            python=_pickled(target), ports=[p.port for p in service.ports])
    name = options.pop("name", None)
    opts = {**_OPTIONS, **_WHERE, **options, **_where}
    if not opts["secrets"]:
        print(f"zenthros: serving {', '.join(f'{p.name}={p.port}' for p in service.ports)} with no secrets=; "
              "its URL may be public, so the service should check a key of its own", file=sys.stderr)
    if isinstance(target, (str, list, tuple)):
        if args:
            raise errors.ValidationError("a command takes its arguments inside it: serve(['vllm', 'serve', 'm'])")
        argv = ["sh", "-c", target] if isinstance(target, str) else [str(part) for part in target]
        if not argv or not argv[-1]:
            raise errors.ValidationError("serve: the command is empty")
        return _submit(COMMAND_KIND, name or _job_name(argv), None, argv=argv, backoff=retries, service=service,
                       **opts)
    if not callable(target):
        raise errors.ValidationError(f"serve takes a function or a command, not {type(target).__name__}")
    body = {"fn": target, "args": args, "kwargs": {}, "retries": 0, "retry_on": ()}
    return _submit(CALL_KIND, name or _job_name(target), target, body=body, backoff=retries, service=service,
                   upstream=(args, {}), **opts)


def _service(port, ports):
    """The ports a Service publishes, as the spec's Service."""
    if (port is None) == (ports is None):
        raise errors.ValidationError("serve needs port=N or ports=..., one of them")
    if port is not None:
        ports = {"http": port}
    if isinstance(ports, int) or isinstance(ports, S.ServicePort):
        ports = [ports]
    items = list(ports.items()) if isinstance(ports, dict) else [(None, p) for p in ports]
    out = []
    for name, value in items:
        if isinstance(value, S.ServicePort):
            out.append(value)
            continue
        number, protocol = (value if isinstance(value, (tuple, list)) else (value, "HTTP"))
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise errors.ValidationError(f"port {value!r}: a number, or (number, 'HTTP'|'TCP')") from None
        if not 1 <= number <= 65535:
            raise errors.ValidationError(f"port {number} is not between 1 and 65535")
        protocol = str(protocol).upper()
        if protocol not in ("HTTP", "TCP"):
            raise errors.ValidationError(f"port {number}: protocol {protocol!r} is HTTP or TCP")
        out.append(S.ServicePort(name=str(name) if name else f"port-{number}", port=number, protocol=protocol))
    if not out:
        raise errors.ValidationError("serve: no port")
    for p in out:
        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", p.name or ""):
            raise errors.ValidationError(f"port name {p.name!r}: lowercase letters, digits and '-'")
    if len({p.port for p in out}) != len(out) or len({p.name for p in out}) != len(out):
        raise errors.ValidationError("serve: each port and each name once")
    return S.Service(ports=out)


# run()'s machine, environment and failure options, with their defaults: map takes the same.
_OPTIONS = dict(cpu=None, memory=None, gpu=None, instance_type="", spot=None, image="",
                python=None, code_dir=".", env=None, secrets=None, timeout=None, after=(), resilient=True,
                labels=None, requirements=None, packages=None, tools=(), keep_for=None)
# Where a call runs, as its spec says it: only on= sets these (machines.place).
_WHERE = dict(pool="", backend="", acquire=None, client=None)


def gather(*calls, return_exceptions=False):
    """The results of calls, in order: gather(a, b) or gather([a, b]). The
    first failure raises, unless return_exceptions puts it in its place."""
    if len(calls) == 1 and isinstance(calls[0], (list, tuple)):
        calls = tuple(calls[0])
    out = []
    for call in calls:
        try:
            out.append(call.result())
        except Exception as exc:
            if not return_exceptions:
                raise
            out.append(exc)
    return out


def get(name):
    """The call named name, from any process that reaches its job: its
    status, logs and result after the one that started it is gone.

    The call's URI and digest come from the job's spec, which only an editor
    of the job can change; a call in the store with other bytes is never
    unpickled (audit 2026-10-03 SEC-4)."""
    current = settings()
    # A job of this project is here, whatever else this computer is logged in to.
    client = local_client_of(name) or job_client(current)
    try:
        obj = client.job_object(name)
    except errors.JobNotFound as exc:
        spread = _get_map(client, name)
        if spread is not None:
            return spread
        raise errors.JobNotFound(
            f"no job named {name!r} (never created, or collected after its TTL); its call is read only "
            f"through the job, whose spec pins its digest ({exc})") from None
    template = (((obj.get("spec") or {}).get("workload") or {}).get("template")) or {}
    command = [str(part) for part in template.get("command") or []]
    note = _note_of_job(obj)
    env = {e.get("name"): e.get("value", "") for e in template.get("env") or [] if isinstance(e, dict)}
    went_in_it = any(carried.carries(part) for part in template.get("command") or [])
    prefix = ((template.get("artifacts") or {}).get("output") or
              f"{carried.ROOT if went_in_it else _store(current, client)}/{name}").rstrip("/")
    submitted = seconds_at((obj.get("metadata") or {}).get("creationTimestamp"))
    if len(command) < 5 or command[1:3] != ["-m", "zenthros.remote"] or command[3] not in ("exec", "map"):
        return RemoteCall(name, prefix, JobHandle(name, client), client, blob_store(prefix, current, client), submitted,
                          kind=COMMAND_KIND)
    call_uri, expected = command[4], env.get(CALL_SHA256_ENV, "")
    store = blob_store(call_uri, current, client)
    try:
        raw = store.get(call_uri)
    except FileNotFoundError:
        raise errors.ValidationError(f"job {name!r} runs {call_uri}, which is not in the store") from None
    if not expected or hashlib.sha256(raw).hexdigest() != expected:
        raise RemoteError(f"{call_uri} is not the call job {name!r} was submitted with (its SHA-256 differs from "
                          f"the spec's {CALL_SHA256_ENV}); refusing to unpickle it", job=name)
    envelope = pickle.loads(raw)
    call = RemoteCall(name, call_uri.rsplit("/", 1)[0], JobHandle(name, client), client, store,
                      envelope.get("submitted_at", submitted), kind=envelope.get("kind", CALL_KIND),
                      count=envelope.get("count", 0), call_uri=call_uri)
    call._envelope = envelope
    call.cache_note = note
    call.cache_key = note.key if note is not None else ""
    return call


def _note_of_job(obj):
    """A job sent with cache=, seen again here: what enters its result."""
    from zenthros.core.env import JobEnv

    if JobEnv.current().job_id:
        return None
    from zenthros import cache as entries

    return entries.note_of_job(obj)


_LABEL_VALUE = re.compile(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?")


def _get_map(client, name):
    """The spread map named name (f.map(on=[...], name=)), from its jobs'
    labels and annotations: its configs, in their order, are read back from
    each job's call (whose digest get() checked). None when no job has it."""
    from zenthros.remote import spread

    if not _LABEL_VALUE.fullmatch(name) or not hasattr(client, "list_jobs"):
        return None
    # Its jobs, not their children: a job of several tasks spreads on its
    # machine into children that inherit its labels but not these annotations.
    found = [o for o in client.list_jobs(f"{MAP_GROUP}={name}")
             if MAP_INDICES in ((o.get("metadata") or {}).get("annotations") or {})]
    if not found:
        return None
    parts, size = [], 0
    # In the order of destinations: <name>-<k>, by k (the API lists by name: -10 before -2).
    for obj in sorted(found, key=lambda o: int(o["metadata"]["name"].rsplit("-", 1)[-1] or 0)):
        tags = obj["metadata"].get("annotations") or {}
        indices = [int(i) for i in tags.get(MAP_INDICES, "").split(",") if i.strip()]
        size = max(size, int(tags.get(MAP_SIZE) or 0), max(indices, default=-1) + 1)
        parts.append((tags.get(MAP_ON, ""), indices, get(obj["metadata"]["name"])))
    configs = [None] * size
    for _, indices, call in parts:
        try:
            items = pickle.loads(call._envelope["call"])["items"]
        except Exception:  # noqa: BLE001 - its code is not importable here: the configs stay unknown
            continue
        for index, item in zip(indices, items):
            configs[index] = item
    return spread.MapCalls(configs, parts, name=name)


def _retry(retries, retry_on):
    retry_on = tuple(retry_on) if isinstance(retry_on, (list, tuple)) else (retry_on,) if retry_on else ()
    for kind in retry_on:
        if not (isinstance(kind, type) and issubclass(kind, BaseException)):
            raise errors.ValidationError(f"retry_on takes exception classes, not {kind!r}")
    retries = int(retries or 0)
    if retries < 0:
        raise errors.ValidationError("retries must be 0 or more")
    if retries and not retry_on:
        raise errors.ValidationError(
            "retries retries the exceptions in retry_on: name them (retry_on=(ConnectionError,)); "
            "a bug is not retried, and a lost machine is moved by resilient=True")
    return {"retries": retries, "retry_on": retry_on}


def _store_or_carried(current, client):
    """Where a call keeps what it needs: its store, or with none, the job itself."""
    if carries_itself(current, client):
        return carried.ROOT
    return _store(current, client)


def carries_itself(current, client):
    """A job with no store it can reach: no bucket, or a cluster that shares no folder."""
    return not client.sees_store() if client.transport == "local" else not current.store and not client.store


def _calls_in_arguments(upstream):
    found = []
    _calls_in(upstream, found)
    return found


def _store(current, client):
    if client.transport == "local":
        # Local keeps its own, whatever bucket remote calls use.
        return client.store
    if current.store:
        return current.store
    # Through zenthros-api the Workspace has a store of its own.
    store = client.store
    if store:
        return store.rstrip("/")
    raise errors.ValidationError(
        "this hub keeps none of your data: give it your bucket once, "
        "`zenthros store s3://your-bucket/zenthros` (zenthros.set_store)")


def _calls_in(value, found):
    from zenthros.remote.spread import MapCalls

    if isinstance(value, RemoteCall):
        found.append(value)
    elif isinstance(value, MapCalls):
        # Its jobs' results are not one value the machine can read; the
        # whole object would travel instead, pickled.
        raise errors.ValidationError(
            f"a spread map ({value.name or 'f.map(on=[...])'}) is an argument: pass its values, calls.result(), "
            "or wait for it with after=[calls]")
    elif isinstance(value, dict):
        for item in value.values():
            _calls_in(item, found)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            _calls_in(item, found)
    return found


def _depends_on(after, upstream):
    names = []
    from zenthros.remote.spread import MapCalls

    waited = [part for item in (after or ()) if not getattr(item, "cached", False)
              for part in (item.calls if isinstance(item, MapCalls) else [item])]
    for item in [*waited, *_calls_in(upstream, [])]:
        if isinstance(item, RemoteCall) and item.service:
            raise errors.ValidationError(
                f"{item.name} is a Service: it never succeeds, so nothing can wait for it; pass its .url()")
        label = item.name if isinstance(item, RemoteCall) else str(item)
        if label and label not in names:
            names.append(label)
    return names


def _submit(kind, name, fn, body=None, argv=None, count=0, backoff=0, upstream=(), service=None, *, pool, backend, cpu,
            memory, gpu, instance_type, spot, image, python, code_dir, env, secrets, timeout, after,
            resilient, labels, requirements=None, packages=None, tools=(), acquire=None, keep_for=None,
            annotations=None, client=None, snapshot=False, note=None, deps=None, contracts=None, resume_from=None,
            same=None):
    if count > MAX_TASKS:
        raise errors.ValidationError(f"{name}: {count} tasks; one job runs at most {MAX_TASKS}: split the items "
                                     "into several maps")
    keep_for = DEFAULT_KEEP_FOR if keep_for is None else keep_for
    gc_policy = _gc_policy(keep_for)
    same = same or {}
    environment = same.get("environment") or _environment(requirements, packages, tools)
    contracts = same.get("contracts") or contracts
    if environment and kind == COMMAND_KIND:
        raise errors.ValidationError("requirements=, packages= and tools= are a function's; a command runs in its "
                                     "image as it is")
    current = settings()
    # One client for the whole submit: each one asks the hub for the
    # Workspace again.
    client = client or job_client(current)
    root = _store_or_carried(current, client)
    prefix = f"{root}/{name}"
    no_bucket = root == carried.ROOT
    if no_bucket and _calls_in_arguments(upstream):
        raise errors.ValidationError(f"{name}: a call's result as an argument travels by a bucket, and this Workspace has "
                                     f"none: {carried.NEEDS_A_BUCKET}, or pass its value (.result())")
    local = client.transport == "local"
    if not pool and not backend and not local:
        raise errors.ValidationError("on= says where it runs: a name from zenthros.targets()")
    if local:
        if kind != COMMAND_KIND and body is not None:
            body = {**body, **_local_paths(body, client)}
        if deps:
            from zenthros import cache as entries

            client.need_paths(entries.dep_paths(deps))
    call_uri, stored, pinned = None, None, {}
    store = blob_store(prefix, current, client)
    code_notes = {}
    if snapshot:
        if same.get("snapshot"):
            stored_code = _rehomed(same["snapshot"], same["client"], store, root)
            code_notes = same["notes"]
        else:
            taken = code_snapshot.take(code_dir or ".")
            stored_code = taken.store(store, root)
            if taken.excluded:
                print(f"zenthros: {name}: left out of the snapshot, as credentials: {', '.join(taken.excluded)}", file=sys.stderr)
            code_notes = {code_snapshot.CODE_SHA256: stored_code["sha256"], code_snapshot.CODE_BASE: taken.commit,
                          code_snapshot.CODE_DIRTY: "true" if taken.dirty else "false"}
        pinned = {CALL_SHA256_ENV: stored_code["sha256"],
                  RUNNER_PACKAGES_ENV: " ".join(p for p in runner_packages(prefix) if not p.startswith("cloudpickle"))}
        command = [python or current.python, "-m", "zenthros.remote", "snapshot", stored_code["uri"], "--", *argv]
        # Only the SDK's core rides inline, for the runner to start.
        code = bundle.pack_code(None, ship_sdk=True) if current.ship_sdk else None
    elif kind == COMMAND_KIND:
        command = argv
        # The command's own zenthros, if any, is the image's; its files are
        # unpacked where it runs, so they ride inline.
        code = base64.b64decode(same["inline"]) if same.get("inline") else \
            None if same else bundle.pack_code(code_dir, ship_sdk=False) if code_dir else None
    else:
        pickled = bundle.dumps(fn, body)
        if len(pickled) > bundle.WARN_CALL_BYTES:
            print(f"zenthros: the call to {_fn_name(fn)} pickles to {len(pickled) >> 20} MiB; "
                  "pass large data by reference (an s3:// URI) instead", file=sys.stderr)
        # The project goes to the store first, by digest, and the call pins
        # it; only the SDK's core rides inline, for the runner to start (SDK-8).
        if same:
            project = same.get("project") and _rehomed(same["project"], same["client"], store, root)
        else:
            project = bundle.store_project(code_dir, store, root)
        if project:
            # So that cleaning the store keeps the code of a job still here.
            code_notes = {code_snapshot.CODE_SHA256: project["sha256"]}
        envelope = {"format": FORMAT, "kind": kind, "python": bundle.python_version(), "submitted_at": time.time(),
                    "count": count, "packages": bundle.loaded_packages(), "function": _fn_name(fn, qualified=True),
                    "call": pickled, "sys_path": same["sys_path"] if same else bundle.import_roots(code_dir), "code": project}
        if environment:
            envelope["environment"] = environment
        if contracts:
            envelope["contracts"] = contracts
        if note is not None and note.mode:
            envelope["outs"] = True
        stored = pickle.dumps(envelope, protocol=4)
        digest = hashlib.sha256(stored).hexdigest()
        # Named by its digest: a second run(name=...) refused for the taken
        # name leaves the live job's call as it was (SDK-6).
        call_uri = f"{prefix}/call-{digest[:16]}.pkl"
        # The runner unpickles the call only if it is these bytes: the digest
        # rides in the spec, which only an editor of the job can change, not
        # in the store, which anyone who can write there could.
        pinned = {CALL_SHA256_ENV: digest, RUNNER_PACKAGES_ENV: " ".join(runner_packages(prefix))}
        command = [python or current.python, "-m", "zenthros.remote", "map" if kind == MAP_KIND else "exec", call_uri]
        code = bundle.pack_code(None, ship_sdk=True) if current.ship_sdk else None
    if local:
        secrets, held = split_local_secrets(secrets)
        client.seal_at_start(name, held)
    sources = secret_sources(secrets)
    access_env, access_secrets = {}, []
    mine = {s.materialization.env_name for s in sources if s.materialization}
    if local or no_bucket:
        pass
    elif not current.store:
        # The Workspace's store, through zenthros-api: sealed to the backend
        # before the job exists, unless the caller brought credentials.
        access_env, access_secrets = client.store_access("" if mine & STORE_CREDENTIALS else backend or "")
    elif backend and client.transport == "zenthros-api" and not mine & STORE_CREDENTIALS and \
            _own_bucket(current.store, current, client):
        # Your own bucket: credentials for its prefix, minted here and
        # sealed to the machine, which the hub never sees.
        from zenthros import userstore

        access_env, access_secrets = userstore.machine_access(current.store, backend, current.workspace)
    if no_bucket:
        code = carried.pack(code, {**store.files, **({call_uri: stored} if call_uri else {})}, bundle.MAX_CODE_BYTES)
    template = S.Template(
        command=command,
        image=image or "",
        env=_env({**access_env, **(current.env or {})}, {**(env or {}), **pinned}, prefix),
        secret_sources=sources + access_secrets,
        artifacts=S.Artifacts(output="" if no_bucket else client.output_for(name) if local else prefix),
        source=S.Source(inline=base64.b64encode(code).decode("ascii")) if code else None,
        restart_policy=S.RestartPolicy(backoff_limit=int(backoff)) if backoff else None,
        distribution=S.Distribution(mode="Independent", count=count) if count > 1 else None,
        service=service,
    )
    annotations = {**(annotations or {}), **code_notes}
    labels = dict(labels or {})
    if note is not None:
        from zenthros import cache as entries

        labels[entries.KEY_LABEL] = note.label()
        annotations[entries.PARTS_ANNOTATION] = note.annotation(name)
    if kind != COMMAND_KIND and service is None:
        annotations[RESULT_COUNT] = str(max(count, 1) if kind == MAP_KIND else 1)
    if secrets and current.untrusted_host_secrets:
        # The controller refuses secrets on a host a third party runs
        # (Vast.ai) unless the job says so (audit 2026-10-03 SEC-6).
        annotations[UNTRUSTED_HOST_SECRETS] = "true"
        print(f"zenthros: {name}: its secrets may go to a machine a third party runs; "
              "that host's owner can read them", file=sys.stderr)
    if current.untrusted_host_spawn:
        # The controller refuses a spawn from a host a third party runs: its
        # owner can act as the job, and a spawn runs a command on its machines.
        annotations[UNTRUSTED_HOST_SPAWN] = "true"
    job = S.Job(
        name=name,
        labels={**(labels or {}), REMOTE_LABEL: "true"},
        annotations=annotations,
        spec=S.Spec(
            workspace=current.workspace,
            pool_ref=pool,
            target=S.Target(backends=[backend]) if backend else None,
            # The controller picks the runtime that runs the image on the pool's
            # machines: containerd on a VM, the container itself on a Container
            # pool, which refuses containerd. Only the controller knows the kind.
            compute=_compute(cpu, memory, gpu, instance_type, spot),
            # acquire=["Create"]: a new machine of the pool, for this job.
            capacity=S.Capacity(acquire=list(acquire)) if acquire else None,
            resilience=_resilience(resilient, pool),
            depends_on=_depends_on(after, upstream),
            resume_from=resume_from,
            active_deadline_seconds=seconds(timeout),
            gc_policy=gc_policy,
            workload=S.Workload(kind="Job", template=template),
        ),
    )
    if call_uri:
        # The call is stored before the job exists: a job never starts without it.
        store.put(call_uri, stored)
    submitted = time.time()
    handle = client.submit(job)
    call = RemoteCall(name, prefix, handle, client, store, submitted, kind=kind, count=count, call_uri=call_uri)
    call.cache_note = note
    call.cache_key = note.key if note is not None else ""
    call.why = note.why if note is not None else ""
    if resume_from is not None:
        call.action = RESUME
    call.code = code_notes.get(code_snapshot.CODE_SHA256, "")
    call.code_notes = code_notes
    if local:
        call.output_uri = client.output_for(name)
    call.service = [p.name for p in service.ports] if service else None
    call.runlog = runlog.open(name, current)
    if call.runlog is not None:
        call.runlog.started({
            "job": name, "kind": "service" if service else kind, "workspace": current.workspace,
            "function": getattr(fn, "__qualname__", "") or (" ".join(argv or []) if argv else ""),
            "args": runlog.cut((body or {}).get("args", ())), "kwargs": runlog.cut((body or {}).get("kwargs", {})),
            "items": runlog.cut((body or {}).get("items")) if body and "items" in body else None,
            "pool": pool, "backend": backend, "cpu": cpu, "memory": memory, "gpu": gpu,
            "instance_type": instance_type, "spot": spot, "image": image, "requirements": requirements,
            "packages": packages, "tools": list(tools or ()), "timeout": timeout, "keep_for": keep_for,
            "ports": call.service, "env": sorted(env or {}),
            "secrets": {str(k): getattr(v, "type", type(v).__name__) for k, v in (secrets or {}).items()},
            "submitted": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(submitted)),
        })
    return call


def _rehomed(stored, client, store, root):
    """Stored code ({uri, sha256}), in the store a new job reads."""
    uri = f"{root}/code/{stored['sha256']}.tar.gz"
    if uri != stored["uri"]:
        store.put(uri, blob_store(stored["uri"], settings(), client).get(stored["uri"]))
    return {"uri": uri, "sha256": stored["sha256"]}


def _options_of(spec, template):
    """What a job's spec says was asked, as run takes it."""
    compute = spec.get("compute") or {}
    gpu = compute.get("gpuModel") or compute.get("gpuCount")
    gc = spec.get("gcPolicy") or {}
    purchase = compute.get("purchaseOption")
    env = {e["name"]: e.get("value", "") for e in template.get("env") or [] if not e["name"].startswith(_OURS)}
    return {"cpu": compute.get("cpu"), "memory": compute.get("memory"), "gpu": gpu, "instance_type": compute.get("instanceType", ""),
            "spot": None if purchase is None else purchase == "Spot", "image": template.get("image") or None,
            "env": env or None, "resilient": (spec.get("resilience") or {}).get("onWorkerLost") != "Fail",
            "timeout": spec.get("activeDeadlineSeconds"), "keep_for": gc.get("ttlSecondsAfterFinished"),
            "after": spec.get("dependsOn") or ()}


_OURS = ("ZENTHROS_", "TASK_", "AWS_", "GOOGLE_")


def _rerun(call, on, resume, upload, name, changes):
    """call.rerun: its verified call, through run, for one way of sending a job."""
    from zenthros import cache as entries

    fresh = get(call.name)
    client = fresh._client
    obj = client.job_object(call.name)
    spec = obj["spec"]
    template = spec["workload"]["template"]
    if fresh.kind == MAP_KIND or template.get("service"):
        raise errors.ValidationError(f"{call.name}: rerun is for one call or command; send a map or a service again")
    sealed = sorted(source["materialization"]["envName"] for source in template.get("secretSources") or []
                    if (source.get("materialization") or {}).get("envName") and
                    source["materialization"]["envName"] not in STORE_CREDENTIALS)
    if sealed and "secrets" not in changes:
        raise errors.ValidationError(f"{call.name} used the secrets {', '.join(sealed)}, sealed for its machine: "
                                     "give them again, rerun(secrets={...})")
    place = on if on is not None else client.place_name if client.transport == "local" else \
        spec.get("poolRef") or (spec.get("target") or {}).get("backends", [""])[0]
    options = {**_options_of(spec, template), **changes}
    current = settings()
    ours = _store_or_carried(current, _client_for(place)) == _store_or_carried(current, client)
    theirs = entries.note_of_job(obj)
    image_now = _cache_image(options, place)
    same_image = options.get("image") == template.get("image") and (on is None or theirs is None or
                                                                    theirs.parts.get("image") == image_now)
    ref, why = None, ""
    if resume:
        if not ours:
            why = "its checkpoint is in a store this place does not use: it starts over"
        elif not same_image:
            why = "the image changed: it starts over"
        else:
            ref, why = _cursor_ref(client, call.name, obj)
    base = re.sub(r"-r[0-9a-f]{4}$", "", call.name)[:50]
    new_name = name or f"{base}-r{uuid.uuid4().hex[:4]}"
    same = {"client": client, "note": theirs if same_image else None}
    command = [str(part) for part in template.get("command") or []]
    if fresh.kind == COMMAND_KIND:
        snapshot = command[1:4] == ["-m", "zenthros.remote", "snapshot"]
        notes = {key: value for key, value in (obj["metadata"].get("annotations") or {}).items()
                 if key in (code_snapshot.CODE_SHA256, code_snapshot.CODE_BASE, code_snapshot.CODE_DIRTY)}
        if snapshot:
            same.update(snapshot={"uri": command[4], "sha256": notes[code_snapshot.CODE_SHA256]}, notes=notes)
        else:
            same["inline"] = client.source_of(call.name)
        started = run(command[6:] if snapshot else command, on=place, name=new_name, snapshot=snapshot, _same=same,
                      _resume=ref, **options)
    else:
        envelope = fresh._envelope
        body = pickle.loads(envelope["call"])
        same.update(project=envelope.get("code"), sys_path=envelope.get("sys_path"),
                    environment=envelope.get("environment"), contracts=envelope.get("contracts"))
        started = run(body["fn"], *body["args"], on=place, name=new_name, upload=upload, python=command[0],
                      retries=body.get("retries", 0), retry_on=body.get("retry_on", ()), _same=same, _resume=ref,
                      **options, **body["kwargs"])
    started.why = why
    return started


# keep_for's bound: a finished job is a Kubernetes object, kept within reason.
MAX_KEEP_FOR = 90 * 86400
# What a job keeps when it names nothing. The result stays readable the next
# day, from this or any other process.
DEFAULT_KEEP_FOR = "7d"
RESUME = "resume"


def _local_paths(body, client):
    """The paths a local call names as arguments: each goes to the container
    read-only, at its own absolute path, and the call gets that path. A
    credential is refused before anything exists."""
    from zenthros import ignore

    found = []

    def swap(value):
        if isinstance(value, str) and value and os.path.exists(value) and not value.startswith("-"):
            path = os.path.abspath(value)
            ignore.refuse_path(path)
            found.append(path)
            return path
        if isinstance(value, (list, tuple)):
            return type(value)(swap(item) for item in value)
        if isinstance(value, dict):
            return {key: swap(item) for key, item in value.items()}
        return value

    changed = {}
    if "args" in body:
        changed["args"], changed["kwargs"] = swap(tuple(body["args"])), swap(dict(body.get("kwargs") or {}))
    if "items" in body:
        changed["items"] = swap(list(body["items"]))
    client.need_paths(found)
    return changed


def _gc_policy(keep_for):
    """keep_for= as the job's GC policy: how long it stays once finished
    (the controller's default, 1 h, without it)."""
    kept = seconds(keep_for, "keep_for")
    if kept is None:
        return None
    if kept > MAX_KEEP_FOR:
        raise errors.ValidationError(f"keep_for {keep_for!r}: at most 90d")
    return S.GCPolicy(ttl_seconds_after_finished=kept)


def _error_text(err):
    """A failure as error.txt says it: its type and message first, then
    the machine's traceback."""
    if isinstance(err, RemoteError):
        text = f"{err.remote_type or 'RemoteError'}: {err}\n"
        if err.remote_traceback and err.remote_traceback.strip() not in str(err):
            # A command's message ends with its log; a function's carries
            # its traceback already.
            text += "\n" + err.remote_traceback.rstrip() + "\n"
        return text
    reason = getattr(err, "reason", "")
    return f"{type(err).__name__}{' ' + reason if reason else ''}: {err}\n"


UNTRUSTED_HOST_SECRETS = "zenthros.ai/secrets-on-untrusted-host"
UNTRUSTED_HOST_SPAWN = "zenthros.ai/spawn-on-untrusted-host"
STORE_CREDENTIALS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", GCS_KEY_ENV}


# A requirement line the machine could not follow: another file, an editable
# or local path. What it names is on this computer, not there.
_LOCAL_REQUIREMENT = re.compile(r"^(-r|-c|-e|--requirement|--constraint|--editable)\b|^(\.|/|~|file:)")
# A requirement that already says what it takes: a version, or a URL.
_SAYS_VERSION = re.compile(r"[=<>!~@]|://")
# A bare one: a name, maybe with extras.
_BARE = re.compile(r"^\s*([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)\s*(\[[^\]]*\])?\s*$")


def _pinned(line):
    """line at the version this computer has, when it names none: the
    function gets there what it ran with here. Named as the package names
    itself (Polars and polars are one line), its extras and its marker
    kept; a package not installed here, or installed from a URL or a folder
    (not an index), stays as written. The version is its public one: a local
    label (torch's +cpu, from the PyTorch index) names a build no other index
    has, and ==2.15.0 takes 2.15.0+cpu where that index is."""
    import importlib.metadata

    requirement, sep, marker = line.partition(";")
    bare = None if _SAYS_VERSION.search(requirement) else _BARE.match(requirement)
    if bare is None:
        return line
    try:
        dist = importlib.metadata.distribution(bare.group(1))
    except importlib.metadata.PackageNotFoundError:
        return line
    if dist.read_text("direct_url.json") is not None:
        return line
    return f"{dist.name}{bare.group(2) or ''}=={dist.version.split('+')[0]}" + (sep + marker if sep else "")


def _environment(requirements, packages, tools):
    """{"requirements", "tools"} for the runner (zenthros.remote.envs), or
    None when the function declares nothing."""
    from zenthros.remote import envs

    lines = []
    if requirements:
        path = os.path.abspath(os.fspath(requirements))
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
        except OSError as exc:
            raise errors.ValidationError(f"requirements={requirements!r}: {exc.strerror} ({path})") from None
        lines += [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]
    if isinstance(packages, str):
        packages = [packages]
    lines += [str(p).strip() for p in packages or () if str(p).strip()]
    for line in lines:
        if _LOCAL_REQUIREMENT.search(line):
            raise errors.ValidationError(
                f"{line!r} names something on this computer, which the machine does not have: list the packages "
                "themselves (a published version, or a git+https:// URL)")
    lines = [_pinned(line) for line in lines]
    tools = sorted(set(tools or ()))
    unknown = [t for t in tools if t not in envs.TOOLS]
    if unknown:
        raise errors.ValidationError(f"tools={unknown}: Zenthros provides {sorted(envs.TOOLS)}")
    if not lines and not tools:
        return None
    return {"requirements": "".join(line + "\n" for line in lines), "tools": tools}


def _env(configured, extra, prefix):
    # Checkpoints go under the call's prefix; the store is picked from this
    # variable, so it names the prefix itself.
    values = {"ZENTHROS_CHECKPOINT_PREFIX": prefix, **(configured or {}), **(extra or {})}
    return [S.EnvVar(name=str(k), value=str(v)) for k, v in sorted(values.items())]


def _compute(cpu, memory, gpu, instance_type, spot):
    from zenthros.machines import k8s_memory

    compute = S.Compute(cpu=str(cpu or ""), memory=k8s_memory(memory) if memory else "", instance_type=instance_type or "")
    if isinstance(gpu, str) and gpu:
        compute.gpu_model, compute.gpu_count = gpu, 1
    elif gpu:
        compute.gpu_count = int(gpu)
    if spot is not None:
        compute.purchase_option = "Spot" if spot else "OnDemand"
    return compute if S.to_plain(compute) else None


def _resilience(resilient, pool):
    """A lost machine (Spot reclaim, termination, a dead agent) moves the call
    to another one, up to the controller's limit, where zenthros.Task and
    zenthros.phase resume from the last acked checkpoint. Off, the call fails
    with the machine."""
    if not resilient:
        return S.Resilience(on_worker_lost="Fail")
    return S.Resilience(on_worker_lost="Reschedule", replace=["Reuse", "Create"] if pool else ["Backends"])


def _fn_name(fn, qualified=False):
    """A function's name, or a callable object's class name (a zenthros.Task)."""
    name = getattr(fn, "__qualname__" if qualified else "__name__", None)
    return name or type(fn).__qualname__


def _job_name(target):
    if isinstance(target, list):
        words = [part.rsplit("/", 1)[-1] for part in target if part and not part.startswith("-")]
        base = next((w.rsplit(".", 1)[0] for w in words if w.endswith((".py", ".sh"))), words[0] if words else "cmd")
    else:
        base = _fn_name(target)
    base = "".join(c if c.isalnum() else "-" for c in base.lower()).strip("-")
    return f"zr-{base[:30].strip('-') or 'fn'}-{uuid.uuid4().hex[:6]}"


@dataclass(frozen=True)
class Cost:
    """What a call cost, as zenthros-api counts it: its running time times
    the price the catalog last observed for its machine's offer. cost is None
    where there is no such price (your own machine, a cluster's pod), and
    basis says what was counted or why nothing was."""

    job: str
    cost: Optional[float] = None
    currency: str = ""
    seconds: float = 0.0
    running: bool = False
    hourly_rate: float = 0.0
    instance_type: str = ""
    market: str = ""
    region: str = ""
    zone: str = ""
    backend: str = ""
    observed_at: str = ""
    basis: str = ""

    @classmethod
    def from_api(cls, view):
        return cls(job=view.get("job", ""), cost=view.get("cost"), currency=view.get("currency", ""),
                   seconds=float(view.get("seconds") or 0), running=bool(view.get("running")),
                   hourly_rate=float(view.get("hourlyRate") or 0), instance_type=view.get("instanceType", ""),
                   market=view.get("market", ""), region=view.get("region", ""), zone=view.get("zone", ""),
                   backend=view.get("backend", ""), observed_at=view.get("observedAt", ""), basis=view.get("basis", ""))

    def __repr__(self):
        minutes, secs = divmod(int(self.seconds), 60)
        took = f"{minutes}m {secs}s" + (" so far" if self.running else "")
        if self.cost is None:
            return f"Cost({self.job}: no price, {took}; {self.basis})"
        money = (lambda v: f"${v:.4f}") if self.currency in ("", "USD") else (lambda v: f"{v:.4f} {self.currency}")
        where = " ".join(p for p in (self.instance_type, self.market) if p)
        rate = f"${self.hourly_rate:g}/h" if self.currency in ("", "USD") else f"{self.hourly_rate:g} {self.currency}/h"
        return (f"Cost({self.job}: {money(self.cost)} for {took} on {where} ({self.zone or self.region}) at {rate}, "
                f"observed {self.observed_at}; {self.basis})")


class RemoteCall:
    """One call: its job, its result, its logs and reports."""

    def __init__(self, name, prefix, handle, client, blob_store, submitted_at=None, kind=CALL_KIND, count=0,
                 call_uri=None):
        self.name = name
        self.prefix = prefix
        self.call_uri = call_uri
        # The stored call, when get() read it back.
        self._envelope = None
        self.handle = handle
        self.kind = kind
        self.count = count
        self._client = client
        self._store = blob_store
        self.timings = {"submitted": submitted_at} if submitted_at else {}
        self.machine = {}
        self.attempts = None
        self.reschedules = 0
        self._outcome = None
        # The records _collect read and verified, for outcomes().
        self._records = None
        # A Service's port names, in the order declared; None for a call.
        self.service = None
        # The machine made for this call (on="local"), and what removes it
        # once the result is collected.
        self.made = None
        self._release = None
        self._interrupted = False
        # Its local record (zenthros.remote.runlog), or None.
        self.runlog = None
        # The name on= sent it to (the default, when on= was not given).
        self.where = ""
        # What the cache knows of it (zenthros.cache.Note), or None.
        self.cache_note = None
        self.cache_key = ""
        self.why = ""
        # uri -> (SHA-256, size) of what _read_verified checked.
        self._verified = {}

    cached = False
    action = "run"

    def __repr__(self):
        return f"RemoteCall({self.name!r}, kind={'service' if self.service else self.kind!r})"

    @property
    def log_dir(self):
        """The folder of this call's local record (call.json, logs.txt,
        error.txt), or None when records are off."""
        record = self.runlog
        return record.dir if record is not None and not record.off else None

    def __reduce__(self):
        if self.service:
            raise TypeError(f"{self.name} is a Service and has no result to pass; pass its .url()")
        # As another call's argument, a call travels as a reference to its result.
        return Upstream, (self.name, self.prefix, self.kind, self.count)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        """Leaving a `with` block cancels the job (a Service stops serving)
        and removes the machine made for it, whether or not it had ended.
        When the block raised, that error is the one raised: a failure to
        cancel is said, not raised over it."""
        try:
            if self.handle.job_status().phase not in TERMINAL_PHASES:
                self.cancel()
        except errors.JobNotFound:
            pass
        except Exception as err:
            if exc is None:
                raise
            print(f"zenthros: could not cancel {self.name}: {err}", file=sys.stderr)
        finally:
            self.release()
        return False

    def endpoints(self):
        """Where the Service answers now, read from its status
        (handle.Endpoint: name, url, scope...). Empty until every port it
        declares accepts connections, and after it stops."""
        return list(self.handle.job_status().endpoints)

    def url(self, name=None, timeout=600, poll_seconds=2.0, health=None):
        """The URL of the Service's port name (default: the first declared),
        once every port it declares answers. Read again each time: a re-run
        or a replacement machine publishes a new one.

        Published means the port accepts connections on its machine. A
        provider's front can lag it: RunPod's proxy answers 404 for some
        seconds after (measured 2026-10-04), and its 404 looks like the app's.
        health="/health" also waits, from here, for a 2xx on that path of the
        URL: the app's own answer, through whatever is in front of it.

        Past timeout, raises errors.WaitTimeout with why there is none yet;
        a Service that ended raises errors.JobFailed or JobCancelled."""
        if not self.service:
            raise TypeError(f"{self.name} is not a Service: it publishes no URL")
        wanted = name or self.service[0]
        if wanted not in self.service:
            raise errors.ValidationError(f"{self.name} has no port named {wanted!r}; it has {self.service}")
        if health is not None and not str(health).startswith("/"):
            raise errors.ValidationError(f"health is a path of the URL, like '/health'; got {health!r}")
        deadline = time.monotonic() + (timeout if timeout is not None else float("inf"))
        status, last_error, probed = None, None, None
        while True:
            try:
                status = self.handle.job_status()
                last_error = None
            except errors.TransientError as exc:
                last_error = exc
            if status is not None:
                if status.phase in TERMINAL_PHASES:
                    raise self._ended(status)
                ready = status.service_ready()
                if ready and ready.get("status") == "True":
                    found = next((e for e in status.endpoints if e.name == wanted), None)
                    if found is not None:
                        self.timings.setdefault("ready", time.time())
                        if health is None:
                            return found.url
                        code, probed = _http_status(found.url.rstrip("/") + health)
                        if code is not None and 200 <= code < 300:
                            self.timings.setdefault("reachable", time.time())
                            return found.url
                        probed = f"{found.url.rstrip('/')}{health} answered {probed}"
            if time.monotonic() >= deadline:
                why = probed if probed and status is not None and status.phase not in TERMINAL_PHASES and \
                    (status.service_ready() or {}).get("status") == "True" else _why_not(status, last_error)
                raise errors.WaitTimeout(f"{self.name}: no endpoint after {timeout}s: {why}", status=status)
            time.sleep(poll_seconds)

    def _ended(self, status):
        if status.phase == CANCELLED:
            return errors.JobCancelled(f"{self.name} was cancelled")
        if status.phase == FAILED:
            return errors.JobFailed(status.message or f"{self.name} failed", reasons=status.reasons, status=status)
        return errors.JobFailed(f"{self.name} ended {status.phase}: a Service is not meant to end", status=status)

    @property
    def output(self):
        """Where the call's data goes: $ZENTHROS_DATA_OUTPUT on the machine."""
        return getattr(self, "output_uri", "") or self.prefix

    def status(self):
        return self.handle.status()

    def logs(self, tail=None):
        """The job's log; tail=N, its last N lines."""
        return self.handle.logs(tail=tail)

    def reports(self):
        """What the code published with zenthros.report, oldest first."""
        return self.handle.reports()

    def replace(self, new, timeout=600, poll_seconds=2.0, health=None, keep_failed=False):
        """Swap this Service for new (another serve(), already started):
        wait until new answers (url(health=)), then cancel this one, and
        return new. If new ends or does not answer within timeout, this one
        keeps serving, new is cancelled (unless keep_failed, to look at it)
        and the error is raised. Both run side by side meanwhile, and a
        service holds its machine alone: new goes on another machine (one
        on this one's is refused at once).

            api = api.replace(servir_v2.serve(on=otra_maquina, port=8000), health="/health")
        """
        if not self.service:
            raise TypeError(f"{self.name} is not a Service: nothing to replace")
        if not isinstance(new, RemoteCall) or not new.service or new is self or new.name == self.name:
            raise errors.ValidationError(f"{self.name}.replace(new): new is another serve()'s call, already started")
        try:
            shared = set(_target_backends(self)) & set(_target_backends(new))
            if shared:
                # A Service holds its machine alone: the controller queues
                # another one behind it, which would wait for this one forever.
                raise errors.ValidationError(
                    f"{new.name} is on {sorted(shared)[0]}, where {self.name} serves: a service holds its machine "
                    "alone, so the new one would wait for the old one to stop. Serve it on another machine")
            new.url(timeout=timeout, poll_seconds=poll_seconds, health=health)
        except BaseException:
            if not keep_failed:
                try:
                    new.cancel()
                except Exception as err:  # noqa: BLE001 - the error to raise is the first
                    print(f"zenthros: could not cancel {new.name}: {err}", file=sys.stderr)
            raise
        self.cancel()
        return new

    def cancel(self):
        """Stop the job. The machine made for it (on= a template or "local",
        keep=False) goes too: a cancelled call has no result to wait for, and
        that machine bills until it is gone (audit 2026-10-16 AUD-041)."""
        try:
            return self.handle.cancel()
        finally:
            self.release()

    def delete(self):
        """Delete the job; the machine made for it goes too."""
        try:
            return self.handle.delete()
        finally:
            self.release()

    def info(self):
        """The job as the controller holds it, and what the machine said."""
        status = self.handle.job_status()
        return {"name": self.name, "kind": self.kind, "output": self.output, "phase": status.phase,
                "reason": status.reason, "message": status.message, "restarts": status.restarts,
                "reschedules": status.worker_lost_retries, "attempts": self.attempts,
                "cleanup_pending": status.cleanup_pending,
                "machine": dict(self.machine), "timings": dict(self.timings),
                "endpoints": [e.url for e in status.endpoints] if self.service else None}

    def result(self, timeout=None, stream_logs=True, cancel_on_interrupt=True):
        """Wait for the job and return the result: a function's value, a
        map's list, a command's 0. Only a job that ended Succeeded returns;
        see RemoteError and errors.JobFailed for the rest. Ctrl-C while it
        waits cancels the job and releases the machine made for it, unless
        cancel_on_interrupt=False leaves it running."""
        if self.service:
            raise TypeError(f"{self.name} is a Service: it runs until cancelled and has no result; "
                            "use .url(), .logs() and .cancel()")
        if self._outcome is None:
            follower = progress = None
            if stream_logs and callable(getattr(self._client, "follow_logs", None)):
                follower = _LogFollower(self._client, self.name, tee=self.runlog.line if self.runlog else None)
                follower.start()
            if stream_logs:
                progress = _ReportFollower(self)
                progress.start()
            failure = None
            try:
                self.handle.wait(timeout=timeout, on_reschedule=self._rescheduled)
            except KeyboardInterrupt:
                self._on_interrupt(cancel_on_interrupt)
                raise
            except (errors.JobFailed, errors.JobCancelled) as exc:
                failure = exc
            except errors.JobNotFound as exc:
                raise self._collected(exc) from None
            finally:
                if follower is not None:
                    follower.stop()
                if progress is not None:
                    progress.stop()
            self.timings["finished"] = time.time()
            self._outcome = self._collect(failure)
            self._record_end()
            self.release()
            if stream_logs and self._outcome[0]:
                self._receipt()
        ok, value = self._outcome
        if ok:
            return value
        raise value

    def _on_interrupt(self, cancel):
        """Ctrl-C stopped the wait: cancel the job and release its machine,
        once, or say how to come back to it. Never raises: the interrupt
        is what the cell must see."""
        if self._interrupted:
            return
        self._interrupted = True
        if self.runlog is not None:
            self.runlog.ended("Interrupted", "", cancelled=bool(cancel))
        if not cancel:
            print(f"zenthros: {self.name} keeps running; zenthros.get(\"{self.name}\") comes back to it",
                  file=sys.stderr)
            return
        try:
            self.cancel()
            said = f"zenthros: interrupted; {self.name} cancelled"
        except Exception as exc:  # noqa: BLE001 - the interrupt is what matters
            said = f"zenthros: interrupted; {self.name} could not be cancelled ({exc}): call .cancel() again"
        try:
            self.release()
        except Exception as exc:  # noqa: BLE001
            said += f"; its machine could not be released ({exc})"
        print(said, file=sys.stderr)

    def _record_end(self):
        """How it ended, in its local record: a failure's error and the end
        of its log (when the log was not followed), and its reports."""
        record = self.runlog
        if record is None or record.off:
            return
        ok, value = self._outcome
        status, error = "Succeeded", ""
        if not ok:
            status = "Cancelled" if isinstance(value, errors.JobCancelled) else "Failed"
            error = _error_text(value)
            if not record.logged():
                try:
                    tail = self.logs(tail=500) or ""
                except (errors.ZenthrosError, OSError):  # a record, not a check
                    tail = ""
                if tail:
                    record.line(tail if tail.endswith("\n") else tail + "\n")
        try:
            reports = user_reports(self.reports())
        except (errors.ZenthrosError, OSError):  # a record, not a check
            reports = []
        if reports:
            # One JSON object a line, what the code published: an assistant
            # reads it as data (zenthros' own result digests are left out).
            record.reports([{"producer_id": r.producer_id, "sequence": r.sequence, "produced_at": r.produced_at,
                             "final": r.final, "payload": r.payload} for r in reports])
        started = self.timings.get("submitted")
        record.ended(status, error, seconds=round(self.timings["finished"] - started, 1) if started else None,
                     machine=self.machine or None)

    def _receipt(self, out=None):
        """One line once the call succeeded: how long it took, where, and
        what it cost when its machine has a price (usability audit U-14). A
        receipt never fails the call."""
        started, ended = self.timings.get("submitted"), self.timings.get("finished")
        line = "✓ done" + (f" in {_took(ended - started)}" if started and ended else "")
        line += f" on {self.where}" if self.where else ""
        if self.kind == MAP_KIND and self.count > 1:
            line += f" ({self.count} tasks)"
        try:
            cost = self.cost()
        except errors.ZenthrosError:  # no price is not a failure
            cost = None
        if cost is not None and cost.cost is not None:
            # Cents of a dollar matter on a short call: four places under one.
            amount = f"{cost.cost:.4f}" if cost.cost < 1 else f"{cost.cost:.2f}"
            line += f" · ${amount}" if cost.currency in ("", "USD") else f" · {amount} {cost.currency}"
        console.show(line, out)

    def release(self):
        """Remove the machine made for this call, once. result() does it
        after the result is collected."""
        release, self._release = self._release, None
        if release is not None:
            release()

    def outcomes(self, timeout=None, stream_logs=True, cancel_on_interrupt=True):
        """A map's items one by one once it ended: [(True, value) or (False,
        its error)], so one failed item does not hide the others' values. A
        call is one item."""
        try:
            value = self.result(timeout=timeout, stream_logs=stream_logs, cancel_on_interrupt=cancel_on_interrupt)
        except Exception as exc:
            if self._outcome is None:
                # It has not ended (a timeout): nothing to say per item.
                raise
            if self.kind != MAP_KIND:
                return [(False, exc)]
            records = self._records
            if records is None:
                # Cancelled: _collect read nothing; the items that finished
                # still have their values.
                try:
                    records = self._read_verified(self._result_uris())
                except RemoteError:
                    return [(False, exc)] * self.count
            return [(False, exc) if r is None else
                    (True, pickle.loads(r["value"])) if r.get("ok") else (False, self._error(r, i))
                    for i, r in enumerate(records)]
        return [(True, v) for v in value] if self.kind == MAP_KIND else [(True, value)]

    def _result_uris(self):
        if self.kind == MAP_KIND:
            return [f"{self.prefix}/{RESULTS}/{i}.pkl" for i in range(self.count)]
        return [f"{self.prefix}/{RESULT}"]

    def _collected(self, missing):
        """The error for a job that is gone. Its result may still be in the
        store, but the digest that vouches for it was the job's (SEC-4)."""
        if self.kind == COMMAND_KIND:
            return errors.JobNotFound(f"{self.name}: the job is gone (collected after its TTL, or deleted): {missing}")
        # One result found is enough to say so: a map's may be thousands.
        present = None
        for uri in self._result_uris():
            try:
                self._store.get(uri)
            except FileNotFoundError:
                continue
            present = uri if self.kind != MAP_KIND else f"{uri} (and maybe the other items')"
            break
        if present:
            return errors.JobNotFound(
                f"{self.name}: the job was collected (its TTL ended, or it was deleted); its result is still at "
                f"{present}, but the digest that vouches for it went with the job, so it is not "
                "read: anyone who can write the store could have replaced it. Keep a job longer with "
                f"keep_for= (keep_for=\"7d\"), or read the result before it is collected. ({missing})")
        return errors.JobNotFound(f"{self.name}: no such job, and no result in the store ({missing})")

    def _rescheduled(self, status):
        self.reschedules += 1
        print(f"zenthros: {self.name} lost its machine ({status.reason or 'worker lost'}); "
              "it continues on a replacement from its last checkpoint", file=sys.stderr, flush=True)

    def _collect(self, failure):
        """(True, value) or (False, the exception result() raises)."""
        if isinstance(failure, errors.JobCancelled):
            return False, failure
        if self.kind == COMMAND_KIND:
            if failure is None:
                return True, 0
            if _platform(failure):
                return False, failure
            return False, self._no_result(failure, "the command failed")
        uris = self._result_uris()
        try:
            records = self._read_verified(uris)
        except RemoteError as exc:
            # A failed job's own reason says more than a result nobody vouched for.
            self._records = [None] * len(uris)
            return False, (failure if failure is not None else exc)
        self._records = records
        for index, record in enumerate(records):
            if record is not None and not record.get("ok"):
                return False, self._error(record, index if self.kind == MAP_KIND else None)
        if failure is not None:
            # Every result there, yet the job failed (a phase never completed,
            # the machine lost past the limit): the value is not the job's.
            if all(r is not None for r in records) or _platform(failure):
                return False, failure
            return False, self._no_result(failure, str(failure))
        missing = [uri for uri, r in zip(uris, records) if r is None]
        if missing:
            return False, RemoteError(f"{self.name}: the job succeeded without writing {', '.join(missing)}",
                                      job=self.name)
        values = [pickle.loads(r["value"]) for r in records]
        self._keep_in_cache(records)
        return True, (values if self.kind == MAP_KIND else values[0])

    def rerun(self, on=None, resume=False, upload=False, name=None, **changes):
        """This call again in a new job, with the code it ran and what it was
        asked, on= another place if you say; **changes are run()'s options.
        resume=True goes on from its last confirmed checkpoint, if it has ended
        and the new place runs the same image; if not it starts over, and .why
        says why. Secrets were sealed for its machine: give them again. What the
        new place cannot reach stops it before anything is made."""
        return _rerun(self, on, resume, upload, name, changes)

    def _keep_in_cache(self, records):
        """Its verified result, as a signed entry. After the check: the cache
        holds what was verified, and only this process's key signs it."""
        note = self.cache_note
        if note is None or not note.mode or self.kind != CALL_KIND:
            return
        from zenthros import cache as entries

        if not note.homes:
            note.homes = _homes_of_client(self._client, settings(), entries)
        digest, size = self._verified[f"{self.prefix}/{RESULT}"]
        entries.publish(note, self, digest, size, records[0])

    def _read_verified(self, uris):
        """The record at each uri (None where there is none), each unpickled
        only once its bytes match a SHA-256 the job reported for it. The
        store is not the job's to vouch for: a Workspace member who may
        presign a PUT, a job with the bucket's credentials, or another host
        can write there (audit 2026-10-03 SEC-4)."""
        raws = {}
        for uri in uris:
            try:
                raws[uri] = self._store.get(uri)
            except FileNotFoundError:
                pass
        wanted = {uri: hashlib.sha256(raw).hexdigest() for uri, raw in raws.items()}
        deadline = time.monotonic() + DIGEST_GRACE
        while True:
            reported, answered = self._reported_digests()
            unvouched = [uri for uri, digest in wanted.items() if digest not in reported.get(uri, ())]
            if not unvouched or time.monotonic() >= deadline:
                break
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        if unvouched and not answered:
            # The API did not answer: no word on the digests is not a job
            # that reported none, and nothing is kept of it.
            raise errors.TransientError(f"{self.name}: could not read the SHA-256s the job reported for "
                                        f"{', '.join(unvouched)}; the job and its result are kept: "
                                        f"zenthros.get({self.name!r}).result() reads them again")
        if unvouched:
            claimed = [uri for uri in unvouched if reported.get(uri)]
            detail = ("its SHA-256 is not the one the job reported, so it was written over after the job"
                      if claimed else "the job reported no SHA-256 for it")
            raise RemoteError(f"{self.name}: refusing to unpickle {', '.join(unvouched)}: {detail}; whoever "
                              "wrote it could run code here", job=self.name)
        self._verified = {uri: (wanted[uri], len(raws[uri])) for uri in raws}
        return [self._record(pickle.loads(raws[uri])) if uri in raws else None for uri in uris]

    def _reported_digests(self):
        """(uri -> the SHA-256s the job's runners reported for it, every
        attempt's: each one is the job's own word; whether the job's status
        and its reports both answered). From the job's status, where the controller keeps them
        past the report store (an in-memory one forgets them on a restart:
        post-fix review R-4), and a distributed job's tasks' too; and from
        its reports."""
        found = {}
        entries = []
        answered = True
        try:
            obj = self._client.job_object(self.name) or {}
            entries.extend((obj.get("status") or {}).get("results") or [])
        except errors.TransientError:
            answered = False
        except errors.JobNotFound:
            pass
        try:
            views = self._client.reports(self.name)
        except errors.TransientError as err:
            # Either source away is no answer: the digest may be only there.
            # A 501 is one: the API keeps no reports, for good.
            views, answered = [], answered and getattr(err, "status", None) == 501
        except errors.NotFound:
            views = []
        entries.extend(view.payload.get(RESULT_REPORT) for view in views)
        for entry in entries:
            if isinstance(entry, dict) and entry.get("uri") and entry.get("sha256"):
                found.setdefault(entry["uri"], set()).add(entry["sha256"])
        return found, answered

    def _record(self, record):
        for key in ("started", "finished"):
            if key in record.get("timings", {}):
                self.timings.setdefault("remote_" + key, record["timings"][key])
        self.machine = self.machine or record.get("machine", {})
        if record.get("attempts") is not None:
            self.attempts = max(self.attempts or 0, record["attempts"])
        return record

    def _no_result(self, failure, detail):
        tail = ""
        try:
            # The end of the log, where the error is: a log cut to its start
            # and then its last lines taken never showed it (SDK-10).
            tail = "\n".join(self.logs(tail=30).splitlines()[-30:])
        except (errors.ZenthrosError, OSError):  # the log only adds to the message
            pass
        err = RemoteError(f"{self.name}: {detail}" + (f"\n--- last log lines ---\n{tail}" if tail else ""),
                          job=self.name)
        err.__cause__ = failure
        return err

    def _error(self, record, index=None):
        where = self.name if index is None else f"{self.name} item {index}"
        err = RemoteError(
            f"{where}: {record.get('type', 'Exception')}: {record.get('message', '')}\n"
            f"--- remote traceback ---\n{record.get('traceback', '')}",
            remote_type=record.get("type", ""), remote_traceback=record.get("traceback", ""), job=self.name,
            reason=record.get("reason", ""))
        from zenthros.remote.runner import rebuild_exception

        try:
            err.__cause__ = rebuild_exception(pickle.loads(record["error"])) if record.get("error") else None
        except Exception:  # noqa: BLE001 - its class is not importable here: the traceback says it
            err.__cause__ = None
        return err

    def cost(self):
        """What the call cost so far (Cost): its running time times the
        price of its machine, as zenthros-api counts it."""
        raw = self._client.job_cost(self.name)
        if raw is None:
            return Cost(job=self.name, basis="this connection has no zenthros-api, which holds the prices")
        return Cost.from_api(raw)

    def overhead(self):
        """Seconds from submit to the code starting, and the code's own
        seconds, once result() returned."""
        t = self.timings
        if "remote_started" not in t or "submitted" not in t:
            return None
        return {"queue_and_start": t["remote_started"] - t["submitted"],
                "compute": t["remote_finished"] - t["remote_started"],
                "total": t.get("finished", t["remote_finished"]) - t["submitted"]}


def _took(seconds):
    """9s, 1m26s, 2h05m: how long a call took, as a receipt says it."""
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _http_status(url, timeout=5.0):
    """(status code or None, what came back) of a GET of url."""
    import urllib.error
    import urllib.request

    from zenthros.jobs.wire import USER_AGENT

    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": USER_AGENT}),
                                    timeout=timeout) as resp:
            return resp.status, f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        return exc.code, f"HTTP {exc.code}"
    except (OSError, ValueError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _why_not(status, error):
    if status is None:
        return f"the status could not be read ({error})"
    ready = status.service_ready()
    if ready:
        return f"{ready.get('reason')}: {ready.get('message')}"
    return f"the job is {status.phase}" + (f" ({status.message})" if status.message else "")


def _platform(failure):
    """A job the platform failed (image, quota, admission, the machine lost
    past the limit), not the code: its reasons name no process failure."""
    codes = [str(r.get("code") or "") for r in getattr(failure, "reasons", []) if isinstance(r, dict)]
    return bool(codes) and PROCESS_FAILED not in codes


def user_reports(reports):
    """The reports the code published: not Zenthros's own (the result
    digests the runner reports)."""
    return [r for r in reports or [] if not str(getattr(r, "producer_id", "")).startswith(RESULT_PRODUCER)]


def last_reports(reports):
    """Each producer's last user report, by producer."""
    last = {}
    for r in user_reports(reports):
        key = (r.sequence, r.produced_at)
        if r.producer_id not in last or key >= last[r.producer_id][0]:
            last[r.producer_id] = (key, r)
    return {producer: r for producer, (_, r) in last.items()}


def report_text(payload, limit=160):
    """A report's payload in one short line: key=value · key=value."""
    parts = []
    for key, value in (payload or {}).items():
        if isinstance(value, float):
            text = f"{value:.4g}"
        elif isinstance(value, (dict, list)):
            text = json.dumps(value, default=str)
        else:
            text = str(value)
        parts.append(f"{key}={text}")
    line = " · ".join(parts)
    return line if len(line) <= limit else line[:limit - 3] + "..."


class _ReportFollower(threading.Thread):
    """While result() waits: a line each time a producer's last report
    changes ("task i" in a map), so the user sees how it goes without
    reading the log."""

    def __init__(self, call, poll_seconds=5.0, out=None):
        super().__init__(daemon=True)
        self._call, self._poll = call, poll_seconds
        self._out = out or sys.stdout
        self._stopping = threading.Event()
        self._seen = {}

    def run(self):
        while not self._stopping.is_set():
            self.check()
            self._stopping.wait(self._poll)

    def check(self):
        try:
            current = last_reports(self._call.reports())
        except (errors.ZenthrosError, OSError):  # progress, not a check
            return
        for producer, report in sorted(current.items()):
            key = (report.sequence, report.report_id, report.produced_at)
            if self._seen.get(producer) == key:
                continue
            self._seen[producer] = key
            task = f" task {producer[len('task-'):]}" if producer.startswith("task-") else ""
            console.show(f"zenthros: {self._call.name}{task} · {report_text(report.payload)}", self._out)

    def stop(self):
        self._stopping.set()
        if self.is_alive():
            self.join(timeout=self._poll + 1)


# The printed lines a log replay is matched against.
LOG_LINES_KEPT = 10_000


class _LogFollower(threading.Thread):
    """Prints the job's log while it runs. zctl logs --watch fails until
    the job has a backend and may end early, so it is restarted, skipping
    the lines already printed."""

    def __init__(self, client, name, poll_seconds=2.0, out=None, tee=None):
        super().__init__(daemon=True)
        self._client, self._name, self._poll = client, name, poll_seconds
        self._out = out or sys.stdout
        # Each new line also goes here (the call's local record).
        self._tee = tee
        self._stopping = threading.Event()
        self._proc = None
        self._followed = False

    def run(self):
        printed = []
        while not self._stopping.is_set():
            try:
                self._proc = self._client.follow_logs(self._name)
            except (errors.ZenthrosError, OSError):  # tried again below
                self._proc = None
            if self._proc is not None:
                self._followed = True
                # A reconnect replays the log from wherever zctl starts it
                # (not always its first line), and after a move to another
                # machine it is that attempt's log. The replay's lines are
                # held while they continue printed from some line on (every
                # such line is a candidate, not only the first equal one);
                # once one reaches printed's end the rest is new, and if none
                # does, the held lines were new too.
                starts, held = list(range(len(printed))), []
                for line in self._proc.stdout:
                    if starts is not None and not any(j + len(held) == len(printed) for j in starts):
                        starts = [j for j in starts if printed[j + len(held)] == line]
                        if starts:
                            held.append(line)
                            continue
                        self._print(printed, held)
                    starts, held = None, []
                    self._print(printed, [line])
                self._proc.wait()
            self._stopping.wait(self._poll)

    def _print_the_log(self):
        """A job that ended before any stream opened: its log, once."""
        try:
            text = self._client.logs(self._name)
        except errors.ZenthrosError:
            return
        self._print([], text.splitlines(keepends=True))

    def _print(self, printed, lines):
        for line in lines:
            printed.append(line)
            console.write(line, self._out)
            if self._tee is not None:
                self._tee(line)
        if len(printed) > 2 * LOG_LINES_KEPT:
            del printed[:-LOG_LINES_KEPT]

    def stop(self, grace=3.0):
        # The last lines reach zctl a moment after the job ends: a stream
        # still open is waited for, up to grace.
        self._stopping.set()
        proc = self._proc
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                proc.terminate()
        self.join(timeout=grace)
        if not self._followed:
            self._print_the_log()


class CloudFunction:
    """fn with a remote side. Calling it runs it here, unchanged;
    .remote() runs it on Zenthros and .submit() starts it there."""

    def __init__(self, fn, options):
        self.fn = fn
        self.options = dict(options)
        self.__name__ = getattr(fn, "__name__", "fn")
        self.__qualname__ = getattr(fn, "__qualname__", self.__name__)
        self.__doc__ = getattr(fn, "__doc__", None)
        self.__module__ = getattr(fn, "__module__", None)
        self.__wrapped__ = fn

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def local(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def submit(self, *args, on=None, params=None, keep=False, dry_run=False, **kwargs):
        """Start fn on Zenthros and return its RemoteCall at once. on= is
        where: a machine (zenthros.create), its name, "local" for a container
        made for this call, removed once its result is read unless
        keep=True, or a template for a new machine (keep=True: a machine made
        from it that stays). Without on=, configure(on=) or ZENTHROS_ON, else
        this computer ("local"). params are fn's keyword arguments.
        dry_run=True sends nothing and makes nothing: it returns where the
        call would run (zenthros.targetlist.Plan), against what fn asks."""
        options = {**self.options, **kwargs}
        options.pop("on", None)
        options.pop("keep", None)
        if dry_run:
            planned = {name: options.get(name, default) for name, default in _OPTIONS.items()}
            return _plan_of_call(self.fn, args, {**(params or {})}, on, planned, keep, options.get("cache", False),
                                 options.get("deps"), options.get("inputs"), options.get("outputs"))
        return run(self.fn, *args, on=on, params=params, keep=keep, **options)

    def remote(self, *args, on=None, params=None, keep=False, dry_run=False, **kwargs):
        """fn on Zenthros, waited for: its value. See submit."""
        if dry_run:
            return self.submit(*args, on=on, params=params, keep=keep, dry_run=True, **kwargs)
        call = self.submit(*args, on=on, params=params, keep=keep, **kwargs)
        try:
            return call.result()
        except BaseException:
            # The caller never gets this call: nothing it made is left
            # billing. Ctrl-C already cancelled it; the error that stopped the
            # wait is the one raised, not a failure to cancel.
            if call._outcome is None and call._release is not None and not call._interrupted:
                try:
                    call.cancel()
                except Exception as err:
                    print(f"zenthros: could not cancel {call.name}: {err}", file=sys.stderr)
            raise

    def map(self, items, on=None, shared=None, dry_run=False, **kwargs):
        """fn over items, a task each: a config (a dict) is fn's keyword
        arguments, over shared's; anything else is its one argument. on= is
        where, as submit's; a list of destinations spreads the configs over
        them (zenthros.remote.spread): one job each, the results in the
        order of items. dry_run=True: how many each would take, and where."""
        from zenthros.remote import spread

        items, shared = list(items), dict(shared or {})
        spread.check(items, shared)
        options = {**self.options, **kwargs}
        if not isinstance(on, (list, tuple)):
            if dry_run:
                return self._spread_plan(items, [on], options)
            return _placed_call(on, options, False, lambda where, placed: map(self.fn, items, _shared=shared,
                                                                              _where=where, **placed),
                                python=bundle.python_version())
        if not on:
            raise errors.ValidationError("on=[]: name at least one destination (zenthros.targets())")
        if not items:
            raise errors.ValidationError("map needs at least one item")
        if dry_run:
            return self._spread_plan(items, on, options)
        where = self._split(items, on)
        name, parts = options.pop("name", None) or _job_name(self.fn), []
        if not _LABEL_VALUE.fullmatch(name) or len(name) > 60:
            raise errors.ValidationError(f"map name={name!r}: lowercase letters, digits and '-', at most 60 "
                                         "(its jobs are name-0, name-1...)")
        options["labels"] = {**(options.get("labels") or {}), MAP_GROUP: name}
        try:
            for k, destination in enumerate(on):
                indices = [i for i, w in enumerate(where) if w == spread.name_of(destination)]
                if not indices:
                    continue
                tags = {MAP_INDICES: ",".join(str(i) for i in indices), MAP_ON: spread.name_of(destination),
                        MAP_SIZE: str(len(items))}
                # _placed_call calls start at once, in this iteration.
                call = _placed_call(destination, options, False, lambda where, placed: map(
                    self.fn, [items[i] for i in indices], _shared=shared, _annotations=tags, name=f"{name}-{k}",  # noqa: B023
                    _where=where, **placed), python=bundle.python_version())
                parts.append((spread.name_of(destination), indices, call))
                if call.runlog is not None:
                    # Which configs this job took: index -> config.
                    call.runlog.note(configs={i: runlog.cut(items[i]) for i in indices})
        except BaseException:
            # All of them start, or none is left running.
            for _, _, call in parts:
                for step in (call.cancel, call.delete, call.release):
                    try:
                        step()
                    except Exception as err:  # noqa: BLE001 - the first error is the one raised
                        print(f"zenthros: could not {step.__name__} {call.name}: {err}", file=sys.stderr)
            raise
        return spread.MapCalls(items, parts, name=name)

    def _split(self, items, on):
        from zenthros.remote import spread

        names = [spread.name_of(destination) for destination in on]
        if len(set(names)) != len(names):
            raise errors.ValidationError(f"on={names}: each destination once")
        free = {}
        if len(names) > 1:
            from zenthros import targetlist

            try:
                current = targetlist.targets()
                free = {name: getattr(current.get(name), "available", 0) for name in names}
            except (errors.ValidationError, errors.TransientError, PermissionError, LookupError):
                # No room known: in turn.
                free = {}
        return spread.split(len(items), [(name, free.get(name, 0)) for name in names])

    def _spread_plan(self, items, on, options):
        from zenthros import targetlist
        from zenthros.remote import spread

        if on == [None]:
            return spread.SpreadPlan({"": (targetlist.plan_call(None, options, python=bundle.python_version()), len(items))})
        where = self._split(items, on)
        plans = {}
        for destination in on:
            count = where.count(spread.name_of(destination))
            if count:
                plans[spread.name_of(destination)] = (targetlist.plan_call(destination, options,
                                                                            python=bundle.python_version()), count)
        return spread.SpreadPlan(plans)

    def serve(self, *args, **kwargs):
        """Run fn as a Service; see zenthros.serve."""
        return serve(self.fn, *args, **{**self.options, **kwargs})

    def with_options(self, **overrides):
        """A copy with other options: train.with_options(cpu="16").remote(x)."""
        return cloud(**{**self.options, **overrides})(self.fn)


def is_output(key):
    """Whether key, a path inside a call's folder, is an output of its job: all
    the folder holds but the call, its result and its checkpoints. The folder is
    $ZENTHROS_DATA_OUTPUT, so a job can name a file anything: only the exact
    names the SDK writes are the SDK's."""
    return not (CALL_OBJECT.fullmatch(key) or key == RESULT or _MAP_RESULT.fullmatch(key) or checkpoint.is_object(key))


def runner_packages(store_uri):
    """What the runner needs where it runs, for a call in store_uri:
    cloudpickle at this process's version, and the store's client."""
    try:
        import cloudpickle

        packages = [f"cloudpickle=={cloudpickle.__version__}"]
    except ImportError:
        packages = ["cloudpickle"]
    if str(store_uri).startswith("s3://"):
        packages.append("boto3")
    elif str(store_uri).startswith("gs://"):
        packages.append("google-cloud-storage")
    return packages


def _pickled(target):
    """The Python a call of target needs on the machine: this one's minor
    for a function (cloudpickle), none for a command."""
    return None if isinstance(target, (str, list, tuple)) else bundle.python_version()


def _named_env(options, on, python):
    """options with env="train" made the image of that environment where on=
    sends the call. A local call with no image runs in the one in use. image=
    wins over both."""
    from zenthros import env as environments
    from zenthros import machines

    options = dict(options)
    name = options.get("env") if isinstance(options.get("env"), str) else ""
    if name:
        options["env"] = None
    if options.get("image"):
        return options
    where = machines.default_on() if on is None else on
    in_use = "" if name or where not in (machines.LOCAL, machines.LOCAL_K8S) else environments.using()
    if name or in_use:
        options["image"] = environments.image_for(name or in_use, where, python)
        if name:
            options["env_needs"] = {"python": environments.python_of(name), "packages": environments.packages_for(name, where)}
    return options


def _placed_call(on, options, keep, start, python=None, ports=None, fresh=False):
    """start(where, options) where on= sends it (machines.place): where is
    the spec's pool, backend or acquire, and options run()'s, as the place
    changed them (the store's credentials, an image a machine made here
    took). The machine made for it goes once the result is read, or at once
    if it does not start; a start that may have made the job keeps it. python: the minor the machine must run (a
    function's call), checked before any machine is made for it; ports: a
    Service's."""
    from zenthros import machines

    options = _named_env(options, on, python)
    placed = machines.place(on, options, keep=keep, python=python, ports=ports)
    if fresh:
        machines.on_a_new_machine(placed)
    needs = (options.pop("env_needs", None) or {}).get("packages")
    if needs and not (placed.made and placed.made.completes_image):
        # Nothing completes its image for it: the call's own packages do.
        options["packages"] = [*needs, *(options.get("packages") or [])]
    where = {key: value for key, value in placed.options.items() if key in _WHERE}
    options.update({key: value for key, value in placed.options.items() if key not in _WHERE})
    try:
        call = start(where, options)
    except errors.TransientError as err:
        if placed.made is not None and not keep:
            if getattr(err, "job", None):
                # A submit whose answer was lost may have made the job: its
                # machine stays for it, named for the caller to delete.
                err.made = placed.made
                err.args = (f"{err}; its machine {placed.made.name} is kept for it: "
                            f"zenthros.machines.delete({placed.made.name!r}) if the job is not there",)
            else:
                machines.delete(placed.made)
        raise
    except BaseException:
        if placed.made is not None and not keep:
            machines.delete(placed.made)
        raise
    if placed.made is not None:
        call.made = placed.made
        if not keep:
            call._release = lambda: machines.delete(placed.made)
    # start runs one destination: its call is a RemoteCall, named where
    # place() sent it.
    call.where = placed.name
    if call.runlog is not None:
        call.runlog.note(on=placed.name, made=placed.made.name if placed.made else None)
    return call


def cloud(fn=None, **options):
    """@zenthros.cloud(gpu=..., packages=[...]) marks fn as runnable on
    Zenthros; see CloudFunction. Options are run()'s; where it runs is each
    call's on=."""
    if fn is not None:
        return CloudFunction(fn, options)

    def wrap(inner):
        return CloudFunction(inner, options)

    return wrap
