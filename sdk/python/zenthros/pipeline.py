# SPDX-License-Identifier: Apache-2.0

"""Steps in a line: what one leaves is what the next takes. It orders them and
says why each one runs or not; the jobs are plain calls (zenthros.run), and each
place's own rules hold.

Without cache= the jobs are chained and it returns at once. With cache= each step
is waited for, because the next one's key is made of what it gave.

A run is on record in .zenthros/runs, for resume(). An interrupted step starts
over: resuming it from its checkpoint is rerun()'s."""

import json
import os
import re
import uuid
from dataclasses import dataclass, field

from zenthros import cache as entries
from zenthros import contracts, errors, reach, upload
from zenthros.remote.call import RESUME, CloudFunction, RemoteCall, get, run

PIPELINE_LABEL = "zenthros.ai/pipeline"
STEP_LABEL = "zenthros.ai/step"

RUN, REUSE, YOUR_FILE, SKIP = "run", "reuse", "your file", "skip"
_NAME = re.compile(r"[^a-z0-9-]+")


def _clean(text, limit=30):
    return _NAME.sub("-", str(text).lower()).strip("-")[:limit].strip("-") or "step"


def _runs_folder(name):
    from zenthros.jobs.local import project_root

    return project_root() / ".zenthros" / "runs" / "pipelines" / _clean(name, 60)


@dataclass
class Step:
    name: str
    fn: object
    options: dict = field(default_factory=dict)

    def contract(self, which):
        return contracts.declared(self.options.get(which))


@dataclass
class StepRun:
    """One step of a run: what it did and why."""

    name: str
    action: str
    why: str = ""
    job: str = ""
    call: object = None
    error: Exception = None

    def to_dict(self):
        out = {"step": self.name, "action": self.action, "why": self.why, "job": self.job}
        if self.call is not None:
            out["state"] = _state_of(self.call)
        if self.error is not None:
            out["error"] = str(self.error)
            out["reason"] = getattr(self.error, "reason", "")
        return out


def _state_of(call):
    try:
        return call.status() if not isinstance(call, RemoteCall) else call.handle.job_status().phase
    except errors.ZenthrosError as err:
        return f"unknown: {err}"


class PipelineRun:
    """A run of a Pipeline: its steps, as they went."""

    def __init__(self, pipeline, run_id, steps):
        self.pipeline, self.id, self.steps = pipeline, run_id, steps
        # Step -> the job of the last run it may take its checkpoint from.
        self.resuming = {}

    def __repr__(self):
        return f"PipelineRun({self.pipeline!r}, {self.id!r}: {self.status()})"

    def __getitem__(self, name):
        return next(step for step in self.steps if step.name == name)

    def status(self):
        """Each step, in a line: "prep: reuse · train: run: depth changed"."""
        return " · ".join(f"{s.name}: {s.action}" + (f": {s.why}" if s.why else "") for s in self.steps)

    def result(self, timeout=None):
        """The last step's value. A step that failed raises its error, and what
        comes after it did not run."""
        last = None
        for step in self.steps:
            if step.error is not None:
                raise step.error
            if step.call is None:
                continue
            try:
                last = step.call.result(timeout=timeout)
            except errors.WaitTimeout:
                raise
            except Exception as err:
                step.error = err
                raise
        return last

    def cancel(self):
        for step in self.steps:
            if step.call is not None:
                step.call.cancel()

    def to_dict(self):
        return {"pipeline": self.pipeline, "run": self.id, "steps": [step.to_dict() for step in self.steps]}


@dataclass
class StepPlan:
    """What a run would do with one step: four separate questions, none of which
    answers another. available: is it there; compatible: has it the shape asked;
    integrity: are they the expected bytes, and how that is known; origin: did a
    job of mine make it."""

    step: str
    action: str
    reason: str = ""
    available: str = ""
    compatible: str = ""
    integrity: str = ""
    origin: str = ""
    transfers: list = field(default_factory=list)
    unchecked: list = field(default_factory=list)
    problems: list = field(default_factory=list)
    target: str = ""
    env: str = ""
    image: str = ""
    resources: dict = field(default_factory=dict)
    key: str = ""

    def to_dict(self):
        return dict(self.__dict__)


@dataclass
class PipelinePlan:
    """What pipe.run would do. Nothing was created, and no data moved."""

    name: str
    steps: list

    @property
    def ok(self):
        return not any(step.problems for step in self.steps)

    @property
    def problems(self):
        return [f"{step.step}: {problem}" for step in self.steps for problem in step.problems]

    def to_dict(self):
        return {"pipeline": self.name, "ok": self.ok, "steps": [step.to_dict() for step in self.steps]}

    def __str__(self):
        width = max(len(step.step) for step in self.steps)
        lines = [f"{self.name} · plan (nothing was created and no data moved)", ""]
        for step in self.steps:
            lines.append(f"{step.step:<{width}}  {step.action:<9}  {step.reason}".rstrip())
            lines += [f"{'':<{width}}  {'':<9}  {fact}" for fact in (step.available, step.compatible, step.integrity,
                                                                     step.origin) if fact]
            lines += [f"{'':<{width}}  problem:   {problem}" for problem in step.problems]
        moved = [item for step in self.steps for item in step.transfers]
        lines += ["", "data that would move"]
        lines += [f"  {item}" for item in moved] or ["  nothing goes up or down from this computer"]
        unchecked = [item for step in self.steps for item in step.unchecked]
        if unchecked:
            lines += ["", "not checked until it runs"] + [f"  {item}" for item in unchecked]
        return "\n".join(lines)


class Pipeline:
    """Pipeline([("prep", prep), ("train", train)], name="churn"). A step is a
    function, or a @zenthros.cloud function with its options. Its first argument
    is what the step above returned."""

    def __init__(self, steps, name="pipeline"):
        self.name = _clean(name, 40)
        self.steps = []
        for item in steps:
            step_name, fn = item
            if not callable(fn):
                raise errors.ValidationError(f"step {step_name}: a function, not {type(fn).__name__}")
            cloud = isinstance(fn, CloudFunction)
            self.steps.append(Step(str(step_name), fn.fn if cloud else fn, dict(fn.options) if cloud else {}))
        names = [step.name for step in self.steps]
        if not names or len(set(names)) != len(names):
            raise errors.ValidationError(f"a Pipeline names each step once, and has at least one: {names}")

    def _index(self, name):
        names = [step.name for step in self.steps]
        if name not in names:
            raise errors.ValidationError(f"{self.name} has no step {name!r}; it has {names}")
        return names.index(name)

    def _start(self, args, start_at, upstream):
        """(the steps to run, their first arguments). What you give is the start
        step's argument, as in any call: no job stands for what did not run."""
        if start_at is None:
            if upstream:
                raise errors.ValidationError("upstream= goes with start_at=: what the step it starts from takes")
            return 0, args
        index = self._index(start_at)
        if args and upstream:
            raise errors.ValidationError("start_at= with upstream= or with arguments, not both")
        if not upstream:
            return index, args
        names = [step.name for step in self.steps[:index]]
        unknown = [name for name in upstream if name not in names]
        if unknown:
            raise errors.ValidationError(f"upstream names {unknown}: not before {start_at} (before it: {names})")
        wanted = names[-1] if names else ""
        if wanted not in upstream:
            raise errors.ValidationError(f"{start_at} takes what {wanted} leaves: upstream={{{wanted!r}: ...}}")
        return index, (_given(upstream[wanted]),)

    def _was_known(self, rows, index, position):
        """Whether what this step takes is known: everything above it is reused."""
        return all(row.action != RUN and row.action != RESUME for row in rows[index:position])

    def _check_params(self, params):
        known = [step.name for step in self.steps]
        unknown = [name for name in params or {} if name not in known]
        if unknown:
            raise errors.ValidationError(f"params names {unknown}: {self.name} has {known}")

    def _place(self, step, on):
        if isinstance(on, dict):
            unknown = [name for name in on if name not in [s.name for s in self.steps]]
            if unknown:
                raise errors.ValidationError(f"on= names {unknown}: {self.name} has {[s.name for s in self.steps]}")
            return on.get(step.name)
        return on

    def run(self, *args, on=None, cache=False, params=None, start_at=None, upstream=None, _resuming=None, **options):
        """Run the steps. args go to the first; params={"train": {"depth": 8}} are
        a step's keyword arguments; on= a place, or {step: place}. start_at= skips
        what comes before it, and upstream={step: value} gives what it leaves: a
        path, a URI, a zenthros.get(...) or a zenthros.cache.find(...). A reference
        a step's place cannot reach stops it before anything is made."""
        self._check_params(params)
        index, first = self._start(args, start_at, upstream)
        first_step = self.steps[index]
        contracts_in = first_step.contract("inputs")
        if contracts_in and upstream:
            _check_given(first_step, first, contracts_in)
        blocked = [answer.why for _, answer in reach.answers(first, self._place(first_step, on))
                   if answer.answer == reach.IMPOSSIBLE]
        if blocked and not options.get("upload"):
            raise errors.ValidationError(f"{first_step.name}: {blocked[0]}")
        started = PipelineRun(self.name, uuid.uuid4().hex[:6], [])
        started.resuming = _resuming or {}
        started.steps += [StepRun(step.name, SKIP, f"you start from {first_step.name}") for step in self.steps[:index]]
        self._go(started, index, first, on, cache, params, options)
        self._record(started, args, on, cache, params, options, start_at)
        return started

    def _go(self, started, index, first, on, cache, params, options):
        value, failed = None, None
        for position, step in enumerate(self.steps[index:]):
            if failed is not None:
                started.steps.append(StepRun(step.name, SKIP, f"{failed} failed"))
                continue
            call_args = first if position == 0 else (value,)
            value, failed = self._one(started, step, call_args, on, cache, params, options)
        return started

    def _one(self, started, step, call_args, on, cache, params, options):
        job = f"{self.name}-{_clean(step.name, 20)}-{started.id}"
        try:
            call_args = _settled_for(call_args, self._place(step, on))
        except Exception as err:
            # The step before it failed here, not where this one would run.
            record = StepRun(step.name, SKIP, str(err).splitlines()[0], job=job, error=err)
            started.steps.append(record)
            return None, step.name
        labels = {**(step.options.get("labels") or {}), PIPELINE_LABEL: self.name, STEP_LABEL: _clean(step.name, 63)}
        merged = {**step.options, **options, "labels": labels, "name": job}
        merged.pop("on", None)
        record = StepRun(step.name, RUN, job=job)
        started.steps.append(record)
        try:
            call = run(step.fn, *call_args, on=self._place(step, on), params=(params or {}).get(step.name),
                       cache=cache, _keyed=not cache, _resume=started.resuming.get(step.name), **merged)
        except errors.ZenthrosError as err:
            record.error, record.why = err, str(err)
            return None, step.name
        record.call, record.action = call, call.action
        record.why = call.why
        record.job = call.name
        if not cache:
            return call, None
        try:
            return call.result(), None
        except Exception as err:
            record.error, record.why = err, str(err).splitlines()[0]
            return None, step.name

    def _record(self, started, args, on, cache, params, options, start_at):
        try:
            arguments = json.dumps(list(args))
        except (TypeError, ValueError):
            arguments = None
        folder = _runs_folder(self.name)
        folder.mkdir(parents=True, exist_ok=True)
        plain = {key: value for key, value in options.items() if _is_json(value)}
        (folder / f"{started.id}.json").write_text(json.dumps({
            "run": started.id, "args": None if arguments is None else json.loads(arguments), "on": on if _is_json(on) else None,
            "cache": cache, "params": params if _is_json(params) else None, "options": plain, "start_at": start_at,
            "steps": [{"step": s.name, "action": s.action, "job": s.job} for s in started.steps]}, indent=1))
        (folder / "last").write_text(started.id)

    def _last(self):
        folder = _runs_folder(self.name)
        try:
            run_id = (folder / "last").read_text().strip()
            return json.loads((folder / f"{run_id}.json").read_text())
        except OSError:
            raise errors.NotFound(f"{self.name} has no run on record here to resume ({folder})") from None

    def resume(self, *args, on=None, params=None, **options):
        """The last run again, from where it stopped. With cache=, each step that
        has a good entry is reused. Without it, the steps of that run that ended
        well are reused by their job, checked as any upstream is. A step cut short,
        with the same code, inputs and environment, goes on from its last confirmed
        checkpoint in a new job; if anything differs it starts over, and says why."""
        last = self._last()
        args = args or tuple(last["args"] or ())
        on = on if on is not None else last["on"]
        params = params if params is not None else last["params"]
        options = {**last["options"], **options}
        before = {step["step"]: step for step in last["steps"]}
        resuming = {name: step["job"] for name, step in before.items() if step.get("job")}
        if last["cache"]:
            return self.run(*args, on=on, cache=last["cache"], params=params, start_at=last["start_at"],
                            _resuming=resuming, **options)
        started = PipelineRun(self.name, uuid.uuid4().hex[:6], [])
        started.resuming = resuming
        index, value = 0, None
        for index, step in enumerate(self.steps):  # noqa: B007 - index is the step it stopped at, read below
            was = before.get(step.name) or {}
            done = _finished_job(was.get("job"))
            if was.get("action") == SKIP or done is None:
                break
            started.steps.append(StepRun(step.name, REUSE, f"{was['job']} of the last run ended well", was["job"], done))
            value = done
        else:
            return started
        first = args if index == 0 else (value,)
        self._go(started, index, first, on, False, params, options)
        self._record(started, args, on, False, params, options, None)
        return started

    def plan(self, *args, on=None, cache=False, params=None, start_at=None, upstream=None, probe=False, resume=False,
             **options):
        """What run would do, step by step, with the same arguments. It makes
        nothing and moves nothing. probe=True asks a place that is already up to
        look at what no rule can answer; it never starts one. resume=True: what
        resume would do."""
        before = {}
        if resume:
            last = self._last()
            before = {step["step"]: step["job"] for step in last["steps"] if step.get("job")}
            args = args or tuple(last["args"] or ())
            on = on if on is not None else last["on"]
            params = params if params is not None else last["params"]
            cache, start_at = last["cache"], last["start_at"]
            options = {**last["options"], **options}
        self._check_params(params)
        index, first = self._start(args, start_at, upstream)
        rows = []
        if upstream:
            rows += [StepPlan(step.name, SKIP, f"you start from {self.steps[index].name}")
                     for step in self.steps[:index - 1]]
            rows.append(_given_row(self.steps[index - 1].name, upstream[self.steps[index - 1].name], self.steps[index]))
        else:
            rows += [StepPlan(step.name, SKIP, f"you start from {self.steps[index].name}") for step in self.steps[:index]]
        known, value = True, first
        for position, step in enumerate(self.steps[index:], start=index):
            place = self._place(step, on)
            row = StepPlan(step.name, RUN)
            row.target = str(getattr(place, "name", "") or place or "")
            call_args = value if position == index else (value,)
            options_now = {**step.options, **options}
            _facts_of_place(row, step, options_now, place)
            if position > 0 and position > index:
                _check_connection(row, self.steps[position - 1], step)
            elif upstream:
                _compare_given(row, first, step)
            if known:
                _check_references(row, call_args, place, probe, sends=options_now.get("upload", False))
            done = _finished_job(before.get(step.name)) if known and not cache else None
            if not known:
                row.reason = f"runs after {self.steps[position - 1].name}, which runs: its input is not known yet"
            elif done is not None:
                row.action, row.origin = REUSE, before[step.name]
                row.reason = f"{before[step.name]} of the last run ended well"
                value = done
            elif cache:
                found = _plan_key(row, step, call_args, (params or {}).get(step.name), options_now, place, cache)
                if found is not None:
                    value = found
                else:
                    known = False
            else:
                row.reason = "cache is off"
                known = False
            if row.action == RUN and step.name in before and position >= index and not row.problems \
                    and self._was_known(rows, index, position):
                _plan_resume(row, step, call_args, (params or {}).get(step.name), options_now, place, before[step.name])
            rows.append(row)
        return PipelinePlan(self.name, rows)


def _plan_resume(row, step, call_args, params, options, place, source):
    """What a step that runs would start from, as run decides it."""
    from zenthros.remote.call import _client_for, _consult_cache, _contracts, _resume_ref

    note, _ = _consult_cache(step.fn, call_args, dict(params or {}), {"code_dir": ".", **options}, place, False, options.get("deps"),
                             _contracts(options.get("inputs"), options.get("outputs")), say=False)
    if note is None:
        return
    ref = _resume_ref(_client_for(place), source, note)
    row.reason = note.why
    if ref is not None:
        row.action, row.origin = RESUME, source


def _is_json(value):
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def _finished_job(name):
    if not name:
        return None
    try:
        call = get(name)
        return call if call.handle.job_status().phase == "Succeeded" else None
    except errors.ZenthrosError:
        return None


def _given(value):
    """What you gave as upstream: a call's value, or the path or URI as it is."""
    if isinstance(value, RemoteCall) or getattr(value, "cached", False):
        return value.result(stream_logs=False)
    return os.path.abspath(value) if isinstance(value, str) and os.path.exists(value) else value


def _check_given(step, first, declared):
    named = entries.named_arguments(step.fn, first, {})
    contracts.check_inputs(declared, named)


def _given_row(name, value, next_step):
    """Four answers about what you gave; none stands in for another."""
    row = StepPlan(name, YOUR_FILE)
    if isinstance(value, RemoteCall) or getattr(value, "cached", False):
        row.reason = f"the result of {value.name}"
        row.available = "its result is there" if getattr(value, "cached", False) else "its job is there"
        row.integrity = "same bytes as reported" if not getattr(value, "cached", False) else "same bytes"
        row.origin = "made by a job of yours" if not getattr(value, "cached", False) else "signed by you"
        return row
    row.reason = str(value)
    is_path = isinstance(value, str) and os.path.exists(value)
    is_uri = isinstance(value, str) and value.startswith(("s3://", "gs://"))
    row.available = "it is there" if is_path else "not looked at" if is_uri else "it is not there"
    declared = next_step.contract("inputs")
    if not is_path and not is_uri:
        row.problems.append(f"{value} is not there")
    elif declared:
        try:
            _check_given(next_step, (_given(value),), declared)
            row.compatible = "it has the shape asked"
        except contracts.ContractError as err:
            row.compatible = f"not the shape asked: {err}"
            row.problems.append(str(err))
    else:
        row.compatible = f"not compared: {next_step.name} declares no inputs="
    row.integrity = "nobody checked its bytes against anything"
    row.origin = "nobody checks how it was made"
    return row


def _facts_of_place(row, step, options, place):
    from zenthros import env as environments

    name = options.get("env") if isinstance(options.get("env"), str) else ""
    row.env = name
    row.image = str(options.get("image") or "")
    row.resources = {what: str(options[what]) for what in ("cpu", "memory", "gpu") if options.get(what) not in (None, "")}
    if name:
        try:
            row.image = row.image or environments.content_of(name)
        except errors.ZenthrosError as err:
            row.problems.append(str(err))
    from zenthros.remote import bundle
    from zenthros.targetlist import plan_call, python_problem

    python = environments.python_of(name) if name and not row.problems else ""
    problem = python_problem(step.name, python, bundle.python_version()) if python else ""
    if problem:
        row.problems.append(problem)
    try:
        row.problems += plan_call(place, options, python=bundle.python_version()).problems
    except errors.ZenthrosError as err:
        row.problems.append(str(err))


def _check_connection(row, upper, lower):
    fits, why = contracts.connects(upper.contract("outputs"), lower.contract("inputs"))
    row.compatible = why
    if fits is False:
        row.problems.append(why)


def _compare_given(row, first, step):
    if step.contract("inputs"):
        row.compatible = "what you gave was checked against its inputs="


def _settled_for(call_args, place):
    """call_args with a call that runs elsewhere as its value: a job waits only for
    jobs of its own place, and the value is what reaches another one."""
    from zenthros.remote.call import _client_for

    def where(client):
        return type(client), getattr(client, "place_name", "")

    here = where(_client_for(place))
    return tuple(value.result() if isinstance(value, RemoteCall) and where(value._client) != here else value
                 for value in call_args)


def _check_references(row, call_args, place, probe, sends=False):
    found = reach.answers(call_args, place)
    name = str(getattr(place, "name", "") or place or "this place")
    for ref, answer in found:
        if answer.answer == reach.IMPOSSIBLE and sends:
            row.transfers.append(f"{row.step} sends {upload.describe(ref)} to the bucket, for {name} to read")
        elif answer.answer == reach.IMPOSSIBLE:
            row.problems.append(answer.why)
        elif answer.answer == reach.UNCHECKED:
            row.unchecked.append(answer.why)
    if probe and row.unchecked:
        row.unchecked = _probed(row, [ref for ref, _ in found], place)
    for ref, _ in found:
        if ref.startswith(("s3://", "gs://")):
            row.transfers.append(f"{row.step} reads {ref} from {name}")


def _probed(row, refs, place):
    """Ask place, if it is up now; a place that would have to start is not asked."""
    from zenthros.core.storage import object_counts
    from zenthros.targetlist import targets

    name = str(getattr(place, "name", "") or place or "")
    current = targets()
    target = current.get(name) if name else None
    if target is None or target.template or target.state != "Available":
        return [f"{name or 'this place'} is not up: not asked (it is never started for this)"] + row.unchecked
    uris = [str(ref) for ref in refs if "://" in str(ref)]
    try:
        # The SDK's own function: none of the directory travels with it.
        seen = run(object_counts, uris, on=place, timeout="2m", code_dir=None).result(stream_logs=False)
    except Exception as err:  # noqa: BLE001 - this is the answer
        row.problems.append(f"{name} cannot read them: {str(err).splitlines()[0]}")
        return []
    row.available = "; ".join(f"{uri}: {count} objects seen from {name}" for uri, count in seen.items())
    return []


def _plan_key(row, step, call_args, params, options, place, mode):
    """Whether this step's entry is good, and if so its value for the next."""
    from zenthros.remote.call import _cache_homes, _cache_image, _contracts, _environment, envs

    environment = _environment(options.get("requirements"), options.get("packages"), options.get("tools"))
    kwargs = dict(params or {})
    try:
        note = entries.note_for(step.fn, call_args, kwargs, code_dir=options.get("code_dir", "."),
                                image=_cache_image(options, place), env_key=envs.key(environment) if environment else "",
                                variables=options.get("env") if isinstance(options.get("env"), dict) else None,
                                secret_names=list(options.get("secrets") or ()), gpu=options.get("gpu"),
                                deps=options.get("deps"), contracts=_contracts(options.get("inputs"), options.get("outputs")),
                                mode=mode)
    except entries.CannotKey as err:
        row.reason = f"its cache cannot be used: {err}"
        return None
    row.key = note.key
    note.homes = _cache_homes(place)
    found = entries.explain(note)
    if found is None:
        row.reason = note.why
        return None
    made = found.manifest.get("produced_by") or {}
    call = entries.CachedCall(found, note)
    row.action = REUSE
    row.reason = f"the result of your run of {made.get('at', '?')} on {made.get('where', '?')}"
    row.available = "its bytes are there"
    row.integrity = "same bytes"
    row.origin = "signed by you"
    hows = sorted(set(note.checks.values()))
    if hows:
        row.integrity += f"; its data known {' and '.join(hows)}"
    return call.result(stream_logs=False)
