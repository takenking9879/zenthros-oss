# SPDX-License-Identifier: Apache-2.0

"""What an agent or a script asks of Zenthros: short dictionaries, the same for
`--json` and for MCP. An agent starts a test and goes on working, and later
reads a few lines, not a whole log.

    run       starts it and returns; a snapshot of the code by default for an agent
    status    state, exit code, reason, how long, what it cost, what it left
    logs      only the lines asked for
    cancel    only what its session started (a person: anything of theirs)
    targets   where it can run, with what it costs

exit_code tells a test that failed (1) from a machine that did (none, with a
reason). Retrying is the caller's decision: nothing here retries in secret.
"""

import datetime
import re

from zenthros import agents, cache, contracts, errors, machines, targetlist
from zenthros import env as environments_of
from zenthros.jobs.handle import CANCELLED, FAILED, QUEUED, SUCCEEDED, TERMINAL_PHASES, status_from_object
from zenthros.pipeline import PIPELINE_LABEL, STEP_LABEL
from zenthros.remote import call as remote_call
from zenthros.remote import snapshot as code_snapshot
from zenthros.remote.options import blob_store, job_client, local_client_of, local_clients, settings

# An agent's run ends even if the agent forgets it, and its record lasts while
# it works on what the run said.
AGENT_TIMEOUT = 2 * 3600
AGENT_KEEP_FOR = "7d"
MAX_ARTIFACTS = 20
DEFAULT_TAIL = 2000
DEFAULT_LIMIT = 100


def _client_of(name):
    return local_client_of(name) or job_client()


def _state(phase):
    return str(phase or QUEUED).lower()


def _when(text):
    if not text:
        return None
    return datetime.datetime.fromisoformat(str(text).replace("Z", "+00:00"))


def _took(status):
    started, ended = _when(status.get("startTime")), _when(status.get("completionTime"))
    if started is None:
        return None
    ended = ended or datetime.datetime.now(datetime.timezone.utc)
    return int((ended - started).total_seconds())


def _to_allow(name):
    return f"zenthros agent allow {name} --max-machines 3"


def _allowance(target):
    if not target.costs or target.name in agents.allowed():
        return {"allowed": True}
    return {"allowed": False, "to_allow": _to_allow(target.name)}


def environments():
    """This project's environments, and which is in use."""
    return {"environments": environments_of.to_rows(environments_of.list())}


def cache_entries():
    """What ran and need not run again, and whether each entry is good."""
    return {"entries": cache.ls()}


def targets():
    """One row per place, whether your agents may use it, how you let them, and
    the environments that run there."""
    found = environments_of.list()
    built = [env.name for env in found if env.build]
    published = [env.name for env in found if env.published]
    return [{**row.to_dict(), **_allowance(row),
             "environments": built if row.name in (machines.LOCAL, machines.LOCAL_K8S) else published}
            for row in targetlist.targets()]


def _running_there(client, target):
    return sum(1 for obj in client.list_jobs(f"{agents.TARGET}={target}")
               if (obj.get("status") or {}).get("phase") not in TERMINAL_PHASES)


def _gatekeep(row, client):
    """A session's job on what costs money needs your word, and fits its limit."""
    if not row.costs:
        return
    allowance = agents.allowed().get(row.name)
    if allowance is None:
        raise errors.ValidationError(f"{row.name} costs {targetlist.price_text(row.price)} and your agents may not use it "
                                     f"yet. To allow it: {_to_allow(row.name)}")
    limit = allowance.get("max_machines")
    if limit and _running_there(client, row.name) >= limit:
        raise errors.ValidationError(f"{row.name} already has {limit} job(s) of your agents under way, the most you allow "
                                     f"at a time: wait for one to end, cancel one, or raise it with zenthros agent allow "
                                     f"{row.name} --max-machines N")


def run(command, on=None, snapshot=None, timeout=None, keep=False, dry_run=False, name=None, code_dir=".", image="",
        cpu=None, memory=None, gpu=None, session="", env="", fresh=False):
    """Start command, and return without waiting. A session is an agent's: a
    snapshot by default, a time limit, and your word before anything that
    costs."""
    snapshot = bool(session) if snapshot is None else snapshot
    options = dict(code_dir=code_dir, image=image, env=env or None, cpu=cpu, memory=memory, gpu=gpu, keep=keep, name=name,
                   keep_for=AGENT_KEEP_FOR if session else None)
    if timeout is None and session:
        timeout = AGENT_TIMEOUT
    row = next((found for found in targetlist.targets() if found.name == on), None) if on else None
    if dry_run:
        plan = remote_call.run(command, on=on, dry_run=True, timeout=timeout, **options)
        return {**plan.to_dict(), "allowed": row is None or _allowance(row)}
    labels = {agents.SESSION: session, agents.TARGET: on} if session and on else {agents.SESSION: session} if session else None

    def start():
        return remote_call.run(command, on=on, snapshot=snapshot, timeout=timeout, labels=labels, fresh=fresh, **options)

    if session and row is not None and row.costs:
        with agents.turn():
            _gatekeep(row, job_client())
            call = start()
    else:
        call = start()
    answer = {"job": call.name, "state": _state(QUEUED), "on": on or "local"}
    notes = getattr(call, "code_notes", None)
    if notes:
        answer.update(code=notes[code_snapshot.CODE_SHA256], base=notes[code_snapshot.CODE_BASE],
                      dirty=notes[code_snapshot.CODE_DIRTY] == "true")
    if row is not None and row.untrusted_host:
        answer["warnings"] = [f"{row.name} runs on a third party's host: its owner can read your code and its data"]
    return answer


def _artifacts(client, prefix):
    """What the job left where its outputs go, the first few. The store may be
    out of reach from here: the job's status still answers."""
    try:
        found = blob_store(prefix, settings(), client).ls(prefix.rstrip("/") + "/")
    except (OSError, NotImplementedError, errors.ZenthrosError) as exc:
        return None, str(exc).splitlines()[0] if str(exc) else type(exc).__name__
    folder = prefix.rstrip("/") + "/"
    kept = [item.uri for item in found if remote_call.is_output(item.uri[len(folder):])]
    return kept[:MAX_ARTIFACTS], None


def _output_of(obj):
    return ((((obj.get("spec") or {}).get("workload") or {}).get("template") or {}).get("artifacts") or {}).get("output")


def _saved_log(client, prefix):
    try:
        return blob_store(prefix, settings(), client).get(f"{prefix.rstrip('/')}/{remote_call.OUTPUT_LOG}")
    except (OSError, NotImplementedError, errors.ZenthrosError):
        return None


def _log_text(client, name, tail):
    """The log from the machine, or what the job saved once the machine is gone.
    With neither, the machine's answer is the one said."""
    try:
        return client.logs(name, tail=tail)
    except errors.ZenthrosError:
        prefix = _output_of(client.job_object(name))
        saved = _saved_log(client, prefix) if prefix else None
        if saved is None:
            raise
    return "\n".join(saved.decode("utf-8", "replace").splitlines()[-tail:])


def status(name):
    """A job, in one short dictionary."""
    client = _client_of(name)
    obj = client.job_object(name)
    meta, stored = obj.get("metadata") or {}, obj.get("status") or {}
    read = status_from_object(obj)
    notes = meta.get("annotations") or {}
    answer = {"job": name, "state": _state(read.phase), "exit_code": read.exit_code, "reason": read.reason,
              "message": read.message, "took_s": _took(stored), "code": notes.get(code_snapshot.CODE_SHA256),
              "machine": (stored.get("lease") or {}).get("backendID")}
    labels = meta.get("labels") or {}
    for key, label in (("pipeline", PIPELINE_LABEL), ("step", STEP_LABEL), ("cache_key", cache.KEY_LABEL)):
        if labels.get(label):
            answer[key] = labels[label]
    if read.phase in TERMINAL_PHASES:
        reason = _runner_reason(client, name)
        if reason:
            answer["reason"], answer["message"] = reason
        raw = client.job_cost(name)
        answer["cost_usd"] = (raw or {}).get("cost")
        prefix = _output_of(obj)
        if prefix:
            answer["artifacts"], problem = _artifacts(client, prefix)
            if problem:
                answer["artifacts_error"] = problem
    return answer


def _runner_reason(client, name):
    """(code, what it says) a function's own run ended with (OUTPUT_CONTRACT),
    from what the runner reported; None for the others."""
    try:
        views = client.reports(name)
    except errors.ZenthrosError:
        return None
    for view in views:
        said = view.payload.get(contracts.REPORT)
        if isinstance(said, dict) and said.get("reason"):
            return said["reason"], said.get("message", "")
    return None


def jobs(limit=DEFAULT_LIMIT, cursor=0):
    """The jobs here and in your Workspace, newest first."""
    clients = [job_client()]
    clients += [here for here in local_clients() if here is not clients[0]]
    found, unreachable = [], []
    for client in clients:
        try:
            found += client.list_jobs()
        except errors.ZenthrosError as err:
            unreachable.append(f"{client.transport}: {err}")
    found.sort(key=lambda obj: str((obj.get("metadata") or {}).get("creationTimestamp") or ""), reverse=True)
    if any(cache.PARTS_ANNOTATION in ((obj.get("metadata") or {}).get("annotations") or {}) for obj in found):
        # A job sent with cache= and read by nobody enters when it is seen.
        cache.notice_finished(found, cache.homes_here())
    rows = []
    for obj in found[cursor:cursor + limit]:
        read = status_from_object(obj)
        labels = (obj.get("metadata") or {}).get("labels") or {}
        rows.append({"job": (obj.get("metadata") or {}).get("name"), "state": _state(read.phase), "exit_code": read.exit_code,
                     "reason": read.reason, "created": (obj.get("metadata") or {}).get("creationTimestamp"),
                     **({"pipeline": labels[PIPELINE_LABEL], "step": labels.get(STEP_LABEL, "")} if labels.get(PIPELINE_LABEL) else {})})
    answer = {"jobs": rows, "total": len(found), "next_cursor": cursor + limit if cursor + limit < len(found) else None}
    return {**answer, "unreachable": unreachable} if unreachable else answer


def logs(name, tail=DEFAULT_TAIL, grep=None, limit=DEFAULT_LIMIT, cursor=0):
    """The log's last tail lines, those that match grep (a regular expression),
    limit at a time."""
    pattern = None
    if grep:
        try:
            pattern = re.compile(grep)
        except re.error as exc:
            raise errors.ValidationError(f"grep={grep!r} is not a regular expression: {exc}") from None
    lines = _log_text(_client_of(name), name, tail).splitlines()
    if pattern:
        lines = [line for line in lines if pattern.search(line)]
    end = cursor + limit
    return {"job": name, "lines": lines[cursor:end], "total": len(lines), "next_cursor": end if end < len(lines) else None}


def cancel(name, session=""):
    """Stop a job. A session stops only what it started."""
    client = _client_of(name)
    if session:
        labels = (client.job_object(name).get("metadata") or {}).get("labels") or {}
        if labels.get(agents.SESSION) != session:
            raise errors.ValidationError(f"{name} was started by another session or by a person: this one does not stop it")
    client.cancel(name)
    return {"job": name, "cancelled": True}


def exit_code_of(answer):
    """The exit code a command line gives for a job that ended: its process's;
    130 cancelled; 1 failed with none; 0 succeeded."""
    state = answer.get("state")
    if answer.get("exit_code") is not None:
        return answer["exit_code"]
    if state == _state(CANCELLED):
        return 130
    return 0 if state == _state(SUCCEEDED) else 1 if state == _state(FAILED) else 125
