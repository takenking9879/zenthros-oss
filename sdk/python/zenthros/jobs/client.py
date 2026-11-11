# SPDX-License-Identifier: Apache-2.0

"""Public job client: wrap zctl. No controller package imports."""

import json
import os
import tempfile
import time

from zenthros import errors, zctl
from zenthros.jobs.handle import (
    CANCELLED,
    FAILED,
    MAX_LOG_BYTES,
    SUCCEEDED,
    TERMINAL_PHASES,
    JobHandle,
    ReportView,
    as_job_status,
    status_from_object,
)
from zenthros.jobs.spec import Job
from zenthros.logging import create_logger
from zenthros.zctl import Zctl

logger = create_logger("zenthros.jobs", user=True)

DEFAULT_NAMESPACE = "tenant-default"
# How long wait() takes a job it cannot find for one not there yet (read
# just after its create). After that, JobNotFound ends the wait: a job the
# controller collected never comes back (audit 2026-10-03 SDK-4).
JOB_NOT_FOUND_GRACE = 15.0

# A wait warns when its store keeps failing: after TRANSIENT_WARN_AFTER
# seconds of failures, then every TRANSIENT_WARN_EVERY.
TRANSIENT_WARN_AFTER = 60.0
TRANSIENT_WARN_EVERY = 300.0

# The longest one ask of a client that waits for a change holds: zenthros-api
# answers a wait after 25 s at most (pkg/api wait.go).
WAIT_FOR_CHANGE = 25.0


def clip_log(raw, tail=None):
    """raw (bytes) as text, at most MAX_LOG_BYTES: its start for a whole log,
    its end for a tail (audit 2026-10-03 SDK-10)."""
    if len(raw) > MAX_LOG_BYTES:
        raw = raw[-MAX_LOG_BYTES:] if tail is not None else raw[:MAX_LOG_BYTES]
    text = raw.decode("utf-8", "replace")
    if tail is not None:
        text = "\n".join(text.splitlines()[-int(tail):]) if int(tail) > 0 else ""
    return text


def require_job(job):
    """submit() takes a typed zenthros.jobs.spec.Job, nothing else: the one
    description of a job, checked field by field against the CRD."""
    if not isinstance(job, Job):
        raise errors.ValidationError(
            f"submit takes a zenthros.jobs.spec.Job, not {type(job).__name__}; "
            "for a script, zenthros.run(['python', 'x.py'], on=...)")
    if not job.name:
        raise errors.ValidationError("submit: the Job needs a name")
    return job


class JobClient:
    """Submit/status/wait/cancel/logs/reports/artifacts. wait() is written
    once, on job_status(), reports() and cancel()."""

    poll_seconds = 2.0
    # What reaches Zenthros, named in wait()'s last error.
    transport = "zctl"
    # The Workspace's own store, which only zenthros-api has.
    store = ""

    def submit(self, spec):
        raise NotImplementedError

    def store_access(self, backend=""):
        """(env, secrets) a job needs for the Workspace's store: none where
        the Workspace has no store of its own."""
        return {}, []

    def job_cost(self, name):
        """What the job cost so far, or None where nothing holds prices."""
        return None

    def status(self, job_id):
        """The job's phase. A client defines status() or job_status()."""
        if type(self).job_status is JobClient.job_status:
            raise NotImplementedError
        return self.job_status(job_id).phase

    # Whether _status_since waits for a change itself (HTTPJobClient's long
    # poll): then wait() asks again at once, and sleeps poll_seconds only
    # after a failed ask.
    waits_for_change = False

    def _status_since(self, job_id, seen, wait):
        """The job's status and a token of it to send back as seen. This
        client answers at once; one that is told of a change
        (waits_for_change) answers when the job is other than seen names, or
        after at most wait seconds."""
        return self.job_status(job_id), None

    def wait(self, job_id, timeout=None, on_reschedule=None, cancel_on_timeout=False):
        """Block until the job finishes.

        Process completion: prefer the ZenthrosJob phase. A trainer may
        publish ``final=true`` mid-job, so a report snapshot alone is not
        success while the job is still Running. A client that may not reach
        the job (zctl with a controller address and no kube, a VM-only
        client) says so with _final_report_decides(): then, while the phase
        is unknown, a final report is success.

        Pending/Queued/Running plus WORKER_LOST or PREEMPTED is Reschedule
        on the same job (generation bump). Do not raise JobFailed. Terminal
        Failed (Fail policy or retry-limit) still raises JobFailed.
        """
        started = time.monotonic()
        deadline = None if timeout is None else started + float(timeout)
        seen = set()
        logged = None
        last_error = None
        failing_since = warned_at = None
        token = ""
        while True:
            phase = None
            snap = None
            asked = False
            try:
                left = WAIT_FOR_CHANGE if deadline is None else max(1.0, min(WAIT_FOR_CHANGE, deadline - time.monotonic()))
                status, token = self._status_since(job_id, token, left)
                snap = as_job_status(status)
                phase = snap.phase
                asked = True
                failing_since = warned_at = None
            except errors.JobNotFound as err:
                if time.monotonic() - started >= JOB_NOT_FOUND_GRACE:
                    raise
                last_error = err
                phase = None
                snap = None
            except errors.TransientError as err:
                # Keep waiting, but a refusal that never clears is named:
                # a warning after a minute and every five after that, as a
                # wait with no timeout never ends (audit 2026-10-16 AUD-040).
                last_error = err
                phase = None
                snap = None
                now = time.monotonic()
                failing_since = failing_since or now
                if now - failing_since >= TRANSIENT_WARN_AFTER and (warned_at is None or now - warned_at >= TRANSIENT_WARN_EVERY):
                    logger.warning(f"job {job_id}: {self.transport} has failed for {int(now - failing_since)}s, still waiting: {err}")
                    warned_at = now
            if self._final_report_decides():
                try:
                    views = self.reports(job_id)
                    if any(getattr(view, "final", False) for view in views):
                        if phase is None or phase == SUCCEEDED:
                            return SUCCEEDED
                except (errors.TransientError, errors.JobNotFound):
                    pass
            notify_reschedule(snap, on_reschedule, seen)
            if snap is not None and snap.is_reschedule_wait():
                state = (snap.generation, snap.reason, snap.progress_phase)
            else:
                state = None
            # Once per state: a reschedule waits for minutes (the old
            # machine's end), and every poll would log the same line.
            if state is not None and state != logged:
                logger.info(
                    f"Job {job_id} reschedule wait generation={snap.generation} "
                    f"reason={snap.reason} progress_phase={snap.progress_phase}"
                )
            logged = state
            if phase in TERMINAL_PHASES:
                if phase == FAILED:
                    message = getattr(snap, "message", "") or f"job {job_id} failed"
                    raise errors.JobFailed(message, reasons=getattr(snap, "reasons", []), status=snap)
                if phase == CANCELLED:
                    raise errors.JobCancelled(getattr(snap, "message", "") or f"job {job_id} cancelled")
                return phase
            if deadline is not None and time.monotonic() >= deadline:
                if cancel_on_timeout:
                    self.cancel(job_id)
                raise errors.WaitTimeout(
                    f"job {job_id} still {phase or 'unknown'} after {timeout}s"
                    + ("; cancelled" if cancel_on_timeout else "")
                    + (f"; last {self.transport} error: {last_error}" if phase is None and last_error is not None else ""),
                    status=snap,
                )
            if not (self.waits_for_change and asked):
                time.sleep(self.poll_seconds)

    def job_status(self, job_id):
        return as_job_status(self.status(job_id))

    def job_object(self, job_id):
        """The ZenthrosJob as stored, a dict (metadata, spec, status): what
        get() reads a call's URI and pinned digest from."""
        raise NotImplementedError

    def source_of(self, job_id):
        """A job's code as it was sent (a gzipped tar in base64), or "" if it has none."""
        template = self.job_object(job_id)["spec"]["workload"]["template"]
        return (template.get("source") or {}).get("inline") or ""

    def cursor_of(self, job_id):
        """The last checkpoint confirmed for an ended job, or None."""
        return (self.job_object(job_id).get("status") or {}).get("progress")

    def list_jobs(self, selector=""):
        raw = self._run(["get", "zenthrosjob", *self._ns(), "-o", "json", *(["-l", selector] if selector else [])])
        return json.loads(raw).get("items") or []

    def _final_report_decides(self):
        """True when this client may be unable to read the job's phase, so a
        final report stands in for it."""
        return False

    def cancel(self, job_id):
        """Stop the job and keep it: it ends Cancelled with its history."""
        raise NotImplementedError

    def delete(self, job_id):
        """Remove the job; its workload is cancelled first."""
        raise NotImplementedError

    def logs(self, job_id, tail=None):
        """The job's log; tail=N, its last N lines (the end of a log too long
        to read whole, where the error is)."""
        raise NotImplementedError

    def reports(self, job_id):
        raise NotImplementedError

    def artifacts(self, job_id):
        """Every artifact the job's reports name."""
        found = []
        for view in self.reports(job_id):
            found.extend(view.artifacts)
        return found



class ZctlJobClient(JobClient):
    """Shells out to public zctl. No controller package imports."""

    def __init__(self, zctl=None, namespace=None, controller=None, poll_seconds=2.0):
        self._run_zctl = Zctl(zctl)
        self.zctl = self._run_zctl.binary
        # Empty: zctl picks the namespace (ZENTHROS_NAMESPACE, the zctl
        # context, tenant-default), so the SDK and zctl never disagree.
        self.namespace = namespace or os.environ.get("ZENTHROS_NAMESPACE", "")
        self.controller = controller or ""
        self.poll_seconds = poll_seconds

    def submit(self, job):
        """Create the job. The whole spec, typed, is written as JSON, which
        zctl reads as YAML."""
        job = require_job(job)
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
            tmp.write(job.to_json(self.namespace))
        args = ["create", "zenthrosjob", "-f", tmp.name, *self._ns(job.namespace)]
        if job.spec.workspace:
            args.extend(["--workspace", job.spec.workspace])
        try:
            self._run(args)
            logger.info(f"Submitted job {job.name} workspace={job.spec.workspace}")
        except errors.ZctlError as err:
            # A create zctl could not say the end of (a timeout, an exit
            # that is no class) may have made the job: said so, its machine
            # stays for it (remote.call._placed_call). A transient exit made
            # nothing.
            if err.returncode != zctl.EXIT_TRANSIENT:
                err.job = job.name
                err.args = (f"job {job.name}: {err}; it may have been created: "
                            f"zenthros.get({job.name!r}) finds it if it was",)
            raise
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
        return JobHandle(job.name, self)

    def job_status(self, job_id):
        """The job's status as stored: message, reasons, conditions and
        members word for word."""
        return status_from_object(self.job_object(job_id))

    def job_object(self, job_id):
        raw = self._run(["get", "zenthrosjob", *self._ns(), "-o", "json", job_id])
        try:
            return json.loads(raw)
        except json.JSONDecodeError as err:
            raise errors.TransientError(f"zctl get -o json returned non-JSON: {err}") from err

    def _final_report_decides(self):
        # Reports come from the gateway (--controller); the job may not be
        # readable from here.
        return bool(self.controller)

    def cancel(self, job_id):
        self._run(["cancel", "zenthrosjob", *self._ns(), job_id])

    def delete(self, job_id):
        self._run(["delete", "zenthrosjob", *self._ns(), job_id])

    def logs(self, job_id, tail=None):
        args = ["logs", *self._ns()]
        args.extend(self._controller_flags())
        if tail is not None:
            args.extend(["--tail", str(int(tail))])
        args.append(job_id)
        return clip_log(self._run(args).encode("utf-8"), tail)

    def follow_logs(self, job_id):
        """zctl logs --watch as a running process: iterate .stdout for lines.
        It exits when the log ends; the caller stops it with terminate()."""
        return self._run_zctl.popen("logs", *self._ns(), *self._controller_flags(), "--watch", job_id)

    def reports(self, job_id):
        args = ["get", "report", *self._ns()]
        args.extend(self._controller_flags())
        args.append(job_id)
        raw = self._run(args)
        return parse_report_body(job_id, raw)


    def _ns(self, namespace=""):
        ns = namespace or self.namespace
        return ["-n", ns] if ns else []

    def _controller_flags(self):
        # zctl reads ZENTHROS_CONTROLLER / ZENTHROS_TLS_* and the zctl context
        # itself; only an address passed to this client is forwarded.
        return ["--controller", self.controller] if self.controller else []

    def _run(self, args):
        return self._run_zctl(*args)


def parse_report_body(job_id, raw):
    """Turn `zctl get report` JSON (object or array) into ReportView rows."""
    if not raw or not str(raw).strip():
        return []
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as err:
        raise errors.TransientError(f"zctl get report returned non-JSON: {err}") from err
    if isinstance(body, list):
        items = body
    elif isinstance(body, dict):
        items = [body] if body else []
    else:
        raise errors.TransientError("zctl get report returned a non-object")
    views = []
    for item in items:
        if not isinstance(item, dict):
            continue
        views.append(
            ReportView(
                job_id=str(item.get("job_id") or job_id),
                producer_id=str(item.get("producer_id") or ""),
                payload=item.get("payload") if isinstance(item.get("payload"), dict) else {},
                artifacts=list(item.get("artifacts") or []),
                final=bool(item.get("final")),
                produced_at=str(item.get("produced_at") or ""),
                report_id=str(item.get("report_id") or ""),
                sequence=int(item.get("sequence") or 0),
            )
        )
    return views


def notify_reschedule(status, on_reschedule, seen):
    """Fire on_reschedule once per (generation, reason) while non-terminal."""
    if status is None or on_reschedule is None:
        return
    if not status.is_reschedule_wait():
        return
    key = (int(status.generation or 0), status.reason)
    if key in seen:
        return
    seen.add(key)
    on_reschedule(status)


def submit(job, client=None):
    """submit(job) -> JobHandle. job is a zenthros.jobs.spec.Job; the default
    client is ZctlJobClient."""
    return (client or ZctlJobClient()).submit(job)
