# SPDX-License-Identifier: Apache-2.0

"""JobHandle and report view types."""

import calendar
import time
from dataclasses import dataclass, field
from typing import Optional

# Lifecycle phases as returned by public zctl get (and the fake).
QUEUED = "Queued"
PENDING = "Pending"
RUNNING = "Running"
SUCCEEDED = "Succeeded"
FAILED = "Failed"
CANCELLED = "Cancelled"

TERMINAL_PHASES = frozenset({SUCCEEDED, FAILED, CANCELLED})


def seconds_at(stamp):
    """Epoch seconds of a Kubernetes timestamp (2026-10-19T08:00:00Z); None when unset."""
    try:
        return calendar.timegm(time.strptime(stamp or "", "%Y-%m-%dT%H:%M:%SZ"))
    except ValueError:
        return None

# Non-terminal reasons: wait on the same handle. Failed + these is JobFailed.
WORKER_LOST = "WORKER_LOST"
PREEMPTED = "PREEMPTED"
MACHINE_LIFETIME = "MACHINE_LIFETIME"
RESCHEDULE_REASONS = frozenset({WORKER_LOST, PREEMPTED, MACHINE_LIFETIME})

# Public log API is byte-capped (same order as report payload).
MAX_LOG_BYTES = 256 * 1024


@dataclass
class ReportView:
    """One ExecutionReport snapshot as read by JobHandle.reports()."""

    job_id: str
    producer_id: str
    payload: dict
    artifacts: list = field(default_factory=list)
    final: bool = False
    produced_at: str = ""
    report_id: str = ""
    sequence: int = 0


# A Service's condition: True while every endpoint it declares is published.
SERVICE_READY = "ServiceReady"


@dataclass
class Endpoint:
    """Where a running Service answers (status.endpoints). url is the run's:
    a re-run or a replacement machine publishes another."""

    name: str = ""
    url: str = ""
    port: int = 0
    scope: str = ""  # Public | Private | Cluster
    backend: str = ""
    attempt: int = 0
    ready_at: str = ""


def endpoints_from_status(status):
    return [Endpoint(name=str(e.get("name") or ""), url=str(e.get("url") or ""), port=int(e.get("port") or 0),
                     scope=str(e.get("scope") or ""), backend=str(e.get("backend") or ""),
                     attempt=int(e.get("attempt") or 0), ready_at=str(e.get("readyAt") or ""))
            for e in (status.get("endpoints") or []) if isinstance(e, dict)]


@dataclass
class JobStatus:
    """One poll of a job. status() stays a phase string for callers.

    message, reasons, conditions and members are the job's status as
    stored, word for word (zctl get zj NAME -o json).
    """

    phase: str = QUEUED
    reason: str = ""
    generation: int = 0
    progress_phase: str = ""
    worker_lost_retries: int = 0
    message: str = ""
    reasons: list = field(default_factory=list)
    conditions: list = field(default_factory=list)
    members: list = field(default_factory=list)
    # children: the groups this job spawned (status.children), each with
    # its children's phases and words.
    children: list = field(default_factory=list)
    restarts: int = 0
    # A Service's: where it answers now, and whether every endpoint is
    # published (the ServiceReady condition, whose reason says why not).
    endpoints: list = field(default_factory=list)
    lifecycle: str = ""
    # cleanup_pending: the job reads Cancelled while its backend may still
    # run the workload (the controller has not cancelled it there yet; the
    # Cleanup condition says why, when the backend refused or cannot be
    # reached).
    cleanup_pending: bool = False
    # What the workload's process returned, once it ended and returned one: a
    # machine that was lost has none (reason says so).
    exit_code: Optional[int] = None

    def is_reschedule_wait(self):
        if self.phase in TERMINAL_PHASES:
            return False
        return self.reason in RESCHEDULE_REASONS

    def service_ready(self):
        """The ServiceReady condition as a dict, or None."""
        return next((c for c in self.conditions if isinstance(c, dict) and c.get("type") == SERVICE_READY), None)


def as_job_status(value):
    if isinstance(value, JobStatus):
        return value
    if isinstance(value, dict):
        return JobStatus(
            phase=str(value.get("phase") or QUEUED),
            reason=str(value.get("reason") or ""),
            generation=int(value.get("generation") or 0),
            progress_phase=str(value.get("progress_phase") or ""),
            worker_lost_retries=int(value.get("worker_lost_retries") or 0),
        )
    if value is None or value == "":
        return JobStatus(phase=QUEUED)
    return JobStatus(phase=str(value))


class JobHandle:
    """Lifecycle handle. Concrete clients implement the methods."""

    def __init__(self, job_id, client):
        self.job_id = job_id
        self._client = client

    def status(self):
        return self._client.status(self.job_id)

    def job_status(self):
        return as_job_status(self._client.job_status(self.job_id))

    def wait(self, timeout=None, on_reschedule=None, cancel_on_timeout=False):
        """Block until the job ends. Past timeout, raise WaitTimeout; the job
        keeps running unless cancel_on_timeout."""
        if cancel_on_timeout:
            return self._client.wait(
                self.job_id, timeout=timeout, on_reschedule=on_reschedule,
                cancel_on_timeout=True,
            )
        return self._client.wait(
            self.job_id, timeout=timeout, on_reschedule=on_reschedule
        )

    def cancel(self):
        """Stop the job and keep it as Cancelled."""
        return self._client.cancel(self.job_id)

    def delete(self):
        """Remove the job; its workload is cancelled first."""
        return self._client.delete(self.job_id)

    def logs(self, tail=None):
        if tail is None:
            return self._client.logs(self.job_id)
        return self._client.logs(self.job_id, tail=tail)

    def reports(self):
        """ExecutionReport snapshots. This is the metric read path."""
        return self._client.reports(self.job_id)

    def artifacts(self):
        return self._client.artifacts(self.job_id)


def status_from_object(obj):
    """JobStatus from a stored ZenthrosJob (zctl get zj NAME -o json)."""
    status = obj.get("status") or {}
    reasons = [r for r in (status.get("reasons") or []) if isinstance(r, dict)]
    progress = status.get("progress") or {}
    return JobStatus(
        phase=str(status.get("phase") or QUEUED),
        reason=str(reasons[0].get("code") or "") if reasons else "",
        generation=int((obj.get("metadata") or {}).get("generation") or 0),
        progress_phase=str(progress.get("phase") or ""),
        worker_lost_retries=int(status.get("workerLostRetries") or 0),
        message=str(status.get("message") or ""),
        reasons=reasons,
        conditions=list(status.get("conditions") or []),
        members=list(status.get("members") or []),
        children=list(status.get("children") or []),
        restarts=int(status.get("restarts") or 0),
        endpoints=endpoints_from_status(status),
        lifecycle=str(status.get("lifecycle") or ""),
        cleanup_pending=_cleanup_pending(obj, status),
        exit_code=status.get("exitCode"),
    )


# The controller's marks that a cancelled job's workload is gone
# (api/v1 CleanupPending reads the same two).
WORKLOAD_CLEANED = "zenthros.ai/gc-workload-cleaned"
ORPHAN_REAPED = "zenthros.ai/gc-orphan-reaped"


def _cleanup_pending(obj, status):
    if status.get("phase") != CANCELLED:
        return False
    annotations = (obj.get("metadata") or {}).get("annotations") or {}
    return WORKLOAD_CLEANED not in annotations and ORPHAN_REAPED not in annotations
