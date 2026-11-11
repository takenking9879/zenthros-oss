# SPDX-License-Identifier: Apache-2.0

"""In-process job client for W4. No Ray, no cluster, no zctl."""

from datetime import datetime, timezone

from zenthros import errors
from zenthros.jobs.client import JobClient, notify_reschedule, require_job
from zenthros.jobs.handle import (
    CANCELLED,
    FAILED,
    PENDING,
    QUEUED,
    RUNNING,
    SERVICE_READY,
    SUCCEEDED,
    WORKER_LOST,
    JobHandle,
    JobStatus,
    ReportView,
    as_job_status,
    endpoints_from_status,
)


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def improving_reports(job_id):
    """Deterministic improving metrics + not-significant then significant pair.

    Pair A: 70/100 vs 73/100 is not significant (two-proportion z).
    Pair B: 70/100 vs 90/100 is significant.
    """
    rows = (
        ("baseline", 0.70, 70, False),
        ("candidate_ns", 0.73, 73, False),
        ("improving", 0.85, 85, True),
        ("candidate_sig", 0.90, 90, True),
    )
    views = []
    for index, (label, accuracy, n_correct, versus_baseline_sig) in enumerate(rows):
        views.append(
            ReportView(
                job_id=job_id,
                producer_id="fake-worker",
                payload={
                    "label": label,
                    "metric": "accuracy",
                    "accuracy": accuracy,
                    "n": 100,
                    "n_correct": n_correct,
                    "versus_baseline_significant": versus_baseline_sig,
                    "step": index + 1,
                },
                artifacts=[],
                final=index == len(rows) - 1,
                produced_at=_utc_now(),
                report_id=f"{job_id}-r{index + 1}",
                sequence=index + 1,
            )
        )
    return views


class FakeJobClient(JobClient):
    """Complete fake so W4 can wrap the SDK without Ray."""

    def __init__(self):
        self._jobs = {}
        self._order = []

    def submit(self, job):
        job = require_job(job)
        if job.name in self._jobs:
            raise errors.ValidationError(f"job {job.name} already exists")
        self._jobs[job.name] = {
            "phase": QUEUED,
            "reports": improving_reports(job.name),
            "logs": f"fake job {job.name}\n",
            "failed": False,
            "job": job,
        }
        self._order.append(job.name)
        return JobHandle(job.name, self)

    def job_status(self, job_id):
        job = self._require(job_id)
        script = job.get("script")
        if script:
            idx = min(int(job.get("script_index") or 0), len(script) - 1)
            snap = as_job_status(script[idx])
            job["phase"] = snap.phase
            return snap
        if job["phase"] == QUEUED:
            job["phase"] = RUNNING
        return JobStatus(
            phase=job["phase"],
            reason=str(job.get("reason") or ""),
            generation=int(job.get("generation") or 0),
            progress_phase=str(job.get("progress_phase") or ""),
            worker_lost_retries=int(job.get("worker_lost_retries") or 0),
            message=str(job.get("message") or ""),
            reasons=list(job.get("reasons") or []),
            conditions=list(job.get("conditions") or []),
            endpoints=endpoints_from_status({"endpoints": job.get("endpoints") or []}),
            lifecycle="Service" if job["job"].spec.workload.template.service else "",
        )

    def wait(self, job_id, timeout=None, on_reschedule=None, cancel_on_timeout=False):
        job = self._require(job_id)
        script = job.get("script")
        if script:
            return self._wait_script(job_id, script, on_reschedule)
        if job["phase"] == CANCELLED:
            raise errors.JobCancelled(f"job {job_id} cancelled")
        if job["failed"] or job["phase"] == FAILED:
            job["phase"] = FAILED
            raise errors.JobFailed(f"job {job_id} failed", reasons=job.get("reasons"))
        job["phase"] = SUCCEEDED
        return SUCCEEDED

    def _wait_script(self, job_id, script, on_reschedule):
        job = self._require(job_id)
        seen = set()
        last = None
        for index, step in enumerate(script):
            job["script_index"] = index
            snap = as_job_status(step)
            last = snap
            job["phase"] = snap.phase
            job["reason"] = snap.reason
            job["generation"] = snap.generation
            job["progress_phase"] = snap.progress_phase
            job["worker_lost_retries"] = snap.worker_lost_retries
            notify_reschedule(snap, on_reschedule, seen)
            if snap.phase == CANCELLED:
                raise errors.JobCancelled(f"job {job_id} cancelled")
            if snap.phase == FAILED:
                raise errors.JobFailed(f"job {job_id} failed")
            if snap.phase == SUCCEEDED:
                return SUCCEEDED
        if last is not None and last.phase == SUCCEEDED:
            return SUCCEEDED
        job["phase"] = SUCCEEDED
        return SUCCEEDED

    def script_reschedule(self, job_id, progress_phase="train_epoch", reason=WORKER_LOST):
        """Inject Pending + reason, generation bump, then Running, then Succeeded."""
        self._require(job_id)["script"] = reschedule_success_script(
            progress_phase=progress_phase, reason=reason
        )
        self._require(job_id)["script_index"] = 0

    def script_worker_lost_fail(self, job_id, reason=WORKER_LOST):
        """onWorkerLost=Fail: terminal Failed + WORKER_LOST, no bump wait."""
        self._require(job_id)["script"] = [
            JobStatus(phase=RUNNING, generation=1),
            JobStatus(phase=FAILED, reason=reason, generation=1),
        ]
        self._require(job_id)["script_index"] = 0

    def script_retry_limit_exhausted(self, job_id, limit=3, progress_phase="train_epoch"):
        """Reschedule until workerLostRetryLimit, then Failed + WORKER_LOST."""
        self._require(job_id)["script"] = retry_limit_script(
            limit=limit, progress_phase=progress_phase
        )
        self._require(job_id)["script_index"] = 0

    def cancel(self, job_id):
        job = self._require(job_id)
        job["phase"] = CANCELLED

    def delete(self, job_id):
        self._require(job_id)
        del self._jobs[job_id]

    def logs(self, job_id, tail=None):
        text = self._require(job_id)["logs"]
        return text if tail is None else "\n".join(text.splitlines()[-int(tail):])

    def reports(self, job_id=None):
        if job_id is None:
            if not self._order:
                return []
            job_id = self._order[-1]
        return list(self._require(job_id)["reports"])


    def list_jobs(self, selector=""):
        """The jobs as stored; selector is k=v (equality only, as tests ask)."""
        key, _, value = selector.partition("=")
        return [self.job_object(name) for name in self._order
                if name in self._jobs and (not selector or self._jobs[name]["job"].labels.get(key) == value)]

    def job_object(self, job_id):
        if job_id not in self._jobs:
            raise errors.JobNotFound(f"job {job_id} not found")
        job = self._jobs[job_id]
        status = {"phase": job["phase"]}
        if job.get("endpoints"):
            status["endpoints"] = [dict(e) for e in job["endpoints"]]
        if job.get("results"):
            status["results"] = [dict(r) for r in job["results"]]
        if job.get("progress"):
            status["progress"] = dict(job["progress"])
        manifest = job["job"].to_manifest()
        manifest["metadata"]["uid"] = f"uid-{job_id}"
        return {**manifest, "status": status}

    def publish(self, job_id, endpoints, ready=True, reason="EndpointsPublished", message=""):
        """Test helper: the Service's status as the controller sets it:
        endpoints [{name, url, ...}] and the ServiceReady condition."""
        job = self._require(job_id)
        job["endpoints"] = [dict(e) for e in endpoints]
        job["conditions"] = [{"type": SERVICE_READY, "status": "True" if ready else "False",
                              "reason": reason, "message": message}]

    def submitted(self, job_id):
        """Test helper: the Job submitted as job_id."""
        return self._require(job_id)["job"]

    def fail(self, job_id, *codes):
        """Test helper: next wait() raises JobFailed, with these reason codes
        (PROCESS_FAILED: the code; IMAGE_PULL_FAILED...: the platform)."""
        job = self._require(job_id)
        job["failed"] = True
        job["reasons"] = [{"code": code} for code in codes]

    def _require(self, job_id):
        if job_id not in self._jobs:
            raise errors.ValidationError(f"unknown job {job_id}")
        return self._jobs[job_id]


def reschedule_success_script(progress_phase="train_epoch", reason=WORKER_LOST):
    return [
        JobStatus(phase=RUNNING, generation=1, progress_phase=progress_phase),
        JobStatus(
            phase=PENDING,
            reason=reason,
            generation=2,
            progress_phase=progress_phase,
        ),
        JobStatus(phase=RUNNING, generation=2, progress_phase=progress_phase),
        JobStatus(phase=SUCCEEDED, generation=2, progress_phase=progress_phase),
    ]


def retry_limit_script(limit=3, progress_phase="train_epoch"):
    steps = [JobStatus(phase=RUNNING, generation=1, progress_phase=progress_phase)]
    bound = max(int(limit), 0)
    for gen in range(2, bound + 2):
        steps.append(
            JobStatus(
                phase=PENDING,
                reason=WORKER_LOST,
                generation=gen,
                progress_phase=progress_phase,
                worker_lost_retries=gen - 1,
            )
        )
        if gen <= bound:
            steps.append(
                JobStatus(
                    phase=RUNNING,
                    generation=gen,
                    progress_phase=progress_phase,
                    worker_lost_retries=gen - 1,
                )
            )
        else:
            steps.append(
                JobStatus(
                    phase=FAILED,
                    reason=WORKER_LOST,
                    generation=gen,
                    progress_phase=progress_phase,
                    worker_lost_retries=gen - 1,
                )
            )
    return steps
