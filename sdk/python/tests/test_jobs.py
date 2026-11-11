#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

import unittest

from zenthros import errors
from zenthros.jobs import FakeJobClient, JobHandle
from zenthros.jobs import spec as S
from zenthros.jobs.client import JobClient
from zenthros.jobs.handle import CANCELLED, PENDING, PREEMPTED, SUCCEEDED, WORKER_LOST, JobStatus, as_job_status


def job(name):
    return S.Job(name=name, spec=S.Spec(workspace="ws-a"))


class FakeJobLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeJobClient()

    def test_submit_takes_only_a_typed_job(self):
        # One description of a job: the typed spec, checked against the CRD.
        for loose in ({"name": "job-1", "workspace_id": "ws-a"}, "job-1", None):
            with self.assertRaises(errors.ValidationError):
                self.client.submit(loose)
        with self.assertRaises(errors.ValidationError):
            self.client.submit(S.Job(name=""))

    def test_submit_status_wait_reports(self):
        handle = self.client.submit(job("job-1"))
        self.assertIsInstance(handle, JobHandle)
        self.assertEqual(handle.status(), "Running")
        self.assertEqual(handle.wait(), SUCCEEDED)
        views = handle.reports()
        self.assertGreaterEqual(len(views), 2)
        accuracies = [view.payload["accuracy"] for view in views]
        self.assertEqual(accuracies, sorted(accuracies))
        self.assertTrue(views[-1].final)

    def test_cancel(self):
        handle = self.client.submit(job("job-3"))
        handle.cancel()
        self.assertEqual(handle.status(), CANCELLED)
        with self.assertRaises(errors.JobCancelled):
            handle.wait()

    def test_fake_cancel_keeps_job_delete_removes_it(self):
        handle = self.client.submit(job("job-keep"))
        handle.cancel()
        self.assertEqual(handle.status(), CANCELLED)
        handle.delete()
        with self.assertRaises(Exception):
            handle.status()

    def test_fail(self):
        handle = self.client.submit(job("job-4"))
        self.client.fail("job-4")
        with self.assertRaises(errors.JobFailed):
            handle.wait()


class ParseReportBodyTest(unittest.TestCase):
    def test_empty_object_is_no_reports(self):
        from zenthros.jobs.client import parse_report_body

        self.assertEqual(parse_report_body("zj-1", "{}"), [])
        self.assertEqual(parse_report_body("zj-1", ""), [])

    def test_single_object(self):
        from zenthros.jobs.client import parse_report_body

        views = parse_report_body(
            "zj-1",
            '{"job_id":"zj-1","producer_id":"head","payload":{"value":0.33},"final":true}',
        )
        self.assertEqual(len(views), 1)
        self.assertEqual(views[0].producer_id, "head")
        self.assertEqual(views[0].payload["value"], 0.33)
        self.assertTrue(views[0].final)

    def test_array_from_multiple_producers(self):
        from zenthros.jobs.client import parse_report_body

        views = parse_report_body(
            "zj-1",
            '[{"job_id":"zj-1","producer_id":"head","payload":{"value":0.33},"final":true},'
            '{"job_id":"zj-1","producer_id":"worker","payload":{"n":150},"final":false}]',
        )
        self.assertEqual([view.producer_id for view in views], ["head", "worker"])
        self.assertEqual(views[0].payload["value"], 0.33)
        self.assertEqual(views[1].payload["n"], 150)


class GatewayWaitTest(unittest.TestCase):
    def test_wait_uses_final_report_when_cr_unreachable(self):
        from zenthros.jobs.client import ZctlJobClient
        from zenthros.jobs.handle import SUCCEEDED, ReportView

        client = ZctlJobClient(controller="https://gateway.example", poll_seconds=0)
        client.reports = lambda job_id: [
            ReportView(job_id=job_id, producer_id="ray", payload={"value": 0.9}, final=True)
        ]

        def no_kube(job_id):
            raise errors.TransientError("zctl get zenthrosjob: no kube")

        client.job_status = no_kube
        self.assertEqual(client.wait("zj-1"), SUCCEEDED)


    def test_wait_does_not_succeed_on_final_report_while_cr_running(self):
        from zenthros.jobs.client import ZctlJobClient
        from zenthros.jobs.handle import SUCCEEDED, ReportView

        client = ZctlJobClient(controller="https://gateway.example", poll_seconds=0)
        client.reports = lambda job_id: [
            ReportView(job_id=job_id, producer_id="ray", payload={"value": 0.9}, final=True)
        ]
        phases = iter(["Running", SUCCEEDED])
        client.job_status = lambda job_id: as_job_status(next(phases))
        self.assertEqual(client.wait("zj-1"), SUCCEEDED)

    def test_wait_reschedule_pending_does_not_fail(self):
        from zenthros.jobs.client import ZctlJobClient

        client = ZctlJobClient(poll_seconds=0)
        snaps = iter(
            [
                JobStatus(
                    phase=PENDING,
                    reason=WORKER_LOST,
                    generation=2,
                    progress_phase="train_epoch",
                ),
                JobStatus(phase="Running", generation=2, progress_phase="train_epoch"),
                JobStatus(phase=SUCCEEDED, generation=2),
            ]
        )
        events = []
        client.job_status = lambda job_id: next(snaps)
        self.assertEqual(client.wait("zj-1", on_reschedule=events.append), SUCCEEDED)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].reason, WORKER_LOST)
        self.assertEqual(events[0].generation, 2)

    def test_a_long_reschedule_wait_is_logged_once(self):
        from zenthros.jobs.client import ZctlJobClient

        client = ZctlJobClient(poll_seconds=0)
        waiting = JobStatus(phase=PENDING, reason=PREEMPTED, generation=1, progress_phase="sum")
        snaps = iter([waiting] * 5 + [JobStatus(phase="Running", generation=2), JobStatus(phase=SUCCEEDED, generation=2)])
        client.job_status = lambda job_id: next(snaps)
        with self.assertLogs("zenthros.jobs", level="INFO") as logs:
            self.assertEqual(client.wait("zj-1"), SUCCEEDED)
        self.assertEqual(sum("reschedule wait" in line for line in logs.output), 1)


class RescheduleWaitTest(unittest.TestCase):
    def test_generation_bump_wait_succeeds_and_notifies_once(self):
        client = FakeJobClient()
        handle = client.submit(job("job-r"))
        client.script_reschedule(handle.job_id, progress_phase="train_epoch")
        events = []
        self.assertEqual(handle.wait(on_reschedule=events.append), SUCCEEDED)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].reason, WORKER_LOST)
        self.assertEqual(events[0].generation, 2)
        self.assertEqual(events[0].progress_phase, "train_epoch")
        self.assertEqual(handle.status(), SUCCEEDED)
        self.assertGreaterEqual(len(handle.reports()), 1)

    def test_fail_policy_raises_job_failed(self):
        client = FakeJobClient()
        handle = client.submit(job("job-f"))
        client.script_worker_lost_fail(handle.job_id)
        with self.assertRaises(errors.JobFailed):
            handle.wait()

    def test_retry_limit_exhausted_raises_job_failed(self):
        client = FakeJobClient()
        handle = client.submit(job("job-l"))
        client.script_retry_limit_exhausted(handle.job_id, limit=3)
        events = []
        with self.assertRaises(errors.JobFailed):
            handle.wait(on_reschedule=events.append)
        self.assertGreaterEqual(len(events), 1)


if __name__ == "__main__":
    unittest.main()


class JobClientDefaultsTest(unittest.TestCase):
    """status() and artifacts() are written once, on job_status() and
    reports() (audit 2026-10-03 I.6)."""

    def test_status_is_the_job_status_phase(self):
        class Client(JobClient):
            def job_status(self, job_id):
                return as_job_status("Running")

        self.assertEqual(Client().status("job"), "Running")

    def test_a_client_with_neither_says_so(self):
        with self.assertRaises(NotImplementedError):
            JobClient().status("job")

    def test_artifacts_are_every_reports(self):
        class View:
            def __init__(self, artifacts):
                self.artifacts = artifacts

        class Client(JobClient):
            def reports(self, job_id):
                return [View(["a"]), View(["b", "c"])]

        self.assertEqual(Client().artifacts("job"), ["a", "b", "c"])
