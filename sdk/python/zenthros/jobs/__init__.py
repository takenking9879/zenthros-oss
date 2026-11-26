# SPDX-License-Identifier: Apache-2.0

"""Job lifecycle against public Zenthros interfaces. Not report ingest."""

from zenthros.jobs import spec
from zenthros.jobs.client import JobClient, ZctlJobClient, submit
from zenthros.jobs.handle import JobHandle, JobStatus, ReportView
from zenthros.jobs.http import HTTPJobClient
from zenthros.jobs.spec import Job


def __getattr__(name):
    # The machine gets the SDK's core without the test client (bundle.sdk_files).
    if name == "FakeJobClient":
        from zenthros.jobs.fake import FakeJobClient

        return FakeJobClient
    raise AttributeError(f"module 'zenthros.jobs' has no attribute {name!r}")


__all__ = [
    "FakeJobClient",
    "HTTPJobClient",
    "Job",
    "JobClient",
    "JobHandle",
    "JobStatus",
    "ReportView",
    "ZctlJobClient",
    "spec",
    "submit",
]
