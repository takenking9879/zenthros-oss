# SPDX-License-Identifier: Apache-2.0

"""The SDK takes each zctl refusal for what it means, and reads only what
zctl prints.

operator/pkg/zctl/testdata/sdk-wire.json is what zctl prints for the
commands the SDK runs, and the stderr and exit code of each refusal the SDK
tells apart; pkg/zctl TestZctlPrintsWhatTheSDKReads writes it from zctl
itself and holds each code to its meaning. Here the SDK reads the same file:
a refusal's code or an output's keys changed on one side fail here or there,
without a cluster.
"""
import json
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from wireread import paths, wrap

from zenthros import errors, view, zctl
from zenthros.jobs.client import ZctlJobClient

FIXTURE = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "zctl" / "testdata" / "sdk-wire.json"
WIRE = json.loads(FIXTURE.read_text())

MEANS = {
    "login": errors.LoginRequired,
    "forbidden": errors.WorkspaceForbidden,
    "job-absent": errors.JobNotFound,
    "invalid": errors.ValidationError,
    "transient": errors.ZctlError,
    "unknown": errors.ZctlError,
}


def _said(stdout="", stderr="", returncode=0):
    return lambda args, **_: subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr=stderr)


class ZctlWireTest(unittest.TestCase):
    def test_the_sdk_reads_zctls_exit_codes(self):
        codes = dict(WIRE["exit_codes"])
        self.assertEqual(codes.pop("unknown"), 1)
        self.assertEqual(codes, {"invalid": zctl.EXIT_INVALID, "job-absent": zctl.EXIT_JOB_ABSENT, "login": zctl.EXIT_LOGIN,
                                 "transient": zctl.EXIT_TRANSIENT, "forbidden": zctl.EXIT_FORBIDDEN})

    def test_each_refusal_is_taken_for_what_it_means(self):
        for refusal in WIRE["refusals"]:
            with self.subTest(case=refusal["case"]):
                with mock.patch.object(zctl.subprocess, "run", _said(stderr=refusal["stderr"], returncode=refusal["code"])), \
                        self.assertRaises(errors.ZenthrosError) as caught:
                    zctl.Zctl("zctl")(*refusal["args"])
                self.assertIs(type(caught.exception), MEANS[refusal["means"]], refusal["stderr"])
                if refusal["means"] == "login":
                    # 67 is also no credential at all: zctl says which, and
                    # the SDK claims no expiry it cannot know.
                    self.assertNotIn("expired or was revoked", str(caught.exception))
                if refusal["means"] in ("transient", "unknown"):
                    # A create that exits transient made nothing; any other
                    # may have made the job (ZctlJobClient.submit).
                    self.assertEqual(caught.exception.returncode == zctl.EXIT_TRANSIENT, refusal["means"] == "transient")

    def test_the_sdk_reads_only_keys_zctl_prints(self):
        outputs = WIRE["outputs"]
        drives = {
            "get zenthrosjob -o json": lambda c: (c.job_status("train"), view.JobInfo.from_api(c.job_object("train"))),
            "get report (none)": lambda c: self.assertEqual(c.reports("train"), []),
            "get report (one)": lambda c: self.assertEqual(len(c.reports("train")), 1),
            "get report (many)": lambda c: self.assertEqual(len(c.reports("train")), 2),
        }
        self.assertEqual(set(drives), set(outputs), "every output in sdk-wire.json has a driver here")
        loads = json.loads
        for name, drive in drives.items():
            with self.subTest(output=name):
                seen = set()
                with mock.patch.object(zctl.subprocess, "run", _said(stdout=json.dumps(outputs[name]) + "\n")), \
                        mock.patch("json.loads", lambda raw, *a, **kw: wrap(loads(raw, *a, **kw), "", seen)):
                    drive(ZctlJobClient(zctl="zctl"))
                missing = sorted(p for p in seen if p not in paths(outputs[name]))
                self.assertFalse(missing, f"{name}: the SDK reads keys zctl never prints: {missing}")


if __name__ == "__main__":
    unittest.main()
