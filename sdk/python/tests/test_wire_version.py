# SPDX-License-Identifier: Apache-2.0

"""The SDK says who it is and which format of zenthros-api it speaks.

Every request to a hub carries User-Agent zenthros-sdk/<version>
python/<x.y> <os> and Zenthros-Wire: <n> (operator/pkg/api/version.go). n is
the hub's number in operator/pkg/api/testdata/wire/version.json, so the SDK
of a tree speaks that tree's hub. A hub of another wire answers 426, and the
SDK raises a WireMismatch with the hub's words: which side is older and what
to install. The jobs' calls and the sign-in's both.
"""

import json
import pathlib
import platform
import sys
import unittest

from zenthros import errors, signin
from zenthros.jobs.http import HTTPJobClient
from zenthros.jobs.wire import USER_AGENT, WIRE, sdk_version

from support import FakeAPI

VERSION = pathlib.Path(__file__).resolve().parents[3] / "operator" / "pkg" / "api" / "testdata" / "wire" / "version.json"

OLDER = ("zenthros 0.1.0 speaks wire 0; this hub speaks 1: "
         "pip install -U 'https://dl.example/zenthros-0.2.0-py3-none-any.whl'")


def _status_426(message):
    return 426, json.dumps({"kind": "Status", "apiVersion": "v1", "status": "Failure", "message": message,
                            "reason": "UpgradeRequired", "code": 426}).encode()


class WireVersionTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)

    def test_the_sdk_speaks_its_hubs_wire(self):
        self.assertEqual(WIRE, json.loads(VERSION.read_text())["wire"])

    def test_the_user_agent_names_the_sdk_python_and_the_os(self):
        python = f"python/{sys.version_info.major}.{sys.version_info.minor}"
        self.assertEqual(USER_AGENT, f"zenthros-sdk/{sdk_version()} {python} {platform.system().lower()}")

    def test_every_request_to_the_hub_says_who_it_is(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/train")] = (200, {"metadata": {"name": "train"},
                                                                            "status": {"phase": "Running"}})
        self.api.routes[("GET", "/v1/whoami")] = (200, {"username": "u"})
        self.api.routes[("GET", "/v1/signup")] = (200, {"firebase": {"apiKey": "k", "projectId": "p"}, "terms": {
            "version": "v", "termsURL": "https://t", "privacyURL": "https://p"}})
        client = HTTPJobClient(self.api.url, "risk", "t")
        client.job_status("train")
        client.whoami()
        signin.how_to_sign_in(self.api.url)
        self.assertEqual(len(self.api.requests), 3)
        for request in self.api.requests:
            with self.subTest(path=request["path"]):
                self.assertEqual(request["headers"].get("User-Agent"), USER_AGENT)
                self.assertEqual(request["headers"].get("Zenthros-Wire"), str(WIRE))

    def test_a_hub_of_another_wire_says_what_to_install(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/train")] = _status_426(OLDER)
        self.api.routes[("GET", "/v1/signup")] = _status_426(OLDER)
        with self.assertRaises(errors.WireMismatch) as job:
            HTTPJobClient(self.api.url, "risk", "t").job_status("train")
        with self.assertRaises(errors.WireMismatch) as sign:
            signin.how_to_sign_in(self.api.url)
        for caught in (job.exception, sign.exception):
            self.assertEqual(str(caught), OLDER)
            self.assertEqual(caught.status, 426)
            # Asked again, the hub answers the same.
            self.assertIsInstance(caught, errors.Refused)
            self.assertNotIsInstance(caught, errors.TransientError)
        # Once: never asked again.
        self.assertEqual(len(self.api.requests), 2)


if __name__ == "__main__":
    unittest.main()
