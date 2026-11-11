#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""call.cost(): what a call cost, as zenthros-api counts it (its running
time times the price the catalog last observed for its machine's offer), or
no number and why. The SDK computes nothing of its own."""
import unittest

from zenthros.jobs.client import ZctlJobClient
from zenthros.jobs.http import HTTPJobClient
from zenthros.remote.call import Cost, RemoteCall

from support import FakeAPI

PRICED = {"job": "j", "backend": "vm-a", "running": False, "seconds": 1800.0, "cost": 0.034, "currency": "USD",
          "hourlyRate": 0.068, "instanceType": "c6i.xlarge", "market": "Spot", "region": "us-east-2",
          "zone": "us-east-2b", "observedAt": "2026-10-15T08:00:00Z",
          "basis": "running time × the last price the catalog observed for its machine's offer"}
UNPRICED = {"job": "j", "backend": "ct-1", "running": False, "seconds": 12.5, "cost": None,
            "basis": "ct-1 is not a machine Zenthros rents (your own, or a cluster's): no price to count"}


class CostTest(unittest.TestCase):
    def test_a_priced_call_says_what_and_from_what(self):
        cost = Cost.from_api(PRICED)
        self.assertEqual((cost.cost, cost.currency, cost.hourly_rate, cost.seconds, cost.instance_type, cost.market),
                         (0.034, "USD", 0.068, 1800.0, "c6i.xlarge", "Spot"))
        text = repr(cost)
        for part in ("$0.0340", "30m 0s", "c6i.xlarge Spot", "us-east-2b", "$0.068/h", "running time"):
            self.assertIn(part, text)

    def test_no_price_is_no_number_and_why(self):
        cost = Cost.from_api(UNPRICED)
        self.assertIsNone(cost.cost)
        self.assertIn("not a machine Zenthros rents", repr(cost))
        self.assertNotIn("$", repr(cost))

    def test_a_running_call_says_so(self):
        self.assertIn("so far", repr(Cost.from_api({**PRICED, "running": True})))

    def test_the_call_asks_the_api_by_its_name(self):
        api = FakeAPI()
        self.addCleanup(api.close)
        api.routes[("GET", "/v1/workspaces/risk/jobs/zr-x-1/cost")] = (200, PRICED)
        client = HTTPJobClient(api.url, "risk", "bob-token")
        call = RemoteCall("zr-x-1", "s3://b/p", handle=None, client=client, blob_store=None)
        self.assertEqual(call.cost().cost, 0.034)
        self.assertEqual([(r["method"], r["path"]) for r in api.requests], [("GET", "/v1/workspaces/risk/jobs/zr-x-1/cost")])

    def test_without_zenthros_api_there_is_no_price(self):
        call = RemoteCall("zr-x-1", "s3://b/p", handle=None, client=ZctlJobClient(), blob_store=None)
        cost = call.cost()
        self.assertIsNone(cost.cost)
        self.assertIn("zenthros-api", cost.basis)


if __name__ == "__main__":
    unittest.main()
