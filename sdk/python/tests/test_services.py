#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.services(): what of the Workspace is serving now, for whom
forgot one. Pinned: only Services that have not ended; each with its URL,
how long it has served, when stop_after stops it, and its cost so far; a
cost that cannot be read never hides the Service."""
import unittest
from unittest import mock

from zenthros import errors, remote
from zenthros.remote import servicelist as S

NOW = 1_760_000_000  # 2025-10-09T08:53:20Z


def stamp(seconds_ago):
    import time

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(NOW - seconds_ago))


def job(name, phase="Running", service=True, url="", started=600, stop_after=None):
    template = {"command": ["sh", "-c", "x"]}
    if service:
        template["service"] = {"ports": [{"name": "http", "port": 8000}]}
        if stop_after:
            template["service"]["stopAfterSeconds"] = stop_after
    status = {"phase": phase}
    if started is not None:
        status["firstStartTime"] = stamp(started)
    if url:
        status["endpoints"] = [{"name": "http", "url": url, "port": 8000, "scope": "Private"}]
    return {"metadata": {"name": name, "creationTimestamp": stamp((started or 0) + 30)},
            "spec": {"workload": {"template": template}}, "status": status}


class Client:
    def __init__(self, items, costs=None):
        self.items, self.costs = items, costs or {}

    def list_jobs(self, selector=""):
        return self.items

    def job_cost(self, name):
        found = self.costs[name]
        if isinstance(found, Exception):
            raise found
        return found


class ServicesTest(unittest.TestCase):
    def listed(self, client):
        with remote.configure(client=client), mock.patch.object(S, "_now", lambda: NOW):
            return S.services()

    def test_only_services_that_have_not_ended(self):
        client = Client([job("api", url="http://127.0.0.1:8000"), job("old", phase="Cancelled"),
                         job("train", service=False), job("queued", phase="Pending", started=None)],
                        costs={"api": {"job": "api"}, "queued": {"job": "queued"}})
        self.assertEqual([s.name for s in self.listed(client)], ["api", "queued"])

    def test_each_says_its_url_how_long_when_it_stops_and_its_cost(self):
        client = Client([job("api", url="http://127.0.0.1:8000", started=3600, stop_after=4 * 3600)],
                        costs={"api": {"job": "api", "cost": 0.12, "currency": "USD", "seconds": 3600, "running": True}})
        [api] = self.listed(client)
        self.assertEqual((api.urls, api.serving_seconds, api.stops_in_seconds), (["http://127.0.0.1:8000"], 3600, 3 * 3600))
        self.assertEqual(api.cost.cost, 0.12)
        table = repr(self.listed(client))
        for part in ("api", "http://127.0.0.1:8000", "1h 0m", "3h 0m", "$0.1200"):
            self.assertIn(part, table)

    def test_one_without_stop_after_says_it_runs_until_cancelled(self):
        [api] = self.listed(Client([job("api")], costs={"api": {"job": "api"}}))
        self.assertIsNone(api.stops_in_seconds)
        self.assertIn("until cancelled", repr([api]) + repr(S.ServiceList([api])))

    def test_a_cost_it_cannot_read_never_hides_the_service(self):
        [api] = self.listed(Client([job("api")], costs={"api": errors.TransientError("api down")}))
        self.assertEqual((api.name, api.cost), ("api", None))
        with remote.configure(client=mock.Mock(spec=["list_jobs", "job_cost"], list_jobs=lambda selector="": [job("api")],
                                                   job_cost=mock.Mock(side_effect=errors.TransientError("api down")))), \
                mock.patch.object(S, "_now", lambda: NOW):
            [api] = S.services()
        self.assertIsNone(api.cost)

    def test_it_is_zenthros_services(self):
        import zenthros

        self.assertIs(zenthros.services, S.services)


if __name__ == "__main__":
    unittest.main()
