#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.workspace(): what a member sees of its Workspace through
zenthros-api, with its token and nothing else.

The view reads; it decides nothing. A refusal is the API's words, and a
backend counts as ready only from what the API said about it.
"""
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import zenthros
from zenthros import errors, view
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI, status


def stored_job(name, phase, created, backend="", message=""):
    spec = {"workload": {"kind": "Job"}}
    if backend:
        spec["target"] = {"backends": [backend]}
    return {"metadata": {"name": name, "creationTimestamp": created, "generation": 1}, "spec": spec,
            "status": {"phase": phase, "message": message}}


class ViewTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.devnull + ".none",
                                               "ZENTHROS_SERVER": self.api.url, "ZENTHROS_TOKEN": "bob-token",
                                               "ZENTHROS_WORKSPACE": "risk"}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_it_is_the_connection_in_effect(self):
        self.api.routes[("GET", "/v1/whoami")] = (200, {"username": "bob", "groups": ["risk-team"]})
        ws = zenthros.workspace()
        self.assertEqual((ws.name, ws.server), ("risk", self.api.url))
        self.assertEqual(ws.whoami(), {"username": "bob", "groups": ["risk-team"]})
        self.assertEqual(self.api.requests[-1]["auth"], "Bearer bob-token")
        with zenthros.configure(workspace="ml"):
            self.assertEqual(zenthros.workspace().name, "ml")

    def test_the_store_is_where_its_calls_go(self):
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://zenthros-remote/api/risk"})
        self.assertEqual(zenthros.workspace().store, "s3://zenthros-remote/api/risk")

    def test_jobs_are_the_workspaces_newest_first(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"kind": "ZenthrosJobList", "items": [
            stored_job("old", "Succeeded", "2026-10-01T10:00:00Z"),
            stored_job("new", "Running", "2026-10-13T09:00:00Z", backend="laptop", message="job is Running"),
            stored_job("mid", "Failed", "2026-10-05T10:00:00Z")]})
        jobs = zenthros.workspace().jobs()
        self.assertEqual([j.name for j in jobs], ["new", "mid", "old"])
        self.assertEqual((jobs[0].phase, jobs[0].kind, jobs[0].backends, jobs[0].created, jobs[0].status.message),
                         ("Running", "Job", ["laptop"], "2026-10-13T09:00:00Z", "job is Running"))
        self.assertEqual(self.api.requests[-1]["path"], "/v1/workspaces/risk/jobs?limit=200")

    def test_jobs_take_a_label_selector(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"items": []})
        self.assertEqual(zenthros.workspace().jobs(selector="team=risk"), [])
        self.assertEqual(self.api.requests[-1]["path"], "/v1/workspaces/risk/jobs?limit=200&labelSelector=team%3Drisk")

    def test_a_backend_is_read_as_the_api_writes_it(self):
        # protojson leaves out false, 0 and "" and writes an int64 as a string.
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [
            {"backendId": "laptop", "backendType": "container", "agentVersion": "1.2.3", "healthy": True,
             "runningJobs": 1, "maxJobs": 4, "lastHeartbeatUnix": "1760000000", "ownership": "External",
             "accessMode": "Private", "workspaces": ["risk"], "ageRecipients": ["age1abc"],
             "host": {"arch": "amd64", "cpu": 2, "memoryBytes": "4294967296", "containerImage": "pipeline:1"},
             "capabilities": [{"kind": "Job", "runtimes": ["process"]}]},
            {"backendId": "gone"}]})
        laptop, gone = zenthros.workspace().backends()
        self.assertEqual((laptop.id, laptop.type, laptop.agent_version), ("laptop", "container", "1.2.3"))
        self.assertEqual((laptop.healthy, laptop.drained, laptop.running_jobs, laptop.max_jobs), (True, False, 1, 4))
        self.assertEqual((laptop.last_heartbeat, laptop.access_mode, laptop.workspaces), (1760000000, "Private", ["risk"]))
        self.assertEqual((laptop.cpu, laptop.memory_bytes, laptop.arch, laptop.image), (2, 4 * 1024 ** 3, "amd64", "pipeline:1"))
        self.assertEqual((laptop.age_recipients, laptop.kinds), (["age1abc"], ["Job"]))
        self.assertEqual((gone.id, gone.healthy, gone.last_heartbeat, gone.cpu, gone.workspaces), ("gone", False, 0, 0, []))

    def test_ready_is_healthy_and_open_to_work(self):
        def backend(**fields):
            return view.Backend.from_api({"backendId": "b", **fields})

        self.assertTrue(backend(healthy=True).ready)
        self.assertFalse(backend().ready, "never heard from")
        self.assertFalse(backend(healthy=True, drained=True).ready, "closed to new jobs")
        self.assertTrue(backend(healthy=True, runningJobs=4, maxJobs=4).ready, "busy is not unready: jobs queue")

    def test_one_backend_by_its_id(self):
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [{"backendId": "laptop", "healthy": True}]})
        ws = zenthros.workspace()
        self.assertTrue(ws.backend("laptop").healthy)
        with self.assertRaises(LookupError) as caught:
            ws.backend("other")
        self.assertIn("laptop", str(caught.exception), "it names what the Workspace does have")

    def test_pools_are_the_ones_the_workspace_may_name(self):
        # The API answers with Go's JSON: every field, numbers as numbers.
        self.api.routes[("GET", "/v1/workspaces/risk/pools")] = (200, {"pools": [
            {"name": "spot-4x8", "machineKind": "VM", "provider": "aws", "account": "", "region": "us-east-1",
             "zone": "us-east-1a", "purchasingMode": "Spot", "instanceTypes": ["c7i.xlarge", "c6i.xlarge"],
             "toolchains": ["python", "java"], "minReady": 0, "maxReady": 2, "ready": 1, "allocated": 1,
             "provisioning": 0, "conditions": [{"type": "Ready", "status": "True"}]},
            {"name": "cpu", "machineKind": "VM", "provider": "aws", "region": "us-east-1", "purchasingMode": "OnDemand",
             "instanceTypes": None, "toolchains": [], "minReady": 1, "maxReady": 1, "ready": 0, "allocated": 0,
             "provisioning": 1}]})
        ws = zenthros.workspace()
        spot, cpu = ws.pools()
        self.assertEqual(self.api.requests[-1]["path"], "/v1/workspaces/risk/pools")
        self.assertEqual((spot.name, spot.machine_kind, spot.provider, spot.region, spot.zone),
                         ("spot-4x8", "VM", "aws", "us-east-1", "us-east-1a"))
        self.assertEqual((spot.purchasing_mode, spot.spot, spot.instance_types, spot.toolchains),
                         ("Spot", True, ["c7i.xlarge", "c6i.xlarge"], ["python", "java"]))
        self.assertEqual((spot.min_ready, spot.max_ready, spot.ready, spot.allocated, spot.provisioning), (0, 2, 1, 1, 0))
        self.assertEqual(spot.conditions, [{"type": "Ready", "status": "True"}])
        self.assertEqual((cpu.spot, cpu.instance_types, cpu.provisioning, cpu.conditions), (False, [], 1, []))
        self.assertEqual(ws.pool("cpu").min_ready, 1)
        with self.assertRaises(LookupError) as caught:
            ws.pool("gpu")
        self.assertIn("spot-4x8", str(caught.exception), "it names what the Workspace does have")

    def test_a_refusal_is_the_apis_words(self):
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = status(403, "list zenthrosbackends in tenant-risk is forbidden")
        with self.assertRaises(errors.WorkspaceForbidden) as caught:
            zenthros.workspace().backends()
        self.assertIn("list zenthrosbackends", str(caught.exception))

    def test_without_an_api_connection_it_says_what_is_missing(self):
        with mock.patch.dict(os.environ, {"ZENTHROS_SERVER": ""}):
            with self.assertRaises(errors.ValidationError) as caught:
                zenthros.workspace()
        self.assertIn("zenthros-api", str(caught.exception))


class ClientListsTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token")

    def test_a_list_without_items_is_empty(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"kind": "ZenthrosJobList", "items": None})
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {})
        self.api.routes[("GET", "/v1/workspaces/risk/pools")] = (200, {"pools": None})
        self.assertEqual(self.client.list_jobs(), [])
        self.assertEqual(self.client.backends(), [])
        self.assertEqual(self.client.pools(), [])


if __name__ == "__main__":
    unittest.main()


class UploadTest(unittest.TestCase):
    """ws.upload: local files to the Workspace's store, through URLs the API
    signs, so a job anywhere reads them by URI. The caller holds no cloud
    credentials, and the token never reaches the store."""

    def setUp(self):
        self.api, self.s3 = FakeAPI(), FakeAPI()
        self.addCleanup(self.api.close)
        self.addCleanup(self.s3.close)
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://calls/zenthros/risk"})
        bucket = "s3://calls/"

        def sign(body):
            asked = json.loads(body)
            key = asked["uri"][len(bucket):]
            self.s3.routes[("PUT", "/" + key)] = (200, b"")
            return 200, {"url": f"{self.s3.url}/{key}", "method": asked["method"], "headers": {}}

        self.api.routes[("POST", "/v1/workspaces/risk/objects/presign")] = sign
        self.ws = view.Workspace(HTTPJobClient(self.api.url, "risk", "bob-token"))
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def write(self, rel, data=b"x"):
        path = os.path.join(self.dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    def put(self):
        return {r["path"]: r["body"] for r in self.s3.requests if r["method"] == "PUT"}

    def test_a_directory_goes_under_its_name_without_caches(self):
        self.write("data/a.csv", b"a,b\n1,2\n")
        self.write("data/sub/b.csv", b"b")
        self.write("data/__pycache__/x.pyc")
        self.write("data/.ipynb_checkpoints/a-checkpoint.csv")
        self.write("data/sub/.DS_Store")
        uri = self.ws.upload(os.path.join(self.dir, "data"))
        self.assertEqual(uri, "s3://calls/zenthros/risk/data")
        self.assertEqual(self.put(), {"/zenthros/risk/data/a.csv": b"a,b\n1,2\n", "/zenthros/risk/data/sub/b.csv": b"b"})
        self.assertTrue(all(r["auth"] is None for r in self.s3.requests), "the token reached the store")

    def test_a_file_goes_by_its_name_or_where_it_is_told(self):
        path = self.write("x.csv", b"1")
        self.assertEqual(self.ws.upload(path), "s3://calls/zenthros/risk/x.csv")
        self.assertEqual(self.ws.upload(path, to="inputs/2026/x.csv"), "s3://calls/zenthros/risk/inputs/2026/x.csv")
        self.assertEqual(set(self.put()), {"/zenthros/risk/x.csv", "/zenthros/risk/inputs/2026/x.csv"})

    def test_nothing_goes_outside_the_workspace(self):
        path = self.write("x.csv")
        for to in ("../other/x.csv", "/abs", "s3://calls/zenthros/ml/x", "a//b", ""):
            with self.assertRaises(errors.ValidationError, msg=to):
                self.ws.upload(path, to=to)
        self.assertEqual(self.s3.requests, [])

    def test_too_big_for_one_put_is_refused_before_any_is_sent(self):
        # The big one comes last: nothing goes before it is found.
        self.write("data/a.csv")
        self.write("data/z.bin", b"0123456789")
        with mock.patch.object(view, "MAX_UPLOAD_BYTES", 5):
            with self.assertRaises(errors.ValidationError) as caught:
                self.ws.upload(os.path.join(self.dir, "data"))
        self.assertIn("z.bin", str(caught.exception))
        self.assertEqual(self.s3.requests, [])

    def test_a_workspace_without_a_store_says_so(self):
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk"})
        with self.assertRaises(errors.ValidationError) as caught:
            self.ws.upload(self.write("x.csv"))
        self.assertIn("store", str(caught.exception))

    def test_a_missing_path_is_not_an_empty_upload(self):
        with self.assertRaises(FileNotFoundError):
            self.ws.upload(os.path.join(self.dir, "nope"))


def listing(keys, token=""):
    """A ListObjectsV2 page as S3 writes it."""
    contents = "".join(f"<Contents><Key>{key}</Key><LastModified>2026-10-15T09:30:00.000Z</LastModified>"
                       f"<ETag>&quot;x&quot;</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
                       for key, size in keys)
    more = f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>" if token else \
        "<IsTruncated>false</IsTruncated>"
    return ('<?xml version="1.0" encoding="UTF-8"?>\n<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>calls</Name><KeyCount>{len(keys)}</KeyCount>{contents}{more}</ListBucketResult>").encode()


class StoreReadTest(unittest.TestCase):
    """ws.ls and ws.download: what is in the Workspace's store, back here,
    through URLs the API signs (a listing is a signed ListObjectsV2 of one
    prefix). The caller holds no cloud credentials, and the token never
    reaches the store."""

    def setUp(self):
        self.api, self.s3 = FakeAPI(), FakeAPI()
        self.addCleanup(self.api.close)
        self.addCleanup(self.s3.close)
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://calls/zenthros/risk"})
        self.objects, self.pages, self.asked = {}, {}, []

        def sign(body):
            asked = json.loads(body)
            self.asked.append(asked)
            if asked["method"] == "LIST":
                page = self.pages.get((asked["uri"], asked.get("after", "")), listing([]))
                path = f"/list/{len(self.asked)}"
                self.s3.routes[("GET", path)] = (200, page)
                return 200, {"url": self.s3.url + path, "method": "GET", "headers": {}}
            key = asked["uri"][len("s3://calls/"):]
            data = self.objects.get(key)
            self.s3.routes[("GET", "/" + key)] = (200, data) if data is not None else (404, b"<Error/>")
            return 200, {"url": f"{self.s3.url}/{key}", "method": "GET", "headers": {}}

        self.api.routes[("POST", "/v1/workspaces/risk/objects/presign")] = sign
        self.ws = view.Workspace(HTTPJobClient(self.api.url, "risk", "bob-token"))
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def test_ls_pages_through_the_whole_prefix(self):
        self.pages[("s3://calls/zenthros/risk/out/", "")] = listing([("zenthros/risk/out/a.csv", 3)], token="t1/=")
        self.pages[("s3://calls/zenthros/risk/out/", "t1/=")] = listing([("zenthros/risk/out/b/c.pkl", 1024)])
        found = self.ws.ls("s3://calls/zenthros/risk/out/")
        self.assertEqual([(o.uri, o.size) for o in found],
                         [("s3://calls/zenthros/risk/out/a.csv", 3), ("s3://calls/zenthros/risk/out/b/c.pkl", 1024)])
        self.assertEqual(found[0].modified.isoformat(), "2026-10-15T09:30:00+00:00")
        self.assertEqual([(a["method"], a.get("after", "")) for a in self.asked], [("LIST", ""), ("LIST", "t1/=")])
        self.assertTrue(all(r["auth"] is None for r in self.s3.requests), "the token reached the store")

    def test_ls_takes_a_path_under_the_store_and_defaults_to_all_of_it(self):
        self.pages[("s3://calls/zenthros/risk/", "")] = listing([("zenthros/risk/x", 1)])
        self.pages[("s3://calls/zenthros/risk/out/", "")] = listing([("zenthros/risk/out/y", 2)])
        self.assertEqual([o.uri for o in self.ws.ls()], ["s3://calls/zenthros/risk/x"])
        self.assertEqual([o.uri for o in self.ws.ls("out/")], ["s3://calls/zenthros/risk/out/y"])

    def test_one_object_comes_back_as_bytes_or_to_a_file(self):
        self.objects["zenthros/risk/demo/model.pkl"] = b"model"
        self.assertEqual(self.ws.download("s3://calls/zenthros/risk/demo/model.pkl"), b"model")
        self.assertEqual(self.ws.download("demo/model.pkl"), b"model")
        to = os.path.join(self.dir, "m.pkl")
        self.assertEqual(self.ws.download("demo/model.pkl", to=to), to)
        # A folder that exists takes the object by its name.
        self.assertEqual(self.ws.download("demo/model.pkl", to=self.dir), os.path.join(self.dir, "model.pkl"))
        for path in (to, os.path.join(self.dir, "model.pkl")):
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"model")

    def test_a_folder_comes_back_as_its_tree(self):
        # What ws.upload of a directory gave: its URI, no slash.
        self.objects["zenthros/risk/data/a.csv"] = b"a"
        self.objects["zenthros/risk/data/sub/b.csv"] = b"b"
        # data/ is a folder marker some consoles write: nothing to fetch.
        self.pages[("s3://calls/zenthros/risk/data/", "")] = listing(
            [("zenthros/risk/data/", 0), ("zenthros/risk/data/a.csv", 1), ("zenthros/risk/data/sub/b.csv", 1)])
        to = os.path.join(self.dir, "out")
        got = self.ws.download("s3://calls/zenthros/risk/data", to=to)
        self.assertEqual(got, [os.path.join(to, "a.csv"), os.path.join(to, "sub", "b.csv")])
        with open(os.path.join(to, "sub", "b.csv"), "rb") as fh:
            self.assertEqual(fh.read(), b"b")
        with self.assertRaises(errors.ValidationError) as caught:
            self.ws.download("s3://calls/zenthros/risk/data/")
        self.assertIn("to=", str(caught.exception))

    def test_a_key_that_climbs_out_of_the_folder_writes_nothing(self):
        self.objects["zenthros/risk/data/a.csv"] = b"a"
        self.objects["zenthros/risk/data/../../evil"] = b"x"
        self.pages[("s3://calls/zenthros/risk/data/", "")] = listing(
            [("zenthros/risk/data/a.csv", 1), ("zenthros/risk/data/../../evil", 1)])
        to = os.path.join(self.dir, "out")
        with self.assertRaises(errors.ValidationError):
            self.ws.download("s3://calls/zenthros/risk/data/", to=to)
        self.assertFalse(os.path.exists(to), "something was written before the bad key was found")

    def test_nothing_there_is_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.ws.download("s3://calls/zenthros/risk/nope")
        with self.assertRaises(FileNotFoundError):
            self.ws.download("s3://calls/zenthros/risk/nope", to=os.path.join(self.dir, "x"))
        # Nothing half-written is left where the file would be.
        self.assertEqual(os.listdir(self.dir), [])

    def test_a_path_stays_inside_the_store(self):
        for name in ("../ml/x", "out/../../x", "./x"):
            with self.assertRaises(errors.ValidationError, msg=name):
                self.ws.ls(name)
            with self.assertRaises(errors.ValidationError, msg=name):
                self.ws.download(name)
        self.assertEqual(self.asked, [])
