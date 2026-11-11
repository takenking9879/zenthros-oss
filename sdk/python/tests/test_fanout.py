# SPDX-License-Identifier: Apache-2.0

"""zenthros.spawn / children / map against the wire the Go CLI pins
(operator/pkg/jobcli/testdata/wire.json, TestAgentJobCLISpawnMatchesSDKWire)."""

import json
import os
import unittest
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import errors, fanout
from zenthros.core import agent as client
from zenthros.core.storage import MemoryBlobStore

FIXTURE = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "jobcli" / "testdata" / "wire.json"


def load():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def square(x):
    return x * x


class FakeAgent:
    """Records each POST and answers like the agent's /v1/children."""

    def __init__(self, replies):
        self.calls = []
        self.replies = list(replies)

    def __call__(self, socket_path, http_path, body, headers, timeout):
        self.calls.append((http_path, headers, json.loads(body)))
        status, reply = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        return status, json.dumps(reply).encode("utf-8")


def env(fx, **extra):
    return {**fx["env"], "ZENTHROS_REPORT_ENDPOINT": "unix:///run/r.sock", **extra}


class SpawnWireTest(unittest.TestCase):
    def test_spawn_matches_fixture(self):
        fx = load()
        sp = fx["spawn"]
        agent = FakeAgent([(200, sp["reply"])])
        with mock.patch.dict(os.environ, env(fx)), mock.patch.object(client, "_unix_post", agent):
            group = zenthros.spawn("shards", 4, cmd=["python3", "work.py", "--in", "s3://b/in"],
                                   env={"MODE": "fast", "NOTE": "two words"})
            zenthros.spawn("again", 2, same=True)
            zenthros.children("shards")
        (p1, h1, b1), (_, _, b2), (_, _, b3) = agent.calls
        self.assertEqual(p1, sp["path"])
        self.assertEqual(h1["Authorization"], fx["authorization"])
        self.assertEqual(b1, sp["body"])
        self.assertEqual(b2, sp["same_body"])
        self.assertEqual(b3, sp["list_body"])
        self.assertEqual([c.name for c in group.children], ["train-shards-0", "train-shards-1"])

    def test_wait_fails_with_child_message_verbatim(self):
        fx = load()
        agent = FakeAgent([(200, fx["spawn"]["reply"])])
        with mock.patch.dict(os.environ, env(fx)), mock.patch.object(client, "_unix_post", agent):
            with self.assertRaises(fanout.ChildFailed) as caught:
                zenthros.spawn("shards", 2, cmd=["true"], wait=True, poll_seconds=0)
        self.assertEqual(str(caught.exception),
                         "train-shards-1 ended Failed: process exited with status 3: shard 1: bad row 17\nsecond line")
        self.assertEqual([c.index for c in caught.exception.failed], [1])

    def test_refusal_is_verbatim(self):
        fx = load()
        words = "300 children asked, 0 already spawned: a job may spawn at most 256 " \
                "(controller setting ZENTHROS_SPAWN_MAX_CHILDREN_PER_JOB)"
        agent = FakeAgent([(409, {"error_code": "SPAWN_QUOTA_EXCEEDED", "error": words})])
        with mock.patch.dict(os.environ, env(fx)), mock.patch.object(client, "_unix_post", agent):
            with self.assertRaises(fanout.SpawnRefused) as caught:
                zenthros.spawn("big", 300, same=True)
        self.assertEqual((caught.exception.code, caught.exception.message), ("SPAWN_QUOTA_EXCEEDED", words))
        self.assertEqual(len(agent.calls), 1)

    def test_store_unavailable_is_retried(self):
        fx = load()
        agent = FakeAgent([(503, {"error_code": "STORE_UNAVAILABLE", "error": "hub down"}), (200, fx["spawn"]["reply"])])
        with mock.patch.dict(os.environ, env(fx)), mock.patch.object(client, "_unix_post", agent), \
                mock.patch.object(client.time, "sleep", lambda _: None):
            group = zenthros.spawn("shards", 2, same=True)
        self.assertEqual(len(agent.calls), 2)
        self.assertEqual(len(group.children), 2)

    def test_wait_outlasts_an_agent_away_longer_than_a_call(self):
        """A wait for children lasts hours; one call to the agent, 45 s. A
        controller or Hub restarting for longer (2-3 min measured) failed the
        parent while its children ran on (R4 ronda 5, P2)."""
        fx = load()
        done = {"accepted": True, "children": [
            {"group": "shards", "index": i, "name": f"train-shards-{i}", "phase": "Succeeded", "message": "exited 0"}
            for i in range(2)]}
        away = [(503, {"error_code": "STORE_UNAVAILABLE", "error": "hub down"})] * 16
        agent = FakeAgent(away + [(200, done)])
        group = fanout.Group("shards", 2, [])
        with mock.patch.dict(os.environ, env(fx)), mock.patch.object(client, "_unix_post", agent), \
                mock.patch.object(client.time, "sleep", lambda _: None), \
                mock.patch.object(fanout.time, "sleep", lambda _: None):
            children = group.wait(poll_seconds=0)
        self.assertEqual([c.phase for c in children], ["Succeeded", "Succeeded"])

    def test_spawn_needs_one_of_cmd_or_same(self):
        with self.assertRaises(errors.ValidationError):
            zenthros.spawn("x", 1)
        with self.assertRaises(errors.ValidationError):
            zenthros.spawn("x", 1, cmd=["a"], same=True)

    def test_spawn_needs_a_child(self):
        # wait=True on no children once polled forever.
        with self.assertRaises(errors.ValidationError):
            zenthros.spawn("x", 0, cmd=["a"], wait=True)


class MapTest(unittest.TestCase):
    """map writes the items, spawns one child each, and gathers each child's
    result.json from the output prefix the controller gives it."""

    def run_map(self, items, children_write=True, **kw):
        fx = load()
        store = MemoryBlobStore()
        out = "s3://bucket/runs/r1"
        spawned = {}

        def agent(socket_path, http_path, body, headers, timeout):
            req = json.loads(body)
            if not req.get("list"):
                spawned.update(req)
                # What the controller does: each child runs the command with
                # its index and its own output prefix.
                for i in range(req["count"]):
                    child_env = {**req["env"], "TASK_INDEX": str(i), "TASK_COUNT": str(req["count"]),
                                 "ZENTHROS_DATA_OUTPUT": f"{out}/children/{req['group']}/{i}"}
                    if children_write:
                        with mock.patch.dict(os.environ, child_env):
                            self.assertEqual(req["command"][:4], ["python3", "-m", "zenthros.fanout", "run"])
                            fanout.run_child(req["command"][4], blob_store=store)
            n = spawned["count"]
            rows = [{"group": spawned["group"], "index": i, "name": f"j-{spawned['group']}-{i}", "phase": "Succeeded"}
                    for i in range(n)]
            return 200, json.dumps({"accepted": True, "children": rows}).encode("utf-8")

        with mock.patch.dict(os.environ, env(fx, ZENTHROS_DATA_OUTPUT=out)), \
                mock.patch.object(client, "_unix_post", agent):
            result = fanout.map(square, items, blob_store=store, **kw).result(poll_seconds=0)
        return result, spawned, store

    def test_map_round_trip(self):
        result, spawned, store = self.run_map([1, 2, 3])
        self.assertEqual(result, [1, 4, 9])
        self.assertEqual(spawned["command"][4], "test_fanout:square")
        self.assertTrue(spawned["group"].startswith("map-"))
        items_uri = spawned["env"]["ZENTHROS_MAP_ITEMS"]
        self.assertEqual(items_uri, f"s3://bucket/runs/r1/children/{spawned['group']}/items.json")
        self.assertEqual(json.loads(store.get(items_uri)), [1, 2, 3])

    def test_items_that_are_not_json_are_refused_not_turned_into_text(self):
        import datetime

        with self.assertRaises(TypeError):
            self.run_map([datetime.date(2026, 10, 19)])

    def test_map_name_is_deterministic(self):
        # This job's replacement maps the same work onto the same children.
        _, a, _ = self.run_map([1, 2, 3])
        _, b, _ = self.run_map([1, 2, 3])
        _, c, _ = self.run_map([1, 2, 4])
        self.assertEqual(a["group"], b["group"])
        self.assertNotEqual(a["group"], c["group"])

    def test_map_missing_result_raises(self):
        with self.assertRaises(errors.ValidationError) as caught:
            self.run_map([1, 2], children_write=False)
        self.assertIn("2 of 2 children", str(caught.exception))

    def test_map_needs_output(self):
        fx = load()
        with mock.patch.dict(os.environ, env(fx)):
            os.environ.pop("ZENTHROS_DATA_OUTPUT", None)
            with self.assertRaises(errors.ValidationError) as caught:
                zenthros.map(square, [1])
        self.assertIn("artifacts.output", str(caught.exception))

    def test_map_rejects_unimportable_function(self):
        with mock.patch.dict(os.environ, env(load(), ZENTHROS_DATA_OUTPUT="s3://bucket/runs/r1")):
            with self.assertRaises(errors.ValidationError):
                zenthros.map(lambda x: x, [1])

    def test_inside_a_job_zenthros_map_spawns_children(self):
        # The children run where the job runs: placement is not map's to set.
        with mock.patch.dict(os.environ, env(load(), ZENTHROS_DATA_OUTPUT="s3://bucket/runs/r1")):
            with mock.patch("zenthros.fanout.map", return_value="group") as children_map:
                self.assertEqual(zenthros.map(square, [1, 2], name="g"), "group")
            children_map.assert_called_once_with(square, [1, 2], name="g", env=None)
            with self.assertRaises(errors.ValidationError) as caught:
                zenthros.map(square, [1], on="gpu")
        self.assertIn("on", str(caught.exception))


if __name__ == "__main__":
    unittest.main()


class InJobMapOptionsTest(unittest.TestCase):
    """Inside a job, zenthros.map spawns the job's children: they run where the
    job runs, so placement and waiting options are refused rather than
    ignored, and the wait is the handle's (result(poll_seconds=)). Found by
    TestSDKMapOnMachines, whose head still passed poll_seconds to map."""

    def test_placement_and_wait_options_are_refused_inside_a_job(self):
        from zenthros.remote import call as remote_call

        with mock.patch.dict(os.environ, {"ZENTHROS_JOB_ID": "j", "ZENTHROS_REPORT_ENDPOINT": "unix:///x"}):
            for option in ({"poll_seconds": 2}, {"pool": "p"}, {"cpu": "2"}):
                with self.assertRaises(errors.ValidationError, msg=option):
                    remote_call.map("m:f", [1], **option)
            with mock.patch("zenthros.fanout.map", return_value="handle") as spawned:
                self.assertEqual(remote_call.map("m:f", [1, 2], name="n", env={"A": "1"}), "handle")
            spawned.assert_called_once_with("m:f", [1, 2], name="n", env={"A": "1"})
