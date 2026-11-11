#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""f.map(configs, on=[...], shared=...): a list of configurations, each a
dict of fn's keyword arguments and each one call, spread over several
destinations, its results in the order of configs; and zenthros.grid, every
combination as such a list.

Jobs run here (ExecutingClient); where they go (machines.place) and what
targets() says is free are the only fakes.
"""
import unittest
from unittest import mock

import zenthros
from zenthros import errors, remote
from zenthros.machines import Placement
from zenthros.remote import spread
from zenthros.targetlist import Plan, Target, Targets

from test_remote_api import Env


def variant(data_uri, windows, cpus=1):
    return {"data": data_uri, "windows": windows, "cpus": cpus}


def boom_on_60(data_uri, windows):
    if any(w >= 60 for w in windows):
        raise ValueError(f"no window {windows}")
    return sum(windows)


def square(x):
    return x * x


def producer(windows):
    import os

    return os.environ.get("ZENTHROS_PRODUCER_ID")


def rows(**available):
    return Targets([Target(name=name, kind="Pool", state="Ready", available=n, total=max(n, 1))
                    for name, n in available.items()])


class GridTest(unittest.TestCase):
    def test_every_combination_first_name_slowest(self):
        self.assertEqual(zenthros.grid(learning_rate=[0.05, 0.1], leaves=[15, 31]),
                         [{"learning_rate": 0.05, "leaves": 15}, {"learning_rate": 0.05, "leaves": 31},
                          {"learning_rate": 0.1, "leaves": 15}, {"learning_rate": 0.1, "leaves": 31}])
        self.assertEqual(zenthros.grid(windows=[[5, 20], [20, 60]]), [{"windows": [5, 20]}, {"windows": [20, 60]}])

    def test_values_are_lists(self):
        for bad in ({"a": "abc"}, {"a": 3}, {}):
            with self.assertRaises(errors.ValidationError, msg=bad):
                zenthros.grid(**bad)


class SplitTest(unittest.TestCase):
    def test_free_room_first_then_in_turn(self):
        # a has 2 free, b 1: a b a, then everything is busy and they queue in turn.
        self.assertEqual(spread.split(5, [("a", 2), ("b", 1)]), ["a", "b", "a", "a", "b"])
        self.assertEqual(spread.split(3, [("a", 0), ("b", 5)]), ["b", "b", "b"])

    def test_nothing_free_is_in_turn(self):
        self.assertEqual(spread.split(5, [("a", 0), ("b", 0)]), ["a", "b", "a", "b", "a"])
        self.assertEqual(spread.split(2, [("a", 0)]), ["a", "a"])


class ConfigsTest(Env):
    def setUp(self):
        super().setUp()
        self.placed = []

        def place(on, options, keep=False, python=None, ports=None):
            # No on=: what the real place defaults to, the Workspace's one machine.
            on = "demo-ml" if on is None else on
            self.placed.append(on)
            if on == "nowhere":
                raise errors.ValidationError("on='nowhere': the Workspace has nothing called that")
            return Placement({"pool": on, "acquire": ["Create"]} if on == "aws-spot" else {"pool": on}, name=on)

        for patcher in (mock.patch("zenthros.machines.place", side_effect=place),
                        mock.patch("zenthros.targetlist.targets", return_value=rows(**{"aws-spot": 5, "demo-ml": 1}))):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.fn = zenthros.cloud(variant)

    def jobs(self):
        return [(name, self.client.submitted(name).to_manifest()["spec"]) for name in self.client._jobs]

    def test_get_brings_a_spread_map_back_by_its_name(self):
        configs = [{"windows": [5, 20]}, {"windows": [20, 60]}, {"windows": [10, 30]}]
        calls = self.fn.map(configs, on=["aws-spot", "demo-ml"], shared={"data_uri": "s3://d"}, name="barrido-1",
                            code_dir=None)
        self.assertEqual(calls.name, "barrido-1")
        listed = self.client.list_jobs

        def as_the_cluster_lists(selector=""):
            # Out of order, and with the children a job of several tasks
            # spreads into on a machine: they inherit its labels, not the
            # map's annotations (seen on Floci, 2026-10-15).
            parents = list(reversed(listed(selector)))
            children = [{**p, "metadata": {"name": p["metadata"]["name"] + "-0", "labels": p["metadata"]["labels"]}}
                        for p in parents]
            return children + parents
        with mock.patch.object(self.client, "list_jobs", as_the_cluster_lists):
            again = zenthros.get("barrido-1")
        self.assertIsInstance(again, spread.MapCalls)
        self.assertEqual((again.name, again.configs, again.where), ("barrido-1", configs, calls.where))
        self.assertEqual([c.name for c in again.calls], [c.name for c in calls.calls])
        self.assertEqual(again.result(), calls.result())
        # One of its jobs is still that job.
        self.assertIsInstance(zenthros.get("barrido-1-0"), remote.RemoteCall)

    def test_a_map_brought_back_without_one_of_its_jobs_still_shows(self):
        # Its TTL collected one job: the map comes back with what is left,
        # and its table says which configs are gone instead of raising.
        configs = [{"windows": [5]}, {"windows": [7]}]
        calls = self.fn.map(configs, on=["aws-spot", "demo-ml"], shared={"data_uri": "s3://d"}, name="barrido-2",
                            code_dir=None)
        calls.result(stream_logs=False)
        self.client.delete(calls.calls[1].name)
        again = zenthros.get("barrido-2")
        self.assertEqual(again.configs, [configs[0], None])
        table = repr(again).splitlines()
        self.assertIn("barrido-2-0", table[2])
        self.assertIn("gone", table[3])

    def test_a_map_without_a_name_gets_one_that_brings_it_back(self):
        calls = self.fn.map([{"windows": [5]}, {"windows": [6]}], on=["aws-spot", "demo-ml"], code_dir=None)
        self.assertTrue(calls.name.startswith("zr-variant-"), calls.name)
        self.assertEqual([c.name for c in calls.calls], [f"{calls.name}-0", f"{calls.name}-1"])
        self.assertEqual(zenthros.get(calls.name).where, calls.where)
        self.assertIn(calls.name, repr(calls).splitlines()[0])

    def test_a_name_its_jobs_cannot_carry_is_refused_before_any_job(self):
        for bad in ("Barrido 1", "x" * 61):
            with self.assertRaises(errors.ValidationError, msg=bad):
                self.fn.map([{"windows": [5]}], on=["aws-spot", "demo-ml"], name=bad, code_dir=None)
        self.assertEqual(self.jobs(), [])

    def test_a_spread_map_as_an_argument_says_to_pass_its_results(self):
        calls = self.fn.map([{"windows": [5]}, {"windows": [6]}], on=["aws-spot", "demo-ml"], code_dir=None)
        before = len(self.client._jobs)
        for args in ((calls, [5], 2), ({"runs": [calls]}, [5], 2)):
            with self.assertRaises(errors.ValidationError) as caught:
                remote.run(variant, *args, code_dir=None)
            self.assertIn(".result()", str(caught.exception))
        self.assertEqual(len(self.client._jobs), before, "a job was sent")

    def test_after_a_spread_map_waits_for_each_of_its_jobs(self):
        calls = self.fn.map([{"windows": [5]}, {"windows": [6]}], on=["aws-spot", "demo-ml"], code_dir=None)
        after = remote.run(variant, "s3://d", [5], 2, after=[calls], code_dir=None)
        self.assertEqual(self.client.submitted(after.name).spec.depends_on, [c.name for c in calls.calls])

    def test_a_name_that_is_nothing_says_so(self):
        with self.assertRaises(errors.JobNotFound):
            zenthros.get("nada-1")

    def test_configs_over_two_destinations_come_back_in_their_order(self):
        configs = [{"windows": [5, 20]}, {"windows": [20, 60]}, {"windows": [5, 20, 60, 150]}, {"windows": [10, 30]}]
        calls = self.fn.map(configs, on=["aws-spot", "demo-ml"], shared={"data_uri": "s3://d", "cpus": 2}, code_dir=None)
        self.assertEqual(calls.where, ["aws-spot", "demo-ml", "aws-spot", "aws-spot"])
        self.assertEqual(calls.result(), [variant("s3://d", c["windows"], 2) for c in configs])
        # One job per destination, a task per config it got.
        specs = {spec["poolRef"]: spec for _, spec in self.jobs()}
        self.assertEqual(set(specs), {"aws-spot", "demo-ml"})
        self.assertEqual(specs["aws-spot"]["workload"]["template"]["distribution"]["count"], 3)
        self.assertEqual(specs["aws-spot"]["capacity"], {"acquire": ["Create"]})
        self.assertNotIn("capacity", specs["demo-ml"])
        self.assertNotIn("distribution", specs["demo-ml"]["workload"]["template"])
        self.assertEqual(self.placed, ["aws-spot", "demo-ml"])
        table = repr(calls)
        self.assertIn("aws-spot", table)
        self.assertIn("{'windows': [20, 60]}", table)
        # Each row says its job's phase, as status() gives it.
        self.assertEqual([line.split()[-1] for line in table.splitlines()[2:]], ["Succeeded"] * 4, table)

    def test_each_jobs_local_record_says_where_it_went_and_which_configs(self):
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as folder, remote.configure(log_dir=folder):
            calls = self.fn.map([{"windows": [1]}, {"windows": [2]}, {"windows": [3]}], on=["aws-spot", "demo-ml"],
                                shared={"data_uri": "x"}, code_dir=None)
            asked = {c.name: json.loads((Path(c.log_dir) / "call.json").read_text()) for c in calls.calls}
        by_where = {a["on"]: a["configs"] for a in asked.values()}
        self.assertEqual(by_where, {"aws-spot": {"0": "{'windows': [1]}", "2": "{'windows': [3]}"},
                                    "demo-ml": {"1": "{'windows': [2]}"}})

    def test_each_task_reports_as_its_own_producer(self):
        # zenthros.report in config i is producer task-i: its reports are
        # told apart from the other configs' on the same machine.
        import os

        calls = zenthros.cloud(producer).map([{"windows": [1]}, {"windows": [2]}, {"windows": [3]}],
                                             on=["aws-spot", "demo-ml"], code_dir=None)
        self.assertEqual(calls.result(), ["task-0", "task-0", "task-1"])
        self.assertNotIn("ZENTHROS_PRODUCER_ID", os.environ)

    def test_one_destination_in_a_list_is_the_same(self):
        calls = self.fn.map([{"windows": [1]}, {"windows": [2]}], on=["demo-ml"], shared={"data_uri": "x"}, code_dir=None)
        self.assertEqual(calls.where, ["demo-ml", "demo-ml"])
        self.assertEqual([r["windows"] for r in calls.result()], [[1], [2]])

    def test_a_config_and_shared_naming_the_same_argument_is_refused_before_anything_runs(self):
        with self.assertRaises(errors.ValidationError) as caught:
            self.fn.map([{"windows": [1], "data_uri": "y"}], on=["demo-ml"], shared={"data_uri": "x"}, code_dir=None)
        self.assertIn("data_uri", str(caught.exception))
        self.assertEqual((self.placed, self.jobs()), ([], []))

    def test_an_item_that_is_not_a_dict_is_the_one_argument_as_before(self):
        f = zenthros.cloud(square)
        self.assertEqual(f.map([2, 3], code_dir=None).result(stream_logs=False), [4, 9])
        self.assertEqual(f.map([2, 3], on=["aws-spot", "demo-ml"], code_dir=None).result(), [4, 9])
        g = zenthros.cloud(variant)
        self.assertEqual(g.map(["s3://a"], shared={"windows": [1]}, code_dir=None).result(stream_logs=False),
                         [variant("s3://a", [1])])

    def test_a_failure_says_which_config_and_where_and_the_others_come_back(self):
        f = zenthros.cloud(boom_on_60)
        # aws-spot's job runs configs 0, 2, 3 and 4; 2 and 4 fail, each with its own error.
        configs = [{"windows": [5]}, {"windows": [7]}, {"windows": [60]}, {"windows": [8]}, {"windows": [61]}]
        calls = f.map(configs, on=["aws-spot", "demo-ml"], shared={"data_uri": "x"}, code_dir=None)
        self.assertEqual(calls.where, ["aws-spot", "demo-ml", "aws-spot", "aws-spot", "aws-spot"])
        with self.assertRaises(remote.RemoteError) as caught:
            calls.result()
        self.assertIn("config 2 {'windows': [60]} on aws-spot", str(caught.exception))
        got = calls.result(return_exceptions=True)
        self.assertEqual((got[0], got[1], got[3]), (5, 7, 8))
        self.assertIsInstance(got[2], remote.RemoteError)
        self.assertIsInstance(got[2].__cause__, ValueError)
        self.assertIn("no window [61]", str(got[4]))
        self.assertIn("config 4 {'windows': [61]} on aws-spot", str(got[4]))

    def test_a_failure_still_waits_for_every_destination_and_removes_its_machine(self):
        # Raising at the first failed part once left the later parts running,
        # and a machine made from a template for them billing.
        f = zenthros.cloud(boom_on_60)
        calls = f.map([{"windows": [60]}, {"windows": [7]}], on=["aws-spot", "demo-ml"], shared={"data_uri": "x"},
                      code_dir=None)
        released = []
        for call in calls.calls:
            call._release = lambda name=call.name: released.append(name)
        with self.assertRaises(remote.RemoteError) as caught:
            calls.result(stream_logs=False)
        self.assertIn("config 0 {'windows': [60]} on aws-spot", str(caught.exception))
        self.assertEqual(sorted(released), sorted(call.name for call in calls.calls))

    def test_the_timeout_is_for_the_whole_map(self):
        calls = self.fn.map([{"windows": [1]}, {"windows": [2]}], on=["aws-spot", "demo-ml"], shared={"data_uri": "x"},
                            code_dir=None)
        given = []
        for call in calls.calls:
            call.outcomes = lambda timeout=None, given=given, **kw: given.append(timeout) or [(True, 1)]
        with mock.patch("zenthros.remote.spread.time.monotonic", side_effect=[100.0, 100.0, 107.0]):
            calls.result(timeout=10)
        self.assertEqual(given, [10.0, 3.0])

    def test_a_destination_given_nothing_gets_no_job(self):
        calls = self.fn.map([{"windows": [1]}], on=["demo-ml", "aws-spot"], shared={"data_uri": "x"}, code_dir=None)
        self.assertEqual((calls.where, self.placed, len(self.jobs())), (["demo-ml"], ["demo-ml"], 1))

    def test_a_name_is_one_per_job_and_a_destination_comes_once(self):
        calls = self.fn.map([{"windows": [1]}, {"windows": [2]}], on=["aws-spot", "demo-ml"], shared={"data_uri": "x"},
                            code_dir=None, name="zr-v")
        self.assertEqual([c.name for c in calls.calls], ["zr-v-0", "zr-v-1"])
        with self.assertRaises(errors.ValidationError):
            self.fn.map([{"windows": [1]}], on=["demo-ml", "demo-ml"], shared={"data_uri": "x"}, code_dir=None)

    def test_a_job_that_has_not_ended_is_a_timeout_not_failed_configs(self):
        calls = self.fn.map([{"windows": [1]}, {"windows": [2]}], on=["aws-spot", "demo-ml"], shared={"data_uri": "x"},
                            code_dir=None)
        with mock.patch.object(calls.calls[0].handle, "wait", side_effect=errors.WaitTimeout("still running")):
            with self.assertRaises(errors.WaitTimeout):
                calls.result(timeout=1, return_exceptions=True)

    def test_a_destination_that_cannot_take_them_leaves_nothing_running(self):
        with self.assertRaises(errors.ValidationError):
            # demo-ml's room, then in turn: demo-ml, nowhere.
            self.fn.map([{"windows": [1]}, {"windows": [2]}, {"windows": [3]}], on=["demo-ml", "nowhere"],
                        shared={"data_uri": "x"}, code_dir=None)
        self.assertEqual(self.placed, ["demo-ml", "nowhere"])
        # The job already started on demo-ml is gone with it.
        self.assertEqual(self.jobs(), [])

    def test_delete_and_cancel_reach_every_job(self):
        calls = self.fn.map([{"windows": [1]}, {"windows": [2]}], on=["aws-spot", "demo-ml"],
                            shared={"data_uri": "x"}, code_dir=None)
        calls.result()
        self.assertEqual([c.job for c in calls.cost()], [c.name for c in calls.calls])
        calls.delete()
        self.assertEqual(self.jobs(), [])

    def test_dry_run_says_how_many_go_where_and_makes_nothing(self):
        plans = {"aws-spot": Plan("create", Target("aws-spot", "Template", "Ready", available=5, price={"hourlyRate": 0.03}, detail={"provider": "aws", "purchasingMode": "Spot"})),
                 "demo-ml": Plan("use", Target("demo-ml", "Pool", "Ready", available=1))}
        with mock.patch("zenthros.targetlist.plan_call", side_effect=lambda on, options, keep=False, python=None: plans[on]) as plan_call:
            plan = self.fn.map([{"windows": [1]}, {"windows": [2]}, {"windows": [3]}], on=["aws-spot", "demo-ml"],
                               shared={"data_uri": "x"}, code_dir=None, dry_run=True)
        self.assertEqual(plan.counts, {"aws-spot": 2, "demo-ml": 1})
        self.assertEqual([p for p, _ in plan.plans.values()], [plans["aws-spot"], plans["demo-ml"]])
        self.assertEqual(plan_call.call_count, 2)
        self.assertTrue(plan.ok)
        self.assertIn("aws-spot: 2 configs", repr(plan))
        self.assertIn("$0.03/h", repr(plan))
        self.assertEqual((self.placed, self.jobs()), ([], []))


if __name__ == "__main__":
    unittest.main()
