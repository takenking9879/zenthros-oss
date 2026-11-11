#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.targets(): where the Workspace can run, and dry_run=True.

Pinned: TYPE is what the machine is (vm, container, k8s; a pool), never
the API's kind, and templates are a section of their own; STATUS is one
lowercase word; CPU and MEM are what one machine has, never less what is in
use; how many machines can be used or made now is in each target's detail,
a single number for one machine and free / all for a pool; "local" is the
size a job there gets, and how many of create("local")'s size still fit in
Docker, after the limits of the Zenthros containers already there; no "?"
and no "up to"; dry_run creates nothing, and says before it fails what
does not fit.
"""
import unittest
from unittest import mock

import zenthros
from zenthros import errors, machines, remote
from zenthros import targetlist as targets

from support import local_in_wsl
from test_machines import Env

GIB = 1 << 30
OBSERVED = "2026-10-15T09:00:00Z"

ROWS = [
    {"name": "aws-spot", "type": "Template", "status": "Available", "available": 2, "milliCPU": 4000, "memoryBytes": 8 * GIB,
     "capacityFrom": "catalog", "provider": "aws", "region": "us-east-2", "purchasingMode": "Spot",
     "instanceTypes": ["c6i.xlarge", "c6i.2xlarge"], "limit": 2, "pool": "aws-spot",
     "price": {"hourlyRate": 0.068, "currency": "USD", "instanceType": "c6i.xlarge", "region": "us-east-2",
               "zone": "us-east-2b", "market": "Spot", "observedAt": OBSERVED}},
    {"name": "gpu-pool", "type": "Pool", "status": "Ready", "available": 2, "total": 3, "milliCPU": 4000,
     "memoryBytes": int(15.4 * GIB), "capacityFrom": "agent", "provider": "aws", "purchasingMode": "OnDemand",
     "gpuModel": "NVIDIA A10G", "gpuCount": 1,
     "price": {"hourlyRate": 0.52, "currency": "USD", "instanceType": "g5.xlarge", "market": "OnDemand", "observedAt": OBSERVED}},
    {"name": "cpu-pool", "type": "Pool", "status": "Available", "available": 0, "milliCPU": 4000, "maxMilliCPU": 8000,
     "memoryBytes": 8 * GIB, "maxMemoryBytes": 16 * GIB, "capacityFrom": "catalog", "provider": "aws", "purchasingMode": "OnDemand",
     "price": {"hourlyRate": 0.17, "maxHourlyRate": 0.34, "currency": "USD", "instanceType": "c6i.xlarge", "market": "OnDemand",
               "observedAt": OBSERVED}},
    {"name": "mi-ec2", "type": "External", "status": "Busy", "available": 0, "milliCPU": 16000, "memoryBytes": 62 * GIB,
     "capacityFrom": "agent", "id": "vm-1", "backendType": "vm", "runningJobs": 1, "maxJobs": 1},
    {"name": "mi-pc", "type": "External", "status": "Ready", "available": 1, "milliCPU": 1500, "memoryBytes": 8 * GIB,
     "capacityFrom": "agent", "id": "ct-1", "backendType": "container", "maxJobs": 1},
    {"name": "viejo", "type": "External", "status": "Offline", "available": 0, "milliCPU": 4000, "memoryBytes": 8 * GIB,
     "id": "vm-old", "backendType": "vm", "reason": "no heartbeat from its agent"},
]
CAN = {"joinBackends": True, "createMachines": True}


def answer(rows=ROWS, can=CAN):
    return 200, {"targets": rows, "can": can}


class Targets(Env):
    def setUp(self):
        super().setUp()
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer()
        # Docker here: 8 CPUs and 15.6 GiB; mi-pc's container holds 4 of
        # them and 8 GiB.
        # This computer's kubectl names no cluster here.
        cluster = mock.patch("zenthros.jobs.localk8s.current_context", return_value="")
        cluster.start()
        self.addCleanup(cluster.stop)
        self.docker.answers = {
            "info": (0, "8 16777216000\n"),
            "ps": (0, "4f1c0ffee ct-1\n"),
            "inspect": (0, "4000000000 8589934592\n"),
        }

    def changes(self):
        made = [r for r in self.api.requests if r["method"] != "GET"]
        ran = [args for args in self.docker_args() if args[0] in ("run", "build", "rm", "exec")]
        return made, ran

    def lines(self, table):
        """Each row by its name, in either section (a section's title and
        header lines are left out)."""
        return {line.split()[0]: line.split() for line in str(table).splitlines()
                if line.strip() and not line.startswith(("NAME ", "Templates:"))}


class TableTest(Targets):
    def test_the_columns_and_one_row_per_target(self):
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer([dict(r, gpuCount=0, gpuModel="") for r in ROWS])
        table = targets.targets()
        header = str(table).splitlines()[0].split()
        self.assertEqual(header, ["NAME", "TYPE", "STATUS", "CPU", "MEM", "GPU", "PRICE"])
        rows = self.lines(table)
        # local: what a job here gets, all of Docker's CPUs and 3/4 of its
        # memory; mi-pc's 4 CPUs and 8 GiB leave room for one more of its size.
        self.assertEqual(rows["local"], ["local", "container", "available", "8", "11.7Gi", "-", "-"])
        self.assertEqual(rows["mi-pc"], ["mi-pc", "container", "ready", "1.5", "8Gi", "-", "-"])
        self.assertEqual(rows["gpu-pool"], ["gpu-pool", "pool", "ready", "4", "15.4Gi", "-", "$0.52/h"])
        self.assertEqual(rows["cpu-pool"], ["cpu-pool", "pool", "available", "4–8", "8–16Gi", "-", "$0.17–0.34/h"])
        # Busy still has its 16 CPUs: what it has, not what is free.
        self.assertEqual(rows["mi-ec2"], ["mi-ec2", "vm", "busy", "16", "62Gi", "-", "-"])
        self.assertEqual(rows["viejo"], ["viejo", "vm", "offline", "-", "-", "-", "-"])
        # A template makes machines: its own section, with where it makes them.
        machines_part, templates_part = str(table).split("\n\nTemplates:")
        self.assertNotIn("aws-spot", machines_part)
        self.assertEqual(templates_part.splitlines()[1].split(), ["NAME", "PROVIDER", "STATUS", "CPU", "MEM", "GPU", "PRICE"])
        self.assertEqual(rows["aws-spot"], ["aws-spot", "AWS", "Spot", "available", "4", "8Gi", "-", "$0.068/h"])
        self.assertEqual({t.name: t.type for t in table}, {"local": "container", "mi-pc": "container", "mi-ec2": "vm",
                                                          "gpu-pool": "pool", "cpu-pool": "pool", "aws-spot": "template",
                                                          "viejo": "vm"})
        self.assertNotIn("?", str(table))
        self.assertNotIn("up to", str(table))

    def test_a_cluster_whose_nodes_are_not_known_says_no_count(self):
        # Found by the notebook E2E: a Kubernetes backend's agent does not
        # report its nodes yet, and the table said "1 / 0".
        row = {"name": "cluster-b", "type": "Kubernetes", "status": "Ready", "available": 1, "backendType": "k8s"}
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer([row])
        rows = self.lines(targets.targets())
        self.assertEqual(rows["cluster-b"], ["cluster-b", "k8s", "ready", "-", "-", "-", "-"])
        self.assertNotIn("of 0", repr(targets.targets()["cluster-b"]))

    def test_a_template_machine_with_no_job_is_asleep(self):
        # The notebook E2E (2026-10-20): create(template, idle="2m") is
        # asleep until a call, ready while one runs, asleep again after.
        row = {"name": "vm-dormida", "type": "VM", "status": "Available", "available": 1, "milliCPU": 2000,
               "memoryBytes": 8 * GIB, "template": "aws-spot", "provider": "aws", "reason": "asleep until its first call"}
        for state, status in (("Available", "asleep"), ("Ready", "ready"), ("Busy", "busy")):
            self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer([dict(row, status=state)])
            got = targets.targets()["vm-dormida"]
            self.assertEqual((got.type, got.status), ("vm", status))

    def test_it_is_zenthros_targets(self):
        self.assertIs(zenthros.targets, targets.targets)
        self.assertIn("targets", zenthros.__all__)

    def test_local_first_offline_last(self):
        names = [t.name for t in targets.targets()]
        self.assertEqual(names[0], "local")
        self.assertEqual(names[-1], "viejo")
        self.assertLess(names.index("mi-pc"), names.index("aws-spot"))

    def test_the_gpu_column_says_the_model_and_how_many_past_one(self):
        rows = self.lines(targets.targets())
        self.assertEqual(rows["gpu-pool"][5:7], ["NVIDIA", "A10G"])
        self.assertEqual(rows["mi-pc"][5], "-")
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(
            [dict(r, gpuCount=4) if r["name"] == "gpu-pool" else r for r in ROWS])
        self.assertEqual(self.lines(targets.targets())["gpu-pool"][5:8], ["4×", "NVIDIA", "A10G"])

    def test_html_is_a_table_and_escapes(self):
        rows = ROWS + [{"name": "<b>x</b>", "type": "External", "status": "Ready", "available": 1}]
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(rows)
        html = targets.targets()._repr_html_()
        self.assertIn("<table", html)
        self.assertIn("&lt;b&gt;x&lt;/b&gt;", html)
        self.assertNotIn("<b>x</b>", html)

    def test_detail_says_where_each_number_comes_from(self):
        detail = repr(targets.targets()["aws-spot"])
        for wanted in ("c6i.xlarge", "c6i.2xlarge", "catalog", "us-east-2b", OBSERVED[:10], "limit 2"):
            self.assertIn(wanted, detail)
        self.assertIn("Docker", repr(targets.targets()["local"]))
        with self.assertRaises(KeyError) as raised:
            targets.targets()["nada"]
        self.assertIn("aws-spot", str(raised.exception))

    def test_detail_says_its_python_and_the_ports_a_service_is_reached_on(self):
        rows = [dict(r, python="3.13", ports=[8000, 9000]) if r["name"] == "aws-spot" else r for r in ROWS]
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(rows)
        detail = repr(targets.targets()["aws-spot"])
        self.assertIn("python:     3.13", detail)
        self.assertIn("serves on:  8000, 9000", detail)
        self.assertNotIn("serves on", repr(targets.targets()["local"]))

    def test_to_pandas(self):
        try:
            import pandas  # noqa: F401
        except ImportError:
            self.skipTest("pandas is not installed")
        frame = targets.targets().to_pandas()
        self.assertEqual(list(frame["name"])[0], "local")
        self.assertEqual(frame.set_index("name").loc["mi-pc", "cpu"], 1.5)


class LocalRowTest(Targets):
    def test_without_docker_local_is_unavailable_and_says_why(self):
        self.docker.fail = {"info": (1, "Cannot connect to the Docker daemon")}
        row = targets.targets()["local"]
        self.assertEqual((row.status, row.available), ("unavailable", 0))
        self.assertIn("Docker", row.reason)

    def test_without_the_machines_role_local_still_runs(self):
        """on="local" asks nothing of the hub."""
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(can={"joinBackends": False, "createMachines": False})
        self.assertEqual(targets.targets()["local"].status, "available")

    def test_full_when_no_container_of_that_size_fits(self):
        # Two of 4 CPUs each: Docker's 8 are held.
        self.docker.answers["ps"] = (0, "a ct-1\nb ct-2\n")
        self.docker.answers["inspect"] = (0, "4000000000 4294967296\n4000000000 4294967296\n")
        row = targets.targets()["local"]
        self.assertEqual((row.status, row.available), ("full", 0))

    def test_the_default_size_follows_the_environment(self):
        with mock.patch.dict("os.environ", {machines.LOCAL_CPUS_ENV: "1", machines.LOCAL_MEMORY_ENV: "2Gi"}):
            row = targets.targets()["local"]
        # 4 CPUs and 7.6 GiB left: three of 1 CPU and 2 GiB.
        self.assertEqual((row.milli_cpu, row.memory_bytes, row.available), (1000, 2 * GIB, 3))


class DryRunTest(Targets):
    def test_create_local_dry_run_makes_nothing_and_says_what_it_would(self):
        # 4 CPUs and 7.6 GiB left in Docker: 4 and 6 GiB fit, 8 GiB does not.
        plan = machines.create("local", name="nuevo", cpus=4, memory="6Gi", dry_run=True)
        self.assertEqual(self.changes(), ([], []))
        self.assertTrue(plan.ok, plan.problems)
        self.assertFalse(machines.create("local", cpus=4, memory="8Gi", dry_run=True).ok)
        text = repr(plan)
        for wanted in ("create", "container", "4", "6 GiB", "nuevo"):
            self.assertIn(wanted, text)

    def test_create_local_that_does_not_fit_says_so_before(self):
        plan = machines.create("local", cpus=2, memory="64Gi", dry_run=True)
        self.assertFalse(plan.ok)
        self.assertIn("64 GiB", " ".join(plan.problems))
        self.assertEqual(self.changes(), ([], []))

    def test_create_from_a_template_dry_run_says_type_and_price(self):
        plan = machines.create("aws-spot", name="gpu-1", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertIn("$0.068/h", repr(plan))
        self.assertIn("c6i.xlarge", repr(plan))
        self.assertNotIn("asleep", repr(plan))
        self.assertEqual(self.changes(), ([], []))
        idle = machines.create("aws-spot", name="gpu-1", idle="30m", dry_run=True)
        self.assertIn("asleep until its first call; ends after 30m with no job", repr(idle))
        self.assertEqual(self.changes(), ([], []))

    def test_a_call_here_asks_nothing_of_the_hub(self):
        # Attaching this computer needs the permission; running on it does not.
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(can={"joinBackends": False, "createMachines": False})
        self.assertEqual(targets.plan_call("local", {}).problems, [])
        self.assertFalse(machines.create("local", dry_run=True).ok)

    def test_a_call_dry_run_says_when_the_templates_python_differs(self):
        rows = [dict(r, python="3.13") if r["name"] == "aws-spot" else r for r in ROWS]
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(rows)
        plan = targets.plan_call("aws-spot", {}, python="3.12")
        self.assertFalse(plan.ok)
        self.assertIn("3.13", " ".join(plan.problems))
        self.assertIn("3.12", " ".join(plan.problems))
        self.assertTrue(targets.plan_call("aws-spot", {}, python="3.13").ok)
        # A command carries no pickle: any Python runs it.
        self.assertTrue(targets.plan_call("aws-spot", {}, python=None).ok)
        # A job in its own image runs that image's Python, not the template's.
        self.assertTrue(targets.plan_call("aws-spot", {"image": "me/x:1"}, python="3.12").ok)
        self.assertEqual(self.changes(), ([], []))

    def test_create_without_the_right_says_which_role(self):
        aws = [dict(r, status="Unavailable", available=0, reason="making machines from a template needs the provisioner permission: ask the hub's administrator")
               if r["name"] == "aws-spot" else r for r in ROWS]
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(aws, {"joinBackends": True, "createMachines": False})
        plan = machines.create("aws-spot", dry_run=True)
        self.assertFalse(plan.ok)
        self.assertIn("provisioner permission: ask the hub's administrator", " ".join(plan.problems))

    def test_create_a_name_that_exists_uses_it(self):
        plan = machines.create("local", name="mi-pc", dry_run=True)
        self.assertEqual((plan.action, plan.target.name), ("use", "mi-pc"))

    def test_create_something_unknown_is_refused(self):
        with self.assertRaises(errors.ValidationError):
            machines.create("nada", dry_run=True)


def train(rows):
    return rows


class Submitting(Targets):
    """remote.call.run recorded, not sent: no tests of its own."""

    def setUp(self):
        super().setUp()
        self.submitted = []
        patcher = mock.patch.object(remote.call, "run", lambda *a, **k: self.submitted.append(k))
        patcher.start()
        self.addCleanup(patcher.stop)


class RemoteDryRunTest(Submitting):
    def test_a_function_asking_more_than_the_machine_has_is_told_before(self):
        fn = remote.cloud(memory="16Gi", cpu=2)(train)
        plan = fn.remote(on="mi-pc", params={"rows": 1}, dry_run=True)
        self.assertFalse(plan.ok)
        self.assertIn("16 GiB", " ".join(plan.problems))
        self.assertIn("8 GiB", " ".join(plan.problems))
        self.assertEqual((self.submitted, self.changes()), ([], ([], [])))

    def test_cloud_of_a_function_with_options_is_that_function_with_them(self):
        # cloud(fn, opts) once returned the decorator, and .remote raised AttributeError.
        fn = remote.cloud(train, memory="16Gi", cpu=2)
        self.assertIsInstance(fn, remote.CloudFunction)
        plan = fn.remote(on="mi-pc", params={"rows": 1}, dry_run=True)
        self.assertIn("16 GiB", " ".join(plan.problems))

    @local_in_wsl
    def test_on_local_is_a_container_of_the_functions_size(self):
        fn = remote.cloud(memory="2Gi", cpu=1)(train)
        plan = fn.remote(on="local", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertEqual((plan.action, plan.milli_cpu, plan.memory_bytes), ("create", 1000, 2 * GIB))
        self.assertIn("removed after", repr(plan))
        self.assertEqual((self.submitted, self.changes()), ([], ([], [])))

    def test_on_a_template_says_a_machine_is_made_for_the_call_and_its_price(self):
        fn = remote.cloud(memory="4Gi")(train)
        plan = fn.submit(on="aws-spot", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertEqual(plan.action, "create")
        self.assertIn("$0.068/h", repr(plan))
        self.assertEqual(self.submitted, [])

    def test_a_busy_machine_is_said(self):
        fn = remote.cloud(memory="4Gi")(train)
        plan = fn.remote(on="mi-ec2", dry_run=True)
        self.assertTrue(plan.ok)
        self.assertIn("busy", repr(plan))

    def test_an_offline_machine_is_a_problem(self):
        fn = remote.cloud(memory="4Gi")(train)
        plan = fn.remote(on="viejo", dry_run=True)
        self.assertFalse(plan.ok)
        self.assertIn("offline", " ".join(plan.problems))


NODES = [
    {"name": "a", "milliCPU": 4000, "memoryBytes": 16 * GIB, "freeMilliCPU": 1000, "freeMemoryBytes": 2 * GIB},
    {"name": "b", "milliCPU": 8000, "memoryBytes": 32 * GIB, "freeMilliCPU": 3750, "freeMemoryBytes": int(26.5 * GIB)},
    {"name": "c", "milliCPU": 4000, "memoryBytes": 64 * GIB, "gpuCount": 2, "freeMilliCPU": 4000, "freeMemoryBytes": 40 * GIB},
]
K8S = {"name": "k8s-lab", "type": "Kubernetes", "status": "Ready", "available": 2, "total": 3, "milliCPU": 8000,
       "memoryBytes": 32 * GIB, "gpuCount": 2, "capacityFrom": "agent", "id": "cluster-b", "backendType": "k8s", "nodes": NODES}


class KubernetesTest(Submitting):
    """A cluster is shown by node, never its sum: a job runs on one node.
    dry_run asks each node for what the function asks, not the table's
    room (2 CPUs and 4 GiB free)."""

    def setUp(self):
        super().setUp()
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(ROWS + [K8S])

    def test_the_row_is_its_largest_node_and_its_nodes_with_room(self):
        rows = self.lines(targets.targets())
        self.assertEqual(rows["k8s-lab"][:5], ["k8s-lab", "k8s", "ready", "8", "32Gi"])
        # The most GPUs one node has; their model is not reported.
        self.assertEqual(rows["k8s-lab"][5:7], ["2×", "GPU"])
        self.assertIn("2 of 3 nodes with room for a job", repr(targets.targets()["k8s-lab"]))

    def test_the_detail_lists_each_node_and_their_sum(self):
        detail = repr(targets.targets()["k8s-lab"])
        self.assertIn("2 of 3 nodes with room", detail)
        self.assertNotIn("machines free", detail)
        self.assertIn("16 CPUs, 112 GiB in 3 nodes", detail)
        node_b = next(line for line in detail.splitlines() if line.split()[:1] == ["b"])
        self.assertEqual(node_b.split(), ["b", "8", "CPUs,", "32", "GiB;", "free", "3.75,", "26.5", "GiB"])
        self.assertIn("2 GPUs", next(line for line in detail.splitlines() if line.split()[:1] == ["c"]))

    def test_a_function_that_fits_says_on_how_many_nodes(self):
        plan = remote.cloud(cpu=3, memory="8Gi")(train).remote(on="k8s-lab", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertEqual(plan.action, "use")
        self.assertIn("fits on 2 of 3 nodes now", repr(plan))
        self.assertEqual((self.submitted, self.changes()), ([], ([], [])))
        # a has 2 GiB free but a single CPU.
        two = remote.cloud(cpu=2, memory="2Gi")(train).remote(on="k8s-lab", dry_run=True)
        self.assertIn("fits on 2 of 3 nodes now", repr(two))

    def test_memory_of_one_node_and_cpu_of_another_is_not_a_fit_nor_a_misfit(self):
        # c has 4 CPUs and 64 GiB: the function fits it, though the row
        # (its largest node, b) shows 32 GiB; c has 40 GiB free, not 48.
        plan = remote.cloud(cpu=4, memory="48Gi")(train).remote(on="k8s-lab", dry_run=True)
        self.assertFalse(plan.ok)
        self.assertEqual(len(plan.problems), 1, plan.problems)
        self.assertIn("free now", plan.problems[0])
        self.assertNotIn("has 32 GiB", plan.problems[0])
        bigger = remote.cloud(cpu=8, memory="48Gi")(train).remote(on="k8s-lab", dry_run=True)
        self.assertEqual(len(bigger.problems), 1, bigger.problems)
        self.assertIn("no node of k8s-lab has 8 CPUs and 48 GiB;", bigger.problems[0])
        self.assertNotIn("free now", bigger.problems[0])

    def test_a_full_cluster_takes_a_function_that_fits_a_node(self):
        full = dict(K8S, status="Full", available=0)
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(ROWS + [full])
        plan = remote.cloud(cpu=1, memory="2Gi")(train).remote(on="k8s-lab", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertIn("fits on 3 of 3 nodes now", repr(plan))
        nothing = remote.cloud()(train).remote(on="k8s-lab", dry_run=True)
        self.assertIn("busy", " ".join(nothing.problems))

    def test_an_offline_cluster_is_a_problem_whatever_fits(self):
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(ROWS + [dict(K8S, status="Offline", available=0)])
        plan = remote.cloud(cpu=1, memory="2Gi")(train).remote(on="k8s-lab", dry_run=True)
        self.assertIn("offline", " ".join(plan.problems))

    def test_a_cluster_without_nodes_keeps_the_rows_check(self):
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(ROWS + [
            {"name": "old", "type": "Kubernetes", "status": "Ready", "available": 1, "backendType": "k8s"}])
        plan = remote.cloud(cpu=64, memory="512Gi")(train).remote(on="old", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertNotIn("nodes", repr(plan))


if __name__ == "__main__":
    unittest.main()
