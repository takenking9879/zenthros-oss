#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.create("k8s:<cluster>"): a pod of a Kubernetes backend of the
Workspace as a machine, a container that joins it as create("local")'s does.

Pinned: the pod's join token is sealed to the cluster's agent before the
job exists and is never in the job; the machine is returned only once it
is ready, and one that never gets there is undone, retired before its job
goes; delete retires before it deletes the pod's job; dry_run makes
nothing and says on how many nodes the pod fits now.
"""
import json
import os
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

from zenthros import errors, machines
from zenthros import targetlist as targets

from support import machine_side
from test_local import JOIN, PRIVATE, TOKEN
from test_machines import Env
from test_targets import K8S, ROWS, answer

CLUSTER = {"backendId": "cluster-b", "name": "k8s-lab", "backendType": "k8s", "healthy": True,
           "ageRecipients": ["age1cluster"]}
MI_POD = {**PRIVATE, "name": "mi-pod", "host": {"cpu": 2, "memoryBytes": 4 << 30}}
SEALED = {"type": "encrypted-payload", "encryptedPayload": {"ciphertext": "-----BEGIN AGE ENCRYPTED FILE-----\nxyz",
                                                             "keyId": "k1", "recipients": ["age1cluster"]},
          "materialization": {"mode": "env", "envName": "ZENTHROS_JOIN"}}


class PodTest(Env):
    def setUp(self):
        super().setUp()
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, {**JOIN, "name": "mi-pod"})
        self.api.routes[("POST", "/v1/workspaces/risk/seal")] = (200, {"secrets": [SEALED]})
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = (201, {"metadata": {"name": "machine-ct-1"}})
        self.api.routes[("DELETE", "/v1/workspaces/risk/jobs/machine-ct-1")] = (200, {})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"items": [
            {"metadata": {"name": "machine-ct-1", "labels": {"zenthros.ai/machine-backend": "ct-1"}}}]})
        # The name is free and the cluster is there; then the pod's agent
        # registers; then it is ready.
        self.listed = [[CLUSTER], [CLUSTER], [CLUSTER], [CLUSTER, MI_POD]]

    def posted(self, path):
        return [json.loads(r["body"]) for r in self.api.requests if r["method"] == "POST" and r["path"].endswith(path)]

    def asked(self):
        return [e for e in self.events if e.startswith("api ") and not e.startswith("api GET")]

    def test_a_pod_of_the_cluster_is_a_machine_returned_once_ready(self):
        m = machines.create("k8s:k8s-lab", name="mi-pod", cpus=2, memory="4Gi")
        self.assertEqual((m.name, m.id, m.target, m.job), ("mi-pod", "ct-1", "k8s:k8s-lab", "machine-ct-1"))
        # The join, then its token sealed to the cluster, then the job.
        self.assertEqual(self.asked(), ["api POST /backends", "api POST /seal", "api POST /jobs"])
        self.assertEqual(self.posted("/backends"), [{"name": "mi-pod"}])
        self.assertEqual(self.posted("/seal"), [{"backend": "cluster-b", "env": {"ZENTHROS_JOIN": JOIN["joinScript"]}}])
        job = self.posted("/jobs")[0]
        self.assertNotIn(TOKEN, json.dumps(job), "the join token is in the job")
        self.assertEqual(job["metadata"]["name"], "machine-ct-1")
        self.assertEqual(job["metadata"]["labels"]["zenthros.ai/machine-backend"], "ct-1")
        spec = job["spec"]
        self.assertEqual(spec["target"], {"backends": ["cluster-b"]})
        template = spec["workload"]["template"]
        self.assertEqual(spec["workload"]["kind"], "Job")
        self.assertEqual(template["image"], f"python:{machines.python_minor()}-slim-bookworm")
        self.assertEqual(template["workers"], {"min": 0, "max": 0, "cpu": "2", "memory": "4Gi"})
        self.assertEqual(template["secretSources"], [SEALED])
        # A spent token is not tried again.
        self.assertEqual(template["restartPolicy"], {"backoffLimit": 0})
        script = template["command"][-1]
        self.assertEqual(template["command"][:2], ["sh", "-c"])
        # The runner's packages, then the agent (no secret), then the join
        # from the sealed variable, which the pod's last process does not keep.
        steps = ["pip install", "cloudpickle", JOIN["installScript"].strip(), 'join="$ZENTHROS_JOIN"', 'unset "$name"',
                 '"$join" | sh', "unset join", "exec sleep infinity"]
        at = [script.find(step) for step in steps]
        self.assertNotIn(-1, at, script)
        self.assertEqual(at, sorted(at), script)
        # Ready before it is returned: the last thing asked is the list that said so.
        self.assertEqual(self.events[-1], "api GET /backends")
        self.assertEqual(self.docker.calls, [], "a pod needs no Docker here")

    def test_the_cluster_is_named_by_its_name_or_its_id(self):
        m = machines.create("k8s:cluster-b", name="mi-pod")
        self.assertEqual(m.target, "k8s:cluster-b")
        self.assertEqual(self.posted("/seal")[0]["backend"], "cluster-b")
        cpus, memory = machines.local_defaults()
        workers = self.posted("/jobs")[0]["spec"]["workload"]["template"]["workers"]
        self.assertEqual((workers["cpu"], workers["memory"]), (str(cpus), memory))

    def test_sizes_are_what_kubernetes_reads(self):
        # "8g" is docker's; Kubernetes reads "8g" as nothing and "512m" as
        # half a byte.
        machines.create("k8s:k8s-lab", name="mi-pod", cpus=1.5, memory="8g")
        workers = self.posted("/jobs")[0]["spec"]["workload"]["template"]["workers"]
        self.assertEqual((workers["cpu"], workers["memory"]), ("1.5", "8Gi"))
        self.assertEqual([machines.k8s_memory(m) for m in ("4Gi", "1.5Gi", "512Mi", "512m", 2)],
                         ["4Gi", "1536Mi", "512Mi", "512Mi", "2Gi"])

    def test_one_that_never_gets_ready_is_undone_retired_before_its_job_goes(self):
        self.listed = [[CLUSTER], [CLUSTER], [CLUSTER]]
        with mock.patch.object(machines, "READY_TIMEOUT", 0):
            with self.assertRaises(errors.WaitTimeout):
                machines.create("k8s:k8s-lab", name="mi-pod")
        self.assertEqual(self.asked()[-2:], ["api DELETE /backends/ct-1", "api DELETE /jobs/machine-ct-1"])

    def test_a_seal_that_fails_retires_the_join_and_makes_no_job(self):
        self.api.routes[("POST", "/v1/workspaces/risk/seal")] = (409, {"kind": "Status", "message": "backend cluster-b advertises no age key"})
        with self.assertRaises(Exception) as caught:
            machines.create("k8s:k8s-lab", name="mi-pod")
        self.assertIn("age key", str(caught.exception))
        self.assertEqual(self.asked(), ["api POST /backends", "api POST /seal", "api DELETE /backends/ct-1"])

    def test_not_a_cluster_is_refused_before_anything_is_made(self):
        self.listed = [[CLUSTER, MI_POD]]
        for target in ("k8s:mi-pod", "k8s:nada", "k8s:"):
            with self.assertRaises(errors.ValidationError) as caught:
                machines.create(target, name="otro")
            self.assertIn("k8s-lab", str(caught.exception), target)
        self.assertEqual(self.asked(), [])

    def test_delete_retires_before_the_pods_job_goes(self):
        self.listed = [[CLUSTER, MI_POD]]
        machines.delete("mi-pod")
        self.assertEqual(self.asked(), ["api DELETE /backends/ct-1", "api DELETE /jobs/machine-ct-1"])
        # Its job is found by the backend its pod joined as.
        self.assertIn(("GET", "/v1/workspaces/risk/jobs?limit=200&labelSelector=zenthros.ai%2Fmachine-backend%3Dct-1"),
                      [(r["method"], r["path"]) for r in self.api.requests])
        self.assertNotIn("rm", [args[0] for args in self.docker_args()])

    def test_delete_of_the_machine_create_returned_names_its_job(self):
        self.listed = [[CLUSTER, MI_POD]]
        machines.delete(machines.Machine(name="mi-pod", id="ct-1", target="k8s:k8s-lab", job="machine-ct-1"))
        self.assertEqual(self.asked(), ["api DELETE /backends/ct-1", "api DELETE /jobs/machine-ct-1"])
        self.assertEqual(self.docker.calls, [])

    def test_delete_of_a_vm_asks_for_no_pod(self):
        self.listed = [[CLUSTER, {"backendId": "vm-1", "name": "mi-ec2", "backendType": "vm", "healthy": True}]]
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/vm-1")] = (200, {"backendId": "vm-1"})
        machines.delete("mi-ec2")
        self.assertEqual(self.asked(), ["api DELETE /backends/vm-1"])
        self.assertNotIn("api GET /jobs", self.events)

    def test_on_the_pod_is_its_backend(self):
        self.listed = [[CLUSTER, MI_POD]]
        self.assertEqual(machines.place("mi-pod", {}).options, {"backend": "ct-1"})


@machine_side
class PodScriptTest(unittest.TestCase):
    """The pod's script, run by sh, with pip and sleep that do nothing."""

    def run_script(self, env):
        with tempfile.TemporaryDirectory() as bin_dir:
            for tool in ("pip", "sleep"):
                path = os.path.join(bin_dir, tool)
                with open(path, "w") as f:
                    f.write("#!/bin/sh\nexit 0\n")
                os.chmod(path, stat.S_IRWXU)
            script = machines.pod_script("echo installed\n")
            done = subprocess.run(["sh", "-c", script], capture_output=True, text=True, timeout=30,
                                  env={"PATH": f"{bin_dir}:/usr/bin:/bin", **env})
        self.assertEqual(done.returncode, 0, done.stderr)
        return done.stdout

    def test_the_join_sees_nothing_of_the_job_that_started_its_pod(self):
        # The pod is a job of the cluster's agent, which gives it its own
        # report endpoint and token; the machine's agent read them as its
        # own and its jobs' reports went to the cluster's agent, which
        # refused their tokens (found by the k8s pod E2E).
        out = self.run_script({"ZENTHROS_REPORT_ENDPOINT": "http://zenthros-agent.zenthros-system.svc:9445",
                               "ZENTHROS_REPORT_TOKEN": "outer-token", "ZENTHROS_JOB_ID": "outer-job",
                               "ZENTHROS_JOIN": "env; echo JOINED"})
        self.assertIn("installed", out)
        self.assertIn("JOINED", out)
        joined_env = out.split("installed", 1)[1]
        self.assertNotIn("ZENTHROS_", joined_env)
        self.assertNotIn("outer-token", joined_env)


class PodDryRunTest(Env):
    def setUp(self):
        super().setUp()
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = answer(ROWS + [K8S])
        self.docker.answers = {"info": (0, "8 16777216000\n"), "ps": (0, "")}

    def test_dry_run_says_on_how_many_nodes_it_fits_and_makes_nothing(self):
        plan = machines.create("k8s:k8s-lab", name="mi-pod", cpus=3, memory="8Gi", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertEqual((plan.action, plan.name, plan.milli_cpu, plan.memory_bytes), ("create", "mi-pod", 3000, 8 << 30))
        self.assertIn("fits on 2 of 3 nodes now", repr(plan))
        self.assertIn("a pod of k8s-lab", repr(plan))
        self.assertFalse([r for r in self.api.requests if r["method"] != "GET"])
        by_id = machines.create("k8s:cluster-b", cpus=3, memory="8Gi", dry_run=True)
        self.assertEqual(by_id.target.name, "k8s-lab")

    def test_dry_run_of_a_pod_no_node_holds_says_so(self):
        plan = targets.plan_create("k8s:k8s-lab", cpus=16, memory="8Gi")
        self.assertFalse(plan.ok)
        self.assertIn("no node of k8s-lab has 16 CPUs", plan.problems[0])

    def test_dry_run_on_what_is_not_a_cluster_is_refused(self):
        with self.assertRaises(errors.ValidationError):
            targets.plan_create("k8s:aws-spot")


if __name__ == "__main__":
    unittest.main()
