# SPDX-License-Identifier: Apache-2.0

"""Pools, fleets and Prewarms from the SDK: the whole spec, status verbatim."""

import json
import tempfile
import unittest

from zenthros import infra as I
from zenthros.errors import ValidationError, ZctlError
from zenthros.infra import InfraClient
from zenthros.zctl import EXIT_INVALID

from support import script_fake
from test_spec import REPO, calls, fake_zctl, sdk_leaves

CRDS = {
    I.VMPoolSpec: "infrastructure.zenthros.io_vmpools.yaml",
    I.VMFleetSpec: "infrastructure.zenthros.io_vmfleets.yaml",
    I.PrewarmSpec: "infrastructure.zenthros.io_prewarms.yaml",
}


def crd_leaves(filename):
    import yaml

    doc = yaml.safe_load((REPO / "operator" / "config" / "crd" / filename).read_text())
    root = doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
    out = set()

    def walk(node, path):
        if node.get("properties"):
            for k, v in node["properties"].items():
                walk(v, path + [k])
            return
        items = node.get("items") or {}
        if node.get("type") == "array" and items.get("properties"):
            walk(items, path + ["[]"])
            return
        out.add(".".join(path))

    walk(root, [])
    return out


class InfraParityTest(unittest.TestCase):
    def test_infra_parity_with_crd(self):
        for cls, filename in CRDS.items():
            with self.subTest(crd=filename):
                try:
                    crd = crd_leaves(filename)
                except ImportError:
                    self.skipTest("PyYAML is needed to read the CRD")
                sdk = sdk_leaves(cls)
                self.assertEqual(sorted(crd - sdk), [], "CRD fields the SDK cannot write")
                self.assertEqual(sorted(sdk - crd), [], "SDK fields the CRD does not have")

    def test_manifests(self):
        fleet = I.VMFleet(name="mix", spec=I.VMFleetSpec(
            members=[I.FleetMember(pool_ref="spot", min=0, max=8, weight=3), I.FleetMember(pool_ref="od", min=1)],
            total=I.FleetTotal(min_ready=1, max=12),
            constraints=[I.FleetConstraint(count_by="purchasingMode", value="Spot", max_fraction="0.75")]))
        m = fleet.to_manifest()
        self.assertEqual((m["apiVersion"], m["kind"], m["metadata"]), (I.API_VERSION, "VMFleet", {"name": "mix"}))
        self.assertEqual(m["spec"]["members"][0], {"poolRef": "spot", "min": 0, "max": 8, "weight": 3})
        self.assertEqual(m["spec"]["constraints"], [{"countBy": "purchasingMode", "value": "Spot", "maxFraction": "0.75"}])
        pool = I.VMPool(name="gpu", spec=I.VMPoolSpec(scaling=I.Scaling(min_ready=0, idle_ttl="10m"),
                                                   runtime=I.PoolRuntime(toolchains=[I.Toolchain(name="node", version="22")])))
        self.assertEqual(pool.to_manifest()["spec"], {"scaling": {"minReady": 0, "idleTTL": "10m"},
                                                      "runtime": {"toolchains": [{"name": "node", "version": "22"}]}})
        # A Container pool: no network, the image is what every job runs in.
        pod = I.VMPool(name="pods", spec=I.VMPoolSpec(
            machine_kind="Container", provider_ref=I.NameRef(name="runpod"),
            instance_policy=I.InstancePolicy(image="python:3.12-slim-bookworm", allowed_types=["small"]),
            runtime=I.PoolRuntime(toolchains=[I.Toolchain(name="python", packages=["requests"])])))
        self.assertEqual(pod.to_manifest()["spec"], {
            "machineKind": "Container", "providerRef": {"name": "runpod"},
            "instancePolicy": {"image": "python:3.12-slim-bookworm", "allowedTypes": ["small"]},
            "runtime": {"toolchains": [{"name": "python", "packages": ["requests"]}]}})
        # A pool whose machines are created in a registered cloud account; a
        # pool that names none says nothing of it.
        theirs = I.VMPool(name="vision", spec=I.VMPoolSpec(provider_ref=I.ProviderRef(name="aws", account="vision")))
        self.assertEqual(theirs.to_manifest()["spec"], {"providerRef": {"name": "aws", "account": "vision"}})
        ours = I.VMPool(name="ours", spec=I.VMPoolSpec(provider_ref=I.ProviderRef(name="aws")))
        self.assertEqual(ours.to_manifest()["spec"], {"providerRef": {"name": "aws"}})
        # A joined Spot pool in two zones, and a pool whose machines publish a port.
        nodes = I.VMPool(name="nodes", spec=I.VMPoolSpec(
            network_ref=I.NetworkRef(name="subnet-a", zones=[I.NetworkZone(name="subnet-b", zone="us-east-1b")]),
            instance_policy=I.InstancePolicy(disk_gb=200, min_cuda="12.9", security_group_ids=["sg-1"]),
            scaling=I.Scaling(replicas=3), interruption=I.Interruption(capacity_rebalance=True),
            exposed_ports=[I.ExposedPort(port=8000, protocol="HTTP")],
            join=I.Join(kubernetes=I.KubernetesJoin(
                backend_id="cluster-a", join_method="K3s", node_labels={"tier": "gpu"},
                node_taints=[I.NodeTaint(key="gpu", value="true", effect="NoSchedule")]))))
        self.assertEqual(nodes.to_manifest()["spec"], {
            "networkRef": {"name": "subnet-a", "zones": [{"name": "subnet-b", "zone": "us-east-1b"}]},
            "instancePolicy": {"diskGB": 200, "minCUDA": "12.9", "securityGroupIDs": ["sg-1"]},
            "scaling": {"replicas": 3}, "interruption": {"capacityRebalance": True},
            "exposedPorts": [{"port": 8000, "protocol": "HTTP"}],
            "join": {"kubernetes": {"backendID": "cluster-a", "joinMethod": "K3s", "nodeLabels": {"tier": "gpu"},
                                    "nodeTaints": [{"key": "gpu", "value": "true", "effect": "NoSchedule"}]}}})
        pw = I.Prewarm(name="w", spec=I.PrewarmSpec(targets=I.PrewarmTargets(pools=["gpu"]),
                                                    items=[I.PrewarmItem(image="ghcr.io/o/t:1")], on_new_members=True))
        self.assertEqual(pw.to_manifest()["spec"], {"targets": {"pools": ["gpu"]}, "items": [{"image": "ghcr.io/o/t:1"}],
                                                    "onNewMembers": True})


@script_fake("zctl")
class InfraClientTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def test_create_writes_the_object(self):
        zctl, log = fake_zctl(self.dir.name)
        InfraClient(zctl).create(I.Prewarm(name="w", spec=I.PrewarmSpec(items=[I.PrewarmItem(image="x:1")])))
        recorded = calls(log)
        self.assertEqual(recorded[0][:3], ["create", "prewarm", "-f"])
        self.assertEqual(json.loads(recorded[1]["file"])["spec"]["items"], [{"image": "x:1"}])

    def test_get_keeps_status_verbatim(self):
        refusal = 'failed to resolve reference "ghcr.io/o/x:1": 403 Forbidden\nsecond line'
        stored = {"kind": "Prewarm", "status": {"targets": [{"backendID": "m1", "phase": "Failed",
                                                             "items": [{"ref": "ghcr.io/o/x:1", "phase": "Failed", "message": refusal}]}]}}
        zctl, log = fake_zctl(self.dir.name, json.dumps(stored))
        got = InfraClient(zctl).get(I.Prewarm, "w")
        self.assertEqual(calls(log)[0], ["get", "prewarm", "w", "-o", "json"])
        self.assertEqual(got["status"]["targets"][0]["items"][0]["message"], refusal)

    def test_scale_and_delete(self):
        zctl, log = fake_zctl(self.dir.name)
        c = InfraClient(zctl)
        c.scale_pool("gpu", min_ready=0, max_ready=4)
        c.scale_fleet("mix", min_ready=2)
        c.delete("VMPool", "gpu", "--wait")
        self.assertEqual(calls(log), [["scale", "vmpool", "gpu", "--min", "0", "--max", "4"],
                                      ["scale", "vmfleet", "mix", "--min-ready", "2"],
                                      ["delete", "vmpool", "gpu", "--wait"]])

    def test_refusal_is_verbatim(self):
        text = 'VMFleet.infrastructure.zenthros.io "mix" is invalid: spec.members: Required value'
        zctl, _ = fake_zctl(self.dir.name, code=EXIT_INVALID, stderr=text)
        with self.assertRaises(ValidationError) as caught:
            InfraClient(zctl).create(I.VMFleet(name="mix"))
        self.assertEqual(str(caught.exception), text)

    def test_other_refusals_keep_zctls_exit_code(self):
        zctl, _ = fake_zctl(self.dir.name, code=2, stderr="the controller is not reachable")
        with self.assertRaises(ZctlError) as caught:
            InfraClient(zctl).get("VMPool", "gpu")
        self.assertEqual((str(caught.exception), caught.exception.returncode), ("the controller is not reachable", 2))


if __name__ == "__main__":
    unittest.main()
