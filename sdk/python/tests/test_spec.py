# SPDX-License-Identifier: Apache-2.0

"""The SDK speaks the whole ZenthrosJob spec and reads status word for word."""

import json
import os
import re
import stat
import sys
import tempfile
import textwrap
import typing
import unittest
from dataclasses import fields, is_dataclass
from pathlib import Path
from unittest import mock

from zenthros import errors
from zenthros.jobs import spec as S
from zenthros.jobs.client import ZctlJobClient
from zenthros.zctl import EXIT_INVALID

from support import script_fake

REPO = Path(__file__).resolve().parents[3]
CRD = REPO / "operator" / "config" / "crd" / "zenthros.ai_zenthrosjobs.yaml"

# Leaves the SDK does not model field by field, and why.
OPAQUE = {
    # The user's own object and Spark's own settings: passed through as dicts.
    "workload.template.manifest.object",
    "workload.template.spark.sparkConf",
}


def sdk_leaves(cls, prefix=""):
    """Every CRD path the dataclasses can write. A list's element type is
    named in metadata "of", in the module of the class that declares it."""
    module = sys.modules[cls.__module__]
    out = set()
    hints = typing.get_type_hints(cls)
    for f in fields(cls):
        key = f.metadata.get("json") or S._camel(f.name)
        path = f"{prefix}{key}"
        kind = hints[f.name]
        args = [a for a in typing.get_args(kind) if a is not type(None)]
        inner = args[0] if args else kind
        of = f.metadata.get("of")
        if of:
            out |= sdk_leaves(getattr(module, of), path + ".[].")
        elif is_dataclass(inner):
            out |= sdk_leaves(inner, path + ".")
        else:
            out.add(path)
    return out


def crd_leaves():
    import yaml

    doc = yaml.safe_load(CRD.read_text())
    root = doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
    out = set()

    def walk(node, path):
        props = node.get("properties")
        if props:
            # A field the CRD refuses outright (rule "!has(self.x)") is
            # written by no client: the controller's own, such as an
            # encrypted payload's workspaceSecret.
            refused = {r.get("rule", "")[len("!has(self."):-1] for r in node.get("x-kubernetes-validations") or []
                       if re.fullmatch(r"!has\(self\.\w+\)", r.get("rule", ""))}
            for k, v in props.items():
                if k not in refused:
                    walk(v, path + [k])
            return
        items = node.get("items") or {}
        if node.get("type") == "array" and items.get("properties"):
            walk(items, path + ["[]"])
            return
        out.add(".".join(path))

    walk(root, [])
    return out


def sdk_classes(cls, prefix=""):
    """{CRD object path: the dataclass that writes it}."""
    module = sys.modules[cls.__module__]
    out = {prefix.rstrip("."): cls}
    hints = typing.get_type_hints(cls)
    for f in fields(cls):
        key = f.metadata.get("json") or S._camel(f.name)
        args = [a for a in typing.get_args(hints[f.name]) if a is not type(None)]
        inner = args[0] if args else hints[f.name]
        of = f.metadata.get("of")
        if of:
            out.update(sdk_classes(getattr(module, of), f"{prefix}{key}.[]."))
        elif is_dataclass(inner):
            out.update(sdk_classes(inner, f"{prefix}{key}."))
    return out


def crd_required_scalars():
    """{object path: required integer and boolean keys whose zero is valid}:
    a typed client (zctl) always sends them. A required count with a minimum
    of 1 is the caller's to set, through either door."""
    import yaml

    doc = yaml.safe_load(CRD.read_text())
    root = doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
    out = {}

    def walk(node, path):
        props = node.get("properties")
        if props:
            scalars = [k for k in node.get("required", [])
                       if props.get(k, {}).get("type") in ("integer", "boolean") and props[k].get("minimum", 0) <= 0]
            if scalars:
                out[".".join(path)] = scalars
            for k, v in props.items():
                walk(v, path + [k])
            return
        items = node.get("items") or {}
        if node.get("type") == "array" and items.get("properties"):
            walk(items, path + ["[]"])

    walk(root, [])
    return out


class SpecParityTest(unittest.TestCase):
    def test_a_job_is_named_as_the_crd_names_it(self):
        """API_VERSION, KIND and RESOURCE (the plural a Status names a job
        by, which tells a job gone from another 404) are the CRD's."""
        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML is needed to read the CRD")
        crd = yaml.safe_load(CRD.read_text())["spec"]
        self.assertEqual((S.API_VERSION, S.KIND, S.RESOURCE),
                         (crd["group"] + "/" + crd["versions"][0]["name"], crd["names"]["kind"], crd["names"]["plural"]))

    def test_spec_parity_with_crd(self):
        try:
            crd = crd_leaves()
        except ImportError:
            self.skipTest("PyYAML is needed to read the CRD")
        sdk = sdk_leaves(S.Spec)
        missing = sorted(crd - sdk - OPAQUE)
        extra = sorted(sdk - crd - OPAQUE)
        self.assertEqual(missing, [], f"CRD fields the SDK cannot write: {missing}")
        self.assertEqual(extra, [], f"SDK fields the CRD does not have: {extra}")

    def test_required_scalars_are_always_written(self):
        # zctl decodes into the Go type, which writes workers.min=0; the
        # SDK's JSON through zenthros-api reaches the CRD as it is, so a
        # required 0 or false must be in it (2026-09-30: workers.min/max
        # "Required value" through the API, not through zctl).
        try:
            required = crd_required_scalars()
        except ImportError:
            self.skipTest("PyYAML is needed to read the CRD")
        classes = sdk_classes(S.Spec)
        self.assertIn("workload.template.workers", required)
        for path, keys in required.items():
            if path not in classes:
                continue
            plain = S.to_plain(classes[path]())
            for key in keys:
                self.assertIn(key, plain, f"{path}.{key} is required by the CRD; the SDK must write its default")

    def test_full_spec_renders_crd_keys(self):
        job = S.Job(
            name="train",
            labels={"team": "ml"},
            spec=S.Spec(
                workspace="ws-a",
                pool_ref="gpu",
                compute=S.Compute(runtime="containerd", purchase_option="Spot"),
                resilience=S.Resilience(on_worker_lost="Reschedule", job_grace="45s",
                                        preempt_notice=S.PreemptNotice(signal="SIGUSR1")),
                placement=S.Placement(separate_machines=False,
                                      spread=[S.SpreadRule(topology_key="zone", max_skew=1)]),
                gc_policy=S.GCPolicy(ttl_seconds_after_finished=0),
                active_deadline_seconds=3600,
                workload=S.Workload(kind="Job", template=S.Template(
                    command=["python3", "train.py"], args=["--epochs", "3"],
                    image="ghcr.io/org/train:1",
                    env=[S.EnvVar(name="A", value='quoted "x"')],
                    distribution=S.Distribution(count=4, tasks_per_node=1),
                    restart_policy=S.RestartPolicy(backoff_limit=0),
                )),
            ),
        )
        m = json.loads(job.to_json("tenant-default"))
        spec = m["spec"]
        self.assertEqual(m["metadata"], {"name": "train", "namespace": "tenant-default", "labels": {"team": "ml"}})
        self.assertEqual(spec["poolRef"], "gpu")
        self.assertEqual(spec["resilience"]["preemptNotice"], {"signal": "SIGUSR1"})
        # A false or zero that was set is written; unset stays out.
        self.assertIs(spec["placement"]["separateMachines"], False)
        self.assertEqual(spec["gcPolicy"], {"ttlSecondsAfterFinished": 0})
        self.assertEqual(spec["workload"]["template"]["restartPolicy"], {"backoffLimit": 0})
        self.assertEqual(spec["workload"]["template"]["distribution"],
                         {"mode": "Independent", "count": 4, "tasksPerNode": 1})
        self.assertEqual(spec["workload"]["template"]["env"], [{"name": "A", "value": 'quoted "x"'}])
        self.assertNotIn("cancel", spec)
        self.assertNotIn("target", spec)


def fake_zctl(directory, stdout="", code=0, stderr=""):
    """An executable that records its argv and prints stdout."""
    log = Path(directory) / "calls.jsonl"
    out = Path(directory) / "stdout"
    out.write_text(stdout)
    script = Path(directory) / "zctl"
    script.write_text(textwrap.dedent(f"""\
        #!{sys.executable}
        import json, sys
        with open({str(log)!r}, "a") as f:
            f.write(json.dumps(sys.argv[1:]) + "\\n")
            for i, a in enumerate(sys.argv):
                if a == "-f":
                    f.write(json.dumps({{"file": open(sys.argv[i + 1]).read()}}) + "\\n")
        sys.stdout.write(open({str(out)!r}).read())
        sys.stderr.write({stderr!r})
        sys.exit({code})
        """))
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return str(script), log


def calls(log):
    return [json.loads(line) for line in Path(log).read_text().splitlines()]


STORED = {
    "metadata": {"name": "train", "generation": 3},
    "status": {
        "phase": "Failed",
        "message": 'member rank 1 failed: exit status 1: RuntimeError: NCCL timeout\nsecond "line"',
        "reasons": [{"code": "PROCESS_FAILED", "detail": "exit status 1: RuntimeError: NCCL timeout"}],
        "conditions": [{"type": "Cleanup", "status": "True", "message": "rayjobs.ray.io \"x\" is forbidden"}],
        "members": [{"rank": 0, "job": "train-0", "phase": "Cancelled"}],
        "workerLostRetries": 1,
        "restarts": 2,
        "children": [{"name": "shards", "count": 1, "failed": 1, "children": [
            {"index": 0, "job": "train-shards-0", "phase": "Failed", "message": "exit 3: bad row\nsecond"}]}],
    },
}


@script_fake("zctl")
class ZctlClientTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def test_status_json_keeps_message_verbatim(self):
        zctl, log = fake_zctl(self.dir.name, json.dumps(STORED))
        client = ZctlJobClient(zctl=zctl, namespace="tenant-default", poll_seconds=0)
        status = client.job_status("train")
        self.assertEqual(calls(log)[0], ["get", "zenthrosjob", "-n", "tenant-default", "-o", "json", "train"])
        self.assertEqual(status.message, STORED["status"]["message"])
        self.assertEqual(status.reason, "PROCESS_FAILED")
        self.assertEqual(status.conditions[0]["message"], 'rayjobs.ray.io "x" is forbidden')
        self.assertEqual((status.generation, status.worker_lost_retries, status.restarts), (3, 1, 2))
        self.assertEqual(status.children[0]["children"][0]["message"], "exit 3: bad row\nsecond")
        self.assertEqual(status.members[0]["job"], "train-0")
        with self.assertRaises(errors.JobFailed) as caught:
            client.wait("train")
        self.assertEqual(str(caught.exception), STORED["status"]["message"])
        self.assertEqual(caught.exception.reasons[0]["code"], "PROCESS_FAILED")

    def test_zctl_resolves_namespace_and_gateway(self):
        # With nothing set, the SDK passes neither -n nor gateway flags: zctl
        # takes them from ZENTHROS_* or the zctl context, so both agree.
        zctl, log = fake_zctl(self.dir.name, "")
        env = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        with mock.patch.dict(os.environ, env, clear=True):
            client = ZctlJobClient(zctl=zctl, poll_seconds=0)
            client.logs("train")
            client.cancel("train")
            self.assertNotIn("namespace", S.Job(name="train").to_manifest()["metadata"])
        self.assertEqual(calls(log)[0], ["logs", "train"])
        self.assertEqual(calls(log)[1], ["cancel", "zenthrosjob", "train"])

    def test_wait_timeout_raises_WaitTimeout(self):
        running = {"metadata": {"name": "train"}, "status": {"phase": "Running"}}
        zctl, log = fake_zctl(self.dir.name, json.dumps(running))
        client = ZctlJobClient(zctl=zctl, namespace="tenant-default", poll_seconds=0)
        with self.assertRaises(errors.WaitTimeout) as caught:
            client.wait("train", timeout=0)
        self.assertIn("still Running", str(caught.exception))
        self.assertFalse(any(c[0] == "cancel" for c in calls(log) if isinstance(c, list)))
        with self.assertRaises(errors.WaitTimeout):
            client.wait("train", timeout=0, cancel_on_timeout=True)
        self.assertIn(["cancel", "zenthrosjob", "-n", "tenant-default", "train"], calls(log))

    def test_wait_surfaces_zctl_refusals(self):
        # A usage refusal is permanent: it reaches the caller at once, whole.
        text = "zctl: unexpected arguments [-o json] (place flags before the name)"
        zctl, _ = fake_zctl(self.dir.name, code=EXIT_INVALID, stderr=text)
        client = ZctlJobClient(zctl=zctl, poll_seconds=0)
        with self.assertRaises(errors.ValidationError) as caught:
            client.wait("train", timeout=30)
        self.assertIn(text, str(caught.exception))
        # One that may clear is retried, and named if the wait runs out.
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        zctl, _ = fake_zctl(other.name, code=1, stderr="Unable to connect to the server: dial tcp 10.0.0.1:6443: i/o timeout")
        with self.assertRaises(errors.WaitTimeout) as caught:
            ZctlJobClient(zctl=zctl, poll_seconds=0).wait("train", timeout=0)
        self.assertIn("last zctl error: Unable to connect to the server: dial tcp 10.0.0.1:6443: i/o timeout", str(caught.exception))

    def test_cancel_uses_zctl_cancel(self):
        zctl, log = fake_zctl(self.dir.name)
        client = ZctlJobClient(zctl=zctl, namespace="ns")
        client.cancel("train")
        client.delete("train")
        self.assertEqual(calls(log), [["cancel", "zenthrosjob", "-n", "ns", "train"],
                                      ["delete", "zenthrosjob", "-n", "ns", "train"]])

    def test_submit_typed_job_writes_the_whole_spec(self):
        zctl, log = fake_zctl(self.dir.name)
        client = ZctlJobClient(zctl=zctl, namespace="tenant-default")
        job = S.Job(name="train", spec=S.Spec(workspace="ws-a", target=S.Target(backends=["m1", "m2"]),
                                             workload=S.Workload(template=S.Template(
                                                 command=["sh", "-c", "echo 'a: b' # not yaml"],
                                                 distribution=S.Distribution(mode="Independent", count=2)))))
        handle = client.submit(job)
        self.assertEqual(handle.job_id, "train")
        recorded = calls(log)
        self.assertEqual(recorded[0][:4], ["create", "zenthrosjob", "-f", recorded[0][3]])
        self.assertIn("--workspace", recorded[0])
        written = json.loads(recorded[1]["file"])
        self.assertEqual(written["spec"]["workload"]["template"]["command"], ["sh", "-c", "echo 'a: b' # not yaml"])
        self.assertEqual(written["spec"]["target"], {"backends": ["m1", "m2"]})

    def test_zctl_refusal_reaches_caller_verbatim(self):
        text = 'admission webhook "x" denied the request: spec.placement needs spec.target.backends'
        zctl, _ = fake_zctl(self.dir.name, code=1, stderr=text)
        client = ZctlJobClient(zctl=zctl)
        with self.assertRaises(Exception) as caught:
            client.submit(S.Job(name="x", spec=S.Spec(workspace="ws-a")))
        self.assertIn(text, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
