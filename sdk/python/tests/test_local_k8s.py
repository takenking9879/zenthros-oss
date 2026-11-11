# SPDX-License-Identifier: Apache-2.0

"""on="local-k8s": what the SDK asks of kubectl and helm, with both faked. What
they do on a real cluster is tests/e2e/test_quickstart_k8s.py."""

import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from zenthros import errors
from zenthros.jobs import localk8s
from zenthros.jobs.localk8s import LocalK8sJobClient

from support import local_in_wsl

MARK = "abc123"


class FakeKube:
    """kubectl: what the cluster answers, and every call."""

    binary = "kubectl"

    def __init__(self, deployed=None, nodes=(("amd64", 0),), mark=MARK):
        self.deployed, self.nodes, self.mark = deployed, nodes, mark
        self.calls = []

    def run(self, args, stdin=None, timeout=600):
        self.calls.append(args)
        args = [a for a in args]
        if args[:2] == ["get", "deploy"]:
            return 0, json.dumps(self.deployed) if self.deployed else "", ""
        if args[:2] == ["get", "nodes"]:
            items = [{"status": {"nodeInfo": {"architecture": arch}, "allocatable": {"nvidia.com/gpu": str(gpus)}}} for arch, gpus in self.nodes]
            return 0, json.dumps({"items": items}), ""
        if "exec" in args:
            verb = args[args.index("local") + 1]
            if verb == "info":
                return 0, json.dumps({"sharedMark": self.mark}), ""
        return 0, "", ""


class FakeHelm:
    binary = "helm"

    def __init__(self, code=0, err=""):
        self.code, self.err, self.calls = code, err, []

    def run(self, args, stdin=None, timeout=600):
        self.calls.append(args)
        return self.code, "", self.err


def deployment(image, folder):
    return {"spec": {"template": {"spec": {"containers": [{"image": image, "env": [{"name": "ZENTHROS_LOCAL_SHARED", "value": str(folder)}]}]}}},
            "status": {"availableReplicas": 1}}


@local_in_wsl
class Base(unittest.TestCase):
    def setUp(self):
        self.home = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.home, ignore_errors=True))
        env = mock.patch.dict(os.environ, {"HOME": str(self.home), localk8s.IMAGE_ENV: "zenthros/agent:test", localk8s.CHART_ENV: "/chart"})
        env.start()
        self.addCleanup(env.stop)
        self.folder = (self.home / ".zenthros" / "k8s")
        self.folder.mkdir(parents=True)
        (self.folder / localk8s.SHARED_MARK).write_text(MARK + "\n")
        self.asked = []

    def client(self, **kube):
        self.kube, self.helm = FakeKube(**kube), FakeHelm()
        client = LocalK8sJobClient(root=self.home, kube=self.kube, helm=self.helm, ask=self.asked.append)
        return client


class InstallTest(Base):
    def test_the_first_use_asks_then_installs_the_same_chart_with_no_hub(self):
        client = self.client()
        self.assertEqual(client.ensure(None), "zenthros-agent")
        self.assertEqual(len(self.asked), 1)
        args = self.helm.calls[0]
        self.assertEqual(args[:4], ["upgrade", "--install", "zenthros-agent", "/chart"])
        self.assertIn("zenthros-local", args)
        self.assertIn("agent.local.enabled=true", args)
        self.assertIn(f"agent.local.sharedFolder={self.folder.resolve()}", args)
        self.assertIn(f"agent.local.user={os.getuid()}:{os.getgid()}", args)
        self.assertIn("agent.image=zenthros/agent:test", args)

    def test_nothing_is_installed_if_the_answer_is_no(self):
        client = self.client()
        def no(where):
            raise errors.ValidationError("nothing was installed")
        client._ask = no
        with self.assertRaises(errors.ValidationError):
            client.ensure(None)
        self.assertEqual(self.helm.calls, [])

    def test_with_no_terminal_it_says_how_to_answer(self):
        with mock.patch.dict(os.environ, {localk8s.INSTALL_ENV: ""}), mock.patch("sys.stdin", io.StringIO()):
            with self.assertRaisesRegex(errors.ValidationError, localk8s.INSTALL_ENV):
                localk8s._ask_in_a_terminal("k3d-zenthros")

    def test_an_agent_already_there_as_wanted_is_left_alone(self):
        client = self.client(deployed=deployment("zenthros/agent:test", self.folder.resolve()))
        client.ensure(None)
        self.assertEqual((self.asked, self.helm.calls), ([], []))

    def test_another_agent_is_upgraded_without_asking_again(self):
        client = self.client(deployed=deployment("zenthros/agent:old", self.folder.resolve()))
        client.ensure(None)
        self.assertEqual(self.asked, [])
        self.assertEqual(len(self.helm.calls), 1)

    def test_a_cluster_without_the_folder_runs_what_fits_in_the_job(self):
        client = self.client(mark="")
        self.assertEqual(client.ensure(None), localk8s.RELEASE, "the agent is there")
        self.assertFalse(client.sees_store())

    def test_a_cluster_with_the_folder_sees_the_store(self):
        self.assertTrue(self.client().sees_store())

    def test_a_path_on_a_cluster_without_the_folder_says_how_to_share_it(self):
        client = self.client(mark="")
        with self.assertRaisesRegex(errors.ValidationError, "k3d cluster create"):
            client.need_paths([str(localk8s.shared_folder() / "data.csv")])

    def test_what_it_shares_is_known_without_installing_anything(self):
        client = self.client()
        self.assertIsNone(client.shared_now(), "no agent yet: nobody can say")
        self.assertEqual((self.asked, self.helm.calls), ([], []))

    def test_an_agent_already_there_says_if_it_sees_the_folder(self):
        there = deployment("zenthros/agent:test", self.folder.resolve())
        self.assertFalse(self.client(deployed=there, mark="other").shared_now())
        self.assertTrue(self.client(deployed=there).shared_now())

    def test_it_is_ready_once_per_process(self):
        client = self.client(deployed=deployment("zenthros/agent:test", self.folder.resolve()))
        client.ensure(None)
        before = len(self.kube.calls)
        client.ensure(None)
        self.assertEqual(len(self.kube.calls), before)

    def test_every_verb_goes_by_exec_in_its_namespace(self):
        client = self.client()
        args = client._exec_args("zenthros-agent", "status")
        self.assertEqual(args, ["exec", "-i", "-n", "zenthros-local", "deploy/zenthros-agent", "-c", "agent", "--", "/agent", "local", "status"])

    def test_the_context_configured_is_the_one_used(self):
        settings = mock.Mock(kube_context="k3d-other")
        with mock.patch.object(localk8s, "_settings", return_value=settings):
            client = self.client(deployed=deployment("zenthros/agent:test", self.folder.resolve()))
            client.ensure(None)
            self.assertTrue(all(call[:2] == ["--context", "k3d-other"] for call in self.kube.calls), self.kube.calls)


class ChartTest(Base):
    def setUp(self):
        super().setUp()
        os.environ.pop(localk8s.CHART_ENV)
        self.chart = b"a chart"
        self.sums = f"{hashlib.sha256(self.chart).hexdigest()}  ./{localk8s.CHART_FILE}\n".encode()
        self.pin = {"install_base": "http://r", "sha256sums_sha256": hashlib.sha256(self.sums).hexdigest()}
        self.cache = mock.patch.object(localk8s, "cache_root", return_value=self.home / "cache")
        self.cache.start()
        self.addCleanup(self.cache.stop)

    def download(self, url):
        return self.sums if url.endswith("SHA256SUMS") else self.chart

    def test_the_chart_is_checked_against_the_wheels_pin_and_kept(self):
        with mock.patch.object(localk8s, "agent_pin", return_value=self.pin), mock.patch.object(localk8s, "_download", self.download):
            path = self.client()._chart()
        self.assertEqual(Path(path).read_bytes(), self.chart)
        with mock.patch.object(localk8s, "agent_pin", return_value=self.pin), mock.patch.object(localk8s, "_download", side_effect=AssertionError):
            self.assertEqual(self.client()._chart(), path)

    def test_other_sums_are_refused(self):
        pin = {**self.pin, "sha256sums_sha256": "0" * 64}
        with mock.patch.object(localk8s, "agent_pin", return_value=pin), mock.patch.object(localk8s, "_download", self.download):
            with self.assertRaisesRegex(errors.ValidationError, "not the one this SDK pins"):
                self.client()._chart()

    def test_another_chart_is_refused_and_not_kept(self):
        self.chart = b"another chart"
        sums = self.sums
        def download(url):
            return sums if url.endswith("SHA256SUMS") else self.chart
        with mock.patch.object(localk8s, "agent_pin", return_value=self.pin), mock.patch.object(localk8s, "_download", download):
            with self.assertRaisesRegex(errors.ValidationError, "not the one its SHA256SUMS lists"):
                self.client()._chart()
        self.assertFalse((self.home / "cache" / "chart").exists() and list((self.home / "cache" / "chart").rglob(localk8s.CHART_FILE)))

    def test_a_wheel_with_no_pin_says_so(self):
        with mock.patch.object(localk8s, "agent_pin", return_value=None):
            with self.assertRaisesRegex(errors.ValidationError, localk8s.CHART_ENV):
                self.client()._chart()


class FitsTest(Base):
    def manifest(self, image="python:3.13", **compute):
        return {"spec": {"workload": {"template": {"image": image}}, "compute": compute}}

    def test_an_image_of_another_architecture_is_refused_before_anything_is_made(self):
        client = self.client(nodes=[("arm64", 0)])
        client._platforms["only-amd"] = ["amd64"]
        with self.assertRaisesRegex(errors.ValidationError, "only amd64 and this cluster is arm64"):
            client._box_for(self.manifest("only-amd"))
        self.assertEqual(self.helm.calls, [])

    def test_one_it_cannot_read_is_left_to_the_cluster(self):
        client = self.client(nodes=[("arm64", 0)])
        client._platforms["private"] = []
        self.assertEqual(client._box_for(self.manifest("private")).image, "private")

    def test_a_gpu_on_a_cluster_with_none_is_refused(self):
        client = self.client(nodes=[("amd64", 0)])
        client._platforms["python:3.13"] = ["amd64"]
        with self.assertRaisesRegex(errors.ValidationError, "no GPU"):
            client._box_for(self.manifest(gpuCount=1))

    def test_a_gpu_on_a_cluster_that_has_one_fits(self):
        client = self.client(nodes=[("amd64", 2)])
        client._platforms["python:3.13"] = ["amd64"]
        self.assertTrue(client._box_for(self.manifest(gpuCount=1)).gpu)

    def test_a_job_with_no_image_gets_the_default_in_the_job_itself(self):
        client = self.client()
        manifest = self.manifest(image="")
        client._platforms[localk8s._default_image()] = ["amd64"]
        client._box_for(manifest)
        self.assertEqual(manifest["spec"]["workload"]["template"]["image"], localk8s._default_image())

    def test_a_service_is_refused_for_now(self):
        client = self.client()
        with self.assertRaisesRegex(errors.ValidationError, "serve"):
            client._box_for({"spec": {"workload": {"template": {"service": {"ports": []}}}}})

    def test_a_path_outside_the_shared_folder_is_refused(self):
        client = self.client()
        with self.assertRaisesRegex(errors.ValidationError, "folder this cluster shares"):
            client.need_paths(["/datos/ventas"])

    def test_a_path_in_the_shared_folder_goes_through(self):
        self.client().need_paths([str(self.folder / "ventas")])

    def test_the_store_is_in_the_shared_folder(self):
        client = self.client()
        self.assertEqual(client.store, f"file://{self.folder / client.id / 'store'}")
        self.assertEqual(client.output_for("j"), str(self.folder / client.id / "store" / "j"))
        self.assertTrue((self.folder / client.id / "store" / "j").is_dir())

    def test_no_kubectl_says_how_to_get_a_cluster(self):
        client = LocalK8sJobClient(root=self.home)
        with mock.patch("shutil.which", return_value=None):
            with self.assertRaisesRegex(errors.EngineMissing, "k3d cluster create"):
                _ = client.engine


class UninstallTest(Base):
    def jobs(self, *active):
        items = [{"metadata": {"name": name}, "status": {"active": 1}} for name in active]
        original = self.kube.run

        def run(args, stdin=None, timeout=600):
            if args[:2] == ["get", "jobs"]:
                self.kube.calls.append(args)
                return 0, json.dumps({"items": items}), ""
            return original(args, stdin, timeout)
        self.kube.run = run

    def test_it_removes_the_release_and_the_namespace(self):
        client = self.client(deployed=deployment("x", "/y"))
        self.jobs()
        self.assertEqual(client.uninstall(), ["zenthros-local/zenthros-agent"])
        self.assertEqual(self.helm.calls[0][:2], ["uninstall", "zenthros-agent"])
        self.assertIn(["delete", "namespace", "zenthros-local", "--ignore-not-found"], self.kube.calls)

    def test_a_running_job_stops_it(self):
        client = self.client(deployed=deployment("x", "/y"))
        self.jobs("entrena")
        with self.assertRaisesRegex(errors.ValidationError, "entrena still runs"):
            client.uninstall()
        self.assertEqual(self.helm.calls, [])

    def test_a_dry_run_only_says(self):
        client = self.client(deployed=deployment("x", "/y"))
        self.jobs()
        self.assertEqual(client.uninstall(dry_run=True), ["zenthros-local/zenthros-agent"])
        self.assertEqual(self.helm.calls, [])

    def test_nothing_there_is_nothing_to_do(self):
        client = self.client()
        self.assertEqual(client.uninstall(), [])


class PlacementTest(Base):
    def test_on_names_it_with_no_hub_and_targets_shows_the_cluster(self):
        from zenthros import machines
        from zenthros.remote.options import local_k8s_client

        placed = machines._place("local-k8s", {})
        self.assertIs(placed.options["client"], local_k8s_client())
        self.assertEqual(placed.name, "local-k8s")

    def test_no_context_is_no_row(self):
        from zenthros import targetlist

        with mock.patch.object(localk8s, "current_context", return_value=""):
            self.assertIsNone(targetlist.local_k8s_target())

    def test_the_row_says_the_context(self):
        from zenthros import targetlist

        with mock.patch.object(localk8s, "current_context", return_value="k3d-zenthros"):
            row = targetlist.local_k8s_target()
        self.assertEqual(row.name, "local-k8s")
        self.assertIn("k3d-zenthros", row.reason)
        self.assertEqual(row.detail["context"], "k3d-zenthros")


if __name__ == "__main__":
    unittest.main()
