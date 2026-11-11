#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.create / zenthros.delete and where .remote(on=) runs.

Pinned: create returns a machine only once it is ready, and one that never
gets there is undone; a name the Workspace already has is that machine,
not a second one; delete retires before it removes; a machine made for one
call goes after its result is collected, unless keep=True.
"""
import json
import os
import unittest
from unittest import mock

from zenthros import errors, local, machines
from zenthros.jobs import http as jobs_http
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI
from test_local import JOIN, PRIVATE, FakeDocker

NAMED_JOIN = {**JOIN, "name": "mi-pc"}
MI_PC = {**PRIVATE, "name": "mi-pc", "host": {"cpu": 4, "memoryBytes": 8 << 30}}


class Env(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.devnull + ".none",
                                               "ZENTHROS_SERVER": self.api.url, "ZENTHROS_TOKEN": "bob-token",
                                               "ZENTHROS_WORKSPACE": "risk"}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.events = []
        self.docker = FakeDocker(self.events)
        for module in (local, machines):
            patcher = mock.patch.object(module, "_docker", self.docker)
            patcher.start()
            self.addCleanup(patcher.stop)
        for module in (local, machines):
            patcher = mock.patch.object(module, "_sleep", lambda s: None)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, NAMED_JOIN)
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/ct-1")] = (200, {"backendId": "ct-1", "accessRemoved": True})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"items": []})
        self.listed = [[]]

        def backends(_body):
            listed = self.listed.pop(0) if len(self.listed) > 1 else self.listed[0]
            return 200, {"backends": listed}

        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = backends
        original = HTTPJobClient._call

        def recorded(client, method, path, *args, **kwargs):
            self.events.append(f"api {method} {path}")
            return original(client, method, path, *args, **kwargs)
        patcher = mock.patch.object(HTTPJobClient, "_call", recorded)
        patcher.start()
        self.addCleanup(patcher.stop)
        # The image is there: no build.
        self.docker.answers = {}

    def docker_args(self):
        return [args for args, _ in self.docker.calls]


class CreateLocalTest(Env):
    def test_local_is_a_named_container_with_the_limits_asked_returned_once_ready(self):
        self.listed = [[], [], [MI_PC]]  # the name is free; then registering; then ready
        m = machines.create("local", name="mi-pc", cpus=4, memory="8Gi", image="img:1")
        self.assertEqual((m.name, m.id, m.target, m.container), ("mi-pc", "ct-1", "local", "zenthros-mi-pc"))
        run = next(args for args in self.docker_args() if args[0] == "run")
        flags = " ".join(run)
        for wanted in ("--cpus 4", "--memory 8g", "--memory-swap 8g", "--label zenthros.ai/name=mi-pc"):
            self.assertIn(wanted, flags)
        joined = [r for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(joined[0]["body"], b'{"name": "mi-pc"}')
        # Ready before it is returned: the last thing asked is the list that said so.
        self.assertEqual(self.events[-1], "api GET /backends")

    def test_a_name_the_workspace_has_is_that_machine(self):
        self.listed = [[MI_PC]]
        m = machines.create("local", name="mi-pc", cpus=4, memory="8Gi")
        self.assertEqual((m.name, m.id), ("mi-pc", "ct-1"))
        self.assertEqual(self.docker.calls, [], "a second machine was made")
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"])

    def test_one_that_never_gets_ready_is_undone(self):
        self.listed = [[]]
        with mock.patch.object(machines, "READY_TIMEOUT", 0):
            with self.assertRaises(errors.WaitTimeout):
                machines.create("local", name="mi-pc", image="img:1")
        self.assertIn("api DELETE /backends/ct-1", self.events)
        self.assertEqual(self.docker_args()[-1], ["rm", "-f", "zenthros-mi-pc"])
        self.assertLess(self.events.index("api DELETE /backends/ct-1"), self.events.index("docker rm"),
                        "retired before its container went")

    def test_without_docker_it_says_so_before_asking_the_api(self):
        self.docker.fail["info"] = (1, "Cannot connect to the Docker daemon\n")
        with self.assertRaises(errors.ValidationError) as caught:
            machines.create("local", image="img:1")
        self.assertIn("Docker", str(caught.exception))
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"])

    def test_the_default_image_is_built_once_for_this_python(self):
        self.listed = [[], [], [MI_PC]]
        self.docker.fail["image"] = (1, "Error: No such image\n")
        machines.create("local", name="mi-pc")
        build = next((args, stdin) for args, stdin in self.docker.calls if args[0] == "build")
        tag = build[0][build[0].index("-t") + 1]
        self.assertTrue(tag.startswith(f"zenthros-python:{machines.python_minor()}-"), tag)
        self.assertIn(f"FROM python:{machines.python_minor()}-slim", build[1])
        self.assertIn("cloudpickle", build[1])
        run = next(args for args in self.docker_args() if args[0] == "run")
        self.assertEqual(run[-3], tag)

    def test_sizes_are_what_docker_reads(self):
        self.assertEqual([machines.docker_memory(m) for m in ("8Gi", "512Mi", "4g", 2, "1.5Gi")],
                         ["8g", "512m", "4g", "2g", "1536m"])
        with self.assertRaises(errors.ValidationError):
            machines.docker_memory("lots")


class DeleteTest(Env):
    def test_delete_retires_before_the_container_goes(self):
        self.listed = [[MI_PC]]
        self.docker.answers = {"ps": (0, "zenthros-mi-pc\n")}
        machines.delete("mi-pc")
        self.assertEqual([e for e in self.events if e in ("api DELETE /backends/ct-1", "docker rm")],
                         ["api DELETE /backends/ct-1", "docker rm"])
        self.assertEqual(self.docker_args()[-1], ["rm", "-f", "zenthros-mi-pc"])

    def test_a_machine_without_a_container_here_is_only_retired(self):
        self.listed = [[MI_PC]]
        self.docker.answers = {"ps": (0, "")}
        machines.delete("mi-pc")
        self.assertIn("api DELETE /backends/ct-1", self.events)
        self.assertNotIn("rm", [args[0] for args in self.docker_args()])

    def test_ports_go_to_the_container_and_the_machine_says_them(self):
        self.listed = [[], [], [MI_PC]]
        m = machines.create("local", name="mi-pc", image="img:1", ports=[8000])
        run = next(args for args in self.docker_args() if args[0] == "run")
        self.assertIn("127.0.0.1:8000:8000", run)
        self.assertEqual(m.ports, (8000,))

    def test_ports_it_cannot_publish_are_refused_before_anything_is_made(self):
        for bad in ([0], [70000], ["web"], [True], [8080], [8081], 8000):
            with self.assertRaises(errors.ValidationError, msg=bad):
                machines.create("local", name="mi-pc", image="img:1", ports=bad)
        self.assertEqual((self.docker.calls, [r for r in self.api.requests if r["method"] == "POST"]), ([], []))
        with self.assertRaises(errors.ValidationError) as caught:
            machines.create("aws-spot", ports=[8000])
        self.assertIn("ports= is only for create('local')", str(caught.exception))

    def test_a_machine_there_already_must_publish_the_ports_asked(self):
        self.listed = [[MI_PC]]
        self.docker.answers["ps"] = (0, "ports=9000\n")
        with self.assertRaises(errors.ValidationError) as caught:
            machines.create("local", name="mi-pc", ports=[8000])
        self.assertIn("ports=[8000]", str(caught.exception))
        self.docker.answers["ps"] = (0, "ports=8000,9000\n")
        self.assertEqual(machines.create("local", name="mi-pc", ports=[8000]).id, "ct-1")

    def test_an_unknown_name_is_said_with_what_there_is(self):
        self.listed = [[MI_PC]]
        with self.assertRaises(LookupError) as caught:
            machines.delete("viejo")
        self.assertIn("mi-pc", str(caught.exception))

    def test_a_join_no_machine_used_yet_is_given_up_by_its_id(self):
        # It has tokens and no record, so no list shows it: the refusal of its
        # name gave this id, and the API retires it by that.
        self.listed = [[MI_PC]]
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/vm-5e48758e")] = (200, {"backendId": "vm-5e48758e", "tokensRevoked": 1})
        machines.delete("vm-5e48758e")
        self.assertEqual([r["method"] for r in self.api.requests if "vm-5e48758e" in r["path"]], ["DELETE"])


class PlaceTest(Env):
    def test_on_a_machine_or_its_name_is_that_backend(self):
        self.listed = [[MI_PC]]
        m = machines.Machine(name="mi-pc", id="ct-1", target="local")
        self.assertEqual(machines.place(m, {}).options, {"backend": "ct-1"})
        self.assertEqual(machines.place("mi-pc", {}).options, {"backend": "ct-1"})
        self.assertEqual(machines.place("ct-1", {}).options, {"backend": "ct-1"})

    def test_on_a_pool_is_that_pool(self):
        self.listed = [[]]
        self.api.routes[("GET", "/v1/workspaces/risk/pools")] = (200, {"pools": [{"name": "gpu-pool"}]})
        self.assertEqual(machines.place("gpu-pool", {}).options, {"pool": "gpu-pool"})

    def test_on_local_is_the_local_client_and_makes_no_machine(self):
        """A call here needs no account: no join, no backend, no list of them."""
        with mock.patch.object(machines, "create") as create:
            placed = machines.place("local", {"cpu": 2, "memory": "4Gi", "image": "img:1"})
        create.assert_not_called()
        self.assertEqual(placed.name, machines.LOCAL)
        self.assertEqual(placed.options["client"].transport, "local")
        self.assertIsNone(placed.made)

    def test_a_service_on_a_container_here_that_does_not_publish_its_port_is_refused(self):
        self.listed = [[MI_PC]]
        self.docker.answers["ps"] = (0, "ports=\n")
        for on in ("mi-pc", machines.Machine(name="mi-pc", id="ct-1", target="local")):
            with self.assertRaises(errors.ValidationError, msg=on) as caught:
                machines.place(on, {}, ports=[8000])
            self.assertIn("create('local', name='mi-pc', ports=[8000])", str(caught.exception))
        self.assertIn("label=zenthros.ai/backend=ct-1", self.docker_args()[-1])
        self.docker.answers["ps"] = (0, "ports=8000\n")
        self.assertEqual(machines.place("mi-pc", {}, ports=[8000]).options, {"backend": "ct-1"})
        # A machine that is not a container here (a VM, a pod) is the
        # controller's to check.
        self.docker.answers["ps"] = (0, "")
        self.assertEqual(machines.place("mi-pc", {}, ports=[8000]).options, {"backend": "ct-1"})
        self.assertEqual(machines.place("mi-pc", {}).options, {"backend": "ct-1"})

    def test_nowhere_named_is_said_with_the_names_there_are(self):
        self.listed = [[MI_PC]]
        self.api.routes[("GET", "/v1/workspaces/risk/pools")] = (200, {"pools": [{"name": "gpu-pool"}]})
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("nowhere", {})
        for name in ("mi-pc", "gpu-pool", "local"):
            self.assertIn(name, str(caught.exception))


if __name__ == "__main__":
    unittest.main()


TEMPLATES = {"templates": [{"name": "aws-spot", "provider": "aws", "purchasingMode": "Spot", "instanceTypes": ["c6i.xlarge"],
                            "machineKind": "VM", "limit": 2, "used": 0, "toolchains": []}], "machines": []}


class TemplateTest(Env):
    def setUp(self):
        super().setUp()
        self.listed = [[]]
        self.machines = [TEMPLATES]

        def machines(_body):
            answer = self.machines.pop(0) if len(self.machines) > 1 else self.machines[0]
            return 200, answer

        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = machines
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "gpu-1", "template": "aws-spot",
                                                                            "pool": "risk-gpu-1"})
        self.api.routes[("DELETE", "/v1/workspaces/risk/machines/gpu-1")] = (200, {"name": "gpu-1"})

    def mine(self, ready=0, allocated=0):
        return {**TEMPLATES, "machines": [{"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1",
                                           "ready": ready, "allocated": allocated}]}

    def test_a_template_makes_a_vm_returned_once_it_is_up(self):
        self.machines = [TEMPLATES, self.mine(), self.mine(ready=1)]
        m = machines.create("aws-spot", name="gpu-1")
        self.assertEqual((m.name, m.target, m.pool), ("gpu-1", "aws-spot", "risk-gpu-1"))
        posted = [r for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(posted[0]["body"], b'{"template": "aws-spot", "name": "gpu-1", "maxLifetime": "86400s"}')
        self.assertEqual(self.events[-1], "api GET /machines", "returned before it was up")
        self.assertEqual(self.docker.calls, [], "a VM needs no Docker here")

    def test_a_moment_the_api_is_away_while_a_machine_boots_is_waited_out(self):
        # The machine's boot is waited for: an API that answers 503 for a
        # moment (a rollout of zenthros-api) is not yet, not the machine
        # failing. Raised, the machine just made was deleted and the call
        # failed (final review of Ola J, G1).
        answers = [(200, TEMPLATES), (503, {"kind": "Status", "message": "rolling out", "reason": "ServiceUnavailable",
                                            "code": 503}), (200, self.mine(ready=1))]
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (
            lambda _body: answers.pop(0) if len(answers) > 1 else answers[0])
        m = machines.create("aws-spot", name="gpu-1")
        self.assertEqual(m.name, "gpu-1")
        self.assertNotIn("api DELETE /machines/gpu-1", self.events)

    def test_idle_makes_a_machine_that_sleeps_returned_at_once(self):
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "gpu-1", "template": "aws-spot",
                                                                            "pool": "risk-gpu-1", "idle": "30m0s"})
        m = machines.create("aws-spot", name="gpu-1", idle="30m")
        self.assertEqual((m.name, m.pool, m.idle), ("gpu-1", "risk-gpu-1", "30m0s"))
        posted = [r for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(posted[0]["body"], b'{"template": "aws-spot", "name": "gpu-1", "idle": "30m"}')
        # Not waited for: it starts with its first call.
        self.assertEqual(self.events[-1], "api POST /machines")
        # A call goes to its pool, whose Reuse, then Create, starts it.
        self.machines = [{**TEMPLATES, "machines": [{"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1",
                                                      "idle": "30m0s"}]}]
        self.assertEqual(machines.place(m, {}).options, {"pool": "risk-gpu-1"})
        self.assertEqual(machines.create("aws-spot", name="gpu-1").idle, "30m0s")
        self.assertEqual(machines.create("aws-spot", name="gpu-1", idle=1800).idle, "30m0s")

    def test_a_kept_machine_lives_a_day_unless_it_says_otherwise(self):
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1"})
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (lambda _body: (200, self.mine(ready=1)))

        def body(**kwargs):
            self.api.requests.clear()
            machines.create("aws-spot", name="gpu-2", **kwargs)
            return json.loads([r for r in self.api.requests if r["method"] == "POST"][0]["body"])

        self.assertEqual(body()["maxLifetime"], "86400s")
        self.assertEqual(body(max_lifetime="2h")["maxLifetime"], "7200s")
        self.assertEqual(body(max_lifetime="2d")["maxLifetime"], "172800s")
        self.assertNotIn("maxLifetime", body(max_lifetime=None))
        # One with idle ends by itself; a limit applies only if asked for.
        self.assertNotIn("maxLifetime", body(idle="30m"))
        self.assertEqual(body(idle="30m", max_lifetime="2h")["maxLifetime"], "7200s")

    def test_max_lifetime_where_it_applies(self):
        for bad in ("soon", 0):
            with self.assertRaises(errors.ValidationError, msg=bad) as caught:
                machines.create("aws-spot", max_lifetime=bad)
            self.assertIn("max_lifetime", str(caught.exception))
        with self.assertRaises(errors.ValidationError) as caught:
            machines.create("local", max_lifetime="2h")
        self.assertIn("template", str(caught.exception))
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"])

    def container_template(self, **extra):
        template = dict(TEMPLATES["templates"][0], machineKind="Container", cloudIdentity=False, **extra)
        self.machines = [{**TEMPLATES, "templates": [template]}]
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (lambda _body: (200, self.machines[0]))
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1"})

    def posted(self):
        return [json.loads(r["body"]) for r in self.api.requests if r["method"] == "POST"]

    def test_a_container_template_runs_an_image_on_a_machine_of_its_own(self):
        self.container_template()
        self.machines[0] = {**self.machines[0], "machines": [
            {"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1", "ready": 1}]}
        with mock.patch("zenthros.registry.anyone_can_read", return_value=True), \
                mock.patch("zenthros.registry.architectures", return_value=["amd64"]):
            placed = machines.place("aws-spot", {"image": "docker.io/me/train@sha256:a",
                                                 "env_needs": {"python": "3.11", "packages": ["polars==1.0"]}}, python="3.11")
        self.assertEqual(placed.options, {"pool": "risk-gpu-1"})
        self.assertTrue(placed.made.completes_image)
        body = self.posted()[0]
        self.assertEqual((body["image"], body["python"], body["idle"]), ("docker.io/me/train@sha256:a", "3.11", machines.PLACED_IDLE))
        self.assertIn("polars==1.0", body["packages"])
        # A function's runner has to read its call there.
        self.assertTrue(any(package.startswith("cloudpickle") for package in body["packages"]))

    def test_a_command_there_needs_no_runner(self):
        self.container_template()
        with mock.patch("zenthros.registry.anyone_can_read", return_value=True), \
                mock.patch("zenthros.registry.architectures", return_value=["amd64"]):
            machines.place("aws-spot", {"image": "me/x:1"})
        body = self.posted()[0]
        self.assertNotIn("packages", body)
        self.assertNotIn("python", body)

    def test_a_machine_that_cannot_prepare_what_it_was_made_with_says_why(self):
        # Found on RunPod: a package that conflicts with the image. Its agent
        # enrolled and said why, and the wait went on for 20 minutes of machines.
        self.container_template()
        made = {"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1", "ready": 0,
                "runtimeFailure": "adding pip==23.0 would change the image's pip"}
        self.machines[0] = {**self.machines[0], "machines": [made]}
        self.api.routes[("DELETE", "/v1/workspaces/risk/machines/gpu-1")] = (200, {})
        with mock.patch("zenthros.registry.anyone_can_read", return_value=True), \
                mock.patch("zenthros.registry.architectures", return_value=["amd64"]), \
                mock.patch("zenthros.machines._sleep") as slept, \
                self.assertRaisesRegex(errors.ValidationError, "would change the image's pip"):
            machines.place("aws-spot", {"image": "me/x:1", "env_needs": {"packages": ["pip==23.0"]}})
        self.assertEqual(slept.call_count, 0, "nothing to wait for")
        self.assertTrue([r for r in self.api.requests if r["method"] == "DELETE"], "the machine stayed, paid for")

    def test_an_image_its_hosts_cannot_read_makes_no_machine(self):
        self.container_template()
        with mock.patch("zenthros.registry.anyone_can_read", return_value=False), \
                self.assertRaisesRegex(errors.ValidationError, "needs a login"):
            machines.place("aws-spot", {"image": "me/private:1"})
        self.assertEqual(self.posted(), [])

    def test_an_image_for_another_architecture_makes_no_machine(self):
        self.container_template()
        with mock.patch("zenthros.registry.anyone_can_read", return_value=True), \
                mock.patch("zenthros.registry.architectures", return_value=["arm64"]), \
                self.assertRaisesRegex(errors.ValidationError, "only arm64"):
            machines.place("aws-spot", {"image": "me/x:1"})
        self.assertEqual(self.posted(), [])

    def test_a_vm_template_runs_the_jobs_image_with_no_machine_of_its_own(self):
        placed = machines.place("aws-spot", {"image": "me/x:1"})
        self.assertEqual(placed.options, {"pool": "aws-spot"})
        self.assertEqual(self.posted(), [])

    def test_your_own_gs_bucket_on_a_host_with_no_role_is_sealed_to_its_machine(self):
        template = dict(TEMPLATES["templates"][0], cloudIdentity=False, pythonPackages=None)
        self.machines = [{**TEMPLATES, "templates": [template]}]
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (lambda _body: (200, self.machines[0]))
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": [{"name": "gpu-1", "id": "vm-9", "pool": "risk-gpu-1"}]})
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1"})
        client = HTTPJobClient.__new__(HTTPJobClient)
        source = mock.Mock(encrypted_payload="sealed")
        source.materialization.env_name = "GOOGLE_STORE_KEY"
        with mock.patch("zenthros.remote.options._configured", {"store": "gs://b/z"}), \
                mock.patch("zenthros.userstore.machine_access", return_value=({}, [source])) as access:
            self.assertTrue(machines._needs_sealed_store(client, "aws-spot", template, "3.12", {}))
            carried = machines._sealed_store(machines._api(), "gpu-1", "risk-gpu-1", {})
            self.assertFalse(machines._needs_sealed_store(client, "aws-spot", template, "3.12", {"secrets": {"GOOGLE_STORE_KEY": 1}}))
        self.assertEqual(access.call_args.args[:2], ("gs://b/z", "vm-9"))
        self.assertTrue(access.call_args.kwargs["no_role"])
        self.assertEqual(list(carried["secrets"]), ["GOOGLE_STORE_KEY"])

    def test_idle_seconds_and_where_it_applies(self):
        self.machines = [TEMPLATES, self.mine(ready=1)]
        machines.create("aws-spot", name="gpu-1", idle=1800)
        posted = [r for r in self.api.requests if r["method"] == "POST"]
        self.assertIn(b'"idle": "1800s"', posted[0]["body"])
        for target in ("local", "k8s:cluster-b"):
            with self.assertRaises(errors.ValidationError, msg=target):
                machines.create(target, idle="30m")

    def test_one_that_never_comes_up_is_deleted(self):
        self.machines = [TEMPLATES, self.mine()]
        with mock.patch.object(machines, "VM_READY_TIMEOUT", 0):
            with self.assertRaises(errors.WaitTimeout):
                machines.create("aws-spot", name="gpu-1")
        self.assertIn("api DELETE /machines/gpu-1", self.events)

    def test_a_machine_of_the_workspace_is_found_by_name_and_deleted_as_one(self):
        self.machines = [self.mine(ready=1)]
        m = machines.create("aws-spot", name="gpu-1")
        self.assertEqual(m.pool, "risk-gpu-1")
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"])
        self.assertEqual(machines.place("gpu-1", {}).options, {"pool": "risk-gpu-1"})
        machines.delete("gpu-1")
        self.assertIn("api DELETE /machines/gpu-1", self.events)
        self.assertEqual(self.docker.calls, [])

    def test_on_a_template_is_a_machine_of_its_pool_with_nothing_to_do_or_a_new_one(self):
        placed = machines.place("aws-spot", {})
        self.assertEqual(placed.options, {"pool": "aws-spot"})
        self.assertIsNone(placed.made)
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"])

    def test_fresh_is_always_a_new_machine_of_the_template(self):
        placed = machines.on_a_new_machine(machines.place("aws-spot", {}))
        self.assertEqual(placed.options, {"pool": "aws-spot", "acquire": ["Create"]})
        self.assertIsNone(placed.made)

    def test_fresh_is_refused_on_what_has_no_other_machine(self):
        with self.assertRaises(errors.ValidationError) as caught:
            machines.on_a_new_machine(machines.Placement({"client": object()}, name="local"))
        self.assertIn("fresh=True", str(caught.exception))
        self.assertIn("template", str(caught.exception))

    def test_a_template_whose_python_differs_is_refused_before_a_machine(self):
        pinned = {**TEMPLATES, "templates": [dict(TEMPLATES["templates"][0], python="3.13")]}
        self.machines = [pinned]
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("aws-spot", {}, python="3.12")
        self.assertIn("3.13", str(caught.exception))
        self.assertIn("3.12", str(caught.exception))
        with self.assertRaises(errors.ValidationError):
            machines.place("aws-spot", {}, keep=True, python="3.12")
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"], "a machine was made")
        # Same minor (a patch release apart), or a command (python=None): it goes.
        self.assertEqual(machines.place("aws-spot", {}, python="3.13").options["pool"], "aws-spot")
        self.assertEqual(machines.place("aws-spot", {}, python=None).options["pool"], "aws-spot")

    def test_a_machine_made_from_that_template_is_refused_too(self):
        pinned = {**self.mine(ready=1), "templates": [dict(TEMPLATES["templates"][0], python="3.13.2")]}
        self.machines = [pinned]
        with self.assertRaises(errors.ValidationError):
            machines.place("gpu-1", {}, python="3.12")
        self.assertEqual(machines.place("gpu-1", {}, python="3.13").options, {"pool": "risk-gpu-1"})

    def runner_python(self, packages, store="s3://calls/zenthros/risk"):
        """A template whose python toolchain installs packages (None: the
        API cannot tell), in a Workspace whose store is store."""
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": store})
        # Another Workspace's store is another session's: the process's one
        # client asks the Workspace once (HTTPJobClient.for_connection).
        jobs_http.forget_shared()
        view = dict(TEMPLATES["templates"][0], python="3.13")
        if packages is not None:
            view["pythonPackages"] = packages
        return {**TEMPLATES, "templates": [view]}

    def test_a_template_whose_python_cannot_read_a_call_is_refused_before_a_machine(self):
        # The runner reads the call from the store (boto3 for s3://) and
        # unpickles it (cloudpickle) with the template's Python, which has
        # only its toolchain's packages: found on Vast.ai, "s3:// needs
        # boto3" on a machine already paid for.
        self.machines = [self.runner_python([])]
        for keep in (False, True):
            with self.assertRaises(errors.ValidationError) as caught:
                machines.place("aws-spot", {}, keep=keep, python="3.13")
            for word in ("boto3", "cloudpickle", "packages", "administrator"):
                self.assertIn(word, str(caught.exception))
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"], "a machine was made")
        self.machines = [self.runner_python(["boto3==1.40.0", "Cloudpickle>=3"])]
        self.assertEqual(machines.place("aws-spot", {}, python="3.13").options["pool"], "aws-spot")
        # One missing is named alone.
        self.machines = [self.runner_python(["boto3[crt]==1.40.0"])]
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("aws-spot", {}, python="3.13")
        self.assertIn("cloudpickle", str(caught.exception))
        self.assertNotIn("boto3,", str(caught.exception))

    def test_a_job_in_its_own_image_does_not_run_on_the_templates_python(self):
        # Found on GCP: a VM template with containerd and no packages refused a
        # call in a built environment, whose Python and packages are the image's.
        # The runner completes the image there (RUNNER_PACKAGES_ENV).
        self.machines = [self.runner_python([])]
        placed = machines.place("aws-spot", {"image": "me/x:1"}, python="3.13")
        self.assertEqual(placed.options, {"pool": "aws-spot"})
        # Nor is another minor there a reason: it is not the one that runs it.
        placed = machines.place("aws-spot", {"image": "me/x:1"}, python="3.11")
        self.assertEqual(placed.options, {"pool": "aws-spot"})
        # Without an image the template's Python runs it, and the refusal stands.
        with self.assertRaises(errors.ValidationError):
            machines.place("aws-spot", {}, python="3.13")

        # A store that is not S3 needs no boto3.
        self.machines = [self.runner_python(["cloudpickle"], store="file:///shared/calls")]
        self.assertEqual(machines.place("aws-spot", {}, python="3.13").options["pool"], "aws-spot")
        self.machines = [self.runner_python([])]
        # A command runs no runner; an API that does not say, nothing to tell.
        self.assertEqual(machines.place("aws-spot", {}, python=None).options["pool"], "aws-spot")
        self.machines = [self.runner_python(None)]
        self.assertEqual(machines.place("aws-spot", {}, python="3.13").options["pool"], "aws-spot")

    def roleless(self, untrusted=False, machine_id="vm-7", minted=True):
        """A template whose machines have no cloud identity (Vast.ai, RunPod),
        the Workspace's machine gpu-1 of it (pool risk-gpu-1, its agent
        machine_id), and the hub's sealed credentials for that machine."""
        view = self.runner_python(["boto3", "cloudpickle"])
        view["templates"][0].update(cloudIdentity=False, untrustedHost=untrusted)
        view["machines"] = self.mine(ready=1)["machines"]
        self.machines = [view]
        rows = [{"type": "VM", "name": "other", "pool": "risk-other", "id": "vm-9"},
                {"type": "VM", "name": "gpu-1", "pool": "risk-gpu-1", "id": machine_id},
                {"type": "Template", "name": "aws-spot", "pool": "aws-spot"}]
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": rows})
        sealed = {"env": {"AWS_REGION": "us-east-2"}, "secrets": [
            {"type": "encrypted-payload", "materialization": {"mode": "env", "envName": name},
             "encryptedPayload": {"ciphertext": f"sealed-{name}", "recipients": ["age1machine"], "keyId": "k"}}
            for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")]}
        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = (
            (200, sealed) if minted else (501, {"kind": "Status", "message": "no role to mint from", "code": 501}))
        return machines.Machine(name="gpu-1", target="aws-spot", pool="risk-gpu-1")

    def minted_for(self):
        import json
        return [json.loads(r["body"])["backend"] for r in self.api.requests
                if r["method"] == "POST" and r["path"].endswith("/store/credentials")]

    def test_a_call_on_a_machine_without_a_cloud_identity_brings_credentials_sealed_to_it(self):
        # Its machine has no role to read the store with, and credentials
        # are minted only for a machine named: the SDK names it (the agent
        # of the template's machine) and the call carries what only that
        # machine opens. The job still goes through its pool.
        mi = self.roleless()
        mine = {"HF_TOKEN": object()}
        placed = machines.place(mi, {"secrets": mine, "env": {"MODE": "x"}}, python="3.13")
        self.assertEqual(self.minted_for(), ["vm-7"])
        self.assertEqual(placed.options["pool"], "risk-gpu-1")
        self.assertNotIn("backend", placed.options)
        secrets = placed.options["secrets"]
        self.assertIs(secrets["HF_TOKEN"], mine["HF_TOKEN"])
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            self.assertEqual((secrets[name].type, secrets[name].payload.ciphertext), ("encrypted-payload", f"sealed-{name}"))
            self.assertEqual(secrets[name].payload.recipients, ["age1machine"])
        self.assertEqual(placed.options["env"], {"AWS_REGION": "us-east-2", "MODE": "x"})
        # By its name too.
        self.assertEqual(machines.place("gpu-1", {}, python="3.13").options["pool"], "risk-gpu-1")
        self.assertEqual(self.minted_for(), ["vm-7", "vm-7"])

    def test_an_outage_is_no_answer_that_the_machine_needs_no_credentials(self):
        # A 503 from the Workspace's machines (an API rollout) read as "no
        # templates" said the machine needs no credentials: the job went
        # without them and could not read its call there. The outage is
        # raised; only a 404 says there is nothing (R4 ronda 3, G2).
        mi = self.roleless()
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (
            503, {"kind": "Status", "message": "rolling out", "reason": "ServiceUnavailable", "code": 503})
        with self.assertRaises(errors.TransientError):
            machines.place(mi, {}, python="3.13")
        self.assertEqual(self.minted_for(), [])

    def test_a_call_on_the_template_gets_a_machine_first(self):
        # The machine a call on the template would get does not exist yet:
        # one is made for the call (and goes with its result), then sealed to.
        self.roleless()
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "aws-spot-ab12", "template": "aws-spot",
                                                                           "pool": "risk-gpu-1"})
        listed = self.machines[0]
        up = {**listed, "machines": [{"name": "aws-spot-ab12", "template": "aws-spot", "pool": "risk-gpu-1", "ready": 1}]}
        self.machines = [listed, up]
        placed = machines.place("aws-spot", {}, python="3.13")
        self.assertEqual(placed.made.name, "aws-spot-ab12")
        self.assertEqual(placed.options["pool"], "risk-gpu-1")
        self.assertNotIn("acquire", placed.options)
        self.assertEqual(self.minted_for(), ["vm-7"])
        self.assertIn("AWS_ACCESS_KEY_ID", placed.options["secrets"])
        # Made for this call alone: it boots now and still ends after its
        # idle, should this process be gone before it deletes it (audit
        # 2026-10-16 AUD-041). It was a machine kept up for ever.
        import json
        made = [json.loads(r["body"]) for r in self.api.requests if r["method"] == "POST" and r["path"].endswith("/machines")]
        self.assertEqual(made, [{"template": "aws-spot", "name": made[0]["name"], "idle": machines.PLACED_IDLE, "boot": True}])

    def test_a_moment_the_api_is_away_after_the_boot_keeps_the_machine(self):
        # The machine made for the call is up; the API answering 503 for a
        # moment while its credentials are sealed is waited out, as the boot
        # is. Raised, the machine just booted was deleted (final review of
        # Ola J, second pass).
        self.roleless()
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "aws-spot-ab12", "template": "aws-spot",
                                                                           "pool": "risk-gpu-1"})
        listed = self.machines[0]
        self.machines = [listed, {**listed, "machines": [{"name": "aws-spot-ab12", "template": "aws-spot",
                                                          "pool": "risk-gpu-1", "ready": 1}]}]
        rows = self.api.routes[("GET", "/v1/workspaces/risk/targets")][1]
        down = (503, {"kind": "Status", "message": "rolling out", "reason": "ServiceUnavailable", "code": 503})
        answers = [down, (200, rows)]
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = lambda _b: answers.pop(0) if len(answers) > 1 else answers[0]
        sealed = self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")]
        minted = [down, sealed]
        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = (
            lambda _b: minted.pop(0) if len(minted) > 1 else minted[0])
        placed = machines.place("aws-spot", {}, python="3.13")
        self.assertIn("AWS_ACCESS_KEY_ID", placed.options["secrets"])
        self.assertFalse([r for r in self.api.requests if r["method"] == "DELETE"], "the machine was deleted")

    def test_a_third_partys_host_gets_them_only_when_the_caller_accepts(self):
        # A Vast.ai host's owner can read them, as any secret sent there
        # (SEC-6): refused before anything is made, unless configured.
        from zenthros import remote

        mi = self.roleless(untrusted=True)
        for on in (mi, "aws-spot"):
            with self.assertRaises(errors.ValidationError) as caught:
                machines.place(on, {}, python="3.13")
            self.assertIn("untrusted_host_secrets=True", str(caught.exception))
            self.assertIn("zenthros.run(", str(caught.exception))
        self.assertEqual(self.minted_for(), [])
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"], "a machine was made")
        with remote.configure(untrusted_host_secrets=True):
            self.assertIn("AWS_ACCESS_KEY_ID", machines.place(mi, {}, python="3.13").options["secrets"])

    def test_what_needs_no_credentials_gets_none(self):
        mi = self.roleless()
        # A command reads no call; a caller that brings the store's.
        self.assertNotIn("secrets", machines.place(mi, {}, python=None).options)
        brought = {"secrets": {"AWS_ACCESS_KEY_ID": object(), "AWS_SECRET_ACCESS_KEY": object()}}
        self.assertIs(machines.place(mi, brought, python="3.13").options.get("secrets", brought["secrets"]),
                      brought["secrets"])
        # A template with an identity, or an API that does not say.
        for says in (True, None):
            view = self.runner_python(["boto3", "cloudpickle"])
            if says is not None:
                view["templates"][0]["cloudIdentity"] = says
            view["machines"] = self.mine(ready=1)["machines"]
            self.machines = [view]
            self.assertNotIn("secrets", machines.place(mi, {}, python="3.13").options)
        # A store that is not S3.
        view = self.runner_python(["cloudpickle"], store="file:///shared/calls")
        view["templates"][0]["cloudIdentity"] = False
        view["machines"] = self.mine(ready=1)["machines"]
        self.machines = [view]
        self.assertNotIn("secrets", machines.place(mi, {}, python="3.13").options)
        self.assertEqual(self.minted_for(), [])

    def test_a_machine_made_for_the_call_goes_if_it_cannot_be_sealed_to(self):
        self.roleless(machine_id="")
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, {"name": "aws-spot-ab12", "template": "aws-spot",
                                                                           "pool": "risk-gpu-1"})
        self.api.routes[("DELETE", "/v1/workspaces/risk/machines/aws-spot-ab12")] = (200, {"name": "aws-spot-ab12"})
        listed = self.machines[0]
        self.machines = [listed, {**listed, "machines": [{"name": "aws-spot-ab12", "template": "aws-spot",
                                                          "pool": "risk-gpu-1", "ready": 1}]}]
        with self.assertRaises(errors.ValidationError):
            machines.place("aws-spot", {}, python="3.13")
        self.assertIn("DELETE /v1/workspaces/risk/machines/aws-spot-ab12",
                      [f"{r['method']} {r['path']}" for r in self.api.requests])

    def test_no_machine_up_or_no_role_to_mint_is_said_before_the_call(self):
        mi = self.roleless(machine_id="")
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place(mi, {}, python="3.13")
        self.assertIn("no machine up", str(caught.exception))
        mi = self.roleless(minted=False)
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place(mi, {}, python="3.13")
        self.assertIn("credentials", str(caught.exception))

    def test_a_machine_object_made_from_a_template_is_checked_too(self):
        # create(template) then submit(on=mi): the same template, the same
        # checks, before the call is sent.
        mi = machines.Machine(name="gpu-1", target="aws-spot", pool="risk-gpu-1")
        self.machines = [{**self.runner_python([]), "machines": self.mine()["machines"]}]
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place(mi, {}, python="3.13")
        self.assertIn("boto3", str(caught.exception))
        pinned = {**self.mine(), "templates": [dict(TEMPLATES["templates"][0], python="3.13.2")]}
        self.machines = [pinned]
        with self.assertRaises(errors.ValidationError):
            machines.place(mi, {}, python="3.12")
        self.assertEqual(machines.place(mi, {}, python="3.13").options, {"pool": "risk-gpu-1"})
        # Nothing to check: no API call.
        before = len(self.api.requests)
        self.assertEqual(machines.place(mi, {}).options, {"pool": "risk-gpu-1"})
        self.assertEqual(len(self.api.requests), before)

    def test_a_service_on_a_template_without_its_port_is_refused_before_a_machine(self):
        # A service there is reachable only on what its pool exposes
        # (exposedPorts): the controller would refuse it after the fact.
        served = {**TEMPLATES, "templates": [dict(TEMPLATES["templates"][0], ports=[8000])]}
        self.machines = [served]
        for keep in (False, True):
            with self.assertRaises(errors.ValidationError) as caught:
                machines.place("aws-spot", {}, keep=keep, ports=[9000])
            self.assertIn("9000", str(caught.exception))
            self.assertIn("[8000]", str(caught.exception))
            self.assertIn("create('local', ports=[9000])", str(caught.exception))
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"], "a machine was made")
        self.assertEqual(machines.place("aws-spot", {}, ports=[8000]).options["pool"], "aws-spot")
        # A function, not a service: no port to check.
        self.assertEqual(machines.place("aws-spot", {}, ports=None).options["pool"], "aws-spot")
        self.machines = [TEMPLATES]
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("aws-spot", {}, ports=[8000])
        self.assertIn("publishes no port", str(caught.exception))

    def test_a_service_on_a_machine_made_from_that_template_is_refused_too(self):
        self.machines = [{**self.mine(ready=1), "templates": [dict(TEMPLATES["templates"][0], ports=[8000])]}]
        with self.assertRaises(errors.ValidationError):
            machines.place("gpu-1", {}, ports=[9000])
        self.assertEqual(machines.place("gpu-1", {}, ports=[8000]).options, {"pool": "risk-gpu-1"})

    def test_a_template_without_a_pinned_python_is_not_checked(self):
        for version in ("", "3"):
            self.machines = [{**TEMPLATES, "templates": [dict(TEMPLATES["templates"][0], python=version)]}]
            self.assertEqual(machines.place("aws-spot", {}, python="3.12").options["pool"], "aws-spot")

    def test_on_a_template_with_keep_is_a_machine_that_stays(self):
        self.machines = [TEMPLATES, TEMPLATES, self.mine(ready=1)]
        placed = machines.place("aws-spot", {}, keep=True)
        self.assertEqual(placed.options, {"pool": "risk-gpu-1"})
        self.assertEqual(placed.made.pool, "risk-gpu-1")
        posted = [r for r in self.api.requests if r["method"] == "POST"]
        self.assertTrue(posted and b'"template": "aws-spot"' in posted[0]["body"])
        # A machine kept on purpose has no idle: it stays until deleted.
        self.assertNotIn(b'"idle"', posted[0]["body"])
        self.assertNotIn(b'"boot"', posted[0]["body"])
