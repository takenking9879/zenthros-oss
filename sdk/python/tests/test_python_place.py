#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""A function on a machine that has no Python of its own (usability R4).

Pinned: a function sent to a machine whose jobs run in images by default
(containerd), with no image=, runs in python:<the call's minor>-slim; never
over an image= given, never for a command, never on a machine whose jobs run
on it. A function's job names what its runner needs (cloudpickle at this
version, the store's client); a command's names nothing. On the machine,
the runner installs what its Python lacks into a venv it keeps, before
anything is read from the store, and starts again inside it, once.
"""
import io
import os
import unittest
from unittest import mock

import cloudpickle

from zenthros import machines, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import call as remote_call
from zenthros.remote import envs, runner

from test_machines import Env
from test_where import vm

SLIM = "python:3.12-slim-bookworm"
RUNNER_ENV = envs.runner_env


def images_by_default(name, backend_id):
    row = vm(name, backend_id, ["systemd", "containerd"])
    row["capabilities"][0]["defaultRuntime"] = "containerd"
    return row


def double(x):
    return 2 * x


class SlimImageTest(Env):
    def test_a_function_on_a_machine_that_runs_images_gets_its_python(self):
        self.listed = [[images_by_default("vmc", "vm-1")]]
        self.assertEqual(machines.place("vmc", {}, python="3.12").options, {"backend": "vm-1", "image": SLIM})

    def test_the_machine_object_and_its_backend_get_it_too(self):
        row = images_by_default("vmc", "vm-1")
        self.listed = [[row]]
        m = machines.Machine(name="vmc", id="vm-1", target="external")
        self.assertEqual(machines.place(m, {}, python="3.12").options, {"backend": "vm-1", "image": SLIM})
        backend = machines._backends(machines._api())[0]
        self.assertEqual(backend.default_runtime, "containerd")
        self.assertEqual(machines.place(backend, {}, python="3.12").options, {"backend": "vm-1", "image": SLIM})

    def test_an_image_given_a_command_or_jobs_on_the_machine_get_none(self):
        self.listed = [[images_by_default("vmc", "vm-1"), vm("vms", "vm-2", ["systemd", "containerd"])]]
        self.assertEqual(machines.place("vmc", {"image": "img:1"}, python="3.12").options, {"backend": "vm-1"})
        self.assertEqual(machines.place("vmc", {}).options, {"backend": "vm-1"}, "a command runs as the machine is")
        self.assertEqual(machines.place("vms", {}, python="3.12").options, {"backend": "vm-2"},
                         "a machine whose jobs run on it has the installer's Python")

    def test_a_function_on_a_cluster_gets_its_python(self):
        # A cluster runs no pod without an image: a function with none got
        # an empty one (E-06).
        row = {"backendId": "k8s-1", "name": "mi-k3s", "backendType": "k8s", "healthy": True,
               "capabilities": [{"kind": "Job", "runtimes": ["kubernetes"], "defaultRuntime": "kubernetes"}]}
        self.listed = [[row]]
        self.assertEqual(machines.place("mi-k3s", {}, python="3.12").options, {"backend": "k8s-1", "image": SLIM})
        self.assertEqual(machines.place("mi-k3s", {"image": "img:1"}, python="3.12").options, {"backend": "k8s-1"})

    def test_the_image_is_the_calls_minor(self):
        self.assertEqual(machines.python_image("3.11"), "python:3.11-slim-bookworm")


class RunnerPackagesTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeJobClient()
        configured = remote.configure(store="s3://bucket/calls", client=self.client, blob_store=MemoryBlobStore(), on="demo")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)

    def env(self, call):
        return {e.name: e.value for e in self.client.submitted(call.name).spec.workload.template.env}

    def test_a_functions_job_names_what_its_runner_needs(self):
        env = self.env(remote.run(double, 1, code_dir=None))
        self.assertEqual(env[remote_call.RUNNER_PACKAGES_ENV], f"cloudpickle=={cloudpickle.__version__} boto3")

    def test_a_commands_job_names_nothing(self):
        self.assertNotIn(remote_call.RUNNER_PACKAGES_ENV, self.env(remote.run(["echo", "hi"], code_dir=None)))

    def test_the_stores_client_follows_the_store(self):
        self.assertEqual(remote_call.runner_packages("gs://b/p")[1:], ["google-cloud-storage"])
        self.assertEqual(remote_call.runner_packages("file:///tmp/p")[1:], [])

    def test_the_image_place_chose_reaches_the_job(self):
        placed = machines.Placement({"backend": "vm-1", "image": SLIM})
        with mock.patch.object(machines, "place", return_value=placed):
            call = remote.run(double, 1, code_dir=None, on="vmc")
        template = self.client.submitted(call.name).spec.workload.template
        self.assertEqual(template.image, SLIM)


class EnsureRunnerTest(unittest.TestCase):
    def setUp(self):
        self.reexecs = []
        self.made = []
        clean = {k: v for k, v in os.environ.items() if k not in (runner.RUNNER_PACKAGES_ENV, runner.RUNNER_READY_ENV)}
        for patcher in (mock.patch.dict(os.environ, clean, clear=True),
                        mock.patch.object(runner, "_reexec", lambda *a: self.reexecs.append(a)),
                        mock.patch.object(envs, "runner_env", self.runner_env)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def runner_env(self, packages, out=None):
        self.made.append(list(packages))
        return "/cache/py-runner-x/bin/python"

    def test_what_the_python_lacks_is_installed_then_the_runner_starts_again_in_it(self):
        os.environ[runner.RUNNER_PACKAGES_ENV] = "zenthros-no-such-module==1 os"
        runner.ensure_runner(["exec", "s3://b/call.pkl"], out=io.StringIO())
        self.assertEqual(self.made, [["zenthros-no-such-module==1", "os"]], "the venv has all of them, not just the missing")
        (python, argv, env), = self.reexecs
        self.assertEqual((python, argv), ("/cache/py-runner-x/bin/python",
                                          ["/cache/py-runner-x/bin/python", "-m", "zenthros.remote", "exec", "s3://b/call.pkl"]))
        self.assertEqual(env[runner.RUNNER_READY_ENV], "1", "started again, it does not install again")

    def test_a_python_that_has_them_is_left_as_it_is(self):
        os.environ[runner.RUNNER_PACKAGES_ENV] = "os json"
        runner.ensure_runner(["exec", "s3://b/call.pkl"], out=io.StringIO())
        self.assertEqual((self.made, self.reexecs), ([], []))

    def test_started_again_or_with_nothing_named_it_does_nothing(self):
        runner.ensure_runner(["exec", "s3://b/call.pkl"], out=io.StringIO())
        os.environ.update({runner.RUNNER_PACKAGES_ENV: "zenthros-no-such-module", runner.RUNNER_READY_ENV: "1"})
        runner.ensure_runner(["exec", "s3://b/call.pkl"], out=io.StringIO())
        self.assertEqual((self.made, self.reexecs), ([], []))

    def test_it_runs_before_the_store_is_read(self):
        order = []
        with mock.patch.object(runner, "ensure_runner", lambda argv: order.append(("ensure", argv))), \
                mock.patch.object(runner, "execute", lambda uri: order.append(("read", uri)) or 0):
            self.assertEqual(runner.main(["exec", "s3://b/call.pkl"]), 0)
        self.assertEqual(order, [("ensure", ["exec", "s3://b/call.pkl"]), ("read", "s3://b/call.pkl")])

    def test_packages_it_cannot_install_end_the_job_before_the_store_is_read(self):
        def fails(argv):
            raise envs.EnvironmentFailed("pip: no network")
        read = []
        with mock.patch.object(runner, "ensure_runner", fails), \
                mock.patch.object(runner, "execute", lambda uri: read.append(uri) or 0), \
                mock.patch("sys.stderr", io.StringIO()) as err:
            self.assertEqual(runner.main(["exec", "s3://b/call.pkl"]), runner.EXIT_ERROR)
        self.assertEqual(read, [])
        self.assertIn("pip: no network", err.getvalue())

    def test_the_venv_is_one_per_set_of_packages_whatever_their_order(self):
        names = []
        with mock.patch.object(envs, "cache_root", lambda: "/cache"), \
                mock.patch.object(envs, "_venv", lambda root, name, *a, **k: names.append(name) or name):
            RUNNER_ENV(["cloudpickle==3.1.2", "boto3"])
            RUNNER_ENV(["boto3", "cloudpickle==3.1.2"])
            RUNNER_ENV(["cloudpickle==3.1.1", "boto3"])
        self.assertEqual(names[0], names[1])
        self.assertNotEqual(names[0], names[2])
        self.assertTrue(names[0].startswith("runner-"))


if __name__ == "__main__":
    unittest.main()
