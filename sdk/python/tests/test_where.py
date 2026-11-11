#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Where a call runs: on= alone (usability audit 2026-10-17, U-10, U-11).

Pinned: pool=, backend= and acquire= are refused everywhere they used to be
taken, with a message that names on=, before anything is sent; a call with
no on= goes to configure(on=) or ZENTHROS_ON, else to the Workspace's only
machine, and with none or several it lists what on= can name, ready to
copy; without zenthros-api (an administrator's kubeconfig) a name is a
pool's; image= is refused before any job on a machine that runs no images;
a machine named like a template is an error, not the machine silently.
"""
import os
import unittest
from types import SimpleNamespace
from unittest import mock

import zenthros
from zenthros import errors, local, machines, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.remote import options

from test_machines import Env

STORE = "s3://bucket/calls"
TARGETS = ("GET", "/v1/workspaces/risk/targets")


def double(x):
    return 2 * x


def vm(name, backend_id, runtimes=()):
    """A backend as zenthros-api lists it, with the runtimes its agent declares."""
    caps = [{"kind": "Job", "runtimes": list(runtimes)}] if runtimes else []
    return {"backendId": backend_id, "name": name, "backendType": "vm", "healthy": True, "capabilities": caps}


class DefaultOnTest(Env):
    def answer(self, rows):
        self.api.routes[TARGETS] = (200, {"targets": rows, "can": {}})

    def test_no_on_is_this_computer_whatever_else_there_is_and_asks_nothing(self):
        """The same call runs here until it names somewhere else: a default
        that depended on which machines exist moved a call without a word."""
        self.answer([{"name": "my-vm", "type": "External", "status": "Ready"}])
        self.listed = [[vm("my-vm", "vm-1")]]
        self.assertEqual(machines.default_on(), machines.LOCAL)
        self.assertEqual(self.api.requests, [])

    def test_configure_on_wins_and_asks_the_api_nothing(self):
        self.answer([{"name": "my-vm", "type": "External", "status": "Ready"}])
        with remote.configure(on="other"):
            self.assertEqual(machines.default_on(), "other")
        self.assertEqual(self.api.requests, [])

    def test_zenthros_on_is_the_default_too(self):
        with mock.patch.dict(os.environ, {options.ON_ENV: "from-env"}):
            self.assertEqual(machines.default_on(), "from-env")


class WithoutTheAPITest(unittest.TestCase):
    """An administrator's kubeconfig (zctl): no list of machines to look in."""

    def setUp(self):
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, clean, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_name_is_a_pool(self):
        with remote.configure(client=FakeJobClient()):
            self.assertEqual(machines.place("demo", {}).options, {"pool": "demo"})

    def test_no_on_and_no_default_is_this_computer(self):
        with remote.configure(client=FakeJobClient()):
            self.assertEqual(machines.place(None, {}).name, machines.LOCAL)

    def test_the_configured_default_is_a_pool(self):
        with remote.configure(client=FakeJobClient(), on="demo"):
            self.assertEqual(machines.place(None, {}).options, {"pool": "demo"})


class OnTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeJobClient()
        configured = remote.configure(store=STORE, client=self.client, blob_store=MemoryBlobStore(), on="demo")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)

    def test_a_secret_takes_no_other_keyword(self):
        for key in ("pool", "backend"):
            with self.subTest(key), self.assertRaises(TypeError):
                remote.Secret.encrypted("v", **{key: "x"})

    def test_the_default_is_where_a_call_with_no_on_goes(self):
        call = zenthros.cloud(code_dir=None)(double).submit(1)
        self.assertEqual(self.client.submitted(call.name).spec.pool_ref, "demo")


class ImageTest(Env):
    def test_image_on_a_machine_that_runs_no_images_is_refused_before_any_job(self):
        self.listed = [[vm("my-vm", "vm-1", ["systemd"])]]
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("my-vm", {"image": "img:1"})
        self.assertIn("Drop image=", str(caught.exception))
        self.assertFalse([r for r in self.api.requests if r["method"] != "GET"])

    def test_image_on_a_vm_with_containerd_runs_in_it(self):
        self.listed = [[vm("my-vm", "vm-1", ["systemd", "containerd"])]]
        self.assertEqual(machines.place("my-vm", {"image": "img:1"}).options, {"backend": "vm-1"})

    def test_an_agent_that_says_no_runtimes_is_left_to_the_controller(self):
        self.listed = [[vm("my-vm", "vm-1")]]
        self.assertEqual(machines.place("my-vm", {"image": "img:1"}).options, {"backend": "vm-1"})

    def test_a_container_here_runs_no_images(self):
        lb = local.LocalBackend(SimpleNamespace(workspace="risk"), "ct-1", "zenthros-mi-pc", created=True)
        with self.assertRaises(errors.ValidationError):
            machines.place(lb, {"image": "img:1"})
        self.assertEqual(machines.place(lb, {}).options, {"backend": "ct-1"})


class NameTest(Env):
    def test_a_machine_named_like_a_template_is_an_error_not_the_machine(self):
        self.listed = [[vm("aws-spot", "vm-1")]]
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (200, {"templates": [{"name": "aws-spot"}],
                                                                          "machines": []})
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("aws-spot", {})
        self.assertIn("is a machine and a template", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
