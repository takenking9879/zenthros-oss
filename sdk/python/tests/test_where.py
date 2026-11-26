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
        self.assertEqual(machines.place(lb, {}).options, {"backend": "ct-1", "no_role": True})


class NameTest(Env):
    def test_a_machine_named_like_a_template_is_an_error_not_the_machine(self):
        self.listed = [[vm("aws-spot", "vm-1")]]
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (200, {"templates": [{"name": "aws-spot"}],
                                                                          "machines": []})
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("aws-spot", {})
        self.assertIn("is a machine and a template", str(caught.exception))

    def test_a_machine_whose_agent_is_away_is_said_so_with_its_own_remedy(self):
        # A call right after an update met the VM's agent restarting: "nothing
        # called that; it has [..., 'my-vm']". What to do depends on what it is
        # (29.3): a VM is started, a This computer attached again here, a
        # provider's container started there.
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = (200, {"templates": [], "machines": []})
        self.api.routes[("GET", "/v1/workspaces/risk/pools")] = (200, {"pools": []})
        pod = {"backendId": "ct-1", "name": "my-pod", "backendType": "container", "ownership": "External", "offline": True}
        here = {**pod, "backendId": "ct-2", "name": "my-pc"}
        self.listed = [[{**vm("my-vm", "vm-1"), "ownership": "External", "offline": True}, pod, here]]

        def said(name):
            with mock.patch("zenthros.targetlist._local_containers", return_value={"ct-2": (2000, 4 << 30)}):
                with self.assertRaises(errors.ValidationError) as caught:
                    machines.place(name, {})
            return str(caught.exception)

        for name, remedy, not_this in (("my-vm", "start the VM", "--type local"),
                                       ("my-pc", "zenthros attach my-pc --type local", "start the VM"),
                                       ("my-pod", "start it where it runs", "--type local")):
            text = said(name)
            self.assertIn(f"{name}'s agent is not connected now", text)
            self.assertIn(remedy, text, name)
            self.assertNotIn(not_this, text, name)
            self.assertNotIn("nothing called that", text)
        from zenthros import targetlist

        # The hub's targets show an attached machine with no agent Offline
        # (15): a dry run says what the call does.
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": [
            {"name": "my-vm", "type": "External", "status": "Offline", "id": "vm-1", "backendType": "vm",
             "reason": "its agent is not connected"}], "can": {}})
        plan = targetlist.plan_call("my-vm", {})
        self.assertFalse(plan.ok)
        self.assertIn(said("my-vm").removeprefix("on='my-vm': "), plan.problems)

if __name__ == "__main__":
    unittest.main()
