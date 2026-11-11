#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The zenthros command (usability R2): each command is its Python twin.

Pinned: the CLI and the twin make the same API calls in the same order;
a failure is one "✗" line that ends with what to do, exit 1; --print gives
the lines and does not wait; the questions read a number where there is no
terminal (a notebook, a pipe); login --key - reads the key from stdin.
"""
import io
import unittest
from unittest import mock

from zenthros import cli, machines

from test_attach import MI_EC2, Attach


def calls(events):
    return [e for e in events if e.startswith("api ") or e.startswith("ssh") or e.startswith("test run")]


class TwinTest(Attach):
    def test_attach_makes_the_calls_its_twin_makes(self):
        self.listed = [[], [MI_EC2]]
        code = cli.main(["attach", "mi-ec2", "--type", "vm", "--runtime", "containerd", "--ssh", "ubuntu@h"], out=io.StringIO())
        self.assertEqual(code, 0)
        by_cli = calls(self.events)
        self.events.clear()
        self.listed = [[], [MI_EC2]]
        machines.attach("mi-ec2", type="vm", runtime="containerd", host="ubuntu@h")
        self.assertEqual(calls(self.events), by_cli)
        self.assertEqual(by_cli[:3], ["api GET /backends", "ssh check", "api POST /backends"])

    def test_print_gives_the_lines_and_does_not_wait(self):
        out = io.StringIO()
        self.assertEqual(cli.main(["attach", "mi-ec2", "--type", "vm", "--runtime", "systemd", "--print"], out=out), 0)
        text = out.getvalue()
        self.assertIn("On mi-ec2, run:", text)
        self.assertIn("sudo zenadm join 'gw.example:9443' --token", text)
        self.assertTrue(text.rstrip().endswith("Then check with: zenthros targets"))
        self.assertNotIn("Waiting", text)
        self.assertFalse([e for e in self.events if e.startswith("test run")])


class FailureTest(Attach):
    def test_a_failure_is_one_line_and_exit_1(self):
        out = io.StringIO()
        self.assertEqual(cli.main(["attach", "x", "--type", "laptop"], out=out), 1)
        self.assertEqual(out.getvalue(), "✗ unknown machine type 'laptop': use vm, container, kubernetes or local\n")

    def test_detach_of_no_such_machine_names_where_to_look(self):
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": [], "can": {}})
        out = io.StringIO()
        self.assertEqual(cli.main(["detach", "nope"], out=out), 1)
        self.assertEqual(out.getvalue(), "✗ no machine named nope: see zenthros targets\n")


class DetachTest(Attach):
    def test_detach_gives_up_a_join_no_machine_used_yet(self):
        # What the refusal of its name says to run. No target shows it.
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": [], "can": {}})
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/vm-5e48758e")] = (200, {"backendId": "vm-5e48758e", "tokensRevoked": 1})
        out = io.StringIO()
        self.assertEqual(cli.main(["detach", "vm-5e48758e"], out=out), 0)
        self.assertEqual(out.getvalue(), "✓ vm-5e48758e detached\n")

    def test_detach_says_the_machine_keeps_running_and_its_jobs_fail(self):
        # E-13, the CLI twin of delete(): the same warning, then what is
        # left of the machine.
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": [
            {"name": "mi-ec2", "type": "VM", "status": "Busy", "id": "vm-1a2b3c4d", "backendType": "vm",
             "runningJobs": 1, "maxJobs": 1}], "can": {}})
        self.listed = [[{**MI_EC2, "runningJobs": 1}]]
        out = io.StringIO()
        with self.assertLogs("zenthros.machines", "WARNING") as logged:
            self.assertEqual(cli.main(["detach", "mi-ec2"], out=out), 0)
        self.assertEqual(logged.records[0].getMessage(), "mi-ec2 had 1 running job: it fails now that mi-ec2 is detached")
        self.assertEqual(out.getvalue(), "✓ mi-ec2 detached\n"
                                         "Its agent stops. The machine itself is yours and keeps running.\n")
        self.assertIn("api DELETE /backends/vm-1a2b3c4d", self.events)

    def test_detaching_a_cluster_says_how_to_take_its_agent_off(self):
        # A Deployment restarts an agent that exits: helm uninstall removes
        # the release zenthros attach installed (E-06).
        self.api.routes[("GET", "/v1/workspaces/risk/targets")] = (200, {"targets": [
            {"name": "mi-k3s", "type": "Kubernetes", "status": "Ready", "id": "k8s-1a2b3c4d", "backendType": "k8s"}], "can": {}})
        self.listed = [[{**MI_EC2, "backendId": "k8s-1a2b3c4d", "name": "mi-k3s", "backendType": "k8s"}]]
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/k8s-1a2b3c4d")] = (200, {"backendId": "k8s-1a2b3c4d"})
        out = io.StringIO()
        self.assertEqual(cli.main(["detach", "mi-k3s"], out=out), 0)
        self.assertEqual(out.getvalue(), "✓ mi-k3s detached\nIts agent stops. To take it off the cluster: "
                                         "helm uninstall zenthros-k8s-1a2b3c4d -n zenthros-k8s-1a2b3c4d\n")


class SelectTest(unittest.TestCase):
    def test_without_a_terminal_a_number_answers(self):
        stdin, stdout = io.StringIO("9\n2\n"), io.StringIO()
        self.assertEqual(cli.select("Run jobs in", ["containers (containerd)", "directly (systemd)"], stdin=stdin, stdout=stdout), 1)
        self.assertIn("Type a number from 1 to 2.", stdout.getvalue())
        self.assertTrue(stdout.getvalue().endswith("Run jobs in    › directly (systemd)\n"))
        self.assertEqual(cli.select("Machine type", ["VM", "Container"], stdin=io.StringIO("\n"), stdout=io.StringIO()), 0)

    def test_no_answer_at_all_says_which_flag(self):
        with self.assertRaises(ValueError) as raised:
            cli.select("Machine type", ["VM"], stdin=io.StringIO(""), stdout=io.StringIO())
        self.assertIn("pass it as a flag", str(raised.exception))


class LoginTest(unittest.TestCase):
    def test_a_key_from_stdin_stays_out_of_the_shell_history(self):
        with mock.patch("zenthros.login", return_value=mock.Mock(name="ws")) as login, \
                mock.patch("sys.stdin", io.StringIO("zen_abc123\n")):
            self.assertEqual(cli.main(["login", "--key", "-"], out=io.StringIO()), 0)
        login.assert_called_once_with(key="zen_abc123")


if __name__ == "__main__":
    unittest.main()
