#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros attach (usability R3): a machine the user already has joins
the Workspace.

Pinned, above all the order: the checks, then the token, then the two
lines, then the wait, then a test run; a check that fails makes no token.
Also: the two lines are the hub's, one line each, the install first and
with no secret; the answer to "Run jobs in" reaches the hub; what was not
given is asked, and only what applies; over ssh the token travels on its
standard input and never in an error; a failed attach retires what it
joined; a name already attached is that machine; delete only retires it.
"""
import io
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from zenthros import attaching, errors, machines

from support import machine_side
from test_machines import Env

TOKEN = "zt1_" + "0f" * 32 + "_abcdef.0123456789abcdef"
VM_JOIN = {"backendId": "vm-1a2b3c4d", "workspace": "risk", "expiresUnix": "1760001800", "name": "mi-ec2",
           "installScript": "f=\"$(mktemp)\" && curl -fsSL --retry 4 -o \"$f\" 'https://r.example/v1/install-agent.sh' && "
                            "echo 'aa  '\"$f\" | sha256sum -c --quiet && sudo env ZENTHROS_NETWORK=direct bash \"$f\"\n",
           "joinScript": f"sudo zenadm join 'gw.example:9443' --token '{TOKEN}'\n"}
K8S_JOIN = {"backendId": "k8s-1a2b3c4d", "workspace": "risk", "expiresUnix": "1760001800", "name": "mi-k3s",
            "installScript": "unset d; t=\"$(mktemp -d)\" && curl -fsSL 'https://r.example/v1/zenthros-agent-chart.tgz' && d=\"$t\"\n",
            "joinScript": "helm upgrade --install 'zenthros-k8s-1a2b3c4d' \"$d/zenthros-agent-chart.tgz\" -n 'zenthros-k8s-1a2b3c4d'"
                          f" --set-string 'enrollment.token={TOKEN}'\n"}
MI_EC2 = {"backendId": "vm-1a2b3c4d", "name": "mi-ec2", "backendType": "vm", "healthy": True, "maxJobs": 1,
          "accessMode": "Private", "workspaces": ["risk"], "ownership": "External", "host": {"cpu": 2, "memoryBytes": 8 << 30}}


class Attach(Env):
    def setUp(self):
        super().setUp()
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, VM_JOIN)
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/vm-1a2b3c4d")] = (200, {"backendId": "vm-1a2b3c4d"})
        self.ssh_calls = []
        self.ssh_answers = []

        def ssh(args, stdin, timeout):
            self.ssh_calls.append((args, stdin))
            self.events.append("ssh check" if "checks passed" in stdin else "ssh lines")
            return self.ssh_answers.pop(0) if self.ssh_answers else (0, "checks passed\n")
        for patcher in (mock.patch.object(machines, "_ssh", ssh),
                        mock.patch.object(attaching, "test_run", self.fake_test_run)):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.asked = []

    def fake_test_run(self, name, timeout=attaching.TEST_TIMEOUT):
        self.events.append(f"test run {name}")
        return 9.2

    def ask(self, answers):
        answers = list(answers)

        def ask(label, options):
            self.asked.append(label)
            return options.index(answers.pop(0))
        return ask

    def joins(self):
        return [json.loads(r["body"] or b"{}") for r in self.api.requests if r["method"] == "POST" and r["path"].endswith("/backends")]


class OrderTest(Attach):
    def test_over_ssh_checks_then_token_then_lines_then_wait_then_test(self):
        self.listed = [[], [], [MI_EC2]]  # the name is free; then registering; then ready
        out = io.StringIO()
        m = machines.attach("mi-ec2", type="vm", runtime="containerd", host="ubuntu@ec2-1.example",
                            key="~/.ssh/id_ed25519", out=out)
        self.assertEqual((m.name, m.id, m.target), ("mi-ec2", "vm-1a2b3c4d", "external"))
        steps = [e for e in self.events if e in ("ssh check", "api POST /backends", "ssh lines") or e.startswith("test run")]
        self.assertEqual(steps, ["ssh check", "api POST /backends", "ssh lines", "test run mi-ec2"])
        # The machine was ready before its test run: the list said so first.
        self.assertLess(len(self.events) - self.events[::-1].index("api GET /backends"), self.events.index("test run mi-ec2") + 1)
        self.assertEqual(self.joins(), [{"name": "mi-ec2", "type": "vm", "runtime": "containerd"}])
        (check_args, check), (args, lines) = self.ssh_calls
        self.assertIn(os.path.expanduser("~/.ssh/id_ed25519"), args)
        self.assertEqual(args[-2:], ["ubuntu@ec2-1.example", "sudo -n bash -s"])
        self.assertNotIn(TOKEN, " ".join(args))
        self.assertNotIn("--token", check)
        self.assertEqual(lines, VM_JOIN["installScript"] + VM_JOIN["joinScript"])
        self.assertIn("10", check, "containerd's disk is checked")
        self.assertEqual(out.getvalue().splitlines()[-2:], ["✓ connected (2 CPU, 8Gi)", "✓ test run 9s"])

    def test_over_ssh_nothing_is_asked(self):
        # The README's line, from a script or a notebook (no prompt): a VM,
        # its jobs in containers, and one line says so (E-16, 2026-10-19).
        self.listed = [[], [], [MI_EC2]]
        out = io.StringIO()

        def ask(label, options):
            raise AssertionError(f"asked {label}")
        machines.attach("mi-ec2", host="ubuntu@ec2-1.example", ask=ask, out=out)
        self.assertEqual(self.joins(), [{"name": "mi-ec2", "type": "vm", "runtime": "containerd"}])
        self.assertEqual(out.getvalue().splitlines()[0], "mi-ec2: a VM, jobs in containerd (--type, --runtime to choose)")
        # Without ssh the questions stay.
        with self.assertRaises(AssertionError):
            machines.attach(ask=ask, out=io.StringIO())

    def test_a_failed_check_makes_no_token(self):
        self.ssh_answers = [(3, "✗ systemd not found. Looks like a container: zenthros attach mi-ec2 --type container\n")]
        with self.assertRaises(errors.ValidationError) as raised:
            machines.attach("mi-ec2", type="vm", runtime="systemd", host="ubuntu@ec2-1.example")
        self.assertEqual(str(raised.exception), "systemd not found. Looks like a container: zenthros attach mi-ec2 --type container")
        self.assertEqual(self.joins(), [])
        self.assertEqual(len(self.ssh_calls), 1)

    def test_a_failed_ssh_retires_the_join_and_shows_no_token(self):
        self.ssh_answers = [(0, "checks passed\n"), (1, f"curl: (6) Could not resolve host\n--token '{TOKEN}'\n")]
        with self.assertRaises(errors.BackendJoinError) as raised:
            machines.attach("mi-ec2", type="vm", runtime="systemd", host="ubuntu@ec2-1.example")
        self.assertNotIn(TOKEN, str(raised.exception))
        self.assertIn("Could not resolve host", str(raised.exception))
        self.assertIn("api DELETE /backends/vm-1a2b3c4d", self.events)
        self.assertFalse([e for e in self.events if e.startswith("test run")])

    def test_a_vm_that_never_gets_ready_is_retired_and_not_tested(self):
        self.listed = [[]]
        with mock.patch.object(attaching, "WAIT_TIMEOUT", 4):
            with self.assertRaises(errors.WaitTimeout):
                machines.attach("mi-ec2", type="vm", runtime="systemd", host="ubuntu@ec2-1.example", timeout=4)
        self.assertEqual(self.events[-1], "api DELETE /backends/vm-1a2b3c4d")
        self.assertFalse([e for e in self.events if e.startswith("test run")])


class LinesTest(Attach):
    def test_without_a_host_it_is_the_hubs_two_lines(self):
        command = machines.attach("mi-ec2", type="vm", runtime="systemd")
        self.assertEqual(self.ssh_calls, [])
        self.assertEqual(command.lines, [VM_JOIN["installScript"].rstrip("\n"), VM_JOIN["joinScript"].rstrip("\n")])
        self.assertEqual(str(command), "\n".join(command.lines))
        install, join = command.lines
        self.assertNotIn(TOKEN, install)
        self.assertIn(TOKEN, join)
        self.assertEqual(command.backend_id, "vm-1a2b3c4d")
        self.assertIn("single use", repr(command))
        # Nothing waited for, nothing tested: the person pastes first.
        self.assertFalse([e for e in self.events if e.startswith("test run") or e == "api GET /backends"][1:])

    def test_the_cli_way_prints_them_then_waits_and_tests(self):
        self.listed = [[], [MI_EC2]]
        out = io.StringIO()
        machines.attach("mi-ec2", type="vm", runtime="systemd", wait=True, out=out)
        lines = out.getvalue().splitlines()
        on = lines.index("On mi-ec2, run:")
        self.assertEqual(lines[on + 1], VM_JOIN["installScript"].rstrip("\n"))
        self.assertTrue(lines[on + 2].startswith(VM_JOIN["joinScript"].rstrip("\n") + "      # single use, expires "))
        self.assertEqual(lines[-3:], ["Waiting for mi-ec2…", "✓ connected (2 CPU, 8Gi)", "✓ test run 9s"])

    def test_the_printed_lines_paste_as_they_are(self):
        # A container's installer is a heredoc: indented by two spaces, its
        # delimiter was no longer one, and the pasted lines never ended
        # (Colab, E-14, 2026-10-18).
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, {
            **VM_JOIN, "installScript": "sh -s -- --x <<'ZENTHROS_INSTALL'\necho hi\nZENTHROS_INSTALL\n"})
        out = io.StringIO()
        command = machines.attach("mi-ec2", type="vm", runtime="systemd", out=out)
        for shown in (out.getvalue(), repr(command)):
            self.assertIn("\n" + command.lines[0] + "\n", shown)
            self.assertIn("\nZENTHROS_INSTALL\n", shown)
            self.assertIn("\n" + command.lines[1], shown)


class PythonTest(Attach):
    def test_a_vm_that_runs_jobs_directly_asks_for_this_python(self):
        # R4: the hub's installer brings it with uv; a VM needs no Python.
        # A container's agent completes its image's Python to this minor: a
        # python:3.12 container ran no function from a 3.13 notebook (E-04).
        machines.attach("cpu-box", type="vm", runtime="systemd")
        machines.attach("gpu-box", type="vm", runtime="containerd")
        machines.attach("gpu-pod", type="container")
        self.assertEqual(self.joins(), [{"name": "cpu-box", "type": "vm", "runtime": "systemd", "python": machines.python_minor()},
                                        {"name": "gpu-box", "type": "vm", "runtime": "containerd"},
                                        {"name": "gpu-pod", "type": "container", "python": machines.python_minor()}])


class QuestionsTest(Attach):
    def test_what_is_not_given_is_asked_in_order(self):
        machines.attach("mi-ec2", ask=self.ask(["VM", "containers (containerd)"]))
        self.assertEqual(self.asked, ["Machine type", "Run jobs in"])
        self.assertEqual(self.joins(), [{"name": "mi-ec2", "type": "vm", "runtime": "containerd"}])

    def test_a_container_is_not_asked_where_its_jobs_run(self):
        machines.attach("pod-1", ask=self.ask(["Container"]))
        self.assertEqual(self.asked, ["Machine type"])
        self.assertEqual(self.joins(), [{"name": "pod-1", "type": "container", "python": machines.python_minor()}])

    def test_what_does_not_apply_is_refused_before_a_token(self):
        for kwargs in ({"type": "container", "runtime": "containerd"}, {"type": "vm", "runtime": "docker"},
                       {"type": "laptop"}, {"type": "kubernetes", "host": "ubuntu@k3s-1"}):
            with self.assertRaises(errors.ValidationError, msg=kwargs):
                machines.attach("x", **kwargs)
        self.assertEqual(self.joins(), [])

    def test_a_cluster_gets_two_lines_to_run_where_kubectl_reaches_it(self):
        # E-06: no runtime or Python is asked or sent (its jobs run in pods
        # of their image), and the lines say where they run.
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, K8S_JOIN)
        out = io.StringIO()
        command = machines.attach("mi-k3s", ask=self.ask(["Kubernetes"]), out=out)
        self.assertEqual(self.asked, ["Machine type"])
        self.assertEqual(self.joins(), [{"name": "mi-k3s", "type": "kubernetes"}])
        self.assertEqual(command.backend_id, "k8s-1a2b3c4d")
        lines = out.getvalue().splitlines()
        self.assertIn("Where kubectl reaches mi-k3s, run:", lines)
        self.assertTrue(repr(command).startswith("Where kubectl reaches mi-k3s, run:"))
        self.assertEqual(command.script(), K8S_JOIN["installScript"] + K8S_JOIN["joinScript"])

    def test_this_computer_without_docker_makes_no_token(self):
        # E-05: create("local") asks Docker first.
        self.docker.answers["info"] = (127, "docker is not installed")
        with self.assertRaises(errors.ValidationError) as raised:
            machines.attach("mi-pc", type="local")
        self.assertIn("Docker", str(raised.exception))
        self.assertEqual(self.joins(), [])

    def test_the_cli_says_the_same_without_docker(self):
        from zenthros import cli

        self.docker.answers["info"] = (127, "docker is not installed")
        out = io.StringIO()
        self.assertEqual(cli.main(["attach", "mi-pc", "--type", "local"], out=out), 1)
        self.assertRegex(out.getvalue(), r"^✗ Docker is not running here")
        self.assertEqual(self.joins(), [])


class KnownTest(Attach):
    def test_a_name_already_attached_is_that_machine(self):
        self.listed = [[MI_EC2]]
        m = machines.attach("mi-ec2", type="vm", runtime="systemd", host="ubuntu@ec2-1.example")
        self.assertEqual(m.id, "vm-1a2b3c4d")
        self.assertEqual((self.joins(), self.ssh_calls), ([], []))

    def test_delete_only_retires_it(self):
        self.listed = [[MI_EC2]]
        machines.delete(machines.Machine(name="mi-ec2", id="vm-1a2b3c4d", target="external"))
        self.assertIn("api DELETE /backends/vm-1a2b3c4d", self.events)
        self.assertNotIn("rm", [args[0] for args in self.docker_args()])
        self.assertEqual(self.ssh_calls, [])

    def test_delete_with_jobs_running_says_they_fail(self):
        # E-13: detach warns; the jobs fail with the machine's access gone.
        for running, said in ((1, "mi-ec2 had 1 running job: it fails now that mi-ec2 is detached"),
                              (2, "mi-ec2 had 2 running jobs: they fail now that mi-ec2 is detached")):
            with self.subTest(running=running):
                self.listed = [[{**MI_EC2, "runningJobs": running}]]
                with self.assertLogs("zenthros.machines", "WARNING") as logged:
                    machines.delete("mi-ec2")
                self.assertEqual([r.getMessage() for r in logged.records], [said])

    def test_delete_with_no_job_running_says_nothing(self):
        self.listed = [[MI_EC2]]
        with self.assertNoLogs("zenthros.machines", "WARNING"):
            machines.delete("mi-ec2")

    def test_it_is_zenthros_attach(self):
        import zenthros

        self.assertIs(zenthros.attach, machines.attach)


class TestRunTest(unittest.TestCase):
    def test_the_test_run_shows_none_of_the_probes_logs(self):
        call = mock.Mock()
        with mock.patch("zenthros.remote.run", return_value=call) as run:
            attaching.test_run("mi-ec2", timeout=7)
        self.assertEqual(run.call_args.kwargs["on"], "mi-ec2")
        call.result.assert_called_once_with(timeout=7, stream_logs=False)

    def test_the_test_run_carries_none_of_the_directory_it_starts_in(self):
        # hub.sh smoke on the GCP hub (2026-11-06): zenthros attach, run from
        # the repository, packed its 9 MiB into the probe, over the 360 KiB a
        # job without a bucket carries, and the attach failed. The probe is
        # the SDK's own function: it needs none of the person's files.
        call = mock.Mock()
        with mock.patch("zenthros.remote.run", return_value=call) as run:
            attaching.test_run("mi-ec2", timeout=7)
        self.assertIsNone(run.call_args.kwargs.get("code_dir", "."))
        # From a wheel the probe travels by name: its module must ship.
        from zenthros.remote import bundle

        probe = run.call_args.args[0]
        self.assertIn(probe.__module__.replace(".", "/") + ".py", bundle.sdk_files())


class CheckScriptTest(unittest.TestCase):
    def test_the_checks_name_the_fix_in_one_line(self):
        script = attaching.check_script("my-vm", "containerd")
        for want in ("name=my-vm\n", "systemd not found. Looks like a container: zenthros attach $name --type container",
                     "containerd needs 10 GB (images): give the disk 20 GB, or run jobs directly: "
                     "zenthros attach $name --runtime systemd.", "exit 3"):
            self.assertIn(want, script)
        # A stock Ubuntu on AWS has 8 GB: the line says both ways out (E-16).
        self.assertIn("the agent needs 2 GB: free some space.", attaching.check_script("my-vm", "systemd"))

    @machine_side
    def test_the_name_is_shown_never_run(self):
        # The script runs in the machine's shell over ssh: a name is text.
        with tempfile.TemporaryDirectory() as empty:
            marker = os.path.join(empty, "ran")
            name = f'vm"$(touch {marker})`touch {marker}`'
            # No systemctl on PATH: the first check fails and says the name.
            said = subprocess.run(["/bin/bash", "-c", attaching.check_script(name, "containerd")],
                                  capture_output=True, text=True, env={"PATH": empty})
            self.assertEqual(said.returncode, 3, said.stderr)
            self.assertIn(f"zenthros attach {name} --type container", said.stdout)
            self.assertFalse(os.path.exists(marker), "the name ran as a command")


if __name__ == "__main__":
    unittest.main()
