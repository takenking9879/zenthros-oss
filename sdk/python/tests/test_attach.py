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
import datetime
import io
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from zenthros import attaching, errors, local, machines

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
        self.assertRegex(out.getvalue(), r"(?m)^✗ Docker is not running here")
        self.assertEqual(self.joins(), [])

    def test_this_computer_says_it_goes_through_the_hub_before_it_makes_anything(self):
        # on="local" needs no attach: the menu and a line say which is which,
        # before anything is made (here Docker is not even running).
        self.assertEqual(dict(attaching.TYPES)["local"], "This computer (through the hub)")
        self.docker.answers["info"] = (127, "docker is not installed")
        out = io.StringIO()
        with self.assertRaises(errors.ValidationError):
            machines.attach("mi-pc", ask=self.ask(["This computer (through the hub)"]), out=out)
        said = out.getvalue().splitlines()
        self.assertEqual(said[0], "This computer joins your Workspace as mi-pc: jobs reach it from anywhere. "
                                  "To run from this computer only, with no hub, you need no attach: on='local'.")
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



MI_PC = {"backendId": "ct-1", "name": "mi-pc", "backendType": "container", "healthy": True, "maxJobs": 1,
         "accessMode": "Private", "workspaces": ["risk"], "ownership": "External",
         "host": {"cpu": 2, "memoryBytes": 4 << 30}}


class ResizeTest(Attach):
    """zenthros attach NAME --type local --cpus N --memory M: This computer
    changes size in place, with its environments (observaciones 17, 24): it
    was created again, and its environments went with the container."""

    def setUp(self):
        super().setUp()
        self.docker.answers["ps"] = (0, "zenthros-mi-pc\n")

    def test_one_here_changes_size_in_place_and_waits_for_the_hub_to_say_it(self):
        resized = {**MI_PC, "host": {"cpu": 3, "memoryBytes": 6 << 30}}
        self.listed = [[MI_PC], [MI_PC], [MI_PC], [resized]]
        out = io.StringIO()
        m = machines.attach("mi-pc", type="local", cpus=3, memory="6g", out=out)
        self.assertEqual((m.name, m.id), ("mi-pc", "ct-1"))
        args = self.docker_args()
        self.assertIn(["update", "--cpus", "3", "--memory", "6g", "--memory-swap", "6g", "zenthros-mi-pc"], args)
        self.assertFalse([a for a in args if a[0] in ("rm", "run")], "made again")
        self.assertEqual(self.joins(), [])
        self.assertIn("mi-pc: 3 CPUs, 6.0 GiB", out.getvalue())
        self.assertEqual(self.events[-1], "api GET /backends", "returned before the hub said the new size")

    def test_one_running_a_job_is_left_as_it_is(self):
        self.listed = [[{**MI_PC, "runningJobs": 1}]]
        with self.assertRaisesRegex(errors.ValidationError, "runs a job now"):
            machines.attach("mi-pc", type="local", cpus=3, memory="6g")
        self.assertFalse([a for a in self.docker_args() if a[0] == "update"])

    def test_a_new_one_is_made_that_size(self):
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, {**VM_JOIN, "backendId": "ct-1", "name": "mi-pc"})
        self.listed = [[], [], [MI_PC]]
        machines.attach("mi-pc", type="local", cpus=3, memory="6g", test=False, out=io.StringIO())
        run = " ".join(next(a for a in self.docker_args() if a[0] == "run"))
        self.assertIn("--cpus 3 --memory 6g --memory-swap 6g", run)

    def test_a_size_is_for_this_computer(self):
        self.listed = [[MI_EC2]]
        self.docker.answers["ps"] = (0, "")
        with self.assertRaisesRegex(errors.ValidationError, "This computer"):
            machines.attach("mi-ec2", cpus=3)
        with self.assertRaisesRegex(errors.ValidationError, "This computer"):
            machines.attach("new-vm", type="vm", runtime="systemd", cpus=3)
        self.assertEqual(self.joins(), [])

    def test_the_cli_says_it_with_flags(self):
        from zenthros import cli

        with mock.patch.object(machines, "attach") as attach:
            cli.main(["attach", "mi-pc", "--type", "local", "--cpus", "2.5", "--memory", "6g"], out=io.StringIO())
        self.assertEqual((attach.call_args.kwargs["cpus"], attach.call_args.kwargs["memory"]), (2.5, "6g"))


OFFLINE_PC = {"backendId": "ct-1", "name": "mi-pc", "offline": True, "accessMode": "Private", "workspaces": ["risk"]}


class SizeTest(Attach):
    def test_a_size_past_what_docker_has_is_refused_before_anything_changes(self):
        # The agent declares what Docker can give: resize waited 90 s for a
        # size it would never say, then called the applied change a timeout.
        self.docker.answers["info"] = (0, f"4 {8 << 30}\n")
        self.docker.answers["ps"] = (0, "zenthros-mi-pc\n")
        pc = machines.Backend.from_api(MI_PC)
        with self.assertRaisesRegex(errors.ValidationError, "Docker here has 8.0 GiB"):
            machines.resize(pc, memory="12g")
        with self.assertRaisesRegex(errors.ValidationError, "Docker here has 4 CPUs"):
            machines.resize(pc, cpus=6)
        self.assertFalse([a for a in self.docker_args() if a[0] == "update"], "changed before refusing")


class ReviveTest(Attach):
    """A This computer stopped (docker stop, or Docker without its restart)
    comes back by zenthros attach NAME with the hub's agent: its container
    started, the hub's agent installed in it, and started with the
    certificate it has. An agent away says no release to the hub, and the
    one it had may not speak to it: one of an older release may have no
    attach --resume, which asking it back would use. A stopped one is
    offline, in no default list (observaciones P25)."""

    def setUp(self):
        super().setUp()
        self.docker.answers["ps"] = (0, "zenthros-mi-pc\n")
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, UPDATE_JOIN)
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/ct-new")] = (200, {"backendId": "ct-new"})
        listed = self.api.routes[("GET", "/v1/workspaces/risk/backends")]

        def released(body):
            return (*listed(body), RELEASED)
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = released

    def test_a_stopped_one_starts_with_the_hubs_agent_not_its_own(self):
        self.listed = [[OFFLINE_PC], [{**MI_PC, "agentVersion": "v0.1.1"}]]
        out = io.StringIO()
        m = machines.attach("mi-pc", type="local", out=out)
        self.assertEqual((m.name, m.id), ("mi-pc", "ct-1"))
        args = self.docker_args()
        self.assertNotIn(["exec", "zenthros-mi-pc", local.AGENT, "attach", "--resume", "--wait", local.REVIVE_WAIT],
                         args, "its own agent was asked back")
        first_exec = next(i for i, a in enumerate(args) if a[0] == "exec")
        self.assertLess(args.index(["start", "zenthros-mi-pc"]), first_exec, "something ran in a stopped container")
        execs = [stdin for a, stdin in self.docker.calls if a[0] == "exec"]
        self.assertIn("install-container.sh", execs[0])
        self.assertIn(local.RESTART_AGENT, execs[1], "the installed agent is the one started")
        self.assertFalse([a for a in args if a[0] in ("rm", "run")], "made again")
        self.assertIn("✓ mi-pc: back with the hub's agent (v0.1.1), its environments as they were", out.getvalue())
        self.assertEqual(self.events[-1], "api GET /backends", "returned before the hub heard it")

    def test_one_whose_agent_does_not_come_back_says_how_to_make_it_again(self):
        self.listed = [[OFFLINE_PC]]
        self.docker.fail["exec"] = (3, "zenadm join: nothing to resume: this container never attached\n")
        with self.assertRaisesRegex(errors.BackendJoinError, "nothing to resume(.|\n)*zenthros detach mi-pc, then "
                                                             "zenthros attach mi-pc --type local"):
            machines.attach("mi-pc", type="local")

    def test_a_stopped_one_is_deleted_with_its_container(self):
        self.listed = [[OFFLINE_PC]]
        machines.delete("mi-pc")
        self.assertIn("api DELETE /backends/ct-1", self.events)
        self.assertIn(["rm", "-f", "zenthros-mi-pc"], self.docker_args())

    def test_a_connected_one_is_left_as_it_is(self):
        self.listed = [[MI_PC]]
        out = io.StringIO()
        machines.attach("mi-pc", type="local", out=out)
        self.assertFalse([a for a in self.docker_args() if a[0] in ("start", "exec")])
        self.assertIn("✓ mi-pc is already attached", out.getvalue())


def created(hours_ago):
    """docker ps' CreatedAt of a container made hours_ago."""
    at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours_ago)
    return at.strftime("%Y-%m-%d %H:%M:%S %z UTC")


class SweepTest(Attach):
    """A container create made for a machine the hub no longer has (deleted
    from elsewhere, a hub made again) would start again with Docker forever:
    zenthros attach and zenthros targets remove it, and say so. Only on a
    list the hub says holds every machine, offline ones included (a stopped
    This computer is in no other), and never one younger than a join's
    life, which may still be joining (Jorge, 2026-11-20)."""

    def setUp(self):
        super().setUp()
        self.docker.answers["ps"] = (0, f"zenthros-gone\tct-gone\t{created(2)}\n"
                                        f"zenthros-mi-pc\tct-1\t{created(2)}\n"
                                        f"zenthros-joining\tct-new\t{created(0.1)}\n")
        self.whole = True
        self.with_offline = [OFFLINE_PC]

        def backends(_body):
            asked = self.api.requests[-1]["path"]
            answer = {"backends": self.with_offline if asked.endswith("?offline=true") else []}
            if asked.endswith("?offline=true") and self.whole:
                answer["includesOffline"] = True
            return 200, answer
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = backends

    def removed(self):
        return [a[-1] for a in self.docker_args() if a[:2] == ["rm", "-f"]]

    def test_only_an_old_container_of_a_machine_the_hub_no_longer_has_goes(self):
        err = io.StringIO()
        self.assertEqual(machines.sweep(out=err), ["zenthros-gone"])
        self.assertEqual(self.removed(), ["zenthros-gone"], "the stopped mi-pc is offline, not gone")
        self.assertIn("removed the container zenthros-gone: its machine ct-gone is no longer in Workspace risk",
                      err.getvalue())
        ps = next(a for a in self.docker_args() if a[0] == "ps")
        self.assertIn(f"label=zenthros.ai/hub={self.api.url}", ps, "another hub's list says nothing of it")
        self.assertIn("label=zenthros.ai/workspace=risk", ps)

    def test_a_hub_that_does_not_answer_the_list_removes_nothing_and_fails_nothing(self):
        # The sweep is a cleanup on the way of attach and targets: a hub that
        # cannot give its whole list now leaves every container, and the
        # command it rides on goes on.
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (
            400, {"kind": "Status", "reason": "BadRequest", "message": "a gateway replica did not answer"})
        self.assertEqual(machines.sweep(out=io.StringIO()), [])
        self.assertEqual(self.removed(), [])

    def test_a_list_that_does_not_say_it_holds_the_offline_ones_removes_nothing(self):
        self.whole = False
        self.assertEqual(machines.sweep(out=io.StringIO()), [])
        self.assertEqual(self.removed(), [])

    def test_nothing_here_asks_the_hub_nothing(self):
        self.docker.answers["ps"] = (0, "")
        machines.sweep(out=io.StringIO())
        self.assertNotIn("api GET /backends?offline=true", self.events)

    def test_attach_and_targets_sweep(self):
        from zenthros import cli

        self.with_offline = [MI_PC]
        machines.attach("mi-pc", type="local", out=io.StringIO())
        self.assertEqual(self.removed(), ["zenthros-gone"])
        del self.docker.calls[:]
        with mock.patch("zenthros.targets", return_value="TARGETS"), \
                mock.patch("sys.stderr", new_callable=io.StringIO) as err:
            cli.main(["targets"], out=io.StringIO())
        self.assertEqual(self.removed(), ["zenthros-gone"])
        self.assertIn("removed the container zenthros-gone", err.getvalue())


RELEASED = {"Zenthros-Release": "v0.1.1"}
UPDATE_JOIN = {"backendId": "ct-new", "workspace": "risk", "expiresUnix": "1760001800",
               "installScript": "curl -fsSL 'https://r.example/v0.1.1/install-container.sh' | sh\n",
               "joinScript": f"zenadm join 'gw.example:9443' --token '{TOKEN}'\n"}
OLD_PC = {**MI_PC, "agentVersion": "v0.1.0"}


class UpdateTest(Attach):
    """observaciones P11: a machine whose agent is of another release than
    the hub's (Zenthros-Release) is updated in place. This computer's by
    zenthros attach itself, with its environments; a VM's or a provider
    container's by the lines attach prints. The installer comes from a join
    of the hub's, given back at once: the line has no secret."""

    def setUp(self):
        super().setUp()
        self.docker.answers["ps"] = (0, "zenthros-mi-pc\n")
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, UPDATE_JOIN)
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/ct-new")] = (200, {"backendId": "ct-new"})
        listed = self.api.routes[("GET", "/v1/workspaces/risk/backends")]

        def released(body):
            return (*listed(body), RELEASED)
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = released

    def test_this_computer_gets_the_hubs_agent_in_place(self):
        self.listed = [[OLD_PC], [{**MI_PC, "agentVersion": "v0.1.1"}]]
        out = io.StringIO()
        m = machines.attach("mi-pc", type="local", out=out)
        self.assertEqual(m.id, "ct-1")
        execs = [(args, stdin) for args, stdin in self.docker.calls if args[0] == "exec"]
        self.assertEqual([args for args, _ in execs], [["exec", "-i", "zenthros-mi-pc", "sh"]] * 2)
        self.assertIn("install-container.sh", execs[0][1])
        self.assertIn(local.RESTART_AGENT, execs[1][1])
        self.assertLess(self.events.index("api DELETE /backends/ct-new"), self.events.index("docker exec"),
                        "the join is given back before anything runs")
        self.assertNotIn("api DELETE /backends/ct-1", self.events, "the machine itself was retired")
        self.assertFalse([a for a in self.docker_args() if a[0] in ("rm", "run")], "made again")
        self.assertIn("✓ mi-pc: its agent updated from v0.1.0 to v0.1.1, its environments as they were", out.getvalue())
        self.assertEqual(self.events[-1], "api GET /backends", "returned before the hub heard the new agent")

    def test_one_of_zenthros_image_gets_what_its_runner_needs_too(self):
        # One made before its runner needed zstd's backport started each call
        # twice (+0.2 s): updating it brings them, as root, as its image does.
        # One of the person's own image (the test above) is left as it is.
        self.docker.answers["inspect"] = (0, "zenthros-python:3.13-0123456789ab\n")
        self.listed = [[OLD_PC], [{**MI_PC, "agentVersion": "v0.1.1"}]]
        machines.attach("mi-pc", type="local", out=io.StringIO())
        execs = [args for args, _ in self.docker.calls if args[0] == "exec"]
        self.assertEqual(execs[0], ["exec", "-u", "0", "zenthros-mi-pc", "python3", "-m", "pip", "install", "--no-cache-dir",
                                    "--quiet", "--disable-pip-version-check", *machines._runner_packages()])
        self.assertEqual(execs[1:], [["exec", "-i", "zenthros-mi-pc", "sh"]] * 2, "then its agent, as before")

    def test_one_running_a_job_is_left_for_later(self):
        self.listed = [[{**OLD_PC, "runningJobs": 1}]]
        with self.assertRaisesRegex(errors.ValidationError, "runs a job now: its agent updates once it ends"):
            machines.attach("mi-pc", type="local")
        self.assertEqual(self.joins(), [])

    def test_one_of_the_hubs_release_is_left_as_it_is(self):
        self.listed = [[{**MI_PC, "agentVersion": "v0.1.1"}]]
        out = io.StringIO()
        machines.attach("mi-pc", type="local", out=out)
        self.assertEqual(self.joins(), [])
        self.assertIn("✓ mi-pc is already attached", out.getvalue())

    def test_a_vm_and_a_providers_container_get_the_lines_that_update_them(self):
        self.docker.answers["ps"] = (0, "")
        vm = {**MI_EC2, "agentVersion": "v0.1.0",
              "capabilities": [{"kind": "Job", "runtimes": ["containerd"], "defaultRuntime": "containerd"}]}
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, {**VM_JOIN, "backendId": "vm-new"})
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/vm-new")] = (200, {"backendId": "vm-new"})
        self.listed = [[vm]]
        out = io.StringIO()
        found = machines.attach("mi-ec2", out=out)
        said = out.getvalue()
        self.assertIn("mi-ec2's agent is v0.1.0, the hub's v0.1.1. On mi-ec2, run (the machine and its jobs stay):", said)
        self.assertIn(VM_JOIN["installScript"].rstrip(), said)
        # From Python, with nothing written (out=None), the machine says them.
        self.assertEqual(found.update, (VM_JOIN["installScript"].rstrip(),))
        self.assertNotIn(TOKEN, said, "an update needs no join")
        self.assertEqual(self.joins(), [{"type": "vm", "runtime": "containerd"}], "its own runtime's installer")
        self.assertIn("api DELETE /backends/vm-new", self.events)
        self.assertEqual(self.ssh_calls, [])

        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, UPDATE_JOIN)
        self.listed = [[{**OLD_PC, "name": "pod-1", "backendId": "ct-9"}]]
        out = io.StringIO()
        quiet = machines.attach("pod-1")
        machines.attach("pod-1", out=out)
        self.assertTrue(out.getvalue().rstrip().endswith(local.RESTART_AGENT), out.getvalue())
        self.assertEqual(quiet.update[-1], local.RESTART_AGENT, "out=None: the lines are the machine's")


if __name__ == "__main__":
    unittest.main()
