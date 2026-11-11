#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.LocalBackend: this computer as a backend of the Workspace, in a
container, through zenthros-api and docker alone.

What is pinned here: the container is unprivileged and has hard limits; the
join token goes to the container on standard input and never into an
argument, an error or a log; a backend is ready only once it is registered,
healthy, open, accepting work and Private to the Workspace; and removing it
retires it at the API before its container goes.
"""
import os
import stat
import sys
import tempfile
import unittest
from unittest import mock

import zenthros
from zenthros import errors, local
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI, local_in_wsl, status

TOKEN = "zt9xq2.s3cr3tv4lu3d0n0tl0g"
JOIN = {"backendId": "ct-1", "workspace": "risk", "expiresUnix": "1760000900",
        "installScript": "# 1. In the container, install the agent\nsh -s -- --install-base 'https://r.example/v1' <<'ZENTHROS_INSTALL'\n...\nZENTHROS_INSTALL\n",
        "joinScript": "# 2. Then join it.\nzenadm join \\\n  --controller 'gw:9443' \\\n  --token '" + TOKEN + "' \\\n  --backend-id 'ct-1'\n"}
PRIVATE = {"backendId": "ct-1", "backendType": "container", "healthy": True, "maxJobs": 0, "accessMode": "Private",
           "workspaces": ["risk"]}


class FakeDocker:
    """Records each docker call (its arguments and what it got on standard
    input) and answers as docker would for this flow."""

    def __init__(self, events):
        self.events = events
        self.calls = []
        self.fail = {}  # first argument -> (code, output)
        self.answers = {}  # first argument -> (code, output), when it succeeds
        self.path_hint = ""

    def __call__(self, docker, args, stdin=None, timeout=None):
        self.calls.append((list(args), stdin))
        self.events.append("docker " + args[0])
        if args[0] in self.fail:
            return self.fail[args[0]]
        if args[0] in self.answers:
            return self.answers[args[0]]
        if args[0] == "info":
            return 0, "8 16777216000\n"
        if args[0] == "run":
            return 0, "4f1c0ffee\n"
        if args[0] == "exec" and stdin and "ZENTHROS_INSTALL" in stdin:
            out = "Next: paste your one-time 'zenadm join ...' command.\n"
            if self.path_hint:
                out += f'zenadm is in {self.path_hint}, which is not on PATH: run export PATH="{self.path_hint}:$PATH" first\n'
            return 0, out
        if args[0] == "exec" and stdin and "zenadm join" in stdin:
            return 0, "attached as backend ct-1\n"
        if args[0] == "inspect":
            return 0, "true\n"
        return 0, ""


class LocalBackendTest(unittest.TestCase):
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
        patcher = mock.patch.object(local, "_docker", self.docker)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, JOIN)
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/ct-1")] = (200, {"backendId": "ct-1", "tokensRevoked": 1,
                                                                                   "accessRemoved": True})
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [PRIVATE]})
        # Every API call, in the same order as the docker calls.
        original = HTTPJobClient._call

        def recorded(client, method, path, *args, **kwargs):
            self.events.append(f"api {method} {path}")
            return original(client, method, path, *args, **kwargs)
        patcher = mock.patch.object(HTTPJobClient, "_call", recorded)
        patcher.start()
        self.addCleanup(patcher.stop)

    def docker_args(self):
        return [args for args, _ in self.docker.calls]

    def test_create_starts_an_unprivileged_container_with_hard_limits_and_joins_it(self):
        lb = zenthros.LocalBackend.create("zenthros-pipeline:local", cpus=2, memory="4g")
        self.assertEqual((lb.id, lb.workspace, lb.container, lb.created), ("ct-1", "risk", "zenthros-ct-1", True))
        run = self.docker_args()[0]
        self.assertEqual(run[0], "run")
        flags = " ".join(run)
        for wanted in ("--cpus 2", "--memory 4g", "--memory-swap 4g", "--name zenthros-ct-1", "--cap-drop ALL",
                       "--security-opt no-new-privileges", "--label zenthros.ai/backend=ct-1", "--init"):
            self.assertIn(wanted, flags)
        for never in ("--privileged", "-v", "--volume", "--mount", "--cap-add", "--pid", "--network host",
                      "--network=host", "--device"):
            self.assertNotIn(never, run, "the container gets nothing of the host")
        self.assertEqual(run[-3:], ["zenthros-pipeline:local", "sleep", "infinity"])
        install, join = self.docker.calls[1], self.docker.calls[2]
        self.assertEqual(install[0], ["exec", "-i", "zenthros-ct-1", "sh"])
        self.assertIn("ZENTHROS_INSTALL", install[1])
        self.assertEqual(join[0], ["exec", "-i", "zenthros-ct-1", "sh"])
        self.assertIn(TOKEN, join[1], "the token goes in on standard input")
        self.assertEqual(self.events[:4], ["api POST /backends", "docker run", "docker exec", "docker exec"],
                         "the join is asked first: the container is named after the backend")

    def test_ports_are_published_to_this_computer_only_and_the_url_says_so(self):
        zenthros.LocalBackend.create("img")
        self.assertNotIn("-p", self.docker_args()[0], "no port without ports=")
        del self.docker.calls[:]
        zenthros.LocalBackend.create("img", ports=[8000, 9000])
        run = self.docker_args()[0]
        published = [run[i + 1] for i, arg in enumerate(run) if arg == "-p"]
        self.assertEqual(published, ["127.0.0.1:8000:8000", "127.0.0.1:9000:9000"], "never on the network")
        env = [run[i + 1] for i, arg in enumerate(run) if arg == "-e"]
        # The agent publishes what this computer reaches, not the
        # container's own address.
        self.assertEqual(env, ["ZENTHROS_ENDPOINT_TEMPLATE={scheme}://127.0.0.1:{port};scope=Private"])
        self.assertIn("zenthros.ai/ports=8000,9000", run)
        self.assertEqual(run[-3:], ["img", "sleep", "infinity"])

    def test_the_token_is_in_no_argument_and_no_error(self):
        zenthros.LocalBackend.create("img")
        for args in self.docker_args():
            self.assertFalse(any(TOKEN in arg for arg in args), args)
        self.docker.fail["exec"] = (1, f"zenadm join: enroll refused for token {TOKEN}: AlreadyExists\n")
        with self.assertRaises(errors.BackendJoinError) as caught:
            zenthros.LocalBackend.create("img")
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertIn("AlreadyExists", str(caught.exception), "the reason is kept")

    def test_a_create_that_fails_leaves_no_token_and_no_container(self):
        self.docker.fail["exec"] = (1, "sh: python3: not found\n")
        with self.assertRaises(errors.BackendJoinError) as caught:
            zenthros.LocalBackend.create("img")
        self.assertIn("python3: not found", str(caught.exception))
        self.assertIn("api DELETE /backends/ct-1", self.events, "the token is revoked")
        self.assertEqual(self.docker_args()[-1], ["rm", "-f", "zenthros-ct-1"], "and the container it made removed")

    def test_a_create_whose_container_never_started_revokes_its_token(self):
        self.docker.fail["run"] = (125, "docker: Error response from daemon: pull access denied for img\n")
        with self.assertRaises(errors.BackendJoinError) as caught:
            zenthros.LocalBackend.create("img")
        self.assertIn("pull access denied", str(caught.exception))
        self.assertEqual(self.events[-1], "api DELETE /backends/ct-1")

    def test_a_join_that_did_not_attach_this_backend_is_a_failure(self):
        # zenadm join exits 0 when an agent is already attached: to another
        # backend, it is not this one.
        original = self.docker.__call__

        def other(docker, args, stdin=None, timeout=None):
            code, out = original(docker, args, stdin, timeout)
            return (0, "already attached as backend ct-9\n") if stdin and "zenadm join" in stdin else (code, out)
        with mock.patch.object(local, "_docker", other):
            with self.assertRaises(errors.BackendJoinError) as caught:
                zenthros.LocalBackend.create("img")
        self.assertIn("ct-9", str(caught.exception))

    def test_an_installer_off_path_is_followed_as_it_says(self):
        self.docker.path_hint = "/home/zenthros/.local/bin"
        zenthros.LocalBackend.create("img")
        self.assertTrue(self.docker.calls[2][1].startswith('export PATH="/home/zenthros/.local/bin:$PATH"\n'))

    def test_attach_joins_a_running_container_and_creates_nothing(self):
        lb = zenthros.LocalBackend.attach("my-notebook-box")
        self.assertEqual((lb.id, lb.container, lb.created), ("ct-1", "my-notebook-box", False))
        self.assertNotIn("run", [args[0] for args in self.docker_args()])
        self.assertEqual(self.docker_args()[0][:2], ["inspect", "--format"], "it looks before it joins")
        lb.remove()
        self.assertEqual(self.events[-2:], ["api DELETE /backends/ct-1", "docker exec"],
                         "retired first; then its agent is stopped")
        self.assertIn("--stop", self.docker_args()[-1])
        self.assertNotIn("rm", [args[0] for args in self.docker_args()], "a container it did not make stays")

    def test_attach_to_a_container_that_is_not_running_asks_nothing(self):
        self.docker.fail["inspect"] = (1, "Error: No such object: gone\n")
        with self.assertRaises(errors.ValidationError):
            zenthros.LocalBackend.attach("gone")
        self.assertNotIn("api POST /backends", self.events)

    def test_remove_retires_before_the_container_goes(self):
        lb = zenthros.LocalBackend.create("img")
        del self.events[:]
        lb.remove()
        self.assertEqual(self.events, ["api DELETE /backends/ct-1", "docker rm"])
        self.assertEqual(self.docker_args()[-1], ["rm", "-f", "zenthros-ct-1"])

    def test_a_retire_the_api_refuses_keeps_the_container(self):
        lb = zenthros.LocalBackend.create("img")
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/ct-1")] = status(403, "retire zenthrosbackends ct-1 is forbidden")
        with self.assertRaises(errors.WorkspaceForbidden):
            lb.remove()
        self.assertNotIn("rm", [args[0] for args in self.docker_args()],
                         "a backend still joined is not left without its container")


class ReadyTest(unittest.TestCase):
    """Ready is four named checks, each from what the API said."""

    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token")
        self.lb = local.LocalBackend(self.client, "ct-1", container="zenthros-ct-1", created=True)

    def seen(self, *backends):
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": list(backends)})

    def test_each_check_is_named_until_all_hold(self):
        self.seen()
        self.assertEqual((self.lb.status().ready, self.lb.status().waiting_for), (False, "registered"))
        self.seen({**PRIVATE, "healthy": False})
        self.assertEqual(self.lb.status().waiting_for, "healthy")
        self.seen({**PRIVATE, "drained": True})
        self.assertEqual(self.lb.status().waiting_for, "open")
        # An agent installing its runtime advertises -1: it runs nothing yet.
        self.seen({**PRIVATE, "maxJobs": -1})
        self.assertEqual(self.lb.status().waiting_for, "accepting")
        self.seen({**PRIVATE, "accessMode": "Public", "workspaces": ["risk", "ml"]})
        self.assertEqual(self.lb.status().waiting_for, "private")
        self.seen({**PRIVATE, "workspaces": ["ml"]})
        self.assertEqual(self.lb.status().waiting_for, "private", "Private, but to another Workspace")
        self.seen(PRIVATE)
        status_ = self.lb.status()
        self.assertTrue(status_.ready)
        self.assertEqual((status_.waiting_for, status_.registered, status_.healthy, status_.accepting, status_.private),
                         ("", True, True, True, True))

    def test_wait_ready_returns_only_once_work_is_accepted(self):
        states = [[], [{**PRIVATE, "maxJobs": -1}], [{**PRIVATE, "maxJobs": -1}], [PRIVATE]]
        self.seen(*states.pop(0))
        seen = []

        def tick(_seconds):
            seen.append(self.lb.status().waiting_for)
            self.seen(*states.pop(0))
        with mock.patch.object(local, "_sleep", tick):
            backend = self.lb.wait_ready(timeout=60, interval=1)
        self.assertEqual(seen, ["registered", "accepting", "accepting"])
        self.assertTrue(backend.ready)

    def test_wait_ready_waits_out_a_moment_the_api_is_away(self):
        # A wait inside an undo (a joining backend is retired on failure):
        # the API answering 503 for a moment is "not ready yet", as a
        # template machine's boot is (final review of Ola J, G1).
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (503, {"kind": "Status", "message": "rolling out",
                                                                       "reason": "ServiceUnavailable", "code": 503})

        def tick(_seconds):
            self.seen(PRIVATE)
        with mock.patch.object(local, "_sleep", tick):
            backend = self.lb.wait_ready(timeout=60, interval=1)
        self.assertTrue(backend.ready)

    def test_wait_ready_says_what_it_was_waiting_for(self):
        self.seen({**PRIVATE, "healthy": False})
        with mock.patch.object(local, "_sleep", lambda _s: None):
            with self.assertRaises(errors.WaitTimeout) as caught:
                self.lb.wait_ready(timeout=3, interval=1)
        self.assertIn("healthy", str(caught.exception))
        self.assertIn("ct-1", str(caught.exception))


@local_in_wsl
class DockerCallTest(unittest.TestCase):
    """_docker itself: standard input goes to the process's standard input,
    and stderr comes back with stdout, as a terminal shows it."""

    def test_stdin_is_standard_input(self):
        with tempfile.TemporaryDirectory() as scratch:
            fake = os.path.join(scratch, "docker")
            with open(fake, "w") as fh:
                fh.write(f"#!{sys.executable}\nimport sys\n"
                         "print('args', sys.argv[1:])\nprint('stdin', sys.stdin.read())\n"
                         "print('said on stderr', file=sys.stderr)\nsys.exit(3)\n")
            os.chmod(fake, os.stat(fake).st_mode | stat.S_IXUSR)
            code, out = local._docker(fake, ["exec", "-i", "box", "sh"], stdin="secret-on-stdin\n", timeout=30)
        self.assertEqual(code, 3)
        self.assertIn("args ['exec', '-i', 'box', 'sh']", out)
        self.assertIn("stdin secret-on-stdin", out)
        self.assertIn("said on stderr", out)

    def test_no_docker_is_said_plainly(self):
        with self.assertRaises(errors.ValidationError) as caught:
            local._docker("/nonexistent/docker", ["ps"])
        self.assertIn("docker", str(caught.exception))


class ClientTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token")

    def test_join_and_retire_are_the_routes(self):
        self.api.routes[("POST", "/v1/workspaces/risk/backends")] = (201, JOIN)
        self.api.routes[("DELETE", "/v1/workspaces/risk/backends/ct%2F1")] = (200, {"backendId": "ct/1"})
        self.assertEqual(self.client.join_backend()["backendId"], "ct-1")
        self.assertEqual(self.client.retire_backend("ct/1")["backendId"], "ct/1")
        self.assertEqual([r["method"] + " " + r["path"] for r in self.api.requests],
                         ["POST /v1/workspaces/risk/backends", "DELETE /v1/workspaces/risk/backends/ct%2F1"])


if __name__ == "__main__":
    unittest.main()
