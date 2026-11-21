#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""LocalJobClient against an engine that only answers: what it asks the engine
to do, in what order, and that what crosses to an agent is the job the other
transports send. The real engine is in tests/e2e/test_quickstart.py."""
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest import mock

from zenthros import errors, machines, remote
from zenthros.jobs import local as L
from zenthros.jobs import spec as S
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import FAILED, SUCCEEDED
from zenthros.remote import call as remote_call
from zenthros.remote import runner

from support import local_in_wsl


def job(name="j", image="python:3.13", **template):
    return S.Job(name=name, spec=S.Spec(workload=S.Workload(kind="Job", template=S.Template(
        entrypoint="echo hi", image=image, **template))))


class AnsweringEngine:
    """An engine with an agent in each container it made."""

    name, binary, user_flags = "docker", "/fake/docker", ("--user", "1000:1000")
    capacity_format = L.Engine.capacity_format

    def __init__(self):
        self.calls = []
        self.containers = {}
        self.statuses = {}
        self.jobs = 0
        self.reports = "[]"
        self.logs = "line\n"
        self.cursors = {}
        # The jobs the agent let go, by ID.
        self.let_go = []

    def execs(self):
        return [c[5] for c in self.calls if c[0] == "exec"]

    def image_name(self, image):
        return image

    def run(self, args, stdin=None, timeout=600):
        self.calls.append(list(args))
        verb = args[0]
        if verb == "inspect":
            found = self.containers.get(args[-1])
            if not found:
                return 1, "", "Error: No such object"
            return 0, f"{found['state']}|{found['config']}|{found['mounts']}\n", ""
        if verb == "ps":
            return 0, "\n".join(n for n, c in self.containers.items() if c["state"] == "running") + "\n", ""
        if verb == "run":
            name = args[args.index("--name") + 1]
            labels = [args[i + 1] for i, a in enumerate(args) if a == "--label"]
            value = lambda key: next(v.split("=", 1)[1] for v in labels if v.startswith(key + "="))
            self.containers[name] = {"state": "running", "config": value(L.CONFIG_LABEL), "mounts": value(L.MOUNTS_LABEL), "args": args}
            return 0, "id\n", ""
        if verb == "rm":
            self.containers.pop(args[-1], None)
            return 0, "", ""
        if verb == "stop":
            self.containers[args[-1]]["state"] = "exited"
            return 0, "", ""
        if verb == "start":
            self.containers[args[1]]["state"] = "running"
            return 0, "", ""
        if verb == "exec":
            return self.agent(args[2], args[5:], stdin)
        return 0, "", ""

    def run_kept(self, args, stdin, keep, timeout=600):
        """local finish: it sends what it holds, and lets go once kept."""
        self.calls.append(list(args))
        container = self.containers.get(args[2])
        if not container or container["state"] != "running":
            return 1, "", "no such container"
        line = json.dumps({"reports": json.loads(self.reports), "log": self.logs}) + "\n"
        keep(line)
        self.let_go.append(json.loads(stdin)["id"])
        self.cursors.clear()
        return 0, line, ""

    def agent(self, container, args, stdin):
        verb = args[0]
        if container not in self.containers or self.containers[container]["state"] != "running":
            return 1, "", "no such container"
        if verb == "info":
            return 0, "{}", ""
        if verb == "submit":
            self.submitted = json.loads(stdin)
            self.jobs += 1
            return 0, json.dumps({"id": f"id-{self.jobs}", "count": 1, "tasks": [{"rank": 0, "id": f"id-{self.jobs}", "handle": f"h-{self.jobs}"}]}), ""
        if verb == "status":
            return 0, json.dumps(self.statuses.get(json.loads(stdin)["id"], {"phase": "Running"})), ""
        if verb == "wait":
            status = self.statuses.get(json.loads(stdin)["id"], {"phase": "Running"})
            return 0, json.dumps({"status": status, "seen": hashlib.sha256(json.dumps(status).encode()).hexdigest()[:16]}), ""
        if verb == "progress":
            return 0, json.dumps(self.cursors[args[1]]) if args[1] in self.cursors else "", ""
        if verb == "reports":
            return 0, self.reports, ""
        if verb == "logs":
            return 0, self.logs, ""
        if verb == "forget":
            self.cursors.clear()
        return 0, "", ""


class Env(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.folder, True)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        clean.update({"XDG_CACHE_HOME": self.folder + "/cache", L.AGENT_ENV: self.folder + "/agent", machines.LOCAL_CPUS_ENV: "4", machines.LOCAL_MEMORY_ENV: "8g"})
        patcher = mock.patch.dict(os.environ, clean, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.engine = AnsweringEngine()
        os.makedirs(self.folder + "/project/.git")
        self.client = L.LocalJobClient(root=self.folder + "/project", engine=self.engine)

    def created(self):
        return next(c for c in reversed(self.engine.calls) if c[0] == "run")

    def finish(self, name="j", **status):
        self.engine.statuses[self.client._read(name)["local"]["id"]] = {"phase": SUCCEEDED, **status}


class ResumeFromTest(Env):
    """With no controller here, this client does what the controller does."""

    CURSOR = {"seq": 14, "uri": "file:///s/ck", "sha256": "ab", "complete": True}

    def ended(self, name="before", **cursor):
        self.client.submit(job(name))
        self.finish(name)
        uid = self.client._read(name)["manifest"]["metadata"]["uid"]
        if cursor:
            # While it runs the agent holds it, and answers with it; told to
            # forget the job once it was collected, it no longer does.
            self.engine.cursors[uid] = cursor
            self.engine.statuses[self.client._read(name)["local"]["id"]]["progress"] = cursor
        self.client.job_object(name)
        return uid

    def resuming(self, source, uid, name="after"):
        made = job(name)
        made.spec.resume_from = S.ResumeFrom(job=source, uid=uid)
        return made

    def test_the_cursor_of_the_job_it_resumes_from_reaches_the_agent(self):
        uid = self.ended(**self.CURSOR)
        self.client.submit(self.resuming("before", uid))
        self.assertEqual(self.engine.submitted["status"]["progress"], self.CURSOR)
        self.assertEqual(self.engine.submitted["spec"]["resumeFrom"], {"job": "before", "uid": uid})

    def test_the_cursor_outlives_what_the_agent_was_told_to_forget_and_its_container(self):
        self.ended(**self.CURSOR)
        self.assertIn("finish", self.engine.execs())
        self.assertEqual(self.client.cursor_of("before"), self.CURSOR)
        for container in self.engine.containers.values():
            container["state"] = "exited"
        self.assertEqual(self.client.cursor_of("before"), self.CURSOR)

    def test_a_job_you_cancelled_goes_on_from_its_last_checkpoint_too(self):
        self.client.submit(job("stopped"))
        self.engine.cursors[self.client._read("stopped")["manifest"]["metadata"]["uid"]] = self.CURSOR
        self.client.cancel("stopped")
        self.assertEqual(self.client.cursor_of("stopped"), self.CURSOR)

    def test_a_job_that_left_no_checkpoint_gives_none(self):
        self.client.submit(self.resuming("before", self.ended()))
        self.assertNotIn("progress", self.engine.submitted.get("status") or {})

    def test_a_job_with_no_origin_asks_nothing(self):
        self.client.submit(job("alone"))
        self.assertNotIn("progress", self.engine.execs())

    def refused(self, made, says):
        submits = self.engine.jobs
        with self.assertRaisesRegex(errors.Refused, says):
            self.client.submit(made)
        self.assertEqual(self.engine.jobs, submits, "nothing reaches the agent")
        self.assertFalse(self.client.knows(made.name), "a refused job leaves no record")

    def test_an_origin_that_is_gone_is_refused(self):
        self.refused(self.resuming("never", "some-uid"), "is gone")

    def test_another_job_of_that_name_is_refused(self):
        self.ended(**self.CURSOR)
        self.refused(self.resuming("before", "another-uid"), "another job")

    def test_an_origin_that_has_not_ended_is_refused(self):
        self.client.submit(job("before"))
        uid = self.client._read("before")["manifest"]["metadata"]["uid"]
        self.refused(self.resuming("before", uid), "has not ended")


class ContainerTest(Env):
    def test_the_container_holds_nothing_of_this_computer_it_was_not_given(self):
        self.client.submit(job())
        args = self.created()
        joined = " ".join(args)
        self.assertIn("--cap-drop ALL", joined)
        self.assertIn("--security-opt no-new-privileges", joined)
        self.assertIn("--user 1000:1000", joined)
        self.assertIn("--memory 8g --memory-swap 8g", joined, "a job that needs more dies in the container, not this computer")
        self.assertIn("--cpus 4", joined)
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        # The agent, HOME, the agent's state and the project's own folder.
        self.assertEqual(len(volumes), 4, volumes)
        self.assertTrue(any(v.endswith(f":{L.AGENT_PATH}:ro") for v in volumes), "a job cannot change the next start's agent")
        project = str(self.client.project)
        self.assertNotIn(f"{project}:{project}", volumes, "the project is never mounted whole")
        self.assertIn(f"{self.client.root}:{self.client.root}", volumes)

    def test_a_named_path_comes_read_only_at_its_own_path(self):
        self.client.need_paths(["/datos/ventas"])
        self.client.submit(job())
        self.assertIn("/datos/ventas:/datos/ventas:ro", self.created())

    def test_ports_answer_on_this_computer_only(self):
        self.client.submit(job(service=S.Service(ports=[S.ServicePort(name="http", port=8000)])))
        args = self.created()
        self.assertIn("127.0.0.1:8000:8000", args)
        self.assertIn(L.ENDPOINT_TEMPLATE, args)

    def test_it_is_used_again(self):
        self.client.submit(job("a"))
        self.client.submit(job("b"))
        self.assertEqual(sum(1 for c in self.engine.calls if c[0] == "run"), 1)

    def test_a_container_this_process_checked_is_not_checked_again(self):
        """The next job goes straight to it: no ps, inspect or info (0.1-0.2 s
        each), nor the engine's size (0.3 s)."""
        del os.environ[machines.LOCAL_CPUS_ENV], os.environ[machines.LOCAL_MEMORY_ENV]
        with mock.patch("zenthros.machines.docker_capacity", return_value=(8, 16 << 30)) as asked:
            self.client.submit(job("a"))
            before = len(self.engine.calls)
            self.client.submit(job("b"))
        self.assertEqual([c[5] if c[0] == "exec" else c[0] for c in self.engine.calls[before:]], ["submit"])
        self.assertEqual(asked.call_count, 1)
        self.assertIn("--cpus 8", " ".join(self.created()))

    def test_python_keeps_what_it_compiled_with_home(self):
        """python:3.x ships its standard library uncompiled, and the job's user
        cannot write beside it: each Python start compiled it again."""
        self.client.submit(job())
        self.assertIn(f"PYTHONPYCACHEPREFIX={L.PYCACHE_DIR}", self.created())
        self.assertTrue(L.PYCACHE_DIR.startswith(L.HOME_DIR + "/"), "kept with HOME, mounted from this computer")

    def test_another_agent_is_another_container(self):
        """A container of the release before runs its agent, which knows
        none of this SDK's verbs (local wait, local finish)."""
        self.client.submit(job("a"))
        self.finish("a")
        self.client.job_object("a")
        with open(os.environ[L.AGENT_ENV], "w") as built:
            built.write("built again")
        self.client.submit(job("b"))
        self.assertEqual(sum(1 for c in self.engine.calls if c[0] == "run"), 2)

    def test_it_leaves_no_container_behind_and_keeps_what_it_holds_on_this_computer(self):
        """It goes away when it exits idle (--rm): a stopped one stayed for
        good. Its state (spool, checkpoints, logs) is mounted from this
        computer with HOME, so the next one finds them; its socket is not
        on that mount (2026-11-20, observaciones: containers left lying)."""
        self.client.submit(job())
        args = self.created()
        self.assertIn("--rm", args)
        name = args[args.index("--name") + 1]
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        state = next(v for v in volumes if v.endswith(f":{L.STATE_DIR}"))
        self.assertEqual(state, f"{L.cache_root() / 'state' / name}:{L.STATE_DIR}")
        socket = next(a for a in args if a.startswith(f"{L.LOCAL_SOCKET_ENV}="))
        self.assertFalse(socket.split("=", 1)[1].startswith(L.STATE_DIR + "/"))

    def test_a_stopped_one_starts_again(self):
        self.client.submit(job("a"))
        self.finish("a")
        self.client.job_object("a")
        name = next(iter(self.engine.containers))
        self.engine.containers[name]["state"] = "exited"
        self.client.submit(job("b"))
        self.assertIn(["start", name], self.engine.calls)
        self.assertEqual(sum(1 for c in self.engine.calls if c[0] == "run"), 1)

    def test_more_mounts_make_it_again_only_when_idle(self):
        self.client.submit(job("a"))
        self.client.need_paths(["/datos"])
        with self.assertRaises(errors.ValidationError) as caught:
            self.client.submit(job("b"))
        self.assertIn("a still runs", str(caught.exception))
        self.assertFalse(self.client.knows("b"), "a job refused leaves no record")
        self.finish("a")
        self.client.job_object("a")
        self.client.submit(job("c"))
        self.assertIn("/datos:/datos:ro", self.created())

    def test_one_image_at_a_time(self):
        self.client.submit(job("a"))
        with self.assertRaises(errors.ValidationError) as caught:
            self.client.submit(job("b", image="other:1"))
        self.assertIn("zenthros attach", str(caught.exception), "a machine of yours runs both at once")
        self.assertNotIn("local-k8s", str(caught.exception))
        self.finish("a")
        self.client.job_object("a")
        first = next(iter(self.engine.containers))
        self.client.submit(job("c", image="other:1"))
        self.assertEqual(self.engine.containers[first]["state"], "exited", "the idle one stops first")

    def test_a_job_asking_more_than_the_container_has_is_refused_before_anything(self):
        big = job()
        big.spec.compute = S.Compute(memory="64Gi")
        with self.assertRaises(errors.ValidationError) as caught:
            self.client.submit(big)
        self.assertIn("local_memory", str(caught.exception))
        self.assertEqual(self.engine.containers, {})
        self.assertFalse(self.client.knows("j"))

    def test_no_engine_says_what_to_install(self):
        with mock.patch("platform.system", return_value="Linux"), mock.patch("shutil.which", return_value=None), \
                self.assertRaises(errors.EngineMissing) as caught:
            L.find_engine()
        self.assertIn("Docker Desktop", str(caught.exception))
        self.assertIn("Podman", str(caught.exception))

    def test_windows_says_wsl2(self):
        with mock.patch("platform.system", return_value="Windows"), self.assertRaises(errors.EngineMissing) as caught:
            L.find_engine()
        self.assertIn("WSL2", str(caught.exception))

    @local_in_wsl
    def test_each_engine_keeps_files_this_users(self):
        self.assertEqual(L.ENGINES["podman"]("/p").user_flags, ("--userns=keep-id",))
        self.assertEqual(L.ENGINES["docker"]("/d").user_flags[0], "--user")


@local_in_wsl
class JobTest(Env):
    def test_the_job_is_the_one_the_other_transports_send(self):
        fake = FakeJobClient()
        self.client.submit(job())
        fake.submit(job())
        sent = dict(self.engine.submitted)
        other = fake.job_object("j")
        self.assertEqual(sent["spec"], other["spec"])
        self.assertEqual(sent["kind"], other["kind"])
        self.assertEqual(sent["apiVersion"], other["apiVersion"])
        self.assertEqual(sent["metadata"].get("labels"), other["metadata"].get("labels"))

    def test_a_name_in_use_is_a_conflict(self):
        self.client.submit(job())
        with self.assertRaises(errors.Conflict):
            self.client.submit(job())

    def test_a_refused_submit_leaves_no_record(self):
        with mock.patch.object(self.engine, "agent", side_effect=lambda c, a, s: (0, "{}", "") if a[0] == "info" else (1, "", "no")), \
                self.assertRaises(errors.Refused):
            self.client.submit(job())
        self.assertFalse(self.client.knows("j"))

    def test_what_the_agent_holds_is_kept_before_it_is_told_to_forget(self):
        """One command brings it back and lets it go (it was status, reports,
        logs, cancel and forget, a docker exec each), and only once the record
        holds it: a Ctrl-C in between leaves it with the agent, asked again."""
        self.engine.reports = json.dumps([{"payload": {"a": 1}}])
        self.client.submit(job())
        self.finish()
        before = len(self.engine.execs())
        with mock.patch.object(self.client, "_write", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.client.job_object("j")
        self.assertEqual(self.engine.let_go, [], "let go before it was kept")
        self.assertFalse(self.client._read("j")["final"])
        self.assertEqual(self.client.job_object("j")["status"]["phase"], SUCCEEDED)
        self.assertEqual(self.engine.execs()[before:], ["status", "finish", "status", "finish"])
        self.assertEqual(self.engine.let_go, [self.client._read("j")["local"]["id"]])
        self.assertEqual([r["payload"] for r in self.client._read("j")["reports"]], [{"a": 1}])
        calls = len(self.engine.calls)
        # Read again, nothing asks the agent: it has let it go.
        self.assertEqual(self.client.logs("j"), "line\n")
        self.assertEqual(self.client.job_status("j").phase, SUCCEEDED)
        self.assertEqual(len(self.engine.calls), calls)

    def test_wait_asks_the_agent_to_answer_on_a_change(self):
        """One local wait, answered when the job changes, where a status every
        half second ran: wait() never sleeps."""
        self.client.submit(job())
        seen = []
        real = self.engine.agent

        def agent(container, args, stdin):
            if args[0] == "wait":
                seen.append(args[args.index("--seen") + 1])
                if len(seen) == 2:
                    self.finish()
            return real(container, args, stdin)

        with mock.patch.object(self.engine, "agent", side_effect=agent), mock.patch("time.sleep") as slept:
            self.assertEqual(self.client.wait("j"), SUCCEEDED)
        slept.assert_not_called()
        self.assertEqual(seen[0], "", "the first asks at once")
        self.assertNotEqual(seen[1], "", "then for a change from what it saw")
        self.assertNotIn("status", self.engine.execs())
        self.assertEqual(self.engine.execs()[-1], "finish")

    def test_the_container_gone_is_a_lost_worker(self):
        self.client.submit(job())
        self.engine.containers.clear()
        obj = self.client.job_object("j")
        self.assertEqual(obj["status"]["phase"], FAILED)
        self.assertEqual(obj["status"]["reasons"][0]["code"], "WORKER_LOST")

    def test_a_sigkill_is_no_memory_by_itself(self):
        # The agent alone says whether it was memory (observaciones P25, 4d):
        # a SIGKILL from elsewhere read as memory sent people after the wrong
        # cause.
        self.client.submit(job())
        killed = "process was killed by signal killed"
        self.finish(phase=FAILED, exitCode=137, message=killed, reasons=[{"code": "PROCESS_FAILED", "detail": killed}])
        status = self.client.job_object("j")["status"]
        self.assertEqual((status["reasons"][0]["code"], status["message"]), ("PROCESS_FAILED", killed))

    def test_what_the_agent_says_of_memory_says_where_it_is_set_here(self):
        self.client.submit(job())
        said = ("process was killed by signal killed; it probably ran out of memory: the kernel killed a process "
                "of this machine (4.0 GiB) for memory while it ran; use less memory")
        self.finish(phase=FAILED, exitCode=137, message=said, reasons=[{"code": "PROCESS_FAILED", "detail": said}])
        status = self.client.job_object("j")["status"]
        self.assertEqual(status["reasons"][0]["code"], "PROCESS_FAILED")
        self.assertTrue(status["message"].startswith(said), status["message"])
        self.assertIn("zenthros.configure(local_memory=...)", status["message"])

    def test_a_timeout_ends_it(self):
        timed = job()
        timed.spec.active_deadline_seconds = 1
        self.client.submit(timed)
        started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 60))
        self.engine.statuses[self.client._read("j")["local"]["id"]] = {"phase": "Running", "startTime": started}
        obj = self.client.job_object("j")
        self.assertEqual(obj["status"]["reason"], "DEADLINE_EXCEEDED")
        self.assertIn("cancel", self.engine.execs())

    def test_a_cancel_ends_it_for_good(self):
        self.client.submit(job())
        self.client.cancel("j")
        self.assertEqual(self.client.job_object("j")["status"]["phase"], "Cancelled")
        self.assertIn("forget", self.engine.execs())

    def test_old_records_go_and_the_rest_stay(self):
        self.client.submit(job("old"))
        self.finish("old")
        self.client.job_object("old")
        self.client.submit(job("running"))
        self.client.submit(job("recent"))
        self.finish("recent")
        self.client.job_object("recent")
        self.client._collected = False
        self.client._collect_old(now=time.time() + L.DEFAULT_KEEP_SECONDS + 60)
        self.assertFalse(self.client.knows("old"))
        self.assertFalse(self.client.knows("recent"))
        self.assertTrue(self.client.knows("running"), "a job not finished has no age")
        self.client._collected = False
        self.client._collect_old(now=time.time())
        self.assertTrue(self.client.knows("running"))

    def test_clean_spares_what_runs(self):
        self.client.submit(job())
        # ps -a of the engine names the running container with its state.
        real = self.engine.run

        def run(args, stdin=None, timeout=600):
            if args[:2] == ["ps", "-a"]:
                return 0, "".join(f"{n} {c['state']}\n" for n, c in self.engine.containers.items()), ""
            return real(args, stdin, timeout)
        self.engine.run = run
        self.assertEqual(self.client.clean(), [])
        self.finish()
        self.client.job_object("j")
        self.assertEqual(len(self.client.clean()), 1)
        self.assertEqual(self.engine.containers, {})


class WhereTest(Env):
    def test_no_on_is_this_computer(self):
        self.assertEqual(machines.default_on(), machines.LOCAL)

    def test_local_k8s_is_no_place_and_says_what_is(self):
        with self.assertRaises(errors.ValidationError) as caught:
            machines.place("local-k8s", {})
        said = str(caught.exception)
        self.assertIn("zenthros attach NAME --type kubernetes", said)
        self.assertIn("on='local'", said)

    def test_get_finds_a_local_job_whatever_else_is_configured(self):
        """On a computer with zctl and a kubeconfig too, zenthros.get asked the
        hub for a job that lives here."""
        self.client.submit(job())
        hub = FakeJobClient()
        with mock.patch.object(remote_call, "local_client_of", return_value=self.client), \
                remote.configure(client=hub):
            found = remote_call.get("j")
        self.assertIs(found.handle._client, self.client)


class DriftTest(unittest.TestCase):
    def test_the_sdk_is_not_a_package_the_caller_had_loaded(self):
        """The core travels with the call and cloudpickle in the runner's
        packages: a machine without zenthros, or backports.zstd (what only
        this side unpacks logs with), was told it lacked them, on every call."""
        import cloudpickle  # noqa: F401 - loaded, as in any caller

        from zenthros.jobs import logrecords
        from zenthros.remote import bundle

        logrecords.zstd_module()  # backports.zstd before 3.14: loaded by the first log

        loaded = {bundle._canonical(dist) for dist in bundle.loaded_packages()}
        self.assertFalse(loaded & {"zenthros", "cloudpickle", "backports-zstd"}, loaded)
        self.assertLessEqual({"zenthros", "cloudpickle"}, bundle.sdk_distributions())
        differ, missing = runner._package_drift({"not-installed-anywhere": "1.0"})
        self.assertEqual((differ, missing), ([], ["not-installed-anywhere 1.0"]))


class GitTest(unittest.TestCase):
    def test_what_it_keeps_stays_out_of_git(self):
        with tempfile.TemporaryDirectory() as folder:
            client = L.LocalJobClient(root=folder, engine=mock.Mock())
            client._write("x", {"manifest": {}})
            with open(os.path.join(os.path.realpath(folder), ".zenthros", ".gitignore")) as handle:
                self.assertEqual(handle.read().strip(), "*")


class ReleasingEngine(AnsweringEngine):
    """The agent as it is about a job it let go: its status reads lost
    (WORKER_LOST: statusOf in agent/localclient.go) and a finish brings
    nothing. The first status asked waits until a finish let a job go, or
    a second: a collector that read the record before another collected the
    job asks the agent only after."""

    def __init__(self):
        super().__init__()
        self.released = threading.Event()
        self.asked = 0

    def run_kept(self, args, stdin, keep, timeout=600):
        if json.loads(stdin)["id"] in self.let_go:
            line = json.dumps({"reports": [], "log": ""}) + "\n"
            keep(line)
            return 0, line, ""
        code, out, err = super().run_kept(args, stdin, keep, timeout)
        self.released.set()
        return code, out, err

    def agent(self, container, args, stdin):
        if args[0] == "status":
            self.asked += 1
            if self.asked == 1:
                self.released.wait(1)
            if json.loads(stdin)["id"] in self.let_go:
                lost = "job not found"
                return 0, json.dumps({"phase": FAILED, "message": lost, "reason": "WORKER_LOST",
                                      "reasons": [{"code": "WORKER_LOST", "detail": lost}]}), ""
        return super().agent(container, args, stdin)


class OneCollectorTest(Env):
    """A job's end is collected once. In a pipeline two threads look at one
    step: the call that waits on it, and the next step waiting for it to end
    (_start_after). Both collected its end: the second read the record before
    the first wrote it, asked the agent after the agent let the job go, read
    it lost, and wrote that over the first one's record. The step ended
    Failed, or with no reports, so the next step had no SHA-256 to read its
    result by (test_pipeline_e2e d4 and d9, 2026-11-18)."""

    def setUp(self):
        super().setUp()
        self.engine = ReleasingEngine()
        self.client = L.LocalJobClient(root=self.folder + "/project", engine=self.engine)
        self.client.poll_seconds = 0.01

    def test_an_end_collected_by_its_wait_and_by_another_at_once_keeps_what_it_brought(self):
        self.engine.reports = json.dumps([{"payload": {"a": 1}}])
        self.client.submit(job())
        self.finish()
        other = threading.Thread(target=self.client.job_object, args=("j",))
        other.start()
        time.sleep(0.2)  # it read the record and asks the agent
        status, _ = self.client._status_since("j", "", 1)
        other.join(5)
        record = self.client._read("j")
        self.assertEqual((status.phase, record["status"]["phase"]), (SUCCEEDED, SUCCEEDED))
        self.assertEqual([r["payload"] for r in record["reports"]], [{"a": 1}])
        self.assertEqual(self.engine.execs().count("finish"), 1, "its end was collected twice")

    def test_a_step_cancelled_while_it_waits_for_its_upstream_never_starts(self):
        self.client.submit(job("up"))
        waiting = job("down")
        waiting.spec.depends_on = ["up"]
        self.client.submit(waiting)
        self.client.cancel("down")
        self.finish("up")
        for thread in threading.enumerate():
            if thread.name.endswith("(_start_after)"):
                thread.join(5)
        self.assertEqual(self.engine.jobs, 1, "the cancelled step was sent to the agent")
        self.assertEqual(self.client.job_object("down")["status"]["phase"], "Cancelled")


if __name__ == "__main__":
    unittest.main()
