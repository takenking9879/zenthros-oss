#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""What local owes a first user that its transport's own tests do not cover:
secrets that never rest in clear, a command's retries as on a machine, what
`zenthros clean` removes, the row of targets, and a job list with zctl around."""
import io
import json
import os
import unittest
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import cli, errors, seal, targetlist, tools
from zenthros.core.storage import FileBlobStore
from zenthros.jobs import local as L
from zenthros.jobs.client import ZctlJobClient
from zenthros.jobs.handle import FAILED
from zenthros.remote import envs, options, runner

from support import local_in_wsl
from test_local_client import Env, job
from test_seal import keypair


def S_with_deadline():
    made = job()
    made.spec.active_deadline_seconds = 5
    return made


@unittest.skipUnless(seal.available(), "pip install 'zenthros[seal]'")
class SealedHereTest(Env):
    def setUp(self):
        super().setUp()
        _, self.recipient = keypair("zenthros-seal-fixture-identity")
        info = json.dumps({"ageRecipients": [self.recipient]})
        real = self.engine.agent
        self.engine.agent = lambda c, a, s: (0, info, "") if a[0] == "info" else real(c, a, s)
        patcher = mock.patch.object(options, "local_client", return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

    def everything_written(self):
        return b"".join(p.read_bytes() for p in Path(self.client.root).rglob("*") if p.is_file())

    def test_a_local_secret_reaches_the_agent_sealed_and_never_rests_in_clear(self):
        secret = zenthros.Secret.encrypted("s3cr3t-value", on="local")
        self.client.seal_at_start("j", {"API_KEY": secret})
        self.client.submit(job())
        source = self.engine.submitted["spec"]["workload"]["template"]["secretSources"][0]
        self.assertEqual(source["type"], "encrypted-payload")
        self.assertEqual(source["encryptedPayload"]["recipients"], [self.recipient])
        self.assertEqual(source["materialization"]["envName"], "API_KEY")
        self.assertNotIn(b"s3cr3t-value", self.everything_written())
        self.assertNotIn("s3cr3t-value", json.dumps(self.engine.calls))

    def test_a_local_secret_is_refused_anywhere_else(self):
        secret = zenthros.Secret.encrypted("s3cr3t-value", on="local")
        with self.assertRaisesRegex(errors.ValidationError, "only on this computer"):
            options.secret_sources({"API_KEY": secret})


class CommandRetriesTest(Env):
    def failed(self):
        return {"phase": FAILED, "reasons": [{"code": "PROCESS_FAILED"}], "message": "exit 3"}

    def retrying(self):
        from zenthros.jobs import spec as S

        return job(source=S.Source(inline="Y29kZQ=="), restart_policy=S.RestartPolicy(backoff_limit=1))

    def test_a_failed_command_runs_again_as_on_a_machine(self):
        self.client.submit(self.retrying())
        self.engine.statuses[self.client._read("j")["local"]["id"]] = self.failed()
        self.assertEqual(self.client.job_object("j")["status"]["phase"], "Running")
        self.assertEqual(self.engine.jobs, 2)
        status = self.engine.submitted["status"]
        self.assertEqual((status["restarts"], status["runtime"]), (1, "process"), "the agent's next attempt: a<n>")
        self.assertEqual(self.engine.submitted["spec"]["workload"]["template"]["source"]["inline"], "Y29kZQ==")

    def test_once_they_are_spent_it_fails_for_good(self):
        self.client.submit(self.retrying())
        self.engine.statuses[self.client._read("j")["local"]["id"]] = self.failed()
        self.client.job_object("j")
        self.engine.statuses[self.client._read("j")["local"]["id"]] = self.failed()
        self.assertEqual(self.client.job_object("j")["status"]["phase"], FAILED)
        self.assertEqual(self.engine.jobs, 2)

    def test_what_the_process_did_not_fail_is_not_run_again(self):
        self.client.submit(self.retrying())
        self.engine.statuses[self.client._read("j")["local"]["id"]] = {"phase": FAILED, "reasons": [{"code": "OOM_KILLED"}]}
        self.assertEqual(self.client.job_object("j")["status"]["phase"], FAILED)
        self.assertEqual(self.engine.jobs, 1)

    def test_a_job_with_no_retries_does_not_keep_its_code(self):
        from zenthros.jobs import spec as S

        self.client.submit(job(source=S.Source(inline="Y29kZQ==")))
        self.assertNotIn("inline", self.client._read("j")["manifest"]["spec"]["workload"]["template"]["source"])


class FailuresTest(Env):
    def test_the_agent_killed_for_memory_is_the_jobs_memory(self):
        self.client.submit(job())
        name = self.client._read("j")["container"]
        self.engine.containers[name]["state"] = "exited"
        self.engine.containers[name]["oom"] = "true"
        status = self.client.job_object("j")["status"]
        self.assertEqual(status["reasons"][0]["code"], "OOM_KILLED")
        self.assertIn("raise memory=", status["message"])

    def test_no_network_says_so(self):
        self.client.submit(job())
        self.finish(phase=FAILED, message="Temporary failure in name resolution: pypi.org")
        self.assertIn("run this once with network", self.client.job_object("j")["status"]["message"])

    def test_a_child_killed_for_memory_may_be_why(self):
        self.client.submit(job())
        self.engine.containers[self.client._read("j")["container"]]["oom"] = "true"
        self.finish(phase=FAILED, message="process exited with code 1")
        self.assertIn("raise memory=", self.client.job_object("j")["status"]["message"])

    def test_the_output_is_a_folder_a_script_can_open(self):
        path = self.client.output_for("j")
        self.assertTrue(os.path.isdir(path))
        self.assertFalse(path.startswith("file:"))


class TimeLimitTest(Env):
    def test_a_service_the_agent_stopped_says_stop_after(self):
        from zenthros.jobs import spec as S

        self.client.submit(job(service=S.Service(ports=[S.ServicePort(name="http", port=8000)], stop_after_seconds=60)))
        self.finish(phase="Cancelled")
        status = self.client.job_object("j")["status"]
        self.assertEqual((status["phase"], status["reason"]), ("Cancelled", "STOP_AFTER"))

    def test_a_job_the_agent_stopped_ran_past_its_timeout(self):
        self.client.submit(S_with_deadline())
        self.finish(phase="Cancelled")
        status = self.client.job_object("j")["status"]
        self.assertEqual((status["phase"], status["reason"]), (FAILED, "DEADLINE_EXCEEDED"))


class LogFollowerTest(unittest.TestCase):
    def test_a_job_that_ended_before_any_stream_still_prints(self):
        from zenthros.remote.call import _LogFollower

        client = mock.Mock(follow_logs=mock.Mock(return_value=None), logs=mock.Mock(return_value="hola\n"))
        out = io.StringIO()
        follower = _LogFollower(client, "j", poll_seconds=0.01, out=out)
        follower.start()
        follower.stop()
        self.assertEqual(out.getvalue(), "hola\n")


@local_in_wsl
class CleanTest(Env):
    def old_job_folder(self, name):
        folder = Path(self.client.root) / "store" / name
        folder.mkdir(parents=True)
        (folder / "result.pkl").write_bytes(b"x")
        return folder

    def test_results_go_only_when_their_job_is_gone(self):
        self.client.submit(job(name="kept"))
        self.finish("kept")
        self.client.job_object("kept")
        ours = self.old_job_folder("kept")
        orphan = self.old_job_folder("old-job")
        self.assertEqual(self.client.clean_results(dry_run=True), ["old-job"])
        self.assertTrue(orphan.exists())
        self.assertEqual(self.client.clean_results(), ["old-job"])
        self.assertFalse(orphan.exists())
        self.assertTrue(ours.exists())

    def test_what_a_job_left_stays_unless_named(self):
        """Never lose data by default: its call and result go, its outputs stay."""
        folder = self.old_job_folder("old-job")
        (folder / "model.onnx").write_bytes(b"m")
        self.assertEqual(self.client.clean_results(), ["old-job"])
        self.assertFalse((folder / "result.pkl").exists())
        self.assertTrue((folder / "model.onnx").exists())
        self.assertEqual(self.client.clean_results(), [])
        self.assertEqual(self.client.clean_results(outputs=True), ["old-job"])
        self.assertFalse(folder.exists())

    def test_a_good_entry_keeps_what_it_names_and_a_forged_one_nothing(self):
        from zenthros import cache as entries

        folder = self.old_job_folder("old-job")
        forged = self.old_job_folder("forged-job")
        home = entries.Home(self.client.store, FileBlobStore())
        body = {"key": "a" * 64, "result": {"uri": f"{self.client.store}/old-job/result.pkl", "sha256": "0"}, "outs": []}
        home.store.put(home.manifest_uri("a" * 64), json.dumps(entries.signed(body)).encode())
        bad = {"key": "b" * 64, "result": {"uri": f"{self.client.store}/forged-job/result.pkl", "sha256": "0"}, "outs": [],
               "signature": "hmac-sha256:" + "0" * 64}
        home.store.put(home.manifest_uri("b" * 64), json.dumps(bad).encode())
        self.assertEqual(self.client.clean_results(outputs=True), ["forged-job"])
        self.assertTrue((folder / "result.pkl").exists())
        self.assertFalse(forged.exists())

    def test_the_code_of_the_store_is_not_a_job(self):
        self.old_job_folder("code")
        self.assertEqual(self.client.clean_results(), [])

    def test_a_dry_run_removes_no_container(self):
        self.client.submit(job())
        self.finish()
        self.client.job_object("j")
        self.client.containers = lambda: [("zenthros-local-x", "running")]
        self.assertEqual(self.client.clean(dry_run=True), ["zenthros-local-x"])
        self.assertFalse([c for c in self.engine.calls if c[0] == "rm"])

    def test_the_command_says_what_went(self):
        out = io.StringIO()
        with mock.patch.object(options, "local_client", return_value=self.client):
            self.old_job_folder("old-job")
            self.assertEqual(cli.main(["clean", "--results", "--dry-run"], out=out), 0)
        self.assertIn("old-job", out.getvalue())
        self.assertIn("would go", out.getvalue())


class EnginesTest(unittest.TestCase):
    """One line per engine: what is not the same between them."""

    def test_docker(self):
        engine = L.ENGINES["docker"]("/d")
        self.assertEqual(engine.capacity_format, "{{.NCPU}} {{.MemTotal}}")
        self.assertEqual(engine.gpu_flags, ("--gpus", "all"))
        self.assertEqual(engine.image_name("python:3.13"), "python:3.13", "Docker Hub is its default")

    def test_podman(self):
        engine = L.ENGINES["podman"]("/p")
        self.assertEqual(engine.capacity_format, "{{.Host.CPUs}} {{.Host.MemTotal}}")
        self.assertEqual(engine.gpu_flags, ("--device", "nvidia.com/gpu=all"))

    def test_podman_takes_no_short_name_for_granted(self):
        name = L.ENGINES["podman"]("/p").image_name
        self.assertEqual(name("python:3.13"), "docker.io/library/python:3.13")
        self.assertEqual(name("me/geo:3.9"), "docker.io/me/geo:3.9")
        self.assertEqual(name("ghcr.io/me/geo:3.9"), "ghcr.io/me/geo:3.9")
        self.assertEqual(name("localhost/env:1"), "localhost/env:1")

    def test_the_capacity_is_asked_in_each_engines_words(self):
        from zenthros import machines

        with mock.patch.object(machines, "_docker", return_value=(0, "8 1024")) as asked:
            self.assertEqual(machines.docker_capacity("/p/podman", "{{.Host.CPUs}} {{.Host.MemTotal}}"), (8, 1024))
        self.assertIn("{{.Host.CPUs}} {{.Host.MemTotal}}", asked.call_args.args[1])

    def test_an_engine_that_is_off_is_named(self):
        from zenthros import machines

        with mock.patch.object(machines, "_docker", return_value=(1, "cannot connect")):
            with self.assertRaisesRegex(errors.ValidationError, "podman is not running here"):
                machines.docker_capacity("/usr/bin/podman")


class CliJsonTest(Env):
    def test_targets_answer_in_json(self):
        out = io.StringIO()
        with mock.patch.object(tools, "targets", return_value=[{"name": "local"}]):
            self.assertEqual(cli.main(["targets", "--json"], out=out), 0)
        self.assertEqual(json.loads(out.getvalue())[0]["name"], "local")


class TargetsTest(Env):
    def test_local_shows_the_size_a_job_gets(self):
        with mock.patch("zenthros.machines.docker_capacity", return_value=(24, 32 << 30)), \
                mock.patch.object(targetlist, "_local_containers", return_value={}), \
                mock.patch.dict(os.environ, {"ZENTHROS_LOCAL_CPUS": "", "ZENTHROS_LOCAL_MEMORY": ""}):
            row, _ = targetlist.local_target()
        self.assertEqual((row.milli_cpu, row.memory_bytes), (24000, 24 << 30))
        self.assertEqual(row.state, "Available")


class JobListTest(unittest.TestCase):
    def test_zctl_lists_jobs_like_the_other_transports(self):
        calls = []

        def zctl(*args, **_):
            calls.append(args)
            return json.dumps({"items": [{"metadata": {"name": "a"}}]})

        client = ZctlJobClient(zctl=zctl, namespace="tenant-default", poll_seconds=0)
        client._run = lambda args, **_: zctl(*args)
        self.assertEqual([o["metadata"]["name"] for o in client.list_jobs("k=v")], ["a"])
        self.assertEqual(calls[0][:2], ("get", "zenthrosjob"))
        self.assertIn("k=v", calls[0])


class RunnerMessageTest(unittest.TestCase):
    def test_a_runner_already_made_says_nothing(self):
        out = io.StringIO()
        with mock.patch.dict(os.environ, {runner.RUNNER_PACKAGES_ENV: "notapackage==1"}), \
                mock.patch.object(envs, "runner_env", return_value="/x/python") as made, \
                mock.patch.object(runner, "_reexec"):
            os.environ.pop(runner.RUNNER_READY_ENV, None)
            runner.ensure_runner(["exec", "u"], out=out)
            os.environ.pop(runner.RUNNER_READY_ENV, None)
        made.assert_called_once()
        self.assertEqual(out.getvalue(), "", "only making it is news; runner_env says so when it does")


if __name__ == "__main__":
    unittest.main()
