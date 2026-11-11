#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""What an agent and --json read: short, stable dictionaries, one source for
the CLI and MCP. A paid target asks for your permission first; an agent stops
only what its own session started."""
import io
import json
import os
import subprocess
import tempfile
import types
import unittest
from unittest import mock

from zenthros import agents, cli, config, errors, mcp, remote, tools
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import JobStatus, status_from_object
from zenthros.remote import runner, snapshot
from zenthros.targetlist import Target, Targets

from support import assert_mode

STORE = "s3://bucket/calls"


def git(root, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=root, check=True, capture_output=True)


def paid(name="gcp-lab", rate=0.4):
    return Target(name=name, kind="Template", state="Ready", available=1, total=1, milli_cpu=8000, memory_bytes=1 << 34,
                  price={"hourlyRate": rate, "currency": "USD"}, detail={"untrustedHost": False})


def free(name="workstation"):
    return Target(name=name, kind="External", state="Ready", available=1, total=1, milli_cpu=4000, memory_bytes=1 << 33)


class Base(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = folder.name
        patch = mock.patch.dict(os.environ, {config.CONFIG_ENV: os.path.join(self.root, "conf", "config.yaml")})
        patch.start()
        self.addCleanup(patch.stop)
        self.repo = os.path.join(self.root, "repo")
        os.makedirs(self.repo)
        git(self.repo, "init", "-q")
        with open(os.path.join(self.repo, "a.py"), "w") as handle:
            handle.write("print('a')\n")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-qm", "base")
        self.client, self.store = FakeJobClient(), MemoryBlobStore()
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store)
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        rows = mock.patch("zenthros.targetlist.targets", lambda *a, **k: Targets([paid(), free(), paid("vast-4090", 0.2)]))
        rows.start()
        self.addCleanup(rows.stop)
        place = mock.patch("zenthros.machines._place", lambda on, options, **k: types.SimpleNamespace(
            options={"pool": on}, made=None, name=on))
        place.start()
        self.addCleanup(place.stop)


class ExitCodeTest(unittest.TestCase):
    def test_the_status_reads_it(self):
        self.assertEqual(status_from_object({"status": {"phase": "Failed", "exitCode": 7}}).exit_code, 7)
        self.assertEqual(status_from_object({"status": {"phase": "Succeeded", "exitCode": 0}}).exit_code, 0)
        self.assertIsNone(status_from_object({"status": {"phase": "Failed"}}).exit_code)
        self.assertIsNone(JobStatus().exit_code)


class DictTest(Base):
    def test_a_target_says_what_an_agent_chooses_by(self):
        row = paid().to_dict()
        self.assertEqual(row["name"], "gcp-lab")
        self.assertEqual(row["price_per_hour"], 0.4)
        self.assertEqual(row["cpu"], 8)
        self.assertTrue(row["costs"])
        self.assertFalse(free().to_dict()["costs"])
        json.dumps(row)

    def test_targets_say_if_the_agent_may_use_them(self):
        agents.allow("gcp-lab", max_machines=2)
        rows = {row["name"]: row for row in tools.targets()}
        self.assertTrue(rows["gcp-lab"]["allowed"])
        self.assertFalse(rows["vast-4090"]["allowed"])
        self.assertEqual(rows["vast-4090"]["to_allow"], "zenthros agent allow vast-4090 --max-machines 3",
                         "an agent told no has to be able to say how to get a yes")
        self.assertNotIn("to_allow", rows["gcp-lab"])
        self.assertTrue(rows["workstation"]["allowed"], "nothing to pay")


class NoAccountTest(unittest.TestCase):
    def test_with_no_account_this_computer_is_the_one_target(self):
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch.dict(os.environ, {config.CONFIG_ENV: os.path.join(folder, "none")}, clear=False), \
                mock.patch("zenthros.jobs.localk8s.current_context", return_value=""), \
                mock.patch("zenthros.targetlist.local_target", lambda *a, **k: (Target(name="local", kind="Local", state="Ready"), set())):
            os.environ.pop("ZENTHROS_SERVER", None)
            os.environ.pop("ZENTHROS_API_KEY", None)
            rows = tools.targets()
        self.assertEqual([row["name"] for row in rows], ["local"])
        self.assertTrue(rows[0]["allowed"])
        self.assertFalse(rows[0]["costs"])


class RunTest(Base):
    def test_a_free_target_needs_no_permission_and_it_does_not_wait(self):
        got = tools.run(["make", "e2e"], on="workstation", code_dir=self.repo, session="s1")
        self.assertEqual(self.client.job_object(got["job"])["metadata"]["labels"][agents.SESSION], "s1")
        self.assertEqual(len(got["code"]), 64)
        self.assertEqual(len(got["base"]), 40)
        self.assertFalse(got["dirty"])
        self.assertEqual(got["state"], "queued")

    def test_a_snapshot_is_the_default_for_an_agent(self):
        got = tools.run(["make", "e2e"], on="workstation", code_dir=self.repo, session="s1")
        notes = self.client.job_object(got["job"])["metadata"]["annotations"]
        self.assertEqual(notes[snapshot.CODE_SHA256], got["code"])

    def test_a_paid_target_without_permission_is_refused_with_its_price(self):
        with self.assertRaises(errors.ValidationError) as caught:
            tools.run(["make"], on="gcp-lab", code_dir=self.repo, session="s1")
        said = str(caught.exception)
        self.assertIn("$0.4", said)
        self.assertIn("zenthros agent allow gcp-lab", said)
        self.assertEqual(self.client.list_jobs(), [], "nothing was created")

    def test_a_person_without_a_session_is_not_asked(self):
        got = tools.run(["make"], on="gcp-lab", code_dir=self.repo)
        self.assertTrue(got["job"])

    def test_the_cap_counts_what_runs_there(self):
        agents.allow("gcp-lab", max_machines=1)
        tools.run(["make"], on="gcp-lab", code_dir=self.repo, session="s1")
        with self.assertRaises(errors.ValidationError) as caught:
            tools.run(["make"], on="gcp-lab", code_dir=self.repo, session="s2")
        self.assertIn("1 job", str(caught.exception), "it counts jobs under way, waiting ones too, not machines")

    def test_a_cap_is_for_machines_of_that_target_only(self):
        agents.allow("gcp-lab", max_machines=1)
        agents.allow("vast-4090", max_machines=1)
        tools.run(["make"], on="gcp-lab", code_dir=self.repo, session="s1")
        self.assertTrue(tools.run(["make"], on="vast-4090", code_dir=self.repo, session="s1")["job"])

    def test_a_third_partys_host_is_said(self):
        row = paid("vast-4090")
        row.detail = {"untrustedHost": True}
        agents.allow("vast-4090")
        with mock.patch("zenthros.targetlist.targets", lambda *a, **k: Targets([row])):
            got = tools.run(["make"], on="vast-4090", code_dir=self.repo, session="s1")
        self.assertTrue(any("owner" in warning for warning in got["warnings"]))

    def test_an_agents_run_has_a_time_limit(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        spec = self.client.job_object(got["job"])["spec"]
        self.assertEqual(spec["activeDeadlineSeconds"], tools.AGENT_TIMEOUT)

    def test_an_agents_job_outlasts_the_hour(self):
        """The default hour took the job, and its result, while the agent was
        still at work on its fix (E2E 2026-11-02)."""
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        spec = self.client.job_object(got["job"])["spec"]
        self.assertEqual(spec["gcPolicy"]["ttlSecondsAfterFinished"], 7 * 24 * 3600)


class StatusTest(Base):
    def job(self, phase="Failed", **status):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        obj = self.client.job_object(got["job"])
        obj["status"] = {"phase": phase, **status}
        with mock.patch.object(self.client, "job_object", return_value=obj):
            return tools.status(got["job"])

    def test_a_failed_test_says_its_exit_code(self):
        got = self.job(exitCode=1, reasons=[{"code": "PROCESS_FAILED", "detail": "x"}],
                       startTime="2026-11-02T10:00:00Z", completionTime="2026-11-02T10:18:40Z")
        self.assertEqual((got["state"], got["exit_code"], got["reason"], got["took_s"]), ("failed", 1, "PROCESS_FAILED", 1120))
        self.assertEqual(len(got["code"]), 64)

    def test_a_lost_machine_has_no_exit_code(self):
        got = self.job(reasons=[{"code": "WORKER_LOST", "detail": "x"}])
        self.assertIsNone(got["exit_code"])
        self.assertEqual(got["reason"], "WORKER_LOST")

    def test_what_is_in_its_output_is_listed(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        self.store.put(f"{STORE}/{got['job']}/junit.xml", b"<x/>")
        obj = self.client.job_object(got["job"])
        obj["status"] = {"phase": "Failed", "exitCode": 1}
        with mock.patch.object(self.client, "job_object", return_value=obj):
            found = tools.status(got["job"])
        self.assertEqual(found["artifacts"], [f"{STORE}/{got['job']}/junit.xml"])

    def test_what_the_call_itself_wrote_is_not_listed_and_a_jobs_file_of_that_name_is(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        folder = f"{STORE}/{got['job']}/"
        for key in ("call-0123456789abcdef.pkl", "result.pkl", "results/0.pkl", "jobs/uid/gen/1/train/3", "junit.xml", "sub/result.pkl"):
            self.store.put(folder + key, b"x")
        obj = self.client.job_object(got["job"])
        obj["status"] = {"phase": "Failed", "exitCode": 1}
        with mock.patch.object(self.client, "job_object", return_value=obj):
            found = tools.status(got["job"])
        self.assertEqual(found["artifacts"], [folder + "junit.xml", folder + "sub/result.pkl"])


class LogsTest(Base):
    def test_only_the_asked_lines_come_back(self):
        text = "\n".join(["=== RUN a", "--- FAIL: a", "=== RUN b", "--- PASS: b", "--- FAIL: c"])
        with mock.patch.object(self.client, "logs", return_value=text, create=True):
            got = tools.logs("j", grep="--- FAIL")
        self.assertEqual(got["lines"], ["--- FAIL: a", "--- FAIL: c"])
        self.assertEqual(got["total"], 2)
        self.assertIsNone(got["next_cursor"])

    def test_they_come_in_pages(self):
        text = "\n".join(str(i) for i in range(5))
        with mock.patch.object(self.client, "logs", return_value=text, create=True):
            first = tools.logs("j", limit=2)
            second = tools.logs("j", limit=2, cursor=first["next_cursor"])
        self.assertEqual((first["lines"], first["next_cursor"]), (["0", "1"], 2))
        self.assertEqual((second["lines"], second["next_cursor"]), (["2", "3"], 4))

    def test_a_bad_pattern_is_said(self):
        with mock.patch.object(self.client, "logs", return_value="x", create=True), \
                self.assertRaises(errors.ValidationError):
            tools.logs("j", grep="(")

    def gone_machine(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        refusal = errors.TransientError("zenthros-api GET logs: 502: no log channel")
        return got["job"], mock.patch.object(self.client, "logs", side_effect=refusal, create=True)

    def test_a_machine_gone_leaves_the_log_the_job_saved(self):
        """The log channel ends with the machine, minutes after its job
        (E2E 2026-11-02): an agent reads what the job kept among its outputs."""
        job, gone = self.gone_machine()
        self.store.put(f"{STORE}/{job}/{runner.OUTPUT_LOG}", b"=== RUN a\n--- FAIL: a\n--- FAIL: b\nFAIL\n")
        with gone:
            found = tools.logs(job, grep="--- FAIL")
            short = tools.logs(job, tail=1)
        self.assertEqual(found["lines"], ["--- FAIL: a", "--- FAIL: b"])
        self.assertEqual(short["lines"], ["FAIL"])

    def test_with_no_saved_log_the_machines_answer_is_said(self):
        job, gone = self.gone_machine()
        with gone, self.assertRaises(errors.TransientError):
            tools.logs(job)

    def test_the_machines_log_comes_first(self):
        job, _ = self.gone_machine()
        self.store.put(f"{STORE}/{job}/{runner.OUTPUT_LOG}", b"older\n")
        with mock.patch.object(self.client, "logs", return_value="fresh", create=True):
            self.assertEqual(tools.logs(job)["lines"], ["fresh"])


class CancelTest(Base):
    def test_an_agent_cancels_what_its_session_started(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        self.assertEqual(tools.cancel(got["job"], session="s1")["cancelled"], True)

    def test_not_what_another_started(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        with self.assertRaises(errors.ValidationError) as caught:
            tools.cancel(got["job"], session="s2")
        self.assertIn("another", str(caught.exception))
        self.assertNotEqual(self.client.job_status(got["job"]).phase, "Cancelled")

    def test_nor_a_persons_job(self):
        call = remote.run(["make"], on="workstation", code_dir=self.repo)
        with self.assertRaises(errors.ValidationError):
            tools.cancel(call.name, session="s2")

    def test_a_person_cancels_anything(self):
        got = tools.run(["make"], on="workstation", code_dir=self.repo, session="s1")
        self.assertTrue(tools.cancel(got["job"])["cancelled"])


class AllowTest(Base):
    def test_it_lasts_and_it_goes(self):
        agents.allow("gcp-lab", max_machines=3)
        self.assertEqual(agents.allowed(), {"gcp-lab": {"max_machines": 3}})
        agents.deny("gcp-lab")
        self.assertEqual(agents.allowed(), {})

    def test_it_is_yours_alone(self):
        agents.allow("gcp-lab")
        assert_mode(self, agents.path(), 0o600)

    def test_it_is_not_in_the_zenconfig_zctl_reads(self):
        agents.allow("gcp-lab")
        self.assertNotEqual(agents.path(), config.config_path())

    def test_a_bad_file_allows_nothing(self):
        os.makedirs(os.path.dirname(agents.path()), exist_ok=True)
        with open(agents.path(), "w") as handle:
            handle.write("{not json")
        self.assertEqual(agents.allowed(), {})


class CLITest(Base):
    def run_cli(self, *argv):
        out = io.StringIO()
        return cli.main(list(argv), out=out), out.getvalue()

    def test_run_detached_with_json(self):
        code, said = self.run_cli("run", "--on", "workstation", "--code-dir", self.repo, "--snapshot", "--detach", "--json", "--", "make", "e2e")
        self.assertEqual(code, 0)
        got = json.loads(said)
        self.assertEqual(self.client.job_object(got["job"])["spec"]["workload"]["template"]["command"][-2:], ["make", "e2e"])

    def test_jobs_status_logs_cancel_with_json(self):
        _, said = self.run_cli("run", "--on", "workstation", "--code-dir", self.repo, "--detach", "--json", "--", "make")
        name = json.loads(said)["job"]
        code, said = self.run_cli("jobs", "--json")
        self.assertEqual((code, [row["job"] for row in json.loads(said)["jobs"]]), (0, [name]))
        code, said = self.run_cli("status", name, "--json")
        self.assertEqual((code, json.loads(said)["job"]), (0, name))
        with mock.patch.object(self.client, "logs", return_value="a\nb", create=True):
            code, said = self.run_cli("logs", name, "--json", "--grep", "b")
        self.assertEqual((code, json.loads(said)["lines"]), (0, ["b"]))
        code, said = self.run_cli("cancel", name, "--json")
        self.assertEqual((code, json.loads(said)["cancelled"]), (0, True))

    def test_the_job_fails_with_its_own_code(self):
        def waited(*_a, **_k):
            raise errors.JobFailed("no")
        status = {"job": "j", "state": "failed", "exit_code": 7}
        with mock.patch("zenthros.remote.call.RemoteCall.result", waited), mock.patch.object(tools, "status", return_value=status):
            code, _ = self.run_cli("run", "--on", "workstation", "--code-dir", self.repo, "--", "make")
        self.assertEqual(code, 7)

    def test_a_cancelled_one_is_130(self):
        status = {"job": "j", "state": "cancelled", "exit_code": None}
        with mock.patch("zenthros.remote.call.RemoteCall.result", lambda *a, **k: None), mock.patch.object(tools, "status", return_value=status):
            code, _ = self.run_cli("run", "--on", "workstation", "--code-dir", self.repo, "--", "make")
        self.assertEqual(code, 130)

    def test_what_never_started_is_125(self):
        code, said = self.run_cli("run", "--on", "workstation", "--code-dir", self.root, "--snapshot", "--", "make")
        self.assertEqual(code, 125)
        self.assertIn("git", said)

    def test_no_command_is_usage(self):
        code, _ = self.run_cli("run", "--on", "workstation")
        self.assertEqual(code, 2)

    def test_agent_allow(self):
        code, said = self.run_cli("agent", "allow", "gcp-lab", "--max-machines", "3")
        self.assertEqual(code, 0)
        self.assertIn("gcp-lab", said)
        self.assertEqual(agents.allowed()["gcp-lab"]["max_machines"], 3)
        code, said = self.run_cli("agent", "deny", "gcp-lab")
        self.assertEqual(agents.allowed(), {})


class MCPTest(Base):
    def exchange(self, *messages, session="s1"):
        stdin = io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        stdout = io.StringIO()
        mcp.serve(stdin, stdout, session=session)
        return [json.loads(line) for line in stdout.getvalue().splitlines()]

    def call(self, name, arguments, session="s1"):
        answer = self.exchange({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
                               session=session)[0]["result"]
        return answer

    def test_it_says_hello_and_its_five_tools(self):
        hello, listed = self.exchange(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        self.assertEqual(hello["result"]["protocolVersion"], "2025-06-18")
        self.assertEqual(hello["result"]["serverInfo"]["name"], "zenthros")
        self.assertEqual(sorted(t["name"] for t in listed["result"]["tools"]), ["cancel", "logs", "run", "status", "targets"])
        for tool in listed["result"]["tools"]:
            self.assertEqual(tool["inputSchema"]["type"], "object")

    def test_an_unknown_version_gets_the_one_it_speaks(self):
        hello = self.exchange({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "1999-01-01"}})[0]
        self.assertEqual(hello["result"]["protocolVersion"], mcp.PROTOCOL)

    def test_a_notification_is_not_answered(self):
        self.assertEqual(self.exchange({"jsonrpc": "2.0", "method": "notifications/initialized"}), [])

    def test_an_unknown_method_is_an_error(self):
        self.assertEqual(self.exchange({"jsonrpc": "2.0", "id": 9, "method": "nope"})[0]["error"]["code"], -32601)

    def test_a_broken_line_does_not_end_it(self):
        stdout = io.StringIO()
        mcp.serve(io.StringIO("{broken\n" + json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}) + "\n"), stdout, session="s")
        answers = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(answers[0]["error"]["code"], -32700)
        self.assertEqual(answers[1]["result"], {})

    def test_run_goes_with_its_session_and_status_reads_it(self):
        got = self.call("run", {"command": ["make", "e2e"], "on": "workstation", "code_dir": self.repo})
        self.assertFalse(got.get("isError"))
        started = json.loads(got["content"][0]["text"])
        self.assertEqual(self.client.job_object(started["job"])["metadata"]["labels"][agents.SESSION], "s1")
        read = json.loads(self.call("status", {"job": started["job"]})["content"][0]["text"])
        self.assertEqual(read["job"], started["job"])

    def test_a_paid_target_is_refused_as_a_tools_error(self):
        got = self.call("run", {"command": ["make"], "on": "gcp-lab", "code_dir": self.repo})
        self.assertTrue(got["isError"])
        self.assertIn("zenthros agent allow", got["content"][0]["text"])

    def test_another_sessions_job_is_refused(self):
        started = json.loads(self.call("run", {"command": ["make"], "on": "workstation", "code_dir": self.repo})["content"][0]["text"])
        self.assertTrue(self.call("cancel", {"job": started["job"]}, session="s2")["isError"])

    def test_an_unknown_tool_says_so(self):
        answer = self.exchange({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "rm", "arguments": {}}})[0]
        self.assertEqual(answer["error"]["code"], -32602)

    def test_nothing_but_its_answers_goes_to_stdout(self):
        """A print of the SDK on stdout would break the protocol."""
        with mock.patch.object(tools, "targets", lambda: print("noise") or []):
            answers = self.exchange({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "targets", "arguments": {}}})
        self.assertEqual(len(answers), 1)


if __name__ == "__main__":
    unittest.main()
