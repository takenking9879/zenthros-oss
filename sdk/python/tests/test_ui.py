# SPDX-License-Identifier: Apache-2.0

"""zenthros ui: what --json says, on a page, for this computer only. It reads and
keeps nothing; zenthros agent skill: what an agent needs to know, in one file."""

import io
import json
import os
import re
import socket
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

from zenthros import cli, errors, mcp, tools, ui


class Base(unittest.TestCase):
    def setUp(self):
        self.server = ui.serve(port=0)
        self.addCleanup(self.server.stop)

    def ask(self, path, token=None, host=None, method="GET"):
        request = urllib.request.Request(f"http://127.0.0.1:{self.server.port}{path}", method=method)
        request.add_header(ui.TOKEN_HEADER, self.server.token if token is None else token)
        if host:
            request.add_header("Host", host)
        try:
            with urllib.request.urlopen(request, timeout=10) as answer:
                return answer.status, answer.read(), answer.headers
        except urllib.error.HTTPError as refused:
            return refused.code, refused.read(), refused.headers


class Answers(Base):
    def same_as(self, path, function, sample, **called):
        with mock.patch.object(tools, function, return_value=sample) as asked:
            status, body, headers = self.ask(path)
        self.assertEqual(status, 200, body)
        self.assertEqual(json.loads(body), sample)
        self.assertEqual(headers["Content-Type"], "application/json")
        asked.assert_called_once_with(**called)

    def test_each_answer_is_what_json_says(self):
        self.same_as("/api/targets", "targets", [{"name": "local"}])
        self.same_as("/api/jobs?limit=7&cursor=2", "jobs", {"jobs": [], "total": 0}, limit=7, cursor=2)
        self.same_as("/api/environments", "environments", {"environments": []})
        self.same_as("/api/cache", "cache_entries", {"entries": []})

    def test_a_job_and_its_log(self):
        with mock.patch.object(tools, "status", return_value={"job": "a b"}) as asked:
            self.assertEqual(self.ask("/api/jobs/a%20b")[0], 200)
        asked.assert_called_once_with("a b")
        with mock.patch.object(tools, "logs", return_value={"lines": []}) as asked:
            self.assertEqual(self.ask("/api/jobs/a/logs?grep=x&tail=9&limit=3")[0], 200)
        asked.assert_called_once_with("a", tail=9, grep="x", limit=3)

    def test_what_goes_wrong_is_said_with_no_page_of_python(self):
        with mock.patch.object(tools, "status", side_effect=errors.JobNotFound("no job named x")):
            status, body, _ = self.ask("/api/jobs/x")
        self.assertEqual((status, json.loads(body)), (404, {"error": "no job named x"}))
        with mock.patch.object(tools, "jobs", side_effect=errors.TransientError("the hub did not answer")):
            status, body, _ = self.ask("/api/jobs")
        self.assertEqual((status, json.loads(body)), (502, {"error": "the hub did not answer"}))
        self.assertEqual(self.ask("/api/nothing")[0], 404)
        self.assertEqual(self.ask("/api/jobs?limit=x")[0], 400)


class OnlyForYou(Base):
    def test_it_listens_on_this_computer_only(self):
        self.assertEqual(self.server.address, "127.0.0.1")

    def test_no_token_no_answer(self):
        for path in ("/", "/api/jobs"):
            self.assertEqual(self.ask(path, token="")[0], 403)
            self.assertEqual(self.ask(path, token="not-it")[0], 403)
        with mock.patch.object(tools, "jobs", side_effect=AssertionError("asked before the token")):
            self.assertEqual(self.ask("/api/jobs", token="")[0], 403)

    def test_a_name_that_points_here_from_elsewhere_gets_nothing(self):
        # A page of another site, through its own name for this address.
        self.assertEqual(self.ask("/api/jobs", host="evil.example")[0], 403)

    def test_the_first_address_has_the_token_and_the_page_drops_it(self):
        self.assertIn(f"token={self.server.token}", self.server.url)
        status, body, headers = self.ask(f"/?token={self.server.token}", token="")
        self.assertEqual(status, 200)
        self.assertIn("history.replaceState", body.decode())
        self.assertEqual(headers["Referrer-Policy"], "no-referrer")
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        self.assertEqual(headers["X-Frame-Options"], "DENY")

    def test_it_only_reads(self):
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            self.assertEqual(self.ask("/api/jobs/a", method=method)[0], 405)

    def test_a_log_can_put_nothing_on_the_page(self):
        page = self.ask("/")[1].decode()
        self.assertNotIn("innerHTML", page, "what a job prints reaches the page as text only")
        self.assertNotIn("eval(", page)
        self.assertEqual(re.findall(r"""(?:src|href)=["']https?:""", page), [], "nothing from outside")

    def test_it_keeps_nothing(self):
        with tempfile.TemporaryDirectory() as folder:
            cwd = os.getcwd()
            os.chdir(folder)
            try:
                with mock.patch.object(tools, "jobs", return_value={"jobs": []}):
                    self.ask("/")
                    self.ask("/api/jobs")
            finally:
                os.chdir(cwd)  # before the folder goes: Windows removes no folder a process is in
            self.assertEqual(list(Path(folder).iterdir()), [])


class Command(unittest.TestCase):
    def test_it_says_where_and_stops_when_asked(self):
        out = io.StringIO()
        with mock.patch.object(ui.Server, "wait", side_effect=KeyboardInterrupt), mock.patch("webbrowser.open") as opened:
            self.assertEqual(cli.main(["ui", "--no-open"], out=out), 130)
        self.assertRegex(out.getvalue(), r"http://127\.0\.0\.1:\d+/\?token=")
        opened.assert_not_called()

    def test_a_port_in_use_says_so(self):
        with socket.socket() as held:
            held.bind(("127.0.0.1", 0))
            held.listen()
            out = io.StringIO()
            self.assertEqual(cli.main(["ui", "--no-open", "--port", str(held.getsockname()[1])], out=out), 1)
        self.assertIn("--port", out.getvalue())

    def test_jobs_show_the_pipeline_they_are_of(self):
        from zenthros.pipeline import PIPELINE_LABEL, STEP_LABEL

        job = {"metadata": {"name": "churn-train-1", "labels": {PIPELINE_LABEL: "churn", STEP_LABEL: "train"}}, "status": {"phase": "Running"}}
        client = mock.Mock()
        client.list_jobs.return_value = [job]
        with mock.patch.object(tools, "job_client", return_value=client), mock.patch.object(tools, "local_clients", return_value=[]):
            row = tools.jobs()["jobs"][0]
        self.assertEqual((row["pipeline"], row["step"]), ("churn", "train"))


class Skill(unittest.TestCase):
    def test_it_says_when_to_use_it(self):
        text = ui.skill_text()
        head = re.match(r"---\nname: zenthros\ndescription: (.+)\n---\n", text)
        self.assertTrue(head, text[:200])
        self.assertLess(len(head.group(1)), 1024, "what an agent reads to decide")

    def test_it_names_only_what_there_is(self):
        text = ui.skill_text()
        for tool in mcp.TOOLS:
            self.assertIn(f"`{tool['name']}`", text)
        commands = cli.parser()._subparsers._group_actions[0].choices
        for command in re.findall(r"^\s*zenthros ([a-z]+)", text, re.M):
            self.assertIn(command, commands, command)

    def test_it_goes_in_the_project_for_your_agents(self):
        with tempfile.TemporaryDirectory() as folder:
            out = io.StringIO()
            self.assertEqual(cli.main(["agent", "skill", "--to", folder], out=out), 0)
            written = Path(folder) / "zenthros" / "SKILL.md"
            self.assertEqual(written.read_text(), ui.skill_text())
            self.assertIn(str(written), out.getvalue())


class Dictionaries(unittest.TestCase):
    def test_a_machine_and_a_job_as_a_script_reads_them(self):
        from zenthros.machines import Machine
        from zenthros.view import JobInfo

        self.assertEqual(Machine("m", pool="p").to_dict()["pool"], "p")
        json.dumps(Machine("m").to_dict())
        job = JobInfo.from_api({"metadata": {"name": "j"}, "status": {"phase": "Running"}})
        self.assertEqual((job.to_dict()["name"], job.to_dict()["phase"]), ("j", "Running"))
        json.dumps(job.to_dict())
