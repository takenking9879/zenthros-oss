#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Reports, checkpoint commits and child jobs share one POST to the agent:
the same retries, each route's own error type."""

import http.server
import json
import os
import socketserver
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from zenthros import errors
from zenthros.core import agent as agent_mod
from zenthros.core.agent import AgentClient

from support import machine_side

ROUTES = {
    "report": ("/v1/reports", errors.ReportError),
    "ack_checkpoint": ("/v1/checkpoints", errors.CheckpointError),
    "children": ("/v1/children", errors.SpawnRefused),
}


FIXTURE = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "jobcli" / "testdata" / "wire.json"


class Socket:
    def __init__(self, answers):
        self.answers, self.paths = list(answers), []

    def __call__(self, socket_path, path, body, headers, timeout):
        self.paths.append(path)
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Exception):
            raise answer
        status, reply = answer
        return status, json.dumps(reply).encode()


def call(route, answers):
    sock = Socket(answers)
    with mock.patch.object(agent_mod, "_unix_post", sock), mock.patch.object(agent_mod.time, "sleep", lambda _: None):
        client = AgentClient(endpoint="unix:///run/agent.sock", token="t")
        try:
            return getattr(client, route)({"x": 1}), sock
        except Exception as err:  # noqa: BLE001 - returned for the assertions
            return err, sock


class _HangUpOnce(http.server.BaseHTTPRequestHandler):
    """Closes the first request's connection with no answer, then answers 202."""

    calls = 0

    def do_POST(self):
        type(self).calls += 1
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if type(self).calls == 1:
            self.close_connection = True
            return
        body = b'{"accepted":true}'
        self.send_response(202)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def address_string(self):
        return "agent"

    def log_message(self, *args):
        pass


class _Agent:
    def __init__(self, mode):
        self.mode = mode

    def __enter__(self):
        handler = type("Handler", (_HangUpOnce,), {"calls": 0})
        if self.mode == "unix":
            self.dir = tempfile.mkdtemp(prefix="za")
            path = os.path.join(self.dir, "a.sock")
            self.server = socketserver.ThreadingUnixStreamServer(path, handler)
            endpoint = "unix://" + path
        else:
            self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
            endpoint = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return endpoint

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        if self.mode == "unix":
            os.unlink(os.path.join(self.dir, "a.sock"))
            os.rmdir(self.dir)


class AgentClientTest(unittest.TestCase):
    def test_every_route_retries_a_503_the_same_way(self):
        for route, (path, _) in ROUTES.items():
            data, sock = call(route, [(503, {"error": "hub down"}), (503, {}), (200, {"ok": route})])
            self.assertEqual(data, {"ok": route})
            self.assertEqual(sock.paths, [path] * 3, route)

    def test_a_transient_answer_is_retried_for_a_jobs_grace(self):
        """Ten tries 2 s apart and five more 5.4 s apart, the last one 45 s
        after the first: the table `zenthros-agent job` waits by too
        (TestTransientAnswerIsRetriedForAJobsGrace)."""
        retry = json.loads(FIXTURE.read_text(encoding="utf-8"))["retry"]
        self.assertAlmostEqual(sum(retry["waits_seconds"]), retry["total_seconds"])
        self.assertEqual(retry["total_seconds"], 45)
        for route, (path, _) in ROUTES.items():
            sock, waited = Socket([(503, {"error": "hub down"})]), []
            with mock.patch.object(agent_mod, "_unix_post", sock), \
                    mock.patch.object(agent_mod.time, "sleep", waited.append):
                client = AgentClient(endpoint="unix:///run/agent.sock", token="t")
                with self.assertRaises(Exception):
                    getattr(client, route)({"x": 1})
            self.assertEqual(sock.paths, [path] * (len(retry["waits_seconds"]) + 1), route)
            self.assertEqual(waited, retry["waits_seconds"], route)

    def test_every_route_retries_the_agent_restarting(self):
        for route in ROUTES:
            data, sock = call(route, [ConnectionRefusedError("Connection refused"), (200, {"ok": 1})])
            self.assertEqual(data, {"ok": 1}, route)
            self.assertEqual(len(sock.paths), 2)

    def test_a_refusal_is_raised_once_in_the_routes_type(self):
        for route, (_, kind) in ROUTES.items():
            err, sock = call(route, [(409, {"error_code": "LEASE_NOT_LIVE", "error": "lease ended"})])
            self.assertIsInstance(err, kind, route)
            self.assertEqual((err.code, err.message), ("LEASE_NOT_LIVE", "lease ended"))
            self.assertEqual(len(sock.paths), 1)

    def test_a_frozen_cursor_comes_back_typed(self):
        err, _ = call("ack_checkpoint", [(409, {"error_code": "CHECKPOINT_FROZEN", "error": "frozen"})])
        self.assertIsInstance(err, errors.CheckpointFrozen)

    def test_a_call_left_unanswered_is_transient_not_a_refusal(self):
        """Every try transient, or the agent never reached: what was sent may
        have been kept, so it is no refusal of the route (a SpawnRefused says
        nothing was created). It is a TransientError: the caller waits on
        (R4 ronda 5, P2)."""
        for route in ROUTES:
            for answer in ((503, {"error_code": "STORE_UNAVAILABLE", "error": "hub down"}),
                           ConnectionRefusedError("Connection refused")):
                err, _ = call(route, [answer])
                self.assertIsInstance(err, errors.AgentUnanswered, f"{route} {answer}")
                self.assertIsInstance(err, errors.TransientError, f"{route} {answer}")
                self.assertNotIsInstance(err, (errors.SpawnRefused, errors.CheckpointError), f"{route} {answer}")

    def test_what_is_tried_again_is_the_wire_table(self):
        """The table `zenthros-agent job` reads too (TestWhatIsTriedAgainIsTheSDKsTable):
        a 507 SPOOL_FULL is the spool waiting on the controller. Raised at
        once, a job's result digest was lost for good while the hub was down
        (R4 ronda 4, G1)."""
        retry = json.loads(FIXTURE.read_text(encoding="utf-8"))["retry"]
        for route in ROUTES:
            for status in retry["retried_statuses"]:
                data, sock = call(route, [(status, {"error_code": "X", "error": "not now"}), (200, {"ok": 1})])
                self.assertEqual((data, len(sock.paths)), ({"ok": 1}, 2), f"{route} {status}")
            for status in retry["final_statuses"]:
                err, sock = call(route, [(status, {"error_code": "X", "error": "no"})])
                self.assertIsInstance(err, Exception, f"{route} {status}")
                self.assertEqual(len(sock.paths), 1, f"{route} {status}")

    @machine_side
    def test_an_agent_that_closes_without_an_answer_is_tried_again(self):
        """An agent restarting mid-request closes the connection with no
        answer: on a unix socket or over http, it is tried again."""
        for mode in ("unix", "http"):
            with _Agent(mode) as endpoint, mock.patch.object(agent_mod.time, "sleep", lambda _: None):
                data = AgentClient(endpoint=endpoint, token="t").report({"x": 1})
            self.assertEqual(data, {"accepted": True}, mode)

    def test_outside_a_job_each_route_says_so(self):
        for route, (_, kind) in ROUTES.items():
            with self.assertRaises(kind):
                getattr(AgentClient(endpoint="", token=""), route)({})


if __name__ == "__main__":
    unittest.main()
