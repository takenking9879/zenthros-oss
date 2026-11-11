# SPDX-License-Identifier: Apache-2.0

"""Test doubles shared by the tests: the blob store, the agent, zenthros-api
(FakeAPI) and a client that runs calls here (ExecutingClient); and what a
test needs of the system it runs on (sdk-os.yml runs them on Linux, macOS
and Windows)."""

import json
import os
import stat
import sys
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

# What runs on the machine (the runner, its environments, the snapshot, the
# scripts it is given) runs on Linux, as every machine does.
machine_side = unittest.skipUnless(sys.platform == "linux", "the machine's side, which runs on Linux")
# On Windows the SDK runs this computer's jobs (on="local", "local-k8s") in
# WSL2, installed there: jobs.local.WINDOWS_RUNS_IN_WSL says so.
local_in_wsl = unittest.skipIf(os.name == "nt", "on Windows this computer's jobs run in WSL2, the SDK installed there")


def script_fake(tool):
    """For a test whose fake tool is a #! script, which Windows does not run."""
    return unittest.skipIf(os.name == "nt", f"its fake {tool} is a #! script, which Windows does not run")


def assert_mode(test, path, want):
    """path's permission bits are want, where they say who may read it. On
    Windows they only say read-only: scripts/os_smoke.py checks the file's ACL
    there (sdk-os.yml)."""
    if os.name != "nt":
        test.assertEqual(stat.S_IMODE(os.stat(path).st_mode), want, path)


class AckAgent:
    """An AgentClient whose checkpoint commits go to ack(commit)."""

    def __init__(self, ack=None):
        self.acked = []
        self._ack = ack

    def ack_checkpoint(self, commit, timeout=10):
        self.acked.append(commit)
        if self._ack is not None:
            return self._ack(commit)
        return {"accepted": True, "seq": commit["seq"]}


@contextmanager
def job(store, agent=None, env=None, job_id="uid-1", prefix="memory://out", generation="1"):
    """Run inside a job: every Checkpointer made in here (by @phase, Task or a
    trainer) writes to store and acks to agent."""
    from zenthros.core.checkpoint import forget_acked

    forget_acked()
    agent = agent or AckAgent()
    values = {"ZENTHROS_JOB_ID": job_id, "ZENTHROS_GENERATION": generation, "ZENTHROS_CHECKPOINT_PREFIX": prefix}
    values.update(env or {})
    values = {k: v for k, v in values.items() if v is not None}
    with mock.patch.dict(os.environ, values, clear=False), \
            mock.patch("zenthros.core.checkpoint.store_for", lambda uri="", env=None: store), \
            mock.patch("zenthros.core.checkpoint.AgentClient", lambda env=None: agent):
        yield agent


@contextmanager
def no_job():
    """Outside a job: the variables the agent sets are unset."""
    from zenthros.core.env import JOB_VARS

    with mock.patch.dict(os.environ, {}, clear=False):
        for name in JOB_VARS:
            os.environ.pop(name, None)
        yield


class ReportAgent:
    """The agent's report route over HTTP, for a runner in this process or
    another: each snapshot POSTed to /v1/reports joins its job's reports in
    client (a FakeJobClient), as the controller would keep it.

        with ReportAgent(client) as endpoint:
            env["ZENTHROS_REPORT_ENDPOINT"] = endpoint
    """

    def __init__(self, client, job_id=""):
        self.client = client
        self.job_id = job_id
        self.snapshots = []

    def __enter__(self):
        import json
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        from zenthros.jobs.handle import ReportView

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                outer.snapshots.append(body)
                job = outer.client._jobs.get(body.get("job_id") or outer.job_id)
                if job is not None:
                    job["reports"].append(ReportView(
                        job_id=body.get("job_id") or outer.job_id, producer_id=body.get("producer_id", ""),
                        payload=body.get("payload", {}), artifacts=body.get("artifacts", []),
                        final=body.get("final", False), produced_at=body.get("produced_at", ""),
                        report_id=body.get("report_id", ""), sequence=len(job["reports"]) + 1))
                raw = json.dumps({"accepted": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()


@contextmanager
def reporting(client, job_id):
    """A runner called in this process reports to client's job job_id, as
    its agent would: result() then finds the digests the runner reported."""
    with ReportAgent(client, job_id) as endpoint, \
            mock.patch.dict(os.environ, {"ZENTHROS_REPORT_ENDPOINT": endpoint, "ZENTHROS_JOB_ID": job_id}):
        yield endpoint



def execute(uri, store, **kwargs):
    """runner.execute as a machine runs it: the spec pins the SHA-256 of
    call.pkl as submit stored it (or as the test rewrote it)."""
    import hashlib

    from zenthros.remote import runner

    with mock.patch.dict(os.environ, {runner.CALL_SHA256_ENV: hashlib.sha256(store.get(uri)).hexdigest()}):
        return runner.execute(uri, blob_store=store, **kwargs)


def status(code, message, reason="Forbidden"):
    return code, {"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "reason": reason,
                  "message": message}


# The routes zenthros-api has under a job, by method and what follows
# /jobs/NAME, as routes.json (pkg/api TestEachRouteTakesItsSamplesRequest)
# holds them.
_JOB = "/v1/workspaces/{ws}/jobs/{name}"
JOB_ROUTES = {(route.split(" ")[0], route.split(" ")[1][len(_JOB):])
              for route in json.loads((Path(__file__).resolve().parents[3] / "operator" / "pkg" / "api" / "testdata"
                                       / "wire" / "routes.json").read_text())["routes"]
              if route.split(" ")[1].startswith(_JOB)}


def not_there(method, path):
    """What zenthros-api answers a request the table does not route: on a
    route of a job, the job is not there, as Kubernetes and the Gateway say
    it (the Status names a zenthrosjob); else a 404 that names nothing."""
    code, body = status(404, f"no route {(method, path)}", "NotFound")
    parts = path.split("?")[0].split("/")
    if (len(parts) in (6, 7) and parts[1:3] == ["v1", "workspaces"] and parts[4] == "jobs"
            and (method, "/" + parts[6] if len(parts) == 7 else "") in JOB_ROUTES):
        body["details"] = {"name": parts[5], "group": "zenthros.ai", "kind": "zenthrosjobs"}
    return code, body


JOB_J = "/v1/workspaces/risk/jobs/j"


class FakeAPI:
    """Answers per (method, path) from a table; records every request."""

    def __init__(self):
        self.requests = []
        self.routes = {}
        self.phases = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                outer.requests.append({"method": self.command, "path": self.path,
                                       "auth": self.headers.get("Authorization"), "body": body,
                                       "headers": dict(self.headers)})
                key = (self.command, self.path.split("?")[0])
                if key[0] == "GET" and key[1] in (JOB_J, JOB_J + "/wait") and outer.phases:
                    # The job's next phase: as GET answers it, or as a wait
                    # does, with the token of that state.
                    phase = outer.phases.pop(0)
                    job = {"metadata": {"name": "j", "generation": 1},
                           "status": {"phase": phase, "message": f"job is {phase}"}}
                    answer = (200, {"job": job, "seen": phase} if key[1].endswith("/wait") else job)
                else:
                    answer = outer.routes.get(key, not_there(*key))
                    if callable(answer):
                        answer = answer(body)
                code, payload, *headers = answer
                raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "text/plain" if isinstance(payload, bytes) else "application/json")
                self.send_header("Content-Length", str(len(raw)))
                for name, value in (headers[0] if headers else {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(raw)

            do_GET = do_POST = do_DELETE = do_PUT = _answer

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
