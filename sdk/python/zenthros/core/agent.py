# SPDX-License-Identifier: Apache-2.0

"""The agent's local API: one POST, retried the same way for every route.

Reports (/v1/reports), checkpoint commits (/v1/checkpoints) and child jobs
(/v1/children) all go to ZENTHROS_REPORT_ENDPOINT, a unix socket on the
machine or an http URL. The agent restarting (any failure to send or to
read the answer), or a transient answer (408, 429, any 5xx: a 507
SPOOL_FULL is the spool waiting on the controller), is retried, for 45 s,
then raised as AgentUnanswered (a TransientError); anything else is the
controller's answer, raised in the route's own error type.
Every route may be sent again: a report and a commit by their seq, a spawn
by its name. `zenthros-agent job` retries by the same table
(operator/pkg/jobcli/testdata/wire.json).
"""

import http.client
import json
import socket
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from zenthros import errors
from zenthros.core.env import JobEnv

# A job insists with its agent for as long as the controller waits for the
# machine itself (45 s): ten tries 2 s apart, to be back at once after a blip,
# and five more 5.4 s apart, the fifteenth 45 s after the first.
# `zenthros-agent job` waits the same; both are tested against one table
# (tests/test_agent.py).
RETRY_WAITS = (2,) * 9 + (5.4,) * 5
ATTEMPTS = len(RETRY_WAITS) + 1
TIMEOUT = 10


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path, timeout=TIMEOUT):
        super().__init__("localhost", timeout=timeout)
        self.unix_path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.unix_path)
        self.sock = sock


class AgentClient:
    """POST JSON to the agent. endpoint and token default to the job env."""

    def __init__(self, env=None, endpoint=None, token=None):
        env = env or JobEnv.current()
        self.endpoint = endpoint if endpoint is not None else env.endpoint
        self.token = token if token is not None else env.token

    def report(self, snapshot, timeout=TIMEOUT):
        return self.post("/v1/reports", snapshot, timeout=timeout)

    def ack_checkpoint(self, commit, timeout=TIMEOUT):
        return self.post(
            "/v1/checkpoints", commit, timeout=timeout, error=errors.CheckpointError,
            default_code="CHECKPOINT_REJECTED", refusal=_checkpoint_refusal)

    def children(self, body, timeout=90):
        return self.post(
            "/v1/children", body, timeout=timeout, error=errors.SpawnRefused,
            refusal=errors.SpawnRefused,
            not_in_job="ZENTHROS_REPORT_ENDPOINT is not set: spawn runs inside a Zenthros job")

    def post(self, path, body, *, timeout=TIMEOUT, error=errors.ReportError, default_code="REJECTED",
             refusal=errors.from_code, not_in_job="ZENTHROS_REPORT_ENDPOINT is not set"):
        """The decoded JSON answer. Outside a job it raises error; an agent
        that gave only transient answers, or was never reached, for the 45 s,
        AgentUnanswered; any other 4xx refusal(code, message) with the
        controller's words."""
        if not self.endpoint:
            raise error("REJECTED", not_in_job)
        raw_body = json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"}
        status, raw = 0, b""
        for attempt in range(ATTEMPTS):
            last = attempt == ATTEMPTS - 1
            try:
                status, raw = self._send(path, raw_body, headers, timeout)
            except (OSError, http.client.HTTPException) as err:
                if not last:
                    time.sleep(RETRY_WAITS[attempt])
                    continue
                raise errors.AgentUnanswered("AGENT_UNREACHABLE", str(getattr(err, "reason", err))) from err
            if errors.transient_status(status) and not last:
                time.sleep(RETRY_WAITS[attempt])
                continue
            break
        data = {}
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                data = {"error": raw.decode("utf-8", "replace")}
        code, message = data.get("error_code", default_code), data.get("error", f"http {status}")
        if errors.transient_status(status):
            raise errors.AgentUnanswered(code, message)
        if status >= 400:
            raise refusal(code, message)
        return data

    def _send(self, path, body, headers, timeout):
        """(status, raw body). The one place bytes leave for the agent."""
        if self.endpoint.startswith("unix://"):
            return _unix_post(self.endpoint[len("unix://"):], path, body, headers, timeout)
        return _http_post(self.endpoint.rstrip("/") + path, body, headers, timeout)


def _checkpoint_refusal(code, message):
    err = errors.from_code(code, message)
    return err if isinstance(err, errors.CheckpointError) else errors.CheckpointError(code, message)


def _unix_post(socket_path, path, body, headers, timeout):
    conn = UnixHTTPConnection(socket_path, timeout=timeout)
    try:
        conn.request("POST", path, body=body, headers=headers)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def _http_post(url, body, headers, timeout):
    request = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=timeout) as resp:
            return resp.status, resp.read()
    except HTTPError as err:
        return err.code, err.read()
