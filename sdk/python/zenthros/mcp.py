# SPDX-License-Identifier: Apache-2.0

"""`zenthros mcp`: Zenthros for an agent, over MCP on stdin and stdout (one JSON
message per line). Five tools, on zenthros.tools.

    claude mcp add zenthros -- zenthros mcp

Each server process is a session: what it starts carries its label, and only it
stops that (a session of its own name: ZENTHROS_AGENT_SESSION).
"""

import contextlib
import json
import os
import sys
import uuid

from zenthros import errors, tools
from zenthros.jobs.wire import sdk_version

PROTOCOL = "2025-11-25"
KNOWN = {"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}
SESSION_ENV = "ZENTHROS_AGENT_SESSION"

_JOB = {"job": {"type": "string", "description": "The job's name, as run returned it."}}
TOOLS = [
    {"name": "targets", "description": "Where code can run: type, resources, price per hour, whether you may use it, and the command "
                                   "your user runs to allow it (to_allow).",
     "inputSchema": {"type": "object", "properties": {}}, "annotations": {"readOnlyHint": True}},
    {"name": "run",
     "description": "Start a command somewhere and return at once, with its job name. By default it runs on a snapshot of "
                    "the git worktree as it is now, uncommitted changes included: later edits do not touch it. Never waits; "
                    "ask status later, not often. A session that cannot wait gives its user the job's name: the job goes on "
                    "without it, and status and logs answer later, from any session.",
     "inputSchema": {"type": "object", "required": ["command"], "properties": {
         "command": {"description": "A list of words, or a string for sh -c.",
                     "oneOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}]},
         "on": {"type": "string", "description": "A name from targets; none: this computer."},
         "snapshot": {"type": "boolean", "description": "Default true."},
         "timeout": {"type": "string", "description": 'It ends the job: "30m", "2h". Default 2h.'},
         "keep": {"type": "boolean", "description": "Keep the machine for the next run, instead of a new one."},
         "fresh": {"type": "boolean", "description": "A new machine no job ran on, not one with nothing to do: for a final test."},
         "dry_run": {"type": "boolean", "description": "Say what would happen; nothing is created."},
         "name": {"type": "string"},
         "code_dir": {"type": "string", "description": "The repository. Default: the current directory."},
         "image": {"type": "string"}, "env": {"type": "string", "description": "An environment: the ones each target has, in targets."}, "cpu": {"type": "string"}, "memory": {"type": "string"},
         "gpu": {"type": "integer"}}}},
    {"name": "status",
     "description": "A job: state, exit_code (the process's; none when a machine failed, see reason), reason, took_s, code, "
                    "cost_usd, artifacts.",
     "inputSchema": {"type": "object", "required": ["job"], "properties": _JOB}, "annotations": {"readOnlyHint": True}},
    {"name": "logs", "description": "Only the lines asked for of a job's log: grep= first, to read little.",
     "inputSchema": {"type": "object", "required": ["job"], "properties": {
         **_JOB, "tail": {"type": "integer", "description": "Look at its last lines; default 2000."},
         "grep": {"type": "string", "description": "A regular expression."},
         "limit": {"type": "integer", "description": "Lines per answer; default 100."},
         "cursor": {"type": "integer", "description": "next_cursor of the last answer."}}},
     "annotations": {"readOnlyHint": True}},
    {"name": "cancel", "description": "Stop a job this session started.",
     "inputSchema": {"type": "object", "required": ["job"], "properties": _JOB}, "annotations": {"destructiveHint": True}},
]
_NAMES = {tool["name"] for tool in TOOLS}


def _call(name, arguments, session):
    args = dict(arguments)
    if name == "targets":
        return tools.targets()
    if name == "run":
        return tools.run(args.pop("command"), session=session, **args)
    job = args.pop("job")
    if name == "status":
        return tools.status(job)
    if name == "logs":
        return tools.logs(job, **args)
    return tools.cancel(job, session=session)


def _answer(message, session):
    method, params = message.get("method"), message.get("params") or {}
    if method == "initialize":
        asked = params.get("protocolVersion")
        return {"protocolVersion": asked if asked in KNOWN else PROTOCOL, "capabilities": {"tools": {}},
                "serverInfo": {"name": "zenthros", "version": sdk_version()}}
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": TOOLS}
    if method == "tools/call":
        name = params.get("name")
        if name not in _NAMES:
            raise LookupError(f"no tool {name!r}")
        try:
            # What the SDK prints must not reach the protocol's stream.
            with contextlib.redirect_stdout(sys.stderr):
                result = _call(name, params.get("arguments") or {}, session)
        except (errors.ZenthrosError, ValueError, LookupError, PermissionError, RuntimeError, OSError, TypeError) as exc:
            return {"content": [{"type": "text", "text": str(exc) or type(exc).__name__}], "isError": True}
        return {"content": [{"type": "text", "text": json.dumps(result)}], "isError": False}
    raise NotImplementedError(str(method))


def _send(stdout, message):
    stdout.write(json.dumps(message) + "\n")
    stdout.flush()


def serve(stdin=None, stdout=None, session=None):
    """Answer until the client closes stdin."""
    stdin, stdout = stdin or sys.stdin, stdout or sys.stdout
    session = session or os.environ.get(SESSION_ENV) or uuid.uuid4().hex[:12]
    for line in stdin:
        if not line.strip():
            continue
        try:
            message = json.loads(line)
        except ValueError:
            _send(stdout, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "not JSON"}})
            continue
        if "id" not in message:
            continue
        reply = {"jsonrpc": "2.0", "id": message["id"]}
        try:
            reply["result"] = _answer(message, session)
        except NotImplementedError as exc:
            reply["error"] = {"code": -32601, "message": f"no method {exc}"}
        except LookupError as exc:
            reply["error"] = {"code": -32602, "message": str(exc)}
        _send(stdout, reply)
