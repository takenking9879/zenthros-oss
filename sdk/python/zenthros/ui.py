# SPDX-License-Identifier: Apache-2.0

"""zenthros ui: what --json says, on a page, for this computer. It answers from
the same functions as the command line and MCP (zenthros/tools.py), reads only,
and keeps nothing of its own.

It listens on 127.0.0.1, and other programs of this computer and other sites a
browser visits can reach that: so it answers only to its token, printed once, and
only by this address's own names. What a job prints reaches the page as text.
"""

import hmac
import json
import re
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import parse_qs, unquote, urlsplit

from zenthros import errors, tools

ADDRESS = "127.0.0.1"
TOKEN_HEADER = "X-Zenthros-Token"


def _data(name):
    return resources.files("zenthros").joinpath("_data", name).read_text(encoding="utf-8")


def skill_text():
    """What an agent reads to use Zenthros (zenthros agent skill)."""
    return _data("SKILL.md")


def _number(query, name, default):
    value = query.get(name, [default])[0]
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name}={value!r} is not a number") from None


_LOG = re.compile(r"^/api/jobs/([^/]+)/logs$")
_JOB = re.compile(r"^/api/jobs/([^/]+)$")


def _answer(path, query):
    if path == "/api/targets":
        return tools.targets()
    if path == "/api/jobs":
        return tools.jobs(limit=_number(query, "limit", tools.DEFAULT_LIMIT), cursor=_number(query, "cursor", 0))
    if path == "/api/environments":
        return tools.environments()
    if path == "/api/cache":
        return tools.cache_entries()
    log = _LOG.match(path)
    if log:
        grep = query.get("grep", [None])[0]
        return tools.logs(unquote(log.group(1)), tail=_number(query, "tail", tools.DEFAULT_TAIL), grep=grep,
                          limit=_number(query, "limit", tools.DEFAULT_LIMIT))
    job = _JOB.match(path)
    if job:
        return tools.status(unquote(job.group(1)))
    raise errors.NotFound(f"nothing answers at {path}")


class _Handler(BaseHTTPRequestHandler):
    server_version = "zenthros"
    sys_version = ""

    def log_message(self, *_):
        pass

    def _send(self, code, body, kind, extra=()):
        self.send_response(code)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for name, value in extra:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code, answer):
        self._send(code, json.dumps(answer).encode(), "application/json")

    def _allowed(self, query):
        owner = self.server.owner
        given = self.headers.get(TOKEN_HEADER) or query.get("token", [""])[0]
        return self.headers.get("Host") in owner.hosts and hmac.compare_digest(given.encode(), owner.token.encode())

    def _page(self):
        nonce = secrets.token_urlsafe(16)
        policy = f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; connect-src 'self'; base-uri 'none'; form-action 'none'"
        self._send(200, _data("ui.html").replace("__NONCE__", nonce).encode(), "text/html; charset=utf-8",
                   [("Content-Security-Policy", policy), ("X-Frame-Options", "DENY"), ("Referrer-Policy", "no-referrer"),
                    ("X-Content-Type-Options", "nosniff")])

    def do_GET(self):
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        if not self._allowed(query):
            self._json(403, {"error": "open the address zenthros ui printed"})
            return
        if url.path == "/":
            self._page()
            return
        try:
            self._json(200, _answer(url.path, query))
        except errors.NotFound as err:
            self._json(404, {"error": str(err)})
        except ValueError as err:
            self._json(400, {"error": str(err)})
        except (errors.ZenthrosError, OSError) as err:
            self._json(502, {"error": str(err)})

    def _read_only(self):
        self._json(405, {"error": "this page only reads"})

    do_POST = do_PUT = do_PATCH = do_DELETE = _read_only


class Server:
    """A running zenthros ui."""

    def __init__(self, port):
        self.token = secrets.token_urlsafe(24)
        self.address = ADDRESS
        self._http = ThreadingHTTPServer((ADDRESS, port), _Handler)
        self._http.daemon_threads = True
        self._http.owner = self
        self.port = self._http.server_address[1]
        self.hosts = {f"{ADDRESS}:{self.port}", f"localhost:{self.port}"}
        self.url = f"http://{ADDRESS}:{self.port}/?token={self.token}"
        self._thread = threading.Thread(target=self._http.serve_forever, daemon=True)
        self._thread.start()

    def wait(self):
        self._thread.join()

    def stop(self):
        self._http.shutdown()
        self._http.server_close()


def serve(port=0):
    try:
        return Server(port)
    except OSError as err:
        raise ValueError(f"{ADDRESS}:{port} cannot be used ({err.strerror}): pass another with --port") from err
