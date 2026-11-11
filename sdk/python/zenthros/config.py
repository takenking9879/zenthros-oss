# SPDX-License-Identifier: Apache-2.0

"""Where zenthros-api is and how this process proves who it is.

A person or program without a kubeconfig reaches its Workspace through
zenthros-api with a bearer token. Kubernetes checks that token and its RBAC
decides; the SDK only carries it. The zenconfig says where and with what:

    {"current-context": "risk",
     "contexts": {"risk": {
        "server": "https://api.zenthros.acme.com",
        "workspace": "risk",
        "ca-data": "<base64 PEM>",
        "auth": {"type": "token", "token": "..."}}}}

auth is one of token, token-file (a projected ServiceAccount token, re-read
on every request), token-command (a program that prints an ExecCredential,
as kubeconfig exec plugins do: OIDC login helpers work unchanged) or oidc
(an issuer the SDK itself logs in to: zenthros.login(), zenthros.auth):

        "auth": {"type": "oidc", "oidc": {"issuer": "https://idp.acme.com",
                                          "client-id": "zenthros-sdk"}}

Each value comes from the first of: configure(), the environment, the
context. The file is ZENTHROS_CONFIG, else ~/.config/zenthros/config.yaml;
it is JSON, which is also YAML: zctl reads and writes the same file. A
context without a server (an administrator's: the Gateway and a
kubeconfig) is not an API connection: the SDK then drives zctl, as before.
A file that is not JSON is an error that names it, never "no connection".

    ZENTHROS_SERVER      https://... of zenthros-api
    ZENTHROS_TOKEN       the bearer token
    ZENTHROS_TOKEN_FILE  a file holding it
    ZENTHROS_CA_FILE     PEM CA that verifies the server
    ZENTHROS_WORKSPACE   the Workspace
    ZENTHROS_CONTEXT     the context to use
    ZENTHROS_API_KEY     a key (zen_...) from `zctl user add`: a whole
                         context in one string, used instead of the file

A key is what an administrator hands a user: `zctl user add` prints it, and
zenthros.login(key=...) keeps it as the current context.
"""

import base64
import contextlib
import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from zenthros import errors

CONFIG_ENV = "ZENTHROS_CONFIG"
CONTEXT_ENV = "ZENTHROS_CONTEXT"
SERVER_ENV = "ZENTHROS_SERVER"
TOKEN_ENV = "ZENTHROS_TOKEN"
TOKEN_FILE_ENV = "ZENTHROS_TOKEN_FILE"
CA_FILE_ENV = "ZENTHROS_CA_FILE"
WORKSPACE_ENV = "ZENTHROS_WORKSPACE"
API_KEY_ENV = "ZENTHROS_API_KEY"
API_KEY_PREFIX = "zen_"

EXEC_INFO = json.dumps({"apiVersion": "client.authentication.k8s.io/v1", "kind": "ExecCredential",
                        "spec": {"interactive": False}})


def config_path():
    path = os.environ.get(CONFIG_ENV, "")
    if path:
        return path
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(base, "zenthros", "config.yaml")


def parse_api_key(key):
    """The context a key (zctl user add) carries, as the zenconfig holds it.
    The key's own text never appears in an error: it holds a token."""
    body = (key or "").strip()
    if not body.startswith(API_KEY_PREFIX) or len(body) == len(API_KEY_PREFIX):
        raise errors.ValidationError(f"not a Zenthros API key: it starts with {API_KEY_PREFIX}")
    body = body[len(API_KEY_PREFIX):]
    try:
        doc = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    except (ValueError, UnicodeDecodeError):
        raise errors.ValidationError("not a Zenthros API key (it does not decode): run `zenthros login` for a new one") from None
    if not isinstance(doc, dict) or not all(isinstance(doc.get(k), str) and doc.get(k) for k in ("server", "workspace", "token")):
        raise errors.ValidationError("API key: it lacks the server, the Workspace or the token")
    context = {"server": doc["server"].rstrip("/"), "workspace": doc["workspace"]}
    if doc.get("ca-data"):
        context["ca-data"] = doc["ca-data"]
    context["auth"] = {"type": "token", "token": doc["token"]}
    return context


def load_context(name="", path=""):
    """The named context (else the current one) of the zenconfig, as a dict;
    {} when there is no file, it is not JSON, or it names no context."""
    return _load(name, path)[1]


def _load(name="", path=""):
    """load_context, with the name the context was found under. A key in
    the environment is the context, unless a name asks for another."""
    key = os.environ.get(API_KEY_ENV, "")
    if key and not name and not path:
        context = parse_api_key(key)
        return context["workspace"], context
    path = path or config_path()
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        if name:
            raise errors.ValidationError(f"context {name!r}: no zenconfig at {path}") from None
        return "", {}
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # A hand edit's trailing comma read as "no connection" sent the
        # caller to zctl, or told it to set a server the file has.
        raise errors.ValidationError(f"{path} is not a JSON zenconfig: {exc}") from None
    if not isinstance(doc, dict):
        raise errors.ValidationError(f"{path}: a zenconfig is a JSON object")
    name = name or os.environ.get(CONTEXT_ENV, "") or doc.get("current-context") or ""
    contexts = doc.get("contexts") or {}
    if not name:
        return "", {}
    if name not in contexts:
        raise errors.ValidationError(f"{path} has no context {name!r}; it has {sorted(contexts)}")
    context = contexts[name]
    if not isinstance(context, dict):
        raise errors.ValidationError(f"{path}: context {name!r} is not an object")
    return name, context


def write_private(path, text):
    """Write text to path for its owner only (the file 0600, its directory
    0700), replacing the file in one step."""
    directory = os.path.dirname(path)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    scratch = f"{path}.{os.getpid()}.tmp"
    fd = os.open(scratch, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.chmod(scratch, 0o600)
        os.replace(scratch, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(scratch)
        raise


def save_context(name, context, path=""):
    """Write one context into the zenconfig and make it the current one;
    the others stay. A file that is not a JSON zenconfig is refused, never
    rewritten."""
    path = path or config_path()
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        doc = {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise errors.ValidationError(f"{path} is not a JSON zenconfig: not rewriting it") from None
    if not isinstance(doc, dict):
        raise errors.ValidationError(f"{path}: a zenconfig is a JSON object")
    doc.setdefault("contexts", {})[name] = context
    doc["current-context"] = name
    write_private(path, json.dumps(doc, indent=2) + "\n")


def update_context(fields, path=""):
    """Set fields in the current context of the zenconfig (a value of None
    removes the field); returns the context's name. A context that comes
    from a key in the environment has no file to keep them in."""
    if os.environ.get(API_KEY_ENV, "") and not path:
        raise errors.ValidationError(f"{API_KEY_ENV} is the context here, and it has no file to keep this in: "
                                     "run zenthros.login(key=...) once, or set it in the environment instead")
    path = path or config_path()
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        raise errors.ValidationError("not logged in: run `zenthros login` first") from None
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise errors.ValidationError(f"{path} is not a JSON zenconfig: not rewriting it") from None
    name = os.environ.get(CONTEXT_ENV, "") or (doc.get("current-context") if isinstance(doc, dict) else "")
    context = ((doc.get("contexts") or {}).get(name) if isinstance(doc, dict) and name else None)
    if not isinstance(context, dict):
        raise errors.ValidationError("not logged in: run `zenthros login` first")
    for key, value in fields.items():
        if value is None:
            context.pop(key, None)
        else:
            context[key] = value
    write_private(path, json.dumps(doc, indent=2) + "\n")
    return name


def forget_token(name, path=""):
    """Remove the token written into context name; the rest of the context
    and the other contexts stay."""
    path = path or config_path()
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    auth = ((doc.get("contexts") or {}).get(name) or {}).get("auth")
    if not isinstance(auth, dict) or "token" not in auth:
        return
    del auth["token"]
    if set(auth) <= {"type"}:
        del doc["contexts"][name]["auth"]
    write_private(path, json.dumps(doc, indent=2) + "\n")


class TokenSource:
    """The bearer token, fetched when needed: inline, from a file (read
    again every time, as a projected token rotates), or from a command's
    ExecCredential or a login session (fetch: the token and when it
    expires), kept until shortly before it expires."""

    def __init__(self, token="", token_file="", command=None, args=None, fetch=None, login=""):
        if not (token or token_file or command or fetch):
            raise errors.ValidationError("no token: set auth in the zenconfig, ZENTHROS_TOKEN or configure(token=)")
        self._token, self._file = token, token_file
        self._command = [command, *(args or [])] if command else None
        self._fetch = fetch or self._exec
        # Who the token is, for the one client per credential
        # (HTTPJobClient.for_connection): the token, the file, the command,
        # or the login session (the context's name).
        if token:
            self.identity = ("token", token)
        elif token_file:
            self.identity = ("file", token_file)
        elif command:
            self.identity = ("command", tuple(self._command))
        else:
            self.identity = ("login", login or id(fetch))
        self._cached, self._expires = "", 0.0
        self._lock = threading.Lock()

    def __call__(self):
        if self._token:
            return self._token
        if self._file:
            try:
                with open(os.path.expanduser(self._file), encoding="utf-8") as fh:
                    token = fh.read().strip()
            except OSError as exc:
                raise errors.TransientError(f"token file {self._file}: {exc}") from exc
            if not token:
                raise errors.TransientError(f"token file {self._file} is empty")
            return token
        with self._lock:
            if self._cached and time.time() < self._expires - 30:
                return self._cached
            self._cached, self._expires = self._fetch()
            return self._cached

    def _exec(self):
        try:
            done = subprocess.run(self._command, capture_output=True, text=True, timeout=120,
                                  env={**os.environ, "KUBERNETES_EXEC_INFO": EXEC_INFO})
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise errors.TransientError(f"token-command {self._command[0]}: {exc}") from exc
        if done.returncode != 0:
            raise errors.TransientError(f"token-command {self._command[0]} exited {done.returncode}: {done.stderr.strip()}")
        try:
            status = json.loads(done.stdout)["status"]
            token = status["token"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise errors.TransientError(f"token-command {self._command[0]} printed no ExecCredential token") from exc
        expires = float("inf")
        stamp = status.get("expirationTimestamp")
        if stamp:
            expires = datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
        return token, expires


@dataclass
class Connection:
    """One zenthros-api, one Workspace, one credential."""

    server: str
    workspace: str
    token: TokenSource
    ca_data: Optional[bytes] = None


def resolve(server="", token="", workspace="", context=""):
    """The API connection in effect, or None when there is none (no server
    anywhere): the caller then uses zctl. Arguments are configure()'s."""
    name, ctx = _load(context)
    server = server or os.environ.get(SERVER_ENV, "") or ctx.get("server", "")
    if not server:
        return None
    if not server.startswith("https://") and not server.startswith("http://"):
        raise errors.ValidationError(f"server {server!r}: an https:// URL")
    workspace = workspace or os.environ.get(WORKSPACE_ENV, "") or ctx.get("workspace", "")
    if not workspace:
        raise errors.ValidationError(f"zenthros-api at {server}: which Workspace? set it in the context, "
                                     "ZENTHROS_WORKSPACE or configure(workspace=)")
    auth = ctx.get("auth") or {}
    env_token, env_file = os.environ.get(TOKEN_ENV, ""), os.environ.get(TOKEN_FILE_ENV, "")
    if token or env_token or env_file:
        source = TokenSource(token=token or env_token, token_file="" if (token or env_token) else env_file)
    elif auth.get("oidc"):
        from zenthros.auth import Session

        source = TokenSource(fetch=Session(name, auth["oidc"]).token, login=name)
    else:
        command = auth.get("token-command") or {}
        source = TokenSource(token=auth.get("token", ""), token_file=auth.get("token-file", ""),
                             command=command.get("command"), args=command.get("args"))
    ca_data = None
    ca_file = os.environ.get(CA_FILE_ENV, "")
    if ca_file:
        with open(ca_file, "rb") as fh:
            ca_data = fh.read()
    elif ctx.get("ca-data"):
        ca_data = base64.b64decode(ctx["ca-data"])
    return Connection(server=server.rstrip("/"), workspace=workspace, token=source, ca_data=ca_data)
