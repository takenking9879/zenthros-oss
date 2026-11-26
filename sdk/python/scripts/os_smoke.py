# SPDX-License-Identifier: Apache-2.0

"""The SDK on this computer's system (Linux, macOS, Windows), as a person
installs it: `zenthros --help`, `zenthros login --key` against a hub on this
computer, a call through the API, and the key's file readable by its owner
only. Run with the Python of the environment the wheel is installed in
(sdk-os.yml, and the Windows check from WSL): python -I scripts/os_smoke.py.

The zenconfig goes to a directory of its own (ZENTHROS_CONFIG), never the
person's: a plain folder in their profile's temporary folder, so on Windows it
takes the profile's access rules, as ~/.config does (tempfile.mkdtemp would
restrict it itself and hide what the SDK does). The code and the CLI are the
installed ones: every Python here runs with -I, so this folder's zenthros/
does not shadow them.
"""

import base64
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WORKSPACE = "smoke"


class Hub(BaseHTTPRequestHandler):
    """The few answers a key login and a job list need."""

    def log_message(self, *args):
        pass

    def do_GET(self):  # noqa: N802 - http.server's name
        path = self.path.split("?")[0]
        if self.headers.get("Authorization") != "Bearer smoke-token":
            return self._answer(401, {"kind": "Status", "code": 401, "message": "a bearer token is required"})
        if path == f"/v1/workspaces/{WORKSPACE}":
            return self._answer(200, {"name": WORKSPACE, "namespace": "tenant-smoke"})
        if path == f"/v1/workspaces/{WORKSPACE}/jobs":
            return self._answer(200, {"items": [{"metadata": {"name": "j1", "generation": 1},
                                                 "status": {"phase": "Succeeded"}}], "metadata": {}})
        self._answer(404, {"kind": "Status", "code": 404, "message": f"no route {path}"})

    def _answer(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


# Everyone, Authenticated Users, Users, Guests: by SID, as every language of
# Windows names them differently.
OTHERS = {"S-1-1-0", "S-1-5-11", "S-1-5-32-545", "S-1-5-32-546"}
ACL = ("(Get-Acl -LiteralPath $env:SMOKE_PATH).Access | Where-Object AccessControlType -eq Allow | "
       "ForEach-Object { $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value }")


def only_its_owner(path):
    """Who else may read path: [] when only its owner (and, on Windows, the
    system and the administrators) may."""
    if os.name != "nt":
        mode = os.stat(path).st_mode & 0o777
        return [] if mode & 0o077 == 0 else [f"{path}: mode {oct(mode)}"]
    sids = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", ACL],
                          env={**os.environ, "SMOKE_PATH": path}, capture_output=True, text=True, check=True).stdout
    return [f"{path}: {sid}" for sid in sids.split() if sid in OTHERS]


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Hub)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    key = "zen_" + base64.urlsafe_b64encode(json.dumps(
        {"server": url, "workspace": WORKSPACE, "token": "smoke-token"}).encode()).decode().rstrip("=")
    home = os.path.join(tempfile.gettempdir(), f"zenthros-os-smoke-{secrets.token_hex(4)}")
    os.makedirs(home)
    env = {**os.environ, "ZENTHROS_CONFIG": os.path.join(home, "zenthros", "config.yaml")}
    # The CLI of this Python's environment, not another on the PATH.
    cli = shutil.which("zenthros", path=os.path.dirname(sys.executable))
    if not cli:
        sys.exit(f"no zenthros next to {sys.executable}: install the wheel in this environment")
    try:
        subprocess.run([cli, "--help"], env=env, check=True, stdout=subprocess.DEVNULL)
        login = subprocess.run([cli, "login", "--key", "-"], env=env, input=key + "\n", capture_output=True,
                               text=True, encoding="utf-8")
        if login.returncode != 0 or f"workspace: {WORKSPACE}" not in login.stdout:
            sys.exit(f"zenthros login --key: {login.returncode}\n{login.stdout}{login.stderr}")
        listed = subprocess.run([sys.executable, "-I", "-c", "import zenthros; print([j.name for j in zenthros.workspace().jobs()])"],
                                env=env, capture_output=True, text=True, encoding="utf-8")
        if listed.returncode != 0 or "j1" not in listed.stdout:
            sys.exit(f"a call through the API: {listed.returncode}\n{listed.stdout}{listed.stderr}")
        config = env["ZENTHROS_CONFIG"]
        with open(config, encoding="utf-8") as fh:
            if "smoke-token" not in fh.read():
                sys.exit(f"{config} does not keep the key's token")
        others = only_its_owner(config) + only_its_owner(os.path.dirname(config))
        if others:
            sys.exit(f"{config} may be read by others: {others}")
    finally:
        server.shutdown()
        shutil.rmtree(home, ignore_errors=True)
    print(f"OK  {sys.platform} python {sys.version_info.major}.{sys.version_info.minor}: zenthros --help, "
          "login --key, a call through the API, the key readable by its owner only")


if __name__ == "__main__":
    main()
