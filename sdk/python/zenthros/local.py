# SPDX-License-Identifier: Apache-2.0

"""This computer as a backend of your Workspace, in a container.

    lb = zenthros.LocalBackend.create("zenthros-pipeline:local", cpus=2, memory="4g")
    lb.wait_ready()                    # registered, healthy, accepting work, Private to you
    zenthros.run(fn, on=lb)            # only jobs that name it reach it
    lb.remove()                        # retired at the API first, then the container goes

    lb = zenthros.LocalBackend.attach("a-container-already-running")

zenthros-api gives the join (POST /backends, which needs the role
zenthros:workspace-backend-owner): a one-use token, for a backend Private to
the Workspace and Named, so the Scheduler never sends it a job that did not
ask for it. The SDK runs the join's two steps in the container through
docker exec, each on standard input: the token is never an argument, so
it shows in no process list, no `docker inspect` and no error.

The container create() starts has nothing of the host: no privileges, no
capabilities, no mounts, no devices, the default network. Its limits are
hard: memory and memory-swap are the same, so a job that needs more dies in
the container and not the machine. Its jobs run as processes in the image,
as the image's user; the agent needs sh and python3, nothing else. For
zenthros.run the image also needs zenthros' own dependency (cloudpickle) and
boto3: the job reads its call from the Workspace's s3:// store.

The agent is not the container's first process (sleep is, under docker's
init): stopping the container ends its jobs as a lost machine, and the
Controller sends them elsewhere. remove() retires the backend first.
"""

import re
import subprocess
import sys
import time
from dataclasses import dataclass

from zenthros import errors
from zenthros.view import Backend

_sleep = time.sleep

# Where install-container.sh puts the agent (pkg/zenadm/attach.go).
AGENT = "/tmp/zenthros/bin/zenthros-agent"

# The labels of a container create runs here: the backend it joined as, its
# Workspace and the name it was given. The SDK finds its containers by them.
BACKEND_LABEL = "zenthros.ai/backend"
WORKSPACE_LABEL = "zenthros.ai/workspace"
NAME_LABEL = "zenthros.ai/name"
# ports= of create: published on this computer's loopback only, and the
# agent says that address (agent/endpoints.go reads the template), not the
# container's own, which this computer may not reach.
PORTS_LABEL = "zenthros.ai/ports"
ENDPOINT_TEMPLATE = "ZENTHROS_ENDPOINT_TEMPLATE={scheme}://127.0.0.1:{port};scope=Private"

_PATH_HINT = re.compile(r'run (export PATH="[^"]+:\$PATH") first')
_TOKEN = re.compile(r"--token '([^']+)'")


def _docker(docker, args, stdin=None, timeout=600):
    """Run docker with args, stdin on its standard input. Returns the exit
    code and what it printed, stdout and stderr together."""
    try:
        done = subprocess.run([docker, *args], input=stdin, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, timeout=timeout, check=False)
    except FileNotFoundError:
        raise errors.ValidationError(f"no docker at {docker!r}: install Docker, or pass docker= the path") from None
    except subprocess.TimeoutExpired:
        return 124, f"docker {args[0]} did not finish in {timeout} s"
    return done.returncode, done.stdout or ""


@dataclass
class Status:
    """What the API says of the backend, check by check. waiting_for names
    the first check that does not hold yet; empty when all do."""

    registered: bool = False
    healthy: bool = False
    open: bool = False
    accepting: bool = False
    private: bool = False
    waiting_for: str = "registered"
    backend: Backend = None

    @property
    def ready(self):
        return not self.waiting_for


class LocalBackend:
    """A container joined to the Workspace as a backend."""

    def __init__(self, client, backend_id, container, created, docker="docker"):
        self._client = client
        self.id = backend_id
        self.workspace = client.workspace
        self.container = container
        self.created = created
        self.docker = docker

    @classmethod
    def create(cls, image, *, cpus=2, memory="4g", name="", docker="docker", backend_name="", ports=()):
        """Start a container of image with cpus and memory as hard limits,
        and join it. name (the container's) defaults to zenthros-<backend
        id>; backend_name is what the Workspace calls the backend. ports:
        what a Service of it listens on, published on 127.0.0.1 only."""
        client = _api()
        join = client.join_backend(backend_name)
        backend_id = str(join.get("backendId") or "")
        container = name or f"zenthros-{backend_id}"
        lb = cls(client, backend_id, container, created=True, docker=docker)
        run = ["run", "-d", "--init", "--name", container,
               "--cpus", str(cpus), "--memory", str(memory), "--memory-swap", str(memory),
               "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
               "--label", f"{BACKEND_LABEL}={backend_id}", "--label", f"{WORKSPACE_LABEL}={lb.workspace}",
               *(["--label", f"{NAME_LABEL}={backend_name}"] if backend_name else []),
               *_published(ports),
               image, "sleep", "infinity"]
        code, out = _docker(docker, run)
        if code != 0:
            lb._undo(remove_container=False)
            raise errors.BackendJoinError(f"docker run {image}: {out.strip()}")
        lb._join(join)
        return lb

    @classmethod
    def attach(cls, container, *, docker="docker"):
        """Join a container that is already running. It stays yours:
        remove() retires the backend and stops the agent, not the container."""
        code, out = _docker(docker, ["inspect", "--format", "{{.State.Running}}", container])
        if code != 0 or out.strip() != "true":
            raise errors.ValidationError(f"container {container!r} is not running: {out.strip()}")
        client = _api()
        join = client.join_backend()
        lb = cls(client, str(join.get("backendId") or ""), container, created=False, docker=docker)
        lb._join(join)
        return lb

    def _join(self, join):
        """The join's two steps, each on the container's standard input. A
        failure revokes the token and removes a container this made."""
        token = _TOKEN.search(join.get("joinScript") or "")
        secret = token.group(1) if token else ""

        def scrub(text):
            return text.replace(secret, "<token>") if secret else text
        try:
            code, out = _docker(self.docker, ["exec", "-i", self.container, "sh"], stdin="exec 2>&1\n" + join["installScript"])
            if code != 0:
                raise errors.BackendJoinError(f"installing the agent in {self.container}: {scrub(out.strip())}")
            hint = _PATH_HINT.search(out)
            script = (hint.group(1) + "\n" if hint else "") + join["joinScript"]
            code, out = _docker(self.docker, ["exec", "-i", self.container, "sh"], stdin=script)
            if code != 0 or f"attached as backend {self.id}" not in out:
                raise errors.BackendJoinError(f"joining {self.container} as {self.id}: {scrub(out.strip())}")
        except BaseException:
            self._undo(remove_container=self.created)
            raise

    def _undo(self, remove_container):
        try:
            self._client.retire_backend(self.id)
        except errors.ZenthrosError as err:  # the error being undone is raised
            print(f"zenthros: could not retire {self.id}: {err}", file=sys.stderr)
        if remove_container:
            _docker(self.docker, ["rm", "-f", self.container])

    def status(self):
        """Each check from the API's list of the Workspace's backends."""
        found = [b for b in (Backend.from_api(info) for info in self._client.backends()) if b.id == self.id]
        if not found:
            return Status()
        b = found[0]
        checks = {
            "healthy": b.healthy,
            "open": not b.drained,
            # -1 while the agent installs its runtime: it runs nothing yet.
            "accepting": b.max_jobs >= 0,
            "private": b.access_mode == "Private" and b.workspaces == [self.workspace],
        }
        waiting = next((name for name, held in checks.items() if not held), "")
        return Status(registered=True, waiting_for=waiting, backend=b, **checks)

    def wait_ready(self, timeout=300, interval=2):
        """Wait until every check holds; returns the backend as the API
        last described it."""
        waited = 0
        while True:
            # The API away for a moment is "not ready yet": the callers wait
            # inside an undo, and a raise retired the backend.
            try:
                current = self.status()
            except errors.TransientError:
                current = Status(waiting_for="the API")
            if current.ready:
                return current.backend
            if waited >= timeout:
                raise errors.WaitTimeout(f"{self.id} not ready after {timeout} s (waiting for {current.waiting_for}): "
                                         f"see zenthros.targets()")
            _sleep(interval)
            waited += interval

    def remove(self):
        """Retire the backend at the API (revoked, its access gone), then
        remove the container this made, or stop the agent in one it did not.
        A retire the API refuses leaves everything as it was."""
        self._client.retire_backend(self.id)
        if self.created:
            _docker(self.docker, ["rm", "-f", self.container])
        else:
            _docker(self.docker, ["exec", self.container, AGENT, "attach", "--stop"])

    def __repr__(self):
        return f"LocalBackend({self.id!r}, container={self.container!r}, workspace={self.workspace!r})"


def _published(ports):
    """docker run's flags for ports: each on this computer's loopback, the
    URL the agent says for them, and a label that names them."""
    if not ports:
        return []
    flags = [arg for port in ports for arg in ("-p", f"127.0.0.1:{port}:{port}")]
    return [*flags, "-e", ENDPOINT_TEMPLATE, "--label", f"{PORTS_LABEL}={','.join(str(p) for p in ports)}"]


def _api():
    from zenthros.jobs.http import HTTPJobClient
    from zenthros.remote import options

    client = options.job_client()
    if not isinstance(client, HTTPJobClient):
        raise errors.ValidationError("no zenthros-api connection: zenthros.login() first, or set ZENTHROS_SERVER")
    return client
