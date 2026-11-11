# SPDX-License-Identifier: Apache-2.0

"""zenthros attach: connect a machine of the user's own (usability R3).

    $ zenthros attach my-vm                      # asks the two questions
    zenthros.attach("my-vm", type="vm", runtime="containerd")

The machine type decides the installer and how its jobs run:

    VM, containers     install-agent.sh with containerd       containerd
    VM, directly       install-agent.sh                       systemd
    Container          install-container.sh (RunPod, Vast)    process
    This computer      a container here (zenthros.create)     process

The order is the rule, and a test pins it: checks, then the token, then
the two lines, then the wait, then a test run. A check that fails makes
no token. The lines are the hub's: the installer, with no secret, checked
against the hub's pin; then the short join (zt1_), single use for an hour,
which ends only once the machine is connected. host= runs them over ssh.
"""

import os
import shlex
import time

from zenthros import console, errors

TYPES = (("vm", "VM"), ("container", "Container"), ("kubernetes", "Kubernetes"), ("local", "This computer"))
RUNTIMES = (("containerd", "containers (containerd)"), ("systemd", "directly (systemd)"))
_ALIASES = {"vm": "vm", "container": "container", "kubernetes": "kubernetes", "k8s": "kubernetes",
            "local": "local", "this-computer": "local"}
# How long attach waits for the machine, and for its test run.
WAIT_TIMEOUT = 1200
TEST_TIMEOUT = 600
# Free disk a VM needs before anything is installed: containerd keeps images.
DISK_GB = {"containerd": 10, "systemd": 2}


def _kind(value):
    kind = _ALIASES.get(str(value or "").strip().lower())
    if kind is None:
        raise errors.ValidationError(f"unknown machine type {value!r}: use vm, container, kubernetes or local")
    return kind


def ask_type(kind, runtime, ask):
    """The machine type and, for a VM, where its jobs run: what was given,
    else asked (ask(label, options) -> index), in that order."""
    kind = _kind(kind) if kind else TYPES[ask("Machine type", [label for _, label in TYPES])][0]
    if kind != "vm":
        if runtime:
            raise errors.ValidationError(f"runtime= is only for a VM; a {kind} runs its jobs its own way")
        return kind, ""
    if not runtime:
        return kind, RUNTIMES[ask("Run jobs in", [label for _, label in RUNTIMES])][0]
    runtime = str(runtime).strip().lower()
    if runtime not in dict(RUNTIMES):
        raise errors.ValidationError(f"unknown runtime {runtime!r}: use containerd or systemd")
    return kind, runtime


def check_script(name, runtime):
    """What a VM must have before a token is made for it, as a script for
    its shell: systemd (the agent is a unit), the disk containerd or the
    agent needs, and sudo without a password. Each failure is one line
    with what to do, and exit 3."""
    need = DISK_GB.get(runtime, 2)
    # The name is the machine's shell's text, set once: shown, never run.
    return f"""set -u
name={shlex.quote(name)}
fail() {{ echo "✗ $1"; exit 3; }}
[ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1 || fail "systemd not found. Looks like a container: zenthros attach $name --type container"
free=$(df -Pk / | awk 'NR==2 {{print int($4 / 1048576)}}')
[ "$free" -ge {need} ] || fail "$free GB free; {"containerd needs " + str(need) + " GB (images): give the disk 20 GB, or run jobs directly: zenthros attach $name --runtime systemd" if runtime == "containerd" else "the agent needs " + str(need) + " GB: free some space"}."
command -v curl >/dev/null 2>&1 || fail "curl not found. Install it: sudo apt-get install -y curl"
echo "checks passed"
"""


class AttachCommand:
    """The two lines to paste on the machine: the installer (no secret),
    then the join, whose token is single-use and ends at expires_unix.
    wait() returns the Machine once it is connected and a test run came
    back from it."""

    def __init__(self, join, name="", kind="vm"):
        self.backend_id = str(join.get("backendId") or "")
        self.name = name or str(join.get("name") or "") or self.backend_id
        self.kind = kind
        self.expires_unix = int(join.get("expiresUnix") or 0)
        self.install = str(join.get("installScript") or "")
        self.join = str(join.get("joinScript") or "")
        self.lines = [self.install.rstrip("\n"), self.join.rstrip("\n")]

    @property
    def ends(self):
        return time.strftime("%H:%M", time.localtime(self.expires_unix)) if self.expires_unix else "soon"

    def where(self):
        """Where the two lines run: on the machine, or for a cluster where
        kubectl and helm reach it (its current context)."""
        return f"Where kubectl reaches {self.name}" if self.kind == "kubernetes" else f"On {self.name}"

    def script(self):
        """Both lines, as one script for a root shell (sudo bash -s)."""
        return self.install + self.join

    def wait(self, timeout=None, test=True, out=None):
        from zenthros.machines import _api

        return wait_and_test(_api(), self.backend_id, self.name, timeout or WAIT_TIMEOUT, test=test, out=out,
                             retire=False)

    def __str__(self):
        return "\n".join(self.lines)

    def __repr__(self):
        return (f"{self.where()}, run:\n\n" + "\n".join(self.lines) +
                f"\n\nThe join is single use; it ends at {self.ends}. Then .wait() for {self.name}.")


def _say(out, text):
    if out is not None:
        console.show(text, out)


def connected_text(backend):
    from zenthros.targetlist import mem_text

    parts = [f"{backend.cpu} CPU" if backend.cpu else "", mem_text(backend.memory_bytes) if backend.memory_bytes else ""]
    size = ", ".join(p for p in parts if p)
    return f"✓ connected ({size})" if size else "✓ connected"


def _probe():
    """The test run: a function there that comes back with where it ran."""
    import platform

    return platform.python_version()


def test_run(name, timeout=TEST_TIMEOUT):
    """A function round trip on the machine: pickled here, run there, its
    value back. Returns how long it took."""
    from zenthros.remote import run

    started = time.monotonic()
    # Its logs are the probe's, not the person's: one line says it passed
    # (on AWS they were six lines of systemd and package versions, E-16).
    # It carries none of the directory it starts in: the probe is the SDK's,
    # and a project over what a job without a bucket carries refused it.
    run(_probe, on=name, code_dir=None).result(timeout=timeout, stream_logs=False)
    return time.monotonic() - started


def seconds_text(seconds):
    seconds = int(round(seconds))
    return f"{seconds}s" if seconds < 60 else f"{seconds // 60}m{seconds % 60:02d}s"


def wait_and_test(client, backend_id, name, timeout, test=True, out=None, retire=True):
    """The machine, once connected, and once a test run came back from it.
    One that is not connected by timeout is retired when retire is set."""
    from zenthros.local import LocalBackend
    from zenthros.machines import Machine

    _say(out, f"Waiting for {name}…")
    backend = LocalBackend(client, backend_id, "", created=False)
    undo = [(f"retire {backend_id}", lambda: client.retire_backend(backend_id))] if retire else []
    with errors.undone_on_failure(*undo):
        ready = backend.wait_ready(timeout=timeout, interval=5)
    if ready is not None:
        _say(out, connected_text(ready))
    machine = Machine(name=name or backend_id, id=backend_id, target="external")
    if test:
        took = test_run(machine.name)
        _say(out, f"✓ test run {seconds_text(took)}")
    return machine


def ssh_run(ssh, host, key, script, timeout):
    """The exit code and output of script run on host, as root (sudo -n),
    over ssh: on its standard input, never an argument."""
    from zenthros.machines import _ssh

    args = [ssh, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
            *(["-i", os.path.expanduser(key)] if key else []), str(host), "sudo -n bash -s"]
    return _ssh(args, script, timeout)


def attach(name=None, *, type=None, runtime=None, host=None, key=None, ssh="ssh", wait=False, test=True,
           timeout=WAIT_TIMEOUT, ask=None, out=None):
    """Connect a machine of yours to the Workspace, as name. type is vm,
    container, kubernetes or local (this computer); runtime, for a VM, is
    where its jobs run: containerd (in images) or systemd (directly). What
    is not given is asked.

    Without host, returns the AttachCommand: the two lines to paste on the
    machine; its .wait() returns the Machine once connected and tested
    (wait=True waits here, as the CLI does).
    With host ("user@address"), checks the machine over ssh, runs the lines
    there as root, and returns the Machine. type="local" starts a container
    on this computer (zenthros.create). A name already attached is that
    machine."""
    from zenthros import machines

    if ask is None:
        from zenthros.cli import select as ask
    client = machines._api()
    if name:
        found = machines._find(client, name)
        if found is not None:
            _say(out, f"✓ {name} is already attached")
            return machines._machine(found, "external")
    if host is not None and (not type or (_kind(type) == "vm" and not runtime)):
        # Over ssh there is nothing to answer: a VM unless type= says
        # otherwise, its jobs in containers unless runtime= does (the
        # prompt's defaults). A script or a notebook has no prompt (E-16).
        type, runtime = type or "vm", runtime or (RUNTIMES[0][0] if _kind(type or "vm") == "vm" else None)
        if _kind(type) == "vm":
            _say(out, f"{name or host}: a VM, jobs in {runtime} (--type, --runtime to choose)")
    kind, runtime = ask_type(type, runtime, ask)
    if kind == "kubernetes" and host:
        raise errors.ValidationError("host= is for a VM or a container; a cluster's lines run where kubectl reaches it")
    if kind == "local":
        if host:
            raise errors.ValidationError("host= is for a VM or a container; type='local' is this computer")
        # create checks Docker before it asks the hub for anything.
        made = machines.create(machines.LOCAL, name=name)
        _say(out, "✓ connected")
        if test:
            _say(out, f"✓ test run {seconds_text(test_run(made.name))}")
        return made
    if host is not None:
        if kind == "vm":
            code, said = ssh_run(ssh, host, key, check_script(name or "my-vm", runtime), 120)
            if code != 0:
                lines = [line for line in said.strip().splitlines() if line.startswith("✗")]
                raise errors.ValidationError((lines[-1][2:] if lines else f"checking {host}: {said.strip()[-300:]}"))
        join = client.join_backend(name or "", kind=kind, runtime=runtime, python=_python(kind, runtime))
        command = AttachCommand(join, name or "", kind)
        secret = _secret(command.join)
        with errors.undone_on_failure((f"retire {command.backend_id}",
                                       lambda: client.retire_backend(command.backend_id))):
            code, said = ssh_run(ssh, host, key, command.script(), timeout)
            if code != 0:
                tail = "\n".join(said.strip().splitlines()[-15:])
                raise errors.BackendJoinError(f"attaching {host} as {command.backend_id}:\n" +
                                              (tail.replace(secret, "<token>") if secret else tail))
        return wait_and_test(client, command.backend_id, command.name, timeout, test=test, out=out)
    join = client.join_backend(name or "", kind=kind, runtime=runtime, python=_python(kind, runtime))
    command = AttachCommand(join, name or "", kind)
    if out is not None:
        _say(out, f"\n{command.where()}, run:")
        # As they are, not indented: a pasted heredoc ends only at a
        # delimiter in its first column (a container's installer has one).
        for i, line in enumerate(command.lines):
            note = f"      # single use, expires {command.ends}" if i == len(command.lines) - 1 else ""
            _say(out, line + note)
        _say(out, "")
    if not wait:
        return command
    return command.wait(timeout=timeout, test=test, out=out)


def _python(kind, runtime):
    """The minor a machine whose jobs run on its own Python gets: this
    one's, as a function pickled here needs it there. A VM running them
    directly installs it (uv); a container's agent completes its image's
    Python to it. A VM with containerd runs them in an image of it."""
    from zenthros.machines import python_minor

    return python_minor() if (kind == "vm" and runtime == "systemd") or kind == "container" else ""


def _secret(join_line):
    """The token in a join line, to keep out of any error."""
    words = join_line.replace("'", " ").split()
    for i, word in enumerate(words[:-1]):
        if word == "--token":
            return words[i + 1]
    return ""
