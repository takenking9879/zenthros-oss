# SPDX-License-Identifier: Apache-2.0

"""What an agent of yours may spend: `zenthros agent allow <target>`.

An agent reaches Zenthros through zenthros.mcp. A target with nothing to pay
(this computer, a machine of yours) needs no word from you; one that makes
machines does, once, with a limit of how many at a time. It is kept apart from
the zenconfig, which zctl reads strictly.

This is your word to your own agent on your own computer; the hub's limit on
spending is the Workspace's QuotaPolicy.
"""

import contextlib
import json
import os

from zenthros import config, console

if os.name == "nt":
    import errno
    import msvcrt

    def _lock(handle):
        # Its first byte, waited for as long as another agent holds it:
        # LK_LOCK alone gives up after 10 s, with EDEADLOCK.
        handle.seek(0)
        while True:
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                return
            except OSError as err:
                if err.errno != errno.EDEADLOCK:
                    raise
else:
    import fcntl

    def _lock(handle):
        fcntl.flock(handle, fcntl.LOCK_EX)

# Who started a job, for an agent to stop only what it started; and where,
# for the limit of machines. Labels, so a list of the jobs answers both.
SESSION = "zenthros.ai/agent-session"
TARGET = "zenthros.ai/agent-target"
FILE = "agents.json"


def path():
    return os.path.join(os.path.dirname(config.config_path()), FILE)


def allowed():
    """{target: {"max_machines": n or None}}. A file that cannot be read
    allows nothing."""
    try:
        with open(path(), encoding="utf-8") as handle:
            found = json.load(handle)
    except (OSError, ValueError):
        return {}
    return found if isinstance(found, dict) else {}


def _write(found):
    os.makedirs(os.path.dirname(path()), mode=0o700, exist_ok=True)
    partial = f"{path()}.{os.getpid()}.part"
    with open(os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w", encoding="utf-8") as handle:
        json.dump(found, handle)
    os.replace(partial, path())


def allow(target, max_machines=None, out=None):
    """Your agents may use target, with at most max_machines at a time."""
    _write({**allowed(), target: {"max_machines": max_machines}})
    limit = f"at most {max_machines} at a time" if max_machines else "with no limit of machines"
    console.show(f"✓ your agents may use {target}, {limit}. Take it back: zenthros agent deny {target}", out)


def deny(target, out=None):
    found = allowed()
    found.pop(target, None)
    _write(found)
    console.show(f"✓ your agents may not use {target}", out)


@contextlib.contextmanager
def turn():
    """One agent at a time counts and creates: three that start together on a
    limit of one must not all see none running."""
    os.makedirs(os.path.dirname(path()), mode=0o700, exist_ok=True)
    with open(f"{path()}.lock", "a", encoding="utf-8") as handle:
        _lock(handle)
        yield
