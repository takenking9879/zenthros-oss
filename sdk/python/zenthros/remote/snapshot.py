# SPDX-License-Identifier: Apache-2.0

"""A snapshot: the code a command runs elsewhere, as the worktree holds it now,
changes not committed included. Agents go on editing; what already runs does
not change.

Git says what is in the project (tracked, plus new files it does not ignore),
and zenthros.ignore what never leaves this computer, even if it was committed.
It goes to the store by its digest, where a function's project goes, and the
job pins that digest (runner.run_snapshot checks it before anything runs).
"""

import hashlib
import os
import stat
import subprocess
from dataclasses import dataclass, field

from zenthros import errors, ignore
from zenthros.remote import bundle

CODE_SHA256 = "zenthros.ai/code-sha256"
CODE_BASE = "zenthros.ai/code-base"
CODE_DIRTY = "zenthros.ai/code-dirty"
# What local runs left: results, not code.
LOCAL_RESULTS = ".zenthros/"


def _git(root, *args):
    try:
        done = subprocess.run(["git", "-C", root, *args], capture_output=True, check=False)
    except FileNotFoundError:
        raise errors.ValidationError("a snapshot needs git here: install it, or use code_dir=") from None
    if done.returncode != 0:
        raise errors.ValidationError(f"a snapshot needs a git repository, and {root} is not one: "
                                     f"{done.stderr.decode(errors='replace').strip()}")
    return done.stdout


def _state(paths):
    """What says a file changed: its size, time and mode."""
    found = {}
    for name, path in paths.items():
        info = os.stat(path)
        found[name] = (info.st_size, info.st_mtime_ns, info.st_mode)
    return found


@dataclass
class Snapshot:
    """What a snapshot takes: files {path in it: file}, and where it comes from."""

    root: str
    files: dict = field(default_factory=dict)
    excluded: list = field(default_factory=list)
    commit: str = ""
    branch: str = ""
    dirty: bool = False
    # The names git's index marks executable, on Windows, which keeps no
    # executable bit (os.access says yes to every file there); None elsewhere,
    # where each file's own bit says it, committed or not.
    executable: frozenset = None

    def _executable(self, name):
        if self.executable is not None:
            return name in self.executable
        return os.access(self.files[name], os.X_OK)

    def pack(self):
        """The tar.gz: the same bytes for the same contents. A file that
        changes meanwhile is packed again, once; never a mix of two moments."""
        for _ in range(2):
            before = _state(self.files)
            packed = bundle._pack(self.files, bundle.MAX_STORED_CODE_BYTES, self.root, executable=self._executable)
            after = _state(self.files)
            changed = sorted(name for name in before if before[name] != after[name])
            if not changed:
                return packed
        raise errors.ValidationError(f"{', '.join(changed)} keeps changing while the snapshot is taken: try again in a moment")

    def store(self, store, base):
        """{uri, sha256}, at <base>/code/<sha256>.tar.gz as a project's."""
        packed = self.pack()
        digest = hashlib.sha256(packed).hexdigest()
        uri = f"{base}/code/{digest}.tar.gz"
        store.put(uri, packed)
        return {"uri": uri, "sha256": digest}


def take(root):
    """The Snapshot of the repository root is in, at root."""
    root = os.path.abspath(root)
    listed = _git(root, "ls-files", "-co", "--exclude-standard", "-z").decode("utf-8", "surrogateescape")
    files, excluded = {}, []
    for name in sorted(set(filter(None, listed.split("\0")))):
        path = os.path.join(root, name)
        if name.startswith(LOCAL_RESULTS):
            continue
        if ignore.is_credential(name):
            excluded.append(name)
            continue
        # A tracked file removed, a link to a folder, a submodule: no file.
        if os.path.isfile(path) and stat.S_ISREG(os.stat(path).st_mode):
            files[name] = path
    commit = _git(root, "rev-parse", "HEAD").decode().strip()
    branch = _git(root, "rev-parse", "--abbrev-ref", "HEAD").decode().strip()
    dirty = bool(_git(root, "status", "--porcelain").strip())
    executable = None
    if os.name == "nt":
        staged = _git(root, "ls-files", "-s", "-z").decode("utf-8", "surrogateescape")
        executable = frozenset(entry.split("\t", 1)[1] for entry in staged.split("\0") if entry.startswith("100755 "))
    return Snapshot(root=root, files=files, excluded=excluded, commit=commit, branch=branch, dirty=dirty,
                    executable=executable)
