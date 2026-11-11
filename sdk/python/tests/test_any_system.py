# SPDX-License-Identifier: Apache-2.0

"""What the SDK's first run on Windows found (wave 8, sdk-os.yml), pinned
where every system runs it: a Windows console that is not UTF-8, names with
"\\", no os.uname, no executable bit, no local cluster outside WSL2.

PYTHONIOENCODING=cp1252 gives a Linux process the code page of a Windows
program whose output goes to a file or a pipe.
"""

import contextlib
import io
import os
import platform
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from zenthros import console, identity, reach
from zenthros.core.env import JobEnv
from zenthros.core.report import new_snapshot
from zenthros.jobs import localk8s
from zenthros.remote import bundle, snapshot
from zenthros.remote.call import CALL_KIND, RemoteCall

from test_snapshot import Repo, git

SDK = Path(__file__).resolve().parents[1]


def cp1252():
    """A stream as Windows gives a program whose output is a file (its line
    ends left as written: the code page is what is tested)."""
    return io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="strict", newline="\n")


def said(stream):
    stream.flush()
    return stream.buffer.getvalue().decode("cp1252")


class ConsoleTest(unittest.TestCase):
    def test_a_sign_the_console_cannot_show_is_a_question_mark(self):
        out = cp1252()
        console.show("✓ done · 3 █", out)
        self.assertEqual(said(out), "? done · 3 ?\n")

    def test_a_receipt_does_not_fail_the_call_it_closes(self):
        # The receipt is written once the job succeeded: it raised there and
        # the call's result() with it.
        call = types.SimpleNamespace(timings={"submitted": 1.0, "finished": 4.0}, where="gcp-lab", kind=CALL_KIND,
                                     count=1, cost=lambda: None)
        out = cp1252()
        RemoteCall._receipt(call, out)
        self.assertTrue(said(out).startswith("? done in"), said(out))

    def test_the_command_says_its_lines_on_any_code_page(self):
        with tempfile.TemporaryDirectory() as home:
            env = {**os.environ, "PYTHONIOENCODING": "cp1252", "ZENTHROS_CONFIG": os.path.join(home, "config.yaml")}
            # A refusal: cli.main prints it with its own ✗.
            done = subprocess.run([sys.executable, "-c", "import sys; sys.argv[1:] = ['attach', 'x', '--type', 'laptop']; "
                                   "from zenthros.cli import entry; entry()"],
                                  cwd=SDK, env=env, capture_output=True)
        self.assertNotIn(b"UnicodeEncodeError", done.stderr, done.stderr.decode(errors="replace"))
        self.assertEqual(done.returncode, 1)
        self.assertTrue(done.stdout.startswith(b"? "), done.stdout)

    def test_the_command_is_its_entry(self):
        text = (SDK / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('zenthros = "zenthros.cli:entry"', text)


class NamesTest(unittest.TestCase):
    """Windows' relpath says "a\\b": what is compared with the bundle's names
    or hashed into an identity says "a/b" on every system."""

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = folder.name
        cache = mock.patch.dict(os.environ, {"XDG_CACHE_HOME": os.path.join(self.root, "cache")})
        cache.start()
        self.addCleanup(cache.stop)

    @contextlib.contextmanager
    def windows_names(self):
        real = os.path.relpath
        with mock.patch.object(os, "sep", "\\"), \
                mock.patch.object(os.path, "relpath", lambda path, start=os.curdir: real(path, start).replace("/", "\\")):
            yield

    def test_a_file_of_the_project_in_a_folder_travels(self):
        project = os.path.join(self.root, "project")
        os.makedirs(os.path.join(project, "conf"))
        Path(project, "conf", "train.yaml").write_text("lr: 1\n")
        # Run from the project, as its notebook does: Windows' relpath has
        # no answer across drives (the CI's checkout is on D:, its temp on C:).
        with contextlib.chdir(project):
            value = os.path.relpath(os.path.join(project, "conf", "train.yaml"))
            self.assertTrue(reach._travels(value, project))
            with self.windows_names():
                self.assertTrue(reach._travels(value, project), "conf\\train.yaml is not the bundle's conf/train.yaml")

    def test_the_projects_import_roots_are_its_folders_with_slashes(self):
        # `python src\\train.py` puts src on sys.path; the machine puts the
        # project's src/ back (runner, a Linux side). Here, with this
        # system's own names: Windows' short ones (RUNNER~1) included.
        project = os.path.join(self.root, "project")
        os.makedirs(os.path.join(project, "src", "pkg"))
        entries = [os.path.join(project, "src"), os.path.join(project, "src", "pkg"), self.root, project]
        with mock.patch.object(sys, "path", entries):
            self.assertEqual(bundle.import_roots(project), ["src", "src/pkg"])

    def test_a_path_on_another_drive_is_not_the_projects(self):
        # From D:\notebooks a relative path is on D:, the project on C:: not
        # one of the project's, and no ValueError of Windows' relpath
        # (public repository's Windows CI, 2026-11-11).
        project = os.path.join(self.root, "project")
        os.makedirs(project)
        two_drives = ValueError("path is on mount 'D:', start on mount 'C:'")
        with mock.patch.object(os.path, "relpath", side_effect=two_drives):
            self.assertFalse(reach._travels("data.csv", project))

    def test_a_folder_has_one_identity_on_every_system(self):
        folder = os.path.join(self.root, "table")
        os.makedirs(os.path.join(folder, "sub"))
        Path(folder, "sub", "a.txt").write_text("1")
        here = identity.path_digest(folder)[0]
        with self.windows_names():
            self.assertEqual(identity.path_digest(folder)[0], here)


class NoUnameTest(unittest.TestCase):
    def test_a_report_names_its_host_without_os_uname(self):
        # Windows has no os.uname; platform asks the system another way.
        with mock.patch("os.uname", side_effect=AttributeError("no os.uname"), create=True), \
                mock.patch.object(platform, "_uname_cache", None):
            snap = new_snapshot({"loss": 1}, env=JobEnv())
        self.assertTrue(snap["producer_id"])


class ExecutableBitTest(Repo):
    def test_on_windows_the_bit_is_what_git_keeps(self):
        # On disk the reverse of what git keeps: Windows' files say nothing,
        # its index does.
        self.write("scripts/run.sh", "#!/bin/sh\n", 0o644)
        self.write("src/b.py", "print()\n", 0o755)
        git(self.root, "add", "--chmod=+x", "scripts/run.sh")
        git(self.root, "add", "--chmod=-x", "src/b.py")
        with mock.patch.object(snapshot.os, "name", "nt"):
            taken = snapshot.take(self.root)
        with tarfile.open(fileobj=io.BytesIO(taken.pack())) as tar:
            modes = {member.name: member.mode for member in tar.getmembers()}
        self.assertEqual((modes["scripts/run.sh"], modes["src/b.py"], modes["src/a.py"]), (0o755, 0o644, 0o644))


class LocalClusterTest(unittest.TestCase):
    def test_windows_says_wsl2_for_a_local_cluster_too(self):
        with mock.patch("platform.system", return_value="Windows"), self.assertRaises(localk8s.errors.EngineMissing) as caught:
            localk8s.LocalK8sJobClient(root=tempfile.mkdtemp()).engine  # noqa: B018 - the property refuses
        self.assertIn("WSL2", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
