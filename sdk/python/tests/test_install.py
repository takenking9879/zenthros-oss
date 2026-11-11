#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros install (usability R5): packages every function on a machine
sees, as the machine's base for a Python minor.

Pinned: the same list again builds nothing ("up to date"); another is built
beside the current one and swapped in at once, so a job that started on the
old one keeps it; the line says only what changed; another minor's base is
left alone; on sys.path a function's own packages and the runner's come
before the base, and the base before the machine's own; a cluster is
refused before any job, with what to do instead.
"""
import hashlib
import io
import os
import sys
import tempfile
import unittest
from unittest import mock

from zenthros import errors, installing
from zenthros.remote import envs

from support import machine_side
from test_machines import Env

MINOR = f"{sys.version_info.major}.{sys.version_info.minor}"


class FakeInstaller:
    """subprocess.run for envs._venv: a venv is a directory with a
    site-packages; an install writes a .dist-info per pinned line."""

    def __init__(self):
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        if argv[1:3] == ["-m", "venv"]:
            target = argv[-1]
            os.makedirs(os.path.join(target, "bin"))
            os.makedirs(os.path.join(target, "lib", f"python{MINOR}", "site-packages", "pip-24.0.dist-info"))
            open(os.path.join(target, "bin", "python"), "w").close()
        else:
            listed = argv[argv.index("-r") + 1]
            site_dir = os.path.join(os.path.dirname(listed), "lib", f"python{MINOR}", "site-packages")
            with open(listed) as fh:
                pinned = fh.read().split()
            for line in pinned:
                name, version = line.split("==")
                os.makedirs(os.path.join(site_dir, f"{name.replace('-', '_')}-{version}.dist-info"))
        return mock.Mock(returncode=0, stdout="", stderr="")

    def builds(self):
        return sum(1 for argv in self.calls if argv[1:3] == ["-m", "venv"])


@machine_side
class Machine(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        patcher = mock.patch.dict(os.environ, {envs.CACHE_ENV: self.root})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.installer = FakeInstaller()
        self.addCleanup(lambda: __import__("shutil").rmtree(self.root, ignore_errors=True))

    def install(self, text):
        return envs.install_base(text, out=io.StringIO(), run=self.installer)


class BaseTest(Machine):
    def test_the_same_list_again_builds_nothing(self):
        first = self.install("pandas==2.2.3\nxgboost==2.1.3\n")
        self.assertFalse(first["up_to_date"])
        self.assertEqual(first["added"], [("pandas", "2.2.3"), ("xgboost", "2.1.3")], "a venv's own pip is not said")
        # Another order, a comment, a blank line: the same list.
        again = self.install("# ml\nxgboost==2.1.3\n\npandas==2.2.3  # data\n")
        self.assertEqual(again, {"python": MINOR, "up_to_date": True, "added": [], "removed": [], "changed": []})
        self.assertEqual(self.installer.builds(), 1)

    def test_another_list_is_swapped_in_and_says_only_the_difference(self):
        self.install("pandas==2.2.3\nxgboost==2.1.3\n")
        old = os.path.realpath(envs.base_link())
        changed = self.install("pandas==2.1.4\nscikit-learn==1.6.1\n")
        self.assertEqual(changed["added"], [("scikit-learn", "1.6.1")])
        self.assertEqual(changed["changed"], [("pandas", "2.2.3", "2.1.4")])
        self.assertEqual(changed["removed"], [("xgboost", "2.1.3")])
        new = os.path.realpath(envs.base_link())
        self.assertNotEqual(new, old)
        # A job that started on the old base keeps it (E-09).
        self.assertTrue(os.path.isdir(old), "the base a running job uses was removed")
        # The one before that goes.
        self.install("numpy==2.1.0\n")
        self.assertFalse(os.path.exists(old))
        self.assertTrue(os.path.isdir(new))

    def test_a_cache_reached_through_a_symlink_keeps_the_running_base(self):
        # The cache under a symlinked home or a mounted disk: the base a job
        # uses is compared as the same directory, not removed (rmtree).
        home = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(home, ignore_errors=True))
        reached = os.path.join(home, "envs")
        os.symlink(self.root, reached)
        with mock.patch.dict(os.environ, {envs.CACHE_ENV: reached}):
            self.install("pandas==2.2.3\n")
            old = os.path.realpath(envs.base_link())
            self.assertTrue(self.install("pandas==2.2.3\n")["up_to_date"])
            self.install("pandas==2.1.4\n")
        self.assertEqual(self.installer.builds(), 2)
        self.assertTrue(os.path.isdir(old), "the base a running job uses was removed")

    def test_another_minors_base_is_left_alone(self):
        other = os.path.join(self.root, "py-base-0123456789abcdef")
        os.makedirs(other)
        os.symlink(other, os.path.join(self.root, envs.BASE_PREFIX + "3.9"))
        self.install("pandas==2.2.3\n")
        self.install("pandas==2.1.4\n")
        self.install("pandas==2.0.3\n")
        self.assertTrue(os.path.isdir(other), "installing for one Python removed another's base")

    def test_the_base_holds_all_it_was_asked_for(self):
        # It sees none of the machine's packages: a machine with six 1.17.0
        # (boto3's) made pip skip six==1.17.0, and the base held nothing
        # (lab, 2026-10-18).
        self.install("six==1.17.0\n")
        made = next(argv for argv in self.installer.calls if argv[1:3] == ["-m", "venv"])
        self.assertNotIn("--system-site-packages", made)
        self.assertFalse(os.path.exists(os.path.join(envs.base_site(), "zenthros-parent.pth")))

    def test_nothing_to_install_is_refused(self):
        with self.assertRaises(envs.EnvironmentFailed):
            self.install("# nothing\n\n")


class PathTest(Machine):
    def test_the_function_and_the_runner_come_first_then_the_base_then_the_machine(self):
        self.assertIsNone(envs.use_base([]), "no base, nothing added")
        self.install("pandas==2.2.3\n")
        base = envs.base_site()
        function = os.path.join(self.root, "py-0123456789abcdef", "lib", f"python{MINOR}", "site-packages")
        runner = os.path.join(self.root, "py-runner-0123456789abcdef", "lib", f"python{MINOR}", "site-packages")
        machine = f"/usr/local/lib/python{MINOR}/site-packages"
        path = ["/work", f"/usr/local/lib/python{MINOR}", function, runner, machine, "/usr/lib/python3/dist-packages"]
        self.assertEqual(envs.use_base(path), base)
        # The runner's own packages before it: they are the runner's.
        self.assertEqual(path, ["/work", f"/usr/local/lib/python{MINOR}", function, runner, base, machine,
                                "/usr/lib/python3/dist-packages"])
        self.assertIsNone(envs.use_base(path), "added twice")

    def test_the_runner_reads_the_call_before_the_base_and_the_function_after(self):
        # The store's client (boto3) loaded first, so the base cannot break
        # it: a base with six 1.15 did on Python 3.13 (lab, 2026-10-18).
        # The function comes after, and sees the base.
        import pickle

        from zenthros.core.storage import MemoryBlobStore
        from zenthros.remote import bundle, runner

        order = []
        store = MemoryBlobStore()
        uri = "s3://zr-test/calls/f/call-0.pkl"
        store.put(uri, pickle.dumps({"format": 1, "kind": "call", "python": bundle.python_version(), "function": "f",
                                     "call": b"not a pickle", "packages": {}}, protocol=4))
        pinned = {runner.CALL_SHA256_ENV: hashlib.sha256(store.get(uri)).hexdigest()}
        get = store.get
        store.get = lambda key: order.append("store") or get(key)
        with mock.patch.dict(os.environ, pinned), mock.patch.object(envs, "use_base", lambda: order.append("base")), \
                mock.patch.object(runner, "_package_drift", lambda packages: order.append("function") or ([], [])), \
                mock.patch.object(runner, "_run", lambda *args: 0):
            runner.execute(uri, blob_store=store, out=io.StringIO())
        self.assertEqual(order[:3], ["store", "base", "function"])


class InstallTest(Env):
    def setUp(self):
        super().setUp()
        self.ran = []

        def run(fn, *args, on=None, **kwargs):
            self.ran.append((fn, args, on))
            result = mock.Mock()
            result.result.return_value = self.answer
            return result

        patcher = mock.patch("zenthros.remote.run", run)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.listed = [[{"backendId": "vm-1", "name": "my-vm", "backendType": "vm", "healthy": True},
                        {"backendId": "k8s-1", "name": "mi-k3s", "backendType": "k8s", "healthy": True}]]

    def test_the_list_runs_on_the_machine_and_one_line_says_what_changed(self):
        import zenthros

        self.answer = {"python": "3.12", "up_to_date": False, "added": [["xgboost", "2.1.3"], ["scikit-learn", "1.6.1"]],
                       "changed": [], "removed": []}
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            fh.write("scikit-learn\n")
        self.addCleanup(os.remove, fh.name)
        out = io.StringIO()
        zenthros.install("my-vm", requirements=fh.name, packages=["xgboost"], out=out)
        (fn, args, on), = self.ran
        self.assertIs(fn, envs.install_base)
        self.assertEqual((args, on), (("scikit-learn\nxgboost\n",), "my-vm"))
        self.assertRegex(out.getvalue(), r"^my-vm \(py3\.12\): \+ xgboost 2\.1\.3  \+ scikit-learn 1\.6\.1   ✓ \d+s\n$")

        self.answer = {"python": "3.12", "up_to_date": True, "added": [], "changed": [], "removed": []}
        out = io.StringIO()
        zenthros.install("my-vm", packages="xgboost", out=out)
        self.assertEqual(out.getvalue(), "my-vm (py3.12): up to date\n")

    def test_the_cli_is_the_twin(self):
        from zenthros import cli

        self.answer = {"python": "3.12", "up_to_date": True, "added": [], "changed": [], "removed": []}
        out = io.StringIO()
        self.assertEqual(cli.main(["install", "my-vm", "xgboost", "pandas==2.2"], out=out), 0)
        self.assertEqual(self.ran[0][1], ("pandas==2.2\nxgboost\n",))
        self.assertEqual(out.getvalue(), "my-vm (py3.12): up to date\n")

    def test_what_cannot_be_installed_is_refused_before_any_job(self):
        import zenthros

        for kwargs, says in ((dict(name="mi-k3s", packages=["x"]), "pods start fresh"),
                             (dict(name="my-vm"), "nothing to install"),
                             (dict(name="nope", packages=["x"]), "no machine named nope"),
                             (dict(name="my-vm", requirements="/nonexistent/req.txt"), "cannot read")):
            with self.assertRaises(errors.ValidationError, msg=kwargs) as raised:
                zenthros.install(out=io.StringIO(), **kwargs)
            self.assertIn(says, str(raised.exception))
        self.assertEqual(self.ran, [])

    def test_a_change_says_old_and_new(self):
        line = installing.said("my-vm", {"python": "3.12", "added": [], "removed": [["xgboost", "2.1.3"]],
                                          "changed": [["pandas", "2.2.3", "2.1.4"]]}, 21)
        self.assertEqual(line, "my-vm (py3.12): ~ pandas 2.2.3 → 2.1.4  - xgboost 2.1.3   ✓ 21s")


if __name__ == "__main__":
    unittest.main()
