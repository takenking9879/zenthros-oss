#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""A function's own environment on the machine: the packages its decorator
declares, in a venv cached by their hash, and the tools it names (java),
fetched pinned. Built before anything of the caller's is unpickled; the
runner then runs again inside it."""

import hashlib
import io
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from zenthros.remote import envs

from support import machine_side

REPO = Path(__file__).resolve().parents[3]


def jdk_tar(top="jdk-25.0.4.1+1"):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as tar:
        data = b"#!/bin/sh\necho java\n"
        info = tarfile.TarInfo(f"{top}/bin/java")
        info.size, info.mode = len(data), 0o755
        tar.addfile(info, io.BytesIO(data))
    return raw.getvalue()


class Runs:
    """A fake subprocess.run: records argv, does what venv would on disk."""

    def __init__(self, fail_install=False):
        self.calls = []
        self.fail_install = fail_install

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        if "venv" in argv:
            target = Path(argv[-1])
            (target / "bin").mkdir(parents=True, exist_ok=True)
            (target / "bin" / "python").write_text("")
            site = target / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
            site.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(argv, 0, "", "")
        if self.fail_install:
            return subprocess.CompletedProcess(argv, 1, "ERROR: No matching distribution found for nopkg==9\n", "")
        return subprocess.CompletedProcess(argv, 0, "installed\n", "")


class KeyTest(unittest.TestCase):
    def test_the_key_is_what_the_environment_is_made_of(self):
        base = envs.key({"requirements": "pandas==3.0.2\n", "tools": ["java"]})
        self.assertEqual(base, envs.key({"requirements": "pandas==3.0.2\n", "tools": ["java"]}))
        self.assertNotEqual(base, envs.key({"requirements": "pandas==3.0.1\n", "tools": ["java"]}))
        self.assertNotEqual(base, envs.key({"requirements": "pandas==3.0.2\n", "tools": []}))
        with mock.patch.object(envs, "_python_minor", return_value="3.99"):
            self.assertNotEqual(base, envs.key({"requirements": "pandas==3.0.2\n", "tools": ["java"]}))


@machine_side
class PrepareTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        env = mock.patch.dict(os.environ, {envs.CACHE_ENV: self.root})
        env.start()
        self.addCleanup(env.stop)
        self.out = io.StringIO()

    def test_packages_go_to_a_venv_that_sees_the_machines_and_is_built_once(self):
        runs = Runs()
        needs = {"requirements": "scikit-learn==1.8.0\n", "tools": []}
        with mock.patch.object(envs.shutil, "which", return_value=None):
            python, extra = envs.prepare(needs, out=self.out, run=runs)
            self.assertEqual(envs.prepare(needs, out=self.out, run=runs)[0], python)
        target = Path(self.root, "py-" + envs.key(needs) + "-" + envs._parent_key())
        self.assertEqual(python, str(target / "bin" / "python"))
        self.assertEqual(len(runs.calls), 2, "the second prepare built it again")
        self.assertEqual(runs.calls[0], [sys.executable, "-m", "venv", "--system-site-packages", str(target)])
        self.assertEqual(runs.calls[1][:4], [python, "-m", "pip", "install"])
        requirements = runs.calls[1][runs.calls[1].index("-r") + 1]
        self.assertEqual(Path(requirements).read_text(), "scikit-learn==1.8.0\n")
        # A venv made from inside one (the agent's) sees that one's packages.
        pth = next(target.glob("lib/python*/site-packages/zenthros-parent.pth"))
        self.assertTrue(set(envs._parent_site()) <= set(pth.read_text().split()))
        self.assertEqual(extra, {})

    def test_two_jobs_that_need_the_same_one_build_it_once(self):
        import threading
        import time

        runs = Runs()
        slow = lambda argv, **kw: (time.sleep(0.3), runs(argv, **kw))[1]  # noqa: E731
        needs = {"requirements": "polars\n", "tools": []}
        with mock.patch.object(envs.shutil, "which", return_value=None):
            jobs = [threading.Thread(target=envs.prepare, args=(needs,), kwargs={"out": self.out, "run": slow})
                    for _ in range(2)]
            for job in jobs:
                job.start()
            for job in jobs:
                job.join()
        self.assertEqual(len(runs.calls), 2, f"built twice: {runs.calls}")

    def test_another_interpreter_of_the_same_minor_builds_its_own(self):
        # A venv that sees the machine's packages installs only what they
        # lack: built from one interpreter, it was handed to another of the
        # same minor, and once the first was deleted its cloudpickle was gone
        # (the SDK suite, 2026-10-25).
        builds = {
            "the function's": lambda run: envs.prepare({"requirements": "polars\n", "tools": []},
                                                       out=self.out, run=run)[0],
            "the runner's": lambda run: envs.runner_env(["cloudpickle==3.1.2"], out=self.out, run=run),
        }
        for what, build in builds.items():
            with self.subTest(what), mock.patch.object(envs.shutil, "which", return_value=None):
                runs = Runs()
                with mock.patch.object(envs, "_parent_site", return_value=["/venv-a/site-packages"]):
                    first = build(runs)
                with mock.patch.object(envs, "_parent_site", return_value=["/venv-b/site-packages"]):
                    second = build(runs)
                self.assertNotEqual(first, second)
                self.assertEqual(len(runs.calls), 4, f"one env for two interpreters: {runs.calls}")
                pth = next(Path(second).parents[1].glob("lib/python*/site-packages/zenthros-parent.pth"))
                self.assertEqual(pth.read_text().split(), ["/venv-b/site-packages"])

    def test_uv_installs_when_the_machine_has_it(self):
        runs = Runs()
        with mock.patch.object(envs.shutil, "which", return_value="/opt/uv"):
            python, _ = envs.prepare({"requirements": "polars\n", "tools": []}, out=self.out, run=runs)
        self.assertEqual(runs.calls[1][:5], ["/opt/uv", "pip", "install", "--python", python])

    def test_a_failed_install_says_why_and_leaves_nothing_half_made(self):
        needs = {"requirements": "nopkg==9\n", "tools": []}
        with mock.patch.object(envs.shutil, "which", return_value=None):
            with self.assertRaises(envs.EnvironmentFailed) as caught:
                envs.prepare(needs, out=self.out, run=Runs(fail_install=True))
            self.assertIn("No matching distribution found for nopkg==9", str(caught.exception))
            self.assertFalse(Path(self.root, "py-" + envs.key(needs) + "-" + envs._parent_key()).exists())
            runs = Runs()
            envs.prepare(needs, out=self.out, run=runs)
        self.assertEqual(len(runs.calls), 2, "a failed build was taken for a ready one")

    def test_java_is_the_pinned_build_checked_before_it_is_unpacked(self):
        good = jdk_tar()
        pins = {"java": {"version": "25.0.4.1+1", "amd64": (hashlib.sha256(good).hexdigest(), "https://x/jdk.tar.gz"),
                         "arm64": (hashlib.sha256(good).hexdigest(), "https://x/jdk-arm.tar.gz")}}
        fetched = []

        def fetch(url, path):
            fetched.append(url)
            Path(path).write_bytes(good)

        with mock.patch.object(envs, "TOOLS", pins), mock.patch.object(envs, "_arch", return_value="amd64"):
            python, extra = envs.prepare({"requirements": "", "tools": ["java"]}, out=self.out, fetch=fetch)
            again = envs.prepare({"requirements": "", "tools": ["java"]}, out=self.out, fetch=fetch)
        home = Path(self.root, "java-25.0.4.1+1")
        self.assertEqual(python, sys.executable)
        self.assertEqual(extra["JAVA_HOME"], str(home))
        self.assertTrue(extra["PATH"].startswith(str(home / "bin") + os.pathsep))
        self.assertTrue((home / "bin" / "java").exists())
        self.assertEqual((fetched, again), (["https://x/jdk.tar.gz"], (python, extra)))

    def test_two_jobs_that_need_java_fetch_it_once(self):
        import threading
        import time

        good = jdk_tar()
        pins = {"java": {"version": "25.0.4.1+1", "amd64": (hashlib.sha256(good).hexdigest(), "https://x/jdk.tar.gz")}}
        fetched = []

        def fetch(url, path):
            time.sleep(0.3)
            fetched.append(url)
            Path(path).write_bytes(good)

        with mock.patch.object(envs, "TOOLS", pins), mock.patch.object(envs, "_arch", return_value="amd64"):
            jobs = [threading.Thread(target=envs.prepare, args=({"requirements": "", "tools": ["java"]},),
                                     kwargs={"out": self.out, "fetch": fetch}) for _ in range(2)]
            for job in jobs:
                job.start()
            for job in jobs:
                job.join()
        self.assertEqual(len(fetched), 1)

    def test_java_that_is_not_the_pinned_build_is_refused(self):
        pins = {"java": {"version": "25.0.4.1+1", "amd64": ("0" * 64, "https://x/jdk.tar.gz")}}
        with mock.patch.object(envs, "TOOLS", pins), mock.patch.object(envs, "_arch", return_value="amd64"):
            with self.assertRaises(envs.EnvironmentFailed) as caught:
                envs.prepare({"requirements": "", "tools": ["java"]}, out=self.out, fetch=lambda url, path: Path(path).write_bytes(jdk_tar()))
        self.assertIn("SHA-256", str(caught.exception))
        self.assertFalse(Path(self.root, "java-25.0.4.1+1").exists())

    def test_an_unknown_tool_is_refused(self):
        with self.assertRaises(envs.EnvironmentFailed):
            envs.prepare({"requirements": "", "tools": ["cobol"]}, out=self.out)


class PinsTest(unittest.TestCase):
    @unittest.skipUnless((REPO / "install" / "upstream.lock").exists(), "outside the repository")
    def test_java_is_the_installers_pin(self):
        rows = {}
        for line in (REPO / "install" / "upstream.lock").read_text().splitlines():
            parts = line.split()
            if len(parts) == 4 and parts[0].startswith("java@"):
                rows[parts[1]] = (parts[0].split("@", 1)[1], parts[2], parts[3])
        java = envs.TOOLS["java"]
        self.assertEqual({arch: (java["version"], *java[arch]) for arch in ("amd64", "arm64")}, rows)


def demo_wheel(directory):
    """A wheel of one module, zt_demo, made by hand: no build backend."""
    path = Path(directory, "zt_demo-1.0-py3-none-any.whl")
    files = {"zt_demo.py": "VALUE = 42\n",
             "zt_demo-1.0.dist-info/METADATA": "Metadata-Version: 2.1\nName: zt-demo\nVersion: 1.0\n",
             "zt_demo-1.0.dist-info/WHEEL": "Wheel-Version: 1.0\nGenerator: hand\nRoot-Is-Purelib: true\nTag: py3-none-any\n"}
    record = "".join(f"{name},,\n" for name in files) + "zt_demo-1.0.dist-info/RECORD,,\n"
    with zipfile.ZipFile(path, "w") as whl:
        for name, text in {**files, "zt_demo-1.0.dist-info/RECORD": record}.items():
            whl.writestr(name, text)
    return path


@machine_side
class RealVenvTest(unittest.TestCase):
    """The real thing, offline: a venv, a wheel from a directory, and the
    parent's packages still importable."""

    def test_the_venv_has_the_package_and_the_parents(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        wheels = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, wheels)
        demo_wheel(wheels)
        needs = {"requirements": f"--no-index\n--find-links {wheels}\nzt-demo==1.0\n", "tools": []}
        with mock.patch.dict(os.environ, {envs.CACHE_ENV: root}), mock.patch.object(envs.shutil, "which", return_value=None):
            python, _ = envs.prepare(needs, out=io.StringIO())
        probe = "import sys, zt_demo; print(zt_demo.VALUE); print('\\n'.join(sys.path))"
        done = subprocess.run([python, "-c", probe], capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        lines = done.stdout.splitlines()
        self.assertEqual(lines[0], "42")
        for parent in envs._parent_site():
            self.assertIn(parent, lines[1:])


if __name__ == "__main__":
    unittest.main()


class Reexec(Exception):
    pass


class RunnerTest(unittest.TestCase):
    """The runner builds the environment after it checks Python and before
    it unpickles the call, then runs the call again inside it."""

    def setUp(self):
        import pickle

        from zenthros.core.storage import MemoryBlobStore
        from zenthros.remote import bundle

        self.store = MemoryBlobStore()
        self.uri = "s3://zr-test/calls/f/call-0.pkl"
        self.needs = {"requirements": "polars\n", "tools": []}
        # Not a pickle: unpickling it would fail, so a test that gets past
        # the environment and reads it shows the order.
        self.store.put(self.uri, pickle.dumps({"format": 1, "kind": "call", "python": bundle.python_version(),
                                               "function": "f", "call": b"not a pickle", "packages": {},
                                               "environment": self.needs}, protocol=4))
        clean = mock.patch.dict(os.environ, {}, clear=False)
        clean.start()
        self.addCleanup(clean.stop)
        os.environ.pop(envs.READY_ENV, None)

    def run_it(self, **patches):
        from zenthros.remote import runner

        out = io.StringIO()
        # What the spec pins (setUp restores the environment).
        os.environ[runner.CALL_SHA256_ENV] = hashlib.sha256(self.store.get(self.uri)).hexdigest()
        with mock.patch.multiple(runner, **patches):
            try:
                code = runner.execute(self.uri, blob_store=self.store, out=out)
            except Reexec as exc:
                return "reexec", exc.args, out.getvalue()
        return code, None, out.getvalue()

    def test_it_runs_again_inside_the_environment_before_unpickling(self):
        def reexec(path, argv, env):
            raise Reexec(path, argv, env)

        prepare = mock.Mock(return_value=("/envs/py-x/bin/python", {"JAVA_HOME": "/envs/java"}))
        with mock.patch.object(envs, "prepare", prepare):
            code, args, _ = self.run_it(_reexec=reexec)
        self.assertEqual(code, "reexec")
        path, argv, env = args
        self.assertEqual((path, argv), ("/envs/py-x/bin/python",
                                        ["/envs/py-x/bin/python", "-m", "zenthros.remote", "exec", self.uri]))
        self.assertEqual((env[envs.READY_ENV], env["JAVA_HOME"]), (envs.key(self.needs), "/envs/java"))

    def test_inside_the_environment_it_is_not_built_again(self):
        os.environ[envs.READY_ENV] = envs.key(self.needs)
        prepare = mock.Mock(side_effect=AssertionError("built again"))
        with mock.patch.object(envs, "prepare", prepare):
            code, _, _ = self.run_it(_reexec=mock.Mock(side_effect=AssertionError("ran again")), _run=mock.Mock(return_value=0))
        prepare.assert_not_called()
        self.assertNotEqual(code, "reexec")

    def test_tools_alone_need_no_second_run(self):
        self.needs["requirements"] = ""
        import pickle

        envelope = pickle.loads(self.store.get(self.uri))
        envelope["environment"] = self.needs
        self.store.put(self.uri, pickle.dumps(envelope, protocol=4))
        prepare = mock.Mock(return_value=(sys.executable, {"JAVA_HOME": "/envs/java"}))
        with mock.patch.object(envs, "prepare", prepare):
            self.run_it(_reexec=mock.Mock(side_effect=AssertionError("ran again")), _run=mock.Mock(return_value=0))
        self.assertEqual(os.environ.get("JAVA_HOME"), "/envs/java")

    def test_an_environment_that_fails_is_the_calls_result(self):
        import pickle

        failing = mock.Mock(side_effect=envs.EnvironmentFailed("installing the function's packages: no polars"))
        with mock.patch.object(envs, "prepare", failing):
            code, _, out = self.run_it(_reexec=mock.Mock(side_effect=AssertionError("ran again")))
        self.assertEqual(code, 1)
        self.assertIn("no polars", out)
        record = pickle.loads(self.store.get("s3://zr-test/calls/f/result.pkl"))
        self.assertEqual((record["ok"], record["type"]), (False, "EnvironmentFailed"))


def triple(x):
    return 3 * x


class SubmitTest(unittest.TestCase):
    """What the decorator declares rides in the call, read when it is sent:
    the requirements file as it is then, relative to where the caller runs."""

    def setUp(self):
        import pickle

        from zenthros import remote
        from zenthros.core.storage import MemoryBlobStore
        from zenthros.jobs.fake import FakeJobClient

        self.pickle, self.remote = pickle, remote
        self.store = MemoryBlobStore()
        configured = remote.configure(store="s3://zr-test/calls", client=FakeJobClient(), blob_store=self.store,
                                      on="demo")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def environment(self, call):
        return self.pickle.loads(self.store.get(call.call_uri)).get("environment")

    def test_requirements_and_packages_and_tools_ride_in_the_call(self):
        Path(self.dir, "requirements.txt").write_text("# the model\nscikit-learn==1.8.0\n\npandas>=3  # frames\n")
        fn = self.remote.cloud(requirements="requirements.txt", packages=["polars==1.44.2"], tools=["java"])(triple)
        here = os.getcwd()
        os.chdir(self.dir)
        try:
            call = fn.submit(2, code_dir=None)
        finally:
            os.chdir(here)
        self.assertEqual(self.environment(call), {
            "requirements": "scikit-learn==1.8.0\npandas>=3  # frames\npolars==1.44.2\n", "tools": ["java"]})

    def test_a_function_that_declares_nothing_carries_nothing(self):
        call = self.remote.run(triple, 2, code_dir=None)
        self.assertIsNone(self.environment(call))

    def test_what_cannot_reach_the_machine_is_refused_before_anything_is_sent(self):
        from zenthros import errors

        Path(self.dir, "requirements.txt").write_text("-r base.txt\n")
        for bad in ({"requirements": os.path.join(self.dir, "requirements.txt")},
                    {"packages": ["-e ."]}, {"packages": ["./mylib"]}, {"packages": ["/opt/x.whl"]},
                    {"tools": ["cobol"]}, {"requirements": os.path.join(self.dir, "missing.txt")}):
            with self.assertRaises(errors.ValidationError, msg=str(bad)):
                self.remote.run(triple, 2, code_dir=None, **bad)
        self.assertEqual(self.store.data, {})

    def test_a_command_has_no_python_environment(self):
        from zenthros import errors

        with self.assertRaises(errors.ValidationError):
            self.remote.run(["true"], code_dir=None, packages=["polars"])
