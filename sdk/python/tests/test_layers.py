#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The base install is the core (zenthros.core) and the zctl client, stdlib only.

Nothing in the base pulls a third-party package, so `pip install zenthros`
works in any job image, and no module of the SDK imports an ML framework:
it is framework-agnostic (the playground's trainers are its own, mlkit).
"""
import ast
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from zenthros import errors, zctl
from zenthros.infra import InfraClient
from zenthros.jobs.client import ZctlJobClient

SDK = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CORE = ["zenthros", "zenthros.core", "zenthros.core.task", "zenthros.core.checkpoint", "zenthros.core.agent",
        "zenthros.core.report", "zenthros.core.storage", "zenthros.core.env", "zenthros.core.preemption",
        "zenthros.fanout", "zenthros.jobs", "zenthros.jobs.http", "zenthros.config", "zenthros.infra", "zenthros.zctl", "zenthros.errors"]
PACKAGE = os.path.join(SDK, "zenthros")


def _sources(folder):
    for current, _, names in os.walk(folder):
        for name in sorted(names):
            if name.endswith(".py"):
                path = os.path.join(current, name)
                with open(path, encoding="utf-8") as fh:
                    yield os.path.relpath(path, SDK), fh.read()

PROBE = """
import sys
before = set(sys.modules)
for name in sys.argv[1:]:
    __import__(name)
loaded = set(sys.modules) - before
third = sorted({m.split(".")[0] for m in loaded} - set(sys.stdlib_module_names) - {"zenthros"})
print(third)
"""


class LayerTest(unittest.TestCase):
    def test_core_is_stdlib_only(self):
        out = subprocess.run([sys.executable, "-c", PROBE, *CORE], capture_output=True, text=True,
                             cwd=SDK, env={**os.environ, "PYTHONPATH": SDK}, check=True).stdout.strip()
        self.assertEqual(out, "[]", f"third-party modules loaded by the base: {out}")

    def test_core_does_not_import_an_installed_framework(self):
        # A job image with Ray (or torch, xgboost) installed must not pay for
        # it in every `import zenthros`: phase.py used to import ray.
        with tempfile.TemporaryDirectory() as fake:
            for name in ("ray", "torch", "xgboost"):
                os.makedirs(os.path.join(fake, name))
                with open(os.path.join(fake, name, "__init__.py"), "w") as fh:
                    fh.write("")
            probe = PROBE + "print(sorted(m for m in ('ray', 'torch', 'xgboost') if m in sys.modules))\n"
            out = subprocess.run([sys.executable, "-c", probe, *CORE], capture_output=True, text=True,
                                 cwd=SDK, env={**os.environ, "PYTHONPATH": SDK + os.pathsep + fake},
                                 check=True).stdout.strip().splitlines()
        self.assertEqual(out[-1], "[]", f"the base imported an installed framework: {out[-1]}")

    def test_no_module_imports_an_ml_framework(self):
        # The SDK runs any framework's code and checkpoints its files; it
        # imports none of them (pandas only for to_pandas(), on demand).
        frameworks = {"numpy", "polars", "pyarrow", "xgboost", "torch", "ray", "sklearn", "tensorflow", "jax"}
        offenders = []
        for path, source in _sources(PACKAGE):
            for node in ast.walk(ast.parse(source)):
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0:
                    names = [node.module]
                else:
                    continue
                # A contract on Parquet columns reads a footer with pyarrow, on
                # demand: whoever declares one has it.
                allowed = {"pyarrow"} if path == os.path.join("zenthros", "contracts.py") else set()
                offenders += [f"{path}: {n}" for n in names if n.split(".")[0] in frameworks - allowed]
        self.assertEqual(offenders, [])

    def test_core_imports_only_the_stdlib_and_itself(self):
        # zenthros.run ships zenthros/core/ in every call's bundle.
        allowed = {"zenthros.core", "zenthros.errors", "zenthros.logging"}
        offenders = []
        for path, source in _sources(os.path.join(PACKAGE, "core")):
            for node in ast.walk(ast.parse(source)):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0:
                    names = [node.module + "." + a.name for a in node.names] if node.module == "zenthros" \
                        else [node.module]
                for name in names:
                    top = name.split(".")[0]
                    if top == "zenthros":
                        if not any(name == a or name.startswith(a + ".") for a in allowed):
                            offenders.append(f"{path}: {name}")
                    elif top not in sys.stdlib_module_names and top not in ("boto3", "google"):
                        offenders.append(f"{path}: {name}")
        self.assertEqual(offenders, [])

    def test_job_env_is_read_in_one_place(self):
        # The variables the agent sets for a job are read by JobEnv only, so
        # a job, a Task and a trainer agree on what "in a job" means.
        import re

        from zenthros.core.env import JOB_VARS

        read = re.compile(r'(?:environ\.get\(|environ\[|getenv\()\s*"(%s)"' % "|".join(JOB_VARS))
        offenders = []
        for path, source in _sources(PACKAGE):
            if path == os.path.join("zenthros", "core", "env.py"):
                continue
            offenders += [f"{path}: {m}" for m in read.findall(source)]
        self.assertEqual(offenders, [])

    def test_missing_zctl_says_how_to_fix(self):
        # Not transient: a wait() that took it for one waited forever, in
        # silence, for a binary nobody would install (audit 2026-10-16 AUD-040).
        with tempfile.TemporaryDirectory() as d:
            missing = os.path.join(d, "zctl")
            with self.assertRaises(errors.ZctlMissing) as caught:
                InfraClient(zctl=missing).get("vmpool", "x")
            self.assertIn("ZENTHROS_ZCTL", str(caught.exception))
            with self.assertRaises(errors.ValidationError) as caught:
                ZctlJobClient(zctl=missing).cancel("x")
            self.assertNotIsInstance(caught.exception, errors.TransientError)
            self.assertIn("ZENTHROS_ZCTL", str(caught.exception))

    def test_infra_reads_the_same_zctl_variable_as_jobs(self):
        env = {k: v for k, v in os.environ.items() if k != "ZENTHROS_ZCTL"}
        env["ZENTHROS_ZCTL"] = "/opt/bin/zctl"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(InfraClient().zctl, "/opt/bin/zctl")
            self.assertEqual(ZctlJobClient().zctl, "/opt/bin/zctl")


class CheckoutZctlTest(unittest.TestCase):
    """In a checkout, `make -C operator sdk-zctl` builds zctl into bin/, next
    to sdk/; the SDK of that checkout runs it without PATH or a variable
    (test_checkout_layout.py holds the Makefile to that place)."""

    def setUp(self):
        env = {k: v for k, v in os.environ.items() if k != "ZENTHROS_ZCTL"}
        patcher = mock.patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.built = os.path.join(folder.name, "zctl")

    def build(self):
        with open(self.built, "w") as out:
            out.write("#!/bin/sh\n")
        os.chmod(self.built, 0o755)

    def test_a_built_zctl_is_used_before_path(self):
        self.build()
        with mock.patch.object(zctl, "CHECKOUT", self.built):
            self.assertEqual(InfraClient().zctl, self.built)
            self.assertEqual(ZctlJobClient().zctl, self.built)

    def test_the_variable_still_wins(self):
        self.build()
        os.environ["ZENTHROS_ZCTL"] = "/opt/bin/zctl"
        with mock.patch.object(zctl, "CHECKOUT", self.built):
            self.assertEqual(InfraClient().zctl, "/opt/bin/zctl")

    def test_without_one_built_it_is_zctl_on_path(self):
        with mock.patch.object(zctl, "CHECKOUT", self.built):
            self.assertEqual(InfraClient().zctl, "zctl")

if __name__ == "__main__":
    unittest.main()
