# SPDX-License-Identifier: Apache-2.0

"""Every zenthros.ai/* key the SDK names is in the operator's list.

operator/api/v1/testdata/sdk-keys.json lists each label and annotation key
the SDK names, with its constant on each side; operator/test/lint
TestTheSDKsKeysAreTheOperators holds api/v1 and the chart's policy to it.
Here each SDK constant is its key, and the SDK names no key outside the
list, each in its constant only: a label written in one module and
filtered on in another by its own literal parts once one of them changes.
"""
import ast
import importlib
import json
import re
import unittest
from collections import defaultdict
from pathlib import Path

from zenthros.jobs.spec import API_VERSION

ROOT = Path(__file__).resolve().parents[3]
KEYS = json.loads((ROOT / "operator" / "api" / "v1" / "testdata" / "sdk-keys.json").read_text())["keys"]
SDK = ROOT / "sdk" / "python" / "zenthros"
KEY = re.compile(r"zenthros\.ai/[a-z0-9][a-z0-9.-]*")


def _sites():
    """{key: ["file:line", ...]} of each zenthros.ai/* key in the SDK's
    string literals. A docstring names a key; it does not use it."""
    sites = defaultdict(list)
    for path in sorted(SDK.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        docstrings = {id(n.value) for n in ast.walk(tree) if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
                for key in KEY.findall(node.value):
                    if key != API_VERSION:
                        sites[key].append(f"{path.relative_to(SDK.parent).as_posix()}:{node.lineno}")
    return sites


class KeysTest(unittest.TestCase):
    def test_each_sdk_constant_is_its_key(self):
        for entry in KEYS:
            with self.subTest(key=entry["key"]):
                module, name = entry["sdk"].rsplit(".", 1)
                self.assertEqual(getattr(importlib.import_module(module), name), entry["key"])

    def test_the_sdk_names_each_listed_key_in_its_constant_only(self):
        sites = _sites()
        listed = {entry["key"]: entry["sdk"] for entry in KEYS}
        for key, where in sorted(sites.items()):
            with self.subTest(key=key):
                if key not in listed:
                    self.fail(f"the SDK names {key} at {where}: list it in operator/api/v1/testdata/sdk-keys.json")
                module = listed[key].rsplit(".", 1)[0].replace(".", "/") + ".py"
                self.assertEqual(len(where), 1, f"{key} is written at {where}: use {listed[key]}")
                self.assertTrue(where[0].startswith(module + ":"), f"{key} is written at {where}, not in {listed[key]}")
        stale = sorted(set(listed) - set(sites))
        self.assertFalse(stale, f"sdk-keys.json lists {stale}, which the SDK no longer names")


if __name__ == "__main__":
    unittest.main()
