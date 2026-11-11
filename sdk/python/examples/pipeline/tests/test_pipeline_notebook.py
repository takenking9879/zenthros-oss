#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""flow.ipynb is ordinary code: a login, targets(), an upload, two decorated
functions, and .remote(on=, params=). Nothing in it names where S3 is, a
role, a key or zctl (the hub gives a job its store), and the E2E
(tests/e2e/test_notebook_flow.py) runs its functions' cell as it is.

The functions' requirements files are the pins of requirements.txt, so the
machine runs what the tests ran.
"""
import ast
import json
import re
import unittest
from pathlib import Path

PIPELINE = Path(__file__).resolve().parents[1]


def pins(name):
    lines = (PIPELINE / name).read_text().splitlines()
    return dict(line.split("==", 1) for line in (raw.strip() for raw in lines) if line and not line.startswith("#"))


def notebook_code():
    cells = json.loads((PIPELINE / "flow.ipynb").read_text())["cells"]
    return ["".join(c["source"]) for c in cells if c["cell_type"] == "code"]


def functions_cell():
    """The cell that defines the decorated functions: the one the E2E runs."""
    return next(cell for cell in notebook_code() if "@zenthros.cloud" in cell)


def decorators(source):
    """{function name: the @zenthros.cloud keywords, as source}."""
    found = {}
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef):
            for deco in node.decorator_list:
                if isinstance(deco, ast.Call) and ast.unparse(deco.func) == "zenthros.cloud":
                    found[node.name] = {k.arg: ast.literal_eval(k.value) for k in deco.keywords}
    return found


class NotebookTest(unittest.TestCase):
    def setUp(self):
        self.code = "\n".join(notebook_code())

    def test_it_is_ordinary_code(self):
        for call in ("zenthros.login()", "zenthros.targets()", "ws.upload(", 'zenthros.create("local"',
                     "zenthros.delete(", "zenthros.attach(", "dry_run=True"):
            self.assertIn(call, self.code)
        for gone in ("where", "import run", "s3_secrets", "AWS_", "ZENTHROS_CONFIG", "zctl", "role_arn", "secrets="):
            self.assertNotIn(gone, self.code)

    def test_two_functions_declare_what_they_need(self):
        found = decorators(functions_cell())
        self.assertEqual(found["entrenar"], {"requirements": "requirements-polars.txt", "cpu": 2, "memory": "4Gi"})
        self.assertEqual(found["tablas_spark"], {"requirements": "requirements-spark.txt", "tools": ["java"], "cpu": 2,
                                                 "memory": "4Gi"})

    def test_calls_say_where_and_with_what(self):
        remote = re.findall(r"\.remote\((.*?)\)\n", self.code)
        self.assertTrue(remote)
        for args in remote:
            self.assertIn("on=", args)
            self.assertIn("params=", args)

    def test_the_local_data_is_not_in_the_folder_that_travels(self):
        # Everything in the notebook's folder goes with each call (code_dir).
        self.assertIn("tempfile.gettempdir()", self.code)


class RequirementsTest(unittest.TestCase):
    def test_each_functions_requirements_are_the_pipelines_pins(self):
        everything = pins("requirements.txt")
        for name in ("requirements-polars.txt", "requirements-spark.txt"):
            own = pins(name)
            self.assertTrue(own, name)
            for package, version in own.items():
                self.assertEqual(everything.get(package), version, f"{name}: {package}")

    def test_the_spark_function_has_no_scikit_learn_and_the_other_no_spark(self):
        self.assertNotIn("pyspark", pins("requirements-polars.txt"))
        self.assertNotIn("scikit-learn", pins("requirements-spark.txt"))


if __name__ == "__main__":
    unittest.main()
