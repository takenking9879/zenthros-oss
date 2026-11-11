# SPDX-License-Identifier: Apache-2.0

"""The official development environment has every package of requirements-dev.txt.

Under make test (ZENTHROS_DEV_ENV=1), a package of that file that is missing
or at another version fails here, instead of its tests skipping in silence,
and the Python is the one of .python-version. sdk-os.yml installs the same
file on the oldest and newest Python the SDK supports (ZENTHROS_DEV_ENV=pins):
there only the packages are checked, each line whose marker holds there.
Outside both (CI's minimal job, a user's venv) this test skips.
"""

import importlib.metadata
import os
import pathlib
import sys
import unittest

SDK = pathlib.Path(__file__).resolve().parents[1]
MODE = os.environ.get("ZENTHROS_DEV_ENV")


def pins(path: pathlib.Path) -> dict[str, str]:
    """name -> version of each line of path whose marker holds here."""
    from packaging.requirements import Requirement  # with pytest and build, in requirements-dev.txt's set

    out = {}
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        req = Requirement(line)
        if req.marker is not None and not req.marker.evaluate():
            continue
        exact = [s.version for s in req.specifier if s.operator == "=="]
        out[req.name] = exact[0] if len(exact) == 1 else ""
    return out


@unittest.skipUnless(MODE in ("1", "pins"), "outside make test (sdk/python/.venv) and sdk-os.yml")
class DevEnvironmentTest(unittest.TestCase):
    def test_every_pin_is_installed_at_its_version(self):
        wanted = pins(SDK / "requirements-dev.txt")
        self.assertTrue(wanted)
        for name, version in wanted.items():
            with self.subTest(package=name):
                self.assertTrue(version, f"{name} has no single == version in requirements-dev.txt")
                try:
                    have = importlib.metadata.version(name)
                except importlib.metadata.PackageNotFoundError:
                    self.fail(f"{name} is not installed: make env")
                self.assertEqual(have, version, f"{name} {have}, want {version}: make env")

    @unittest.skipUnless(MODE == "1", "sdk-os.yml runs every Python the SDK supports")
    def test_the_python_is_the_one_of_python_version(self):
        want = (SDK / ".python-version").read_text().strip()
        self.assertEqual(f"{sys.version_info.major}.{sys.version_info.minor}", want)

    @unittest.skipUnless(MODE == "1", "the notebooks' E2E run in sdk/python/.venv")
    @unittest.skipUnless((SDK / "tests" / "e2e").is_dir(), "no E2E run the notebooks here (the public tree)")
    def test_what_the_e2e_notebooks_import_is_here(self):
        # tests/e2e runs these notebooks' cells in this process: a package
        # one imports that requirements-dev.txt lacks failed the E2E, not
        # make test (demo_remote_ml.ipynb's matplotlib, Floci suite of
        # 2026-11-08). The notebook's own modules and zenthros are not packages.
        import importlib.util
        import json
        import re

        for notebook in (SDK / "examples" / "remote" / "demo_remote_ml.ipynb", SDK / "examples" / "pipeline" / "flow.ipynb"):
            own = {p.stem for p in notebook.parent.glob("*.py")} | {"zenthros"}
            cells = json.loads(notebook.read_text())["cells"]
            source = "\n".join("".join(c["source"]) for c in cells if c["cell_type"] == "code")
            names = {m.split(".")[0] for m in re.findall(r"^\s*(?:import|from)\s+([\w.]+)", source, re.M)}
            for name in sorted(names - own - set(sys.stdlib_module_names)):
                with self.subTest(notebook=notebook.name, module=name):
                    self.assertIsNotNone(importlib.util.find_spec(name), f"{name} is not in requirements-dev.txt")

    def test_a_marker_picks_the_line_of_this_python(self):
        here = pins(SDK / "requirements-dev.txt")
        self.assertIn("numpy", here, "one numpy line holds on every Python the SDK supports")
        self.assertEqual(sum(name.startswith("xgboost") for name in here), 1, here)
