#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The pipeline's image (examples/pipeline/Dockerfile) is a machine for
zenthros.run: its jobs run `python -m zenthros.remote`, which reads its call
from the Workspace's s3:// store and unpickles it. What the SDK needs for
that (its dependencies, cloudpickle, and the s3 extra, boto3) the image
installs, pinned; the first E2E on a container failed reading the call:
"s3:// needs boto3".
"""
import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def names(lines):
    return {re.split(r"[<>=!~\[ ;]", line.strip(), maxsplit=1)[0].lower()
            for line in lines if line.strip() and not line.strip().startswith("#")}


class PipelineImageTest(unittest.TestCase):
    def test_the_image_has_what_the_runner_needs(self):
        project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
        remote = project["dependencies"] + project["optional-dependencies"]["s3"]
        requirements = (ROOT / "examples" / "pipeline" / "requirements.txt").read_text().splitlines()
        self.assertTrue(names(remote), "the SDK needs nothing to run a call")
        self.assertLessEqual(names(remote), names(requirements))
        for line in requirements:
            if line.strip() and not line.startswith("#"):
                self.assertRegex(line.strip(), r"^[A-Za-z0-9_.-]+==[0-9]", "pinned: the notebook and the machine agree")


if __name__ == "__main__":
    unittest.main()
