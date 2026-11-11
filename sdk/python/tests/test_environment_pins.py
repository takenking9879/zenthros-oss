# SPDX-License-Identifier: Apache-2.0

"""packages= and requirements= lines that name no version run with the one
this computer has (call._environment): what the function ran with here.

Installed here, wherever the SDK is: cloudpickle, its one dependency."""

import importlib.metadata
import os
import tempfile
import unittest
from unittest import mock

from zenthros.remote import call


def _version(name):
    return importlib.metadata.version(name)


def _lines(**kwargs):
    env = call._environment(kwargs.get("requirements"), kwargs.get("packages"), None)
    return env["requirements"].splitlines()


class EnvironmentPinsTest(unittest.TestCase):
    def test_a_bare_name_takes_this_computers_version(self):
        self.assertEqual(_lines(packages=["cloudpickle"]), [f"cloudpickle=={_version('cloudpickle')}"])

    def test_a_line_that_says_what_it_takes_stays(self):
        for line in ("polars>=1", "polars==0.20.1", "polars~=1.0", "polars!=1.2", "polars<2",
                     "polars @ https://example.com/polars.whl", "git+https://github.com/pola-rs/polars"):
            with self.subTest(line):
                self.assertEqual(_lines(packages=[line]), [line])

    def test_extras_and_marker_are_kept(self):
        self.assertEqual(_lines(packages=['cloudpickle[fast]; python_version>"3.9"']),
                         [f'cloudpickle[fast]=={_version("cloudpickle")}; python_version>"3.9"'])

    def test_one_not_installed_here_stays(self):
        self.assertEqual(_lines(packages=["surely-not-installed-zenthros-probe"]), ["surely-not-installed-zenthros-probe"])

    def test_one_installed_from_a_folder_or_a_url_stays(self):
        # pip records where it came from (direct_url.json): no index has it.
        class Installed:
            name, version = "mylib", "0.3.0"

            def __init__(self, direct_url):
                self.direct_url = direct_url

            def read_text(self, name):
                return self.direct_url if name == "direct_url.json" else None

        for direct_url in ('{"url": "file:///home/me/mylib", "dir_info": {"editable": true}}',
                           '{"url": "https://example.com/mylib-0.3.0.whl", "archive_info": {}}'):
            with self.subTest(direct_url), mock.patch.object(importlib.metadata, "distribution",
                                                             return_value=Installed(direct_url)):
                self.assertEqual(_lines(packages=["mylib"]), ["mylib"])
        with mock.patch.object(importlib.metadata, "distribution", return_value=Installed(None)):
            self.assertEqual(_lines(packages=["mylib"]), ["mylib==0.3.0"])

    def test_requirements_lines_are_pinned_alike(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "requirements.txt")
            with open(path, "w") as fh:
                fh.write("# the model\ncloudpickle\npandas>=2\n\n")
            self.assertEqual(_lines(requirements=path, packages=["surely-not-installed-zenthros-probe"]),
                             [f"cloudpickle=={_version('cloudpickle')}", "pandas>=2", "surely-not-installed-zenthros-probe"])

    def test_a_local_version_label_is_left_out(self):
        # torch from the PyTorch index is 2.15.0+cpu: no other index has
        # that build, and ==2.15.0 takes it where that index is (code review
        # 2026-11-08).
        dist = mock.Mock(version="2.15.0+cpu")
        dist.name = "torch"
        dist.read_text.return_value = None
        with mock.patch("importlib.metadata.distribution", return_value=dist):
            self.assertEqual(_lines(packages=["torch"]), ["torch==2.15.0"])

    def test_the_names_spelling_is_one_package(self):
        self.assertEqual(_lines(packages=["CloudPickle"]), _lines(packages=["cloudpickle"]))
        self.assertEqual(_lines(packages=["CloudPickle"]), [f"cloudpickle=={_version('cloudpickle')}"])


if __name__ == "__main__":
    unittest.main()
