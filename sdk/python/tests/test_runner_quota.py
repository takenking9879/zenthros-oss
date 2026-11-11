#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""What the runner says of the machine it runs on: the CPUs it may use.

A container or a pod that is a machine has a CPU quota (create("local",
cpus=1), create("k8s:...", cpus=1)); os.cpu_count() is its host's. The
runner said "24 CPUs" on a one-CPU pod (found by the k8s pod E2E).
"""
import os
import tempfile
import unittest

from zenthros.remote import runner


class QuotaTest(unittest.TestCase):
    def cgroup(self, own, files):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        for path, value in files.items():
            os.makedirs(os.path.join(root.name, path.strip("/")), exist_ok=True)
            with open(os.path.join(root.name, path.strip("/"), "cpu.max"), "w") as f:
                f.write(value)
        self_cgroup = os.path.join(root.name, "self-cgroup")
        with open(self_cgroup, "w") as f:
            f.write(f"0::{own}\n")
        return root.name, self_cgroup

    def test_the_smallest_quota_of_its_cgroup_and_its_parents(self):
        root, me = self.cgroup("/kubepods/pod1/c1", {"/": "max 100000", "/kubepods/pod1": "150000 100000",
                                                     "/kubepods/pod1/c1": "max 100000"})
        self.assertEqual(runner.cpu_quota(root, me), 1.5)
        root, me = self.cgroup("/kubepods/pod1/c1", {"/kubepods": "400000 100000", "/kubepods/pod1": "150000 100000",
                                                     "/kubepods/pod1/c1": "300000 100000"})
        self.assertEqual(runner.cpu_quota(root, me), 1.5)

    def test_no_quota_is_none(self):
        root, me = self.cgroup("/", {"/": "max 100000"})
        self.assertIsNone(runner.cpu_quota(root, me))
        self.assertIsNone(runner.cpu_quota("/nonexistent", "/nonexistent"))

    def test_the_cpus_it_says_are_the_quota_when_smaller(self):
        self.assertEqual(runner.cpus_text(24, 1.0), "1 of 24 CPUs")
        self.assertEqual(runner.cpus_text(24, 1.5), "1.5 of 24 CPUs")
        self.assertEqual(runner.cpus_text(4, 8.0), "4 CPUs")
        self.assertEqual(runner.cpus_text(4, None), "4 CPUs")


if __name__ == "__main__":
    unittest.main()
