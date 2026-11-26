#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The example pipeline's limits (examples/pipeline/limits.py, profiles.py).

Inside a container limited to 2 CPUs, os.cpu_count() says what the host has
(24 here): a pipeline that sized itself from it ran 24 workers in 4 GiB and
was OOM-killed. The pipeline takes the smaller of what its profile asks and
what the container's cgroup allows, and never asks os.cpu_count().
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import limits  # noqa: E402
import profiles  # noqa: E402

GiB = 1 << 30


class CgroupCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = self.dir.name
        self.real_host_cpus, self.real_host_memory = limits.host_cpus, limits.host_memory
        for patcher in (mock.patch.object(limits, "host_cpus", return_value=24),
                        mock.patch.object(limits, "host_memory", return_value=31 * GiB)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def put(self, name, text):
        path = os.path.join(self.root, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(text + "\n")


class CgroupTest(CgroupCase):
    def test_cgroup_v2_as_docker_writes_it(self):
        self.put("cpu.max", "200000 100000")
        self.put("memory.max", str(4 * GiB))
        self.assertEqual((limits.cgroup_cpus(self.root), limits.cgroup_memory(self.root)), (2.0, 4 * GiB))
        self.put("cpu.max", "150000 100000")
        self.assertEqual(limits.cgroup_cpus(self.root), 1.5)

    def test_no_limit_is_none_not_a_number(self):
        self.put("cpu.max", "max 100000")
        self.put("memory.max", "max")
        self.assertEqual((limits.cgroup_cpus(self.root), limits.cgroup_memory(self.root)), (None, None))
        self.assertEqual((limits.cgroup_cpus(self.root + "/absent"), limits.cgroup_memory(self.root + "/absent")),
                         (None, None), "not Linux, or no cgroup mounted")

    def test_cgroup_v1(self):
        self.put("cpu/cpu.cfs_quota_us", "400000")
        self.put("cpu/cpu.cfs_period_us", "100000")
        self.put("memory/memory.limit_in_bytes", str(8 * GiB))
        self.assertEqual((limits.cgroup_cpus(self.root), limits.cgroup_memory(self.root)), (4.0, 8 * GiB))
        self.put("cpu/cpu.cfs_quota_us", "-1")
        self.put("memory/memory.limit_in_bytes", "9223372036854771712")
        self.assertEqual((limits.cgroup_cpus(self.root), limits.cgroup_memory(self.root)), (None, None))

    def test_a_file_that_is_not_a_limit_is_no_limit(self):
        self.put("cpu.max", "garbage")
        self.put("memory.max", "")
        self.assertEqual((limits.cgroup_cpus(self.root), limits.cgroup_memory(self.root)), (None, None))


class BoundsTest(CgroupCase):
    def test_the_smaller_of_the_profile_and_the_container(self):
        self.put("cpu.max", "200000 100000")
        self.put("memory.max", str(4 * GiB))
        self.assertEqual(limits.cpus(4, self.root), 2, "a 4-CPU profile in a 2-CPU container")
        self.assertEqual(limits.cpus(1, self.root), 1, "a profile that asks for less gets less")
        self.assertEqual(limits.memory_bytes(8 * GiB, self.root), 4 * GiB)
        self.assertEqual(limits.memory_bytes(2 * GiB, self.root), 2 * GiB)

    def test_without_a_cgroup_limit_the_machine_is_the_bound(self):
        self.assertEqual(limits.cpus(4, self.root), 4)
        self.assertEqual(limits.cpus(64, self.root), 24)
        self.assertEqual(limits.memory_bytes(64 * GiB, self.root), 31 * GiB)

    def test_part_of_a_cpu_is_one_thread_and_a_fraction_rounds_down(self):
        self.put("cpu.max", "50000 100000")
        self.assertEqual(limits.cpus(4, self.root), 1)
        self.put("cpu.max", "350000 100000")
        self.assertEqual(limits.cpus(4, self.root), 3, "a fourth thread would be throttled")

    def test_os_cpu_count_is_never_asked(self):
        self.put("cpu.max", "200000 100000")
        with mock.patch.object(os, "cpu_count", side_effect=AssertionError("os.cpu_count() sees the host")):
            self.assertEqual(limits.cpus(4, self.root), 2)
            self.assertGreaterEqual(self.real_host_cpus(), 1)

    def test_the_machines_memory_is_meminfos_total(self):
        path = os.path.join(self.root, "meminfo")
        with open(path, "w") as fh:
            fh.write("MemTotal:       32734028 kB\nMemFree:         5626700 kB\nMemAvailable:   26711656 kB\n")
        self.assertEqual(self.real_host_memory(path), 32734028 * 1024)
        self.assertGreater(self.real_host_memory(), GiB, "this machine's own")


class NestedCgroupTest(CgroupCase):
    """A job in a systemd unit of a machine that is itself a container (a
    Floci machine: cgroupns=host). Its limits are an ancestor's: the job's
    own cgroup and the ones between say max, the mount's root has no file.
    Read at the root only, the 2-CPU machine looked like 4 (the E2E)."""

    CONTAINER = "docker/9cc7784f3d99"
    JOB = CONTAINER + "/system.slice/zenthros-26f3.service"

    def own(self, path):
        own = os.path.join(self.root, "self-cgroup")
        with open(own, "w") as fh:
            fh.write(f"0::/{path}\n")
        return own

    def test_the_limit_of_an_ancestor_bounds_the_job(self):
        self.put(f"{self.CONTAINER}/cpu.max", "200000 100000")
        self.put(f"{self.CONTAINER}/memory.max", str(8 * GiB))
        for between in (self.CONTAINER + "/system.slice", self.JOB):
            self.put(f"{between}/cpu.max", "max 100000")
            self.put(f"{between}/memory.max", "max")
        own = self.own(self.JOB)
        self.assertEqual(limits.cpus(4, self.root, own), 2)
        self.assertEqual(limits.memory_bytes(31 * GiB, self.root, own), 8 * GiB)

    def test_the_smallest_limit_on_the_way_up_wins(self):
        self.put(f"{self.CONTAINER}/cpu.max", "400000 100000")
        self.put(f"{self.CONTAINER}/memory.max", str(8 * GiB))
        self.put(f"{self.JOB}/cpu.max", "100000 100000")
        self.put(f"{self.JOB}/memory.max", str(2 * GiB))
        own = self.own(self.JOB)
        self.assertEqual(limits.cpus(4, self.root, own), 1)
        self.assertEqual(limits.memory_bytes(8 * GiB, self.root, own), 2 * GiB)
        self.put("cpu.max", "100000 100000")  # the root's counts too
        self.put(f"{self.JOB}/cpu.max", "max 100000")
        self.assertEqual(limits.cpus(4, self.root, own), 1)

    def test_no_limit_anywhere_is_the_machine(self):
        own = self.own(self.JOB)
        self.assertEqual(limits.cpus(64, self.root, own), 24)
        self.assertEqual(limits.memory_bytes(64 * GiB, self.root, own), 31 * GiB)
        self.assertEqual(limits.cpus(64, self.root, os.path.join(self.root, "no-proc")), 24, "not Linux")


class ProfilesTest(unittest.TestCase):
    def test_the_two_sizes_are_configuration(self):
        small, full = profiles.get("small"), profiles.get("full")
        self.assertEqual((small.cpus, small.memory_gb), (2, 4))
        self.assertEqual((full.cpus, full.memory_gb), (4, 8))
        for size in ("customers", "orders", "months", "files", "train_rows"):
            self.assertGreater(getattr(full, size), getattr(small, size), size)

    def test_every_profile_spans_a_year_and_the_months_it_is_scored_on(self):
        for profile in profiles.PROFILES.values():
            self.assertGreaterEqual(profile.months, profiles.MIN_MONTHS, profile.name)

    def test_the_jvm_leaves_most_of_the_box_to_python_and_the_agent(self):
        for profile in profiles.PROFILES.values():
            heap = profiles.megabytes(profile.spark_driver_memory)
            self.assertLessEqual(heap, profile.memory_gb * 1024 // 2, profile.name)
            self.assertLessEqual(profiles.megabytes(profile.spark_max_result), heap // 4, profile.name)

    def test_the_profile_is_named_by_the_caller_or_the_environment(self):
        with mock.patch.dict(os.environ, {"PIPELINE_PROFILE": "full"}):
            self.assertEqual(profiles.get().name, "full")
            self.assertEqual(profiles.get("small").name, "small")
        with mock.patch.dict(os.environ, {"PIPELINE_PROFILE": ""}):
            self.assertEqual(profiles.get().name, "small", "the small one unless asked")
        with self.assertRaises(ValueError) as caught:
            profiles.get("huge")
        self.assertIn("full", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
