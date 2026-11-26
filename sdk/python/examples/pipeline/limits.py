# SPDX-License-Identifier: Apache-2.0

"""How many CPUs and how much memory this process really has.

Inside a container, os.cpu_count() and /proc/meminfo describe the host: a
container limited to 2 CPUs and 4 GiB on a 24-thread machine sees 24 and
31 GiB. Its real limits are its cgroup's, or an ancestor's: a job in a
systemd unit of a machine that is itself a container (a Floci machine)
sits below the cgroup that holds the limit. The pipeline sizes its threads and
its engines from the smaller of what its profile asks and what the cgroup
allows, so the same code is safe in the 2-CPU container and on the 4-CPU
machine. Standard library only.
"""

import os

CGROUP = "/sys/fs/cgroup"
SELF = "/proc/self/cgroup"
# cgroup v1 writes "no limit" as a number near 2**63.
_UNLIMITED = 1 << 60


def _read(path):
    try:
        with open(path, encoding="ascii") as fh:
            return fh.read().split()
    except (OSError, UnicodeDecodeError):
        return []


def _number(words, index=0):
    try:
        return int(words[index])
    except (IndexError, ValueError):
        return None


def cgroup_cpus(root=CGROUP):
    """The CPU quota of the cgroup at root, in CPUs; None when it has none."""
    quota_period = _read(os.path.join(root, "cpu.max"))  # v2: "200000 100000" or "max 100000"
    quota, period = _number(quota_period), _number(quota_period, 1)
    if quota is None:  # v1
        quota = _number(_read(os.path.join(root, "cpu", "cpu.cfs_quota_us")))
        period = _number(_read(os.path.join(root, "cpu", "cpu.cfs_period_us")))
    if not quota or not period or quota < 0:
        return None
    return quota / period


def cgroup_memory(root=CGROUP):
    """The memory limit of the cgroup at root, in bytes; None when it has none."""
    limit = _number(_read(os.path.join(root, "memory.max")))  # v2: bytes or "max"
    if limit is None:  # v1
        limit = _number(_read(os.path.join(root, "memory", "memory.limit_in_bytes")))
    if not limit or limit >= _UNLIMITED:
        return None
    return limit


def own_cgroups(root=CGROUP, self_cgroup=SELF):
    """The cgroups whose limits bound this process: its own (cgroup v2, from
    self_cgroup) and each one above it, up to root's."""
    path = ""
    for line in " ".join(_read(self_cgroup)).split():
        if line.startswith("0::"):
            path = line[3:]
    found = []
    parts = [part for part in path.split("/") if part]
    for depth in range(len(parts), -1, -1):
        found.append(os.path.join(root, *parts[:depth]))
    return found


def _smallest(values):
    values = [value for value in values if value is not None]
    return min(values) if values else None


def host_cpus():
    """The CPUs this process may run on. Not os.cpu_count(), which counts
    the machine's."""
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):  # not Linux
        return 1


def host_memory(meminfo="/proc/meminfo"):
    """MemTotal, in bytes; 0 when it cannot be read."""
    try:
        with open(meminfo, encoding="ascii") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def cpus(wanted, root=CGROUP, self_cgroup=SELF):
    """Threads to run: what the profile wants, at most what this container
    (else this machine) has. A fraction of a CPU rounds down: one thread
    more than the quota is a thread that waits."""
    quota = _smallest(cgroup_cpus(path) for path in own_cgroups(root, self_cgroup))
    have = int(quota) if quota is not None else host_cpus()
    return max(1, min(int(wanted), have))


def memory_bytes(wanted, root=CGROUP, self_cgroup=SELF):
    """Bytes to plan for: what the profile wants, at most what this
    container (else this machine) has."""
    have = _smallest(cgroup_memory(path) for path in own_cgroups(root, self_cgroup)) or host_memory()
    return min(int(wanted), have) if have else int(wanted)
