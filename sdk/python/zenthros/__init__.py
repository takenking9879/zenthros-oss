# SPDX-License-Identifier: Apache-2.0

"""Zenthros workload SDK.

The core (zenthros.core) talks to Zenthros: the job's env, reports,
checkpoints, the interruption notice, and Task. It is framework-agnostic:
a checkpoint holds JSON state and the files any library saves.
zenthros.run / map / gather / get (zenthros.remote) run code on a machine;
zenthros.serve runs a Service there. zenthros.login() (zenthros.auth) gets
the token this process carries, zenthros.workspace() (zenthros.view) is
what that token sees of its Workspace (zenthros.session() names it: one
client per process, its connections kept), and zenthros.LocalBackend
(zenthros.local) joins this computer to it, in a container.
zenthros.create / zenthros.delete (zenthros.machines) make and remove the
Workspace's machines, zenthros.install puts packages on one for every
function, and @zenthros.cloud's f.remote(on=...) runs there;
zenthros.targets() (zenthros.targetlist) is where it can run.
zenthros.aws makes an hour of S3 credentials for the prefixes a job uses,
sealed to its machine.
"""

from zenthros.core import preemption
from zenthros.core.checkpoint import Checkpointer, CheckpointPolicy, phase
from zenthros.core.env import JobEnv
from zenthros.core.report import Reporter, progress, report
from zenthros.core.task import Task
from zenthros.errors import JobFailed, ZenthrosError
from zenthros.fanout import children, spawn

_REMOTE = {"CloudFunction", "RemoteCall", "RemoteError", "Secret", "cloud", "configure", "gather", "get",
           "last_failure", "map", "run", "serve", "services"}


def __getattr__(name):
    # zenthros.remote loads on first use: `import zenthros` stays the core, and
    # `python -m zenthros.remote` does not find itself already imported.
    if name in _REMOTE:
        from zenthros import remote

        return getattr(remote, name)
    if name == "cache":
        import importlib

        return importlib.import_module("zenthros.cache")
    if name == "Pipeline":
        from zenthros import pipeline

        return pipeline.Pipeline
    if name == "Artifact":
        from zenthros import artifact

        return artifact.Artifact
    if name == "parquet":
        from zenthros import contracts

        return contracts.parquet
    if name == "grid":
        from zenthros.remote import spread

        return spread.grid
    if name in ("workspace", "session"):
        from zenthros import view

        return getattr(view, name)
    if name in ("login", "logout"):
        from zenthros import auth

        return getattr(auth, name)
    if name in ("attach", "create", "delete", "Machine"):
        from zenthros import machines

        return getattr(machines, name)
    if name == "install":
        from zenthros import installing

        return installing.install
    if name == "set_store":
        from zenthros import userstore

        return userstore.set_store
    if name in ("targets", "Targets"):
        from zenthros import targetlist

        return getattr(targetlist, name)
    if name == "LocalBackend":
        from zenthros import local

        return local.LocalBackend
    if name in ("aws", "env", "files"):
        import importlib

        return importlib.import_module(f"zenthros.{name}")
    raise AttributeError(f"module 'zenthros' has no attribute {name!r}")


__all__ = [
    "CheckpointPolicy", "Checkpointer", "CloudFunction", "JobEnv", "JobFailed", "LocalBackend", "Machine", "RemoteCall",
    "Artifact", "Pipeline", "RemoteError", "Reporter", "Secret", "Task", "ZenthrosError", "attach", "cache", "parquet", "children", "cloud", "configure", "create", "delete", "files", "gather", "get", "grid", "install",
    "last_failure", "login", "logout", "map",
    "phase", "preemption", "progress", "report", "run", "serve", "services", "session", "set_store", "spawn", "targets", "workspace",
]
