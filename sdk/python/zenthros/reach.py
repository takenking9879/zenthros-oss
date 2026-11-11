# SPDX-License-Identifier: Apache-2.0

"""Whether a destination can reach a path or a URI. A rule can only rule out: a
role may lack the permission, a credential may have run out, a network may
block. So there are three answers, and never "reachable" without a real try
(plan(probe=True) makes one).

impossible: stopped before anything is made. possible: nothing rules it out,
and its first error will say why. unchecked: Zenthros cannot know."""

import os
from dataclasses import dataclass

from zenthros.artifact import Artifact

IMPOSSIBLE = "impossible"
POSSIBLE = "possible"
UNCHECKED = "unchecked"
_BUCKETS = ("s3://", "gs://")


@dataclass(frozen=True)
class Reach:
    answer: str
    why: str = ""

    def to_dict(self):
        return {"answer": self.answer, "why": self.why}


def is_reference(value):
    """Whether value is a path or a URI to ask about."""
    return isinstance(value, str) and (value.startswith((*_BUCKETS, "file://", "http://", "https://")) or
                                       (bool(value) and not value.startswith("-") and os.path.exists(value)))


def references(value):
    """Every path or URI in a call's arguments, as text."""
    if isinstance(value, Artifact):
        yield value.uri
    elif isinstance(value, str):
        if is_reference(value):
            yield value
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from references(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from references(item)


def _travels(value, code_dir):
    """Whether a relative path names something of the project, which goes with the
    job and is at that same path where it runs."""
    if not code_dir or os.path.isabs(value) or "://" in value:
        return False
    from zenthros.remote import bundle

    try:
        named = os.path.relpath(os.path.abspath(value), os.path.abspath(code_dir)).replace(os.sep, "/")
    except ValueError:  # Windows: on another drive than the project, so not one of its files
        return False
    files = bundle.project_files(code_dir)
    return named in files or any(name.startswith(named.rstrip("/") + "/") for name in files)


def answers(value, target, code_dir="."):
    """[(reference, Reach)] for each path or URI in value, where target (None:
    the default place) would run it. code_dir: the project that goes with it."""
    from zenthros.jobs.local import NETWORK_ENV
    from zenthros.jobs.localk8s import shared_folder
    from zenthros.machines import default_on
    from zenthros.remote.call import _store
    from zenthros.remote.options import job_client, local_client, local_k8s_client, settings

    found = list(references(value))
    if not found:
        return []
    name = str(getattr(target, "name", "") or target or default_on())
    current = settings()
    client = local_client() if name == "local" else local_k8s_client() if name == "local-k8s" else job_client(current)
    offline, store = os.environ.get(NETWORK_ENV) == "none", _store(current, client)
    shared = "" if name == "local-k8s" and client.shared_now() is False else str(shared_folder())
    return [(ref, Reach(POSSIBLE, f"{ref} goes with your project") if name != "local" and _travels(ref, code_dir)
             else of(ref, name, store, offline, shared)) for ref in found]


def of(value, target, store="", offline=False, shared_folder=""):
    """How target ("local", "local-k8s", or any other, remote) reaches value.
    store: the store its calls use, which it has credentials for."""
    if value.startswith(("http://", "https://")):
        if offline:
            return Reach(IMPOSSIBLE, f"{value}: this runs with no network")
        return Reach(UNCHECKED, f"{value}: only the destination can tell")
    if value.startswith(_BUCKETS):
        if offline:
            return Reach(IMPOSSIBLE, f"{value}: this runs with no network; use a local path")
        if store and value.startswith(store.rstrip("/") + "/"):
            return Reach(POSSIBLE, f"{value} is in the store its calls use")
        if target in ("local", "local-k8s"):
            return Reach(POSSIBLE, f"{value}: it needs your credentials there (secrets=)")
        return Reach(UNCHECKED, f"{value} is not in the store its calls use: only {target} can tell; plan(probe=True) asks it")
    return _path(value, target, shared_folder)


def _path(value, target, shared_folder):
    path = os.path.abspath(value[len("file://"):] if value.startswith("file://") else value)
    if target == "local":
        return Reach(POSSIBLE, f"{path} goes in read-only, at its own path") if os.path.exists(path) else \
            Reach(IMPOSSIBLE, f"{path} is not on this computer")
    if target == "local-k8s":
        if shared_folder and (path + os.sep).startswith(shared_folder.rstrip(os.sep) + os.sep):
            return Reach(POSSIBLE, f"{path} is under the folder this cluster shares")
        if not shared_folder:
            from zenthros.jobs.localk8s import not_shared
            from zenthros.jobs.localk8s import shared_folder as folder

            return Reach(IMPOSSIBLE, not_shared(folder()))
        return Reach(IMPOSSIBLE, f"{path} is not under {shared_folder}, the folder this cluster shares: copy it there, "
                                 "or pass a bucket's URI")
    return Reach(IMPOSSIBLE, f"{path} is only on this computer: put it in the bucket (ws.upload) and pass the URI, or upload=True")


# The hosts a template's Container machines run on: what Vast and RunPod offer.
CONTAINER_HOST_ARCH = "amd64"


def image(ref, template):
    """Whether a template's machines (None: an attached machine or a pool, whose
    logins are its own) can pull ref. Asks the registry with no login; nothing is
    pulled. Only a machine's own cloud has a login there: a host someone else runs
    reads what anyone can read."""
    from zenthros import registry
    from zenthros.imagebuild import host_of

    readable = registry.anyone_can_read(ref)
    container = bool(template) and template.get("machineKind") == "Container"
    if readable and container:
        where = registry.architectures(ref)
        if where is not None and CONTAINER_HOST_ARCH not in where:
            return Reach(IMPOSSIBLE, f"{ref} is only {', '.join(where)}, and these machines are {CONTAINER_HOST_ARCH}: "
                                     f"publish it for them (zenthros env push --platform linux/{CONTAINER_HOST_ARCH})")
    if readable:
        return Reach(POSSIBLE, "anyone can read it")
    if readable is None:
        return Reach(UNCHECKED, "its registry did not answer from here")
    if not template:
        return Reach(UNCHECKED, "it needs a login, and only that machine knows whether it has one")
    host = host_of(ref)
    if not container and template.get("cloudIdentity") and any(host == known or host.endswith(known)
                                                               for known in template.get("imageRegistries") or ()):
        return Reach(POSSIBLE, "its machines read that registry with their role, if it has the permission")
    return Reach(IMPOSSIBLE, f"{ref} needs a login, and these machines have none for {host}: "
                             "publish it where anyone can read (zenthros env push NAME docker.io/you/NAME)")
