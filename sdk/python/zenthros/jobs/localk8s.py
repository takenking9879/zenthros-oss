# SPDX-License-Identifier: Apache-2.0

"""on="local-k8s": no hub either, on the cluster kubectl reaches. Each job is a
pod with its own image, and Kubernetes shares out what the nodes have.

    train.remote(x, on="local-k8s")

The same client as local (zenthros.jobs.local) with another way in: the agent
runs from the chart a cluster attached to a Workspace installs, with
agent.local, in a namespace of its own, and is reached by kubectl exec.

What a pod reads and writes is a folder every node has at the same path, so a
URI means the same here and there. The documented cluster has it:

    k3d cluster create zenthros --registry-create zenthros-registry:0.0.0.0:5111 \\
        -v "$HOME/.zenthros/k8s:$HOME/.zenthros/k8s@all"
"""

import hashlib
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from zenthros import errors
from zenthros.jobs.local import (
    AGENT_READY_SECONDS,
    WINDOWS_RUNS_IN_WSL,
    Box,
    Engine,
    LocalJobClient,
    _default_image,
    _offline_hint,
    _settings,
    agent_pin,
    cache_root,
)

TARGET = "local-k8s"
NAMESPACE = "zenthros-local"
RELEASE = "zenthros-agent"
CONTAINER = "agent"
AGENT_BINARY = "/agent"
CHART_FILE = "zenthros-agent-chart.tgz"
# agent.LocalSharedMark: what the agent reads here proves its node has the folder.
SHARED_MARK = ".zenthros-shared"
# A checkout's chart and image, instead of those the wheel pins.
CHART_ENV = "ZENTHROS_LOCAL_CHART"
IMAGE_ENV = "ZENTHROS_LOCAL_AGENT_IMAGE"
# Yes to installing the agent, where no terminal can ask.
INSTALL_ENV = "ZENTHROS_LOCAL_K8S_INSTALL"
CLUSTER_COMMAND = ('k3d cluster create zenthros --registry-create zenthros-registry:0.0.0.0:5111 '
                   '-v "$HOME/.zenthros/k8s:$HOME/.zenthros/k8s@all"')


def no_cluster(why=""):
    return (f"no Kubernetes cluster in kubectl{f' ({why})' if why else ''}: `{CLUSTER_COMMAND}` "
            "(needs Docker), or turn on Docker Desktop's Kubernetes")


def not_shared(folder):
    return (f"this cluster's nodes do not have {folder}, so a pod cannot read a path of this computer. Pass a bucket's "
            f"URI, or create the cluster with that folder: `{CLUSTER_COMMAND}`")


def shared_folder():
    return Path.home() / ".zenthros" / "k8s"


def current_context():
    """The cluster on="local-k8s" means, or "" if kubectl names none."""
    wanted = _settings().kube_context
    if wanted:
        return wanted
    binary = shutil.which("kubectl")
    if not binary:
        return ""
    done = subprocess.run([binary, "config", "current-context"], capture_output=True, text=True, timeout=30, check=False)
    return done.stdout.strip() if done.returncode == 0 else ""


class LocalK8sJobClient(LocalJobClient):
    """Jobs as pods of the cluster kubectl reaches."""

    place_name = "local-k8s"
    poll_seconds = 1.0
    reruns_here = False
    out_of_memory = "it ran out of the memory it asked for: raise memory="

    def __init__(self, root=None, kube=None, helm=None, ask=None):
        super().__init__(root, engine=kube)
        self.root = self.root.parent / "local-k8s"
        self._helm = helm
        self._ask = ask or _ask_in_a_terminal
        self._ready = False
        self._shared = False
        self._nodes_seen = None
        self._platforms = {}
        self._env_images = {}

    # --- where things are -------------------------------------------------

    @property
    def engine(self):
        if self._engine is None:
            if platform.system() == "Windows":
                raise errors.EngineMissing(WINDOWS_RUNS_IN_WSL)
            binary = shutil.which("kubectl")
            if not binary:
                raise errors.EngineMissing(no_cluster("no kubectl here"))
            self._engine = Engine("kubectl", binary)
        return self._engine

    @property
    def helm(self):
        if self._helm is None:
            binary = shutil.which("helm")
            if not binary:
                raise errors.EngineMissing("on='local-k8s' installs the agent with helm: install helm, then run this again")
            self._helm = Engine("helm", binary)
        return self._helm

    @property
    def store_dir(self):
        return shared_folder() / self.id / "store"

    def output_for(self, name):
        folder = self.store_dir / name
        folder.mkdir(parents=True, exist_ok=True)
        return str(folder)

    def _context_args(self):
        wanted = _settings().kube_context
        return ["--context", wanted] if wanted else []

    def _kubectl(self, *args, stdin=None, timeout=120):
        return self.engine.run([*self._context_args(), *args], stdin=stdin, timeout=timeout)

    def need_paths(self, paths):
        """What a pod can read is the shared folder: any other path of this
        computer is not there, and no job should find that out by failing."""
        shared = shared_folder().resolve()
        if paths and not self.sees_store():
            raise errors.ValidationError(not_shared(shared))
        for path in paths:
            if not Path(path).resolve().is_relative_to(shared):
                raise errors.ValidationError(
                    f"{path} is not under {shared}, the folder this cluster shares: copy it there, or pass a bucket's URI")

    # --- the agent --------------------------------------------------------

    def _container_name(self, box):
        return RELEASE

    def _deployment(self):
        """The agent's Deployment as the cluster has it, or None."""
        code, out, err = self._kubectl("get", "deploy", RELEASE, "-n", NAMESPACE, "-o", "json", "--ignore-not-found", timeout=60)
        if code != 0:
            raise errors.ValidationError(no_cluster(err.strip()))
        return json.loads(out) if out.strip() else None

    def _inspect(self, name):
        found = self._deployment()
        if found is None:
            return None
        ready = (found.get("status") or {}).get("availableReplicas") or 0
        return {"state": "running" if ready else "starting", "config": "", "mounts": {}, "oom": False}

    def _exec_args(self, container, *args):
        return [*self._context_args(), "exec", "-i", "-n", NAMESPACE, f"deploy/{container}", "-c", CONTAINER, "--",
                AGENT_BINARY, "local", *args]

    def ensure(self, box):
        """The agent, answering, on a cluster that has the shared folder.
        Installed the first time, once someone says yes."""
        if self._ready:
            return RELEASE
        image = self._agent_image()
        folder = shared_folder().resolve()
        found = self._deployment()
        if found is None:
            self._ask(self._where())
        if found is None or _installed_as(found) != (image, str(folder)):
            self._install(image, folder)
        self.store_dir.mkdir(parents=True, exist_ok=True)
        mark = self._mark(folder)
        self._shared = self._wait_agent(RELEASE).get("sharedMark") == mark
        self._ready = True
        return RELEASE

    def sees_store(self):
        """Whether its pods see the shared folder. If not, a call goes in the job
        itself, and its result in the job's report."""
        self.ensure(None)
        return self._shared

    def shared_now(self):
        """What sees_store would say, installing nothing: None until an agent can say."""
        if self._ready:
            return self._shared
        try:
            if self._deployment() is None:
                return None
            code, out, _ = self._exec(RELEASE, "info")
        except errors.ValidationError:
            return None
        return json.loads(out).get("sharedMark") == self._mark(shared_folder().resolve()) if code == 0 else None

    def _where(self):
        return current_context() or "the current cluster"

    def _mark(self, folder):
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / SHARED_MARK
        if not path.exists():
            path.write_text(secrets.token_hex(8) + "\n")
        return path.read_text().strip()

    def _install(self, image, folder):
        print(f"zenthros: installing the agent on {self._where()} (namespace {NAMESPACE})", file=sys.stderr, flush=True)
        chart = self._chart()
        context = _settings().kube_context
        args = ["upgrade", "--install", RELEASE, chart, "-n", NAMESPACE, "--create-namespace", "--wait", "--timeout", "5m",
                *(["--kube-context", context] if context else []),
                "--set", "agent.local.enabled=true",
                "--set-string", f"agent.local.sharedFolder={_helm_value(folder)}",
                "--set-string", f"agent.local.user={os.getuid()}:{os.getgid()}",
                "--set-string", f"agent.image={_helm_value(image)}"]
        code, _, err = self.helm.run(args, timeout=360)
        if code != 0:
            raise errors.ValidationError(f"installing the agent: {err.strip()}{_offline_hint(err)}")

    def _agent_image(self):
        image = os.environ.get(IMAGE_ENV) or (agent_pin() or {}).get("agent_image")
        if not image:
            raise errors.ValidationError(f"this SDK comes with no agent image (a checkout?): build one, and set {IMAGE_ENV} to it")
        return image

    def _chart(self):
        """The chart, checked against the release this wheel pins: SHA256SUMS
        against the pin, the chart against SHA256SUMS. Kept, so that it needs
        no network again."""
        given = os.environ.get(CHART_ENV)
        if given:
            return given
        pin = agent_pin()
        if not pin:
            raise errors.ValidationError(f"this SDK comes with no agent release (a checkout?): set {CHART_ENV} to the chart")
        chart = cache_root() / "chart" / pin["sha256sums_sha256"][:16] / CHART_FILE
        if chart.exists():
            return str(chart)
        sums = _download(f"{pin['install_base']}/SHA256SUMS")
        if hashlib.sha256(sums).hexdigest() != pin["sha256sums_sha256"]:
            raise errors.ValidationError(f"SHA256SUMS at {pin['install_base']} is not the one this SDK pins: nothing was installed")
        data = _download(f"{pin['install_base']}/{CHART_FILE}")
        if hashlib.sha256(data).hexdigest() not in _listed(sums, CHART_FILE):
            raise errors.ValidationError(f"{CHART_FILE} at {pin['install_base']} is not the one its SHA256SUMS lists: nothing was installed")
        chart.parent.mkdir(parents=True, exist_ok=True)
        chart.write_bytes(data)
        return str(chart)

    def _wait_agent(self, name):
        deadline = time.monotonic() + AGENT_READY_SECONDS
        while True:
            code, out, err = self._exec(name, "info", "--wait", "2s")
            if code == 0:
                return json.loads(out)
            if time.monotonic() >= deadline:
                raise errors.TransientError(f"the agent on {self._where()} did not answer in {AGENT_READY_SECONDS} s: {err.strip()}")
            time.sleep(1)

    # --- what a job needs of the cluster ----------------------------------

    def _box_for(self, manifest):
        """A pod's image and size are its own, so nothing is shared to fix but
        whether the cluster can run it at all. The image goes in the job: no
        container's stands for it."""
        template = manifest["spec"]["workload"]["template"]
        if template.get("service"):
            raise errors.ValidationError("a service with no hub runs on local: serve(on='local')")
        compute = manifest["spec"].get("compute") or {}
        image = template.get("image") or _default_image()
        template["image"] = image
        gpu = bool(compute.get("gpuCount") or compute.get("gpuModel"))
        self._fits(image, gpu)
        return Box(image=image, gpu=gpu)

    def _registry_hosting(self):
        """(address from here, address from the nodes) of the cluster's own
        registry, as a cluster says it (KEP-1755; k3d's --registry-create
        writes it), or None."""
        code, out, err = self._kubectl("get", "configmap", "local-registry-hosting", "-n", "kube-public",
                                       "--ignore-not-found", "-o", r"jsonpath={.data.localRegistryHosting\.v1}", timeout=60)
        if code != 0:
            raise errors.ValidationError(no_cluster(err.strip()))
        said = dict((key.strip(), value.strip()) for key, _, value in (line.partition(":") for line in out.splitlines()))
        host = said.get("host", "")
        return (host, said.get("hostFromContainerRuntime") or host) if host else None

    def image_of_env(self, environment):
        """The image of an environment built here, where this cluster's pods
        can pull it. Its architecture is known without asking a registry."""
        from zenthros import env as environments

        image = self._env_images.get((environment.name, environment.build.get("input")))
        if image is None:
            image = environments.on_cluster(environment, self._registry_hosting())
            self._env_images[(environment.name, environment.build.get("input"))] = image
        self._platforms[image] = [environment.platform.rpartition("/")[2]]
        return image

    def pull_hint(self, message):
        """An environment built here that its nodes cannot reach: nothing to do
        with the network, whatever the error's words."""
        if "localhost/zenthros-env/" in message:
            return ("\nThis cluster does not see the images of this computer's engine, and it has no registry of its "
                    "own to send them to: k3d cluster create zenthros --registry-create "
                    'zenthros-registry:0.0.0.0:5111 -v "$HOME/.zenthros/k8s:$HOME/.zenthros/k8s@all"')
        return super().pull_hint(message)

    def _nodes(self):
        """[(architecture, GPUs)] of the cluster's nodes."""
        if self._nodes_seen is None:
            code, out, err = self._kubectl("get", "nodes", "-o", "json", timeout=60)
            if code != 0:
                raise errors.ValidationError(no_cluster(err.strip()))
            self._nodes_seen = [
                (((node.get("status") or {}).get("nodeInfo") or {}).get("architecture", ""),
                 int(((node.get("status") or {}).get("allocatable") or {}).get("nvidia.com/gpu") or 0))
                for node in json.loads(out).get("items") or []]
        return self._nodes_seen

    def _fits(self, image, gpu):
        """Refused here rather than a pod that never starts, when it is known."""
        nodes = self._nodes()
        if gpu and not any(count for _, count in nodes):
            raise errors.ValidationError("gpu= on a cluster with no GPU: no node here offers nvidia.com/gpu")
        have = sorted({arch for arch, _ in nodes})
        offered = self._image_platforms(image)
        if offered and have and not set(offered) & set(have):
            raise errors.ValidationError(
                f"{image} is only {', '.join(offered)} and this cluster is {', '.join(have)}: publish it for {have[0]}, or use another image")

    def _image_platforms(self, image):
        """The architectures image is published for, or [] if nobody here can
        say (no Docker, no network, a registry only the cluster reaches)."""
        if image not in self._platforms:
            self._platforms[image] = _read_platforms(image)
        return self._platforms[image]

    # --- what ends on its own ---------------------------------------------

    def containers(self):
        return []

    def clean(self, dry_run=False):
        """A job's pods go with it: nothing of this project stays on the cluster."""
        return []

    def clean_envs(self, dry_run=False):
        return []

    def uninstall(self, dry_run=False):
        """The agent and its namespace leave the cluster, with no job running
        there, of this project or any other."""
        if self._deployment() is None:
            return []
        code, out, err = self._kubectl("get", "jobs", "-n", NAMESPACE, "-o", "json", timeout=60)
        if code != 0:
            raise errors.TransientError(f"the jobs on {self._where()}: {err.strip()}")
        running = [job["metadata"]["name"] for job in json.loads(out).get("items") or [] if (job.get("status") or {}).get("active")]
        if running:
            raise errors.ValidationError(f"{', '.join(sorted(running))} still runs on {self._where()}: wait for it, or cancel it")
        if dry_run:
            return [f"{NAMESPACE}/{RELEASE}"]
        context = _settings().kube_context
        code, _, err = self.helm.run(["uninstall", RELEASE, "-n", NAMESPACE, "--wait", *(["--kube-context", context] if context else [])], timeout=300)
        if code != 0:
            raise errors.TransientError(f"helm uninstall: {err.strip()}")
        code, _, err = self._kubectl("delete", "namespace", NAMESPACE, "--ignore-not-found", timeout=300)
        if code != 0:
            raise errors.TransientError(f"deleting {NAMESPACE}: {err.strip()}")
        self._ready = False
        return [f"{NAMESPACE}/{RELEASE}"]


def _installed_as(deployment):
    """(image, shared folder) of the agent a cluster has."""
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    shared = next((e.get("value", "") for e in container.get("env") or [] if e.get("name") == "ZENTHROS_LOCAL_SHARED"), "")
    return container.get("image"), shared


def _helm_value(value):
    """--set reads a comma as the next value."""
    return str(value).replace("\\", "\\\\").replace(",", "\\,")


def _ask_in_a_terminal(where):
    """Installing on someone's cluster is theirs to say."""
    if os.environ.get(INSTALL_ENV):
        return
    if not sys.stdin.isatty():
        raise errors.ValidationError(
            f"on='local-k8s' installs the agent on {where}, in namespace {NAMESPACE}: run it once in a terminal to say yes, "
            f"or set {INSTALL_ENV}=1")
    answer = input(f"Install the Zenthros agent on {where}, in namespace {NAMESPACE}? [y/N] ")
    if answer.strip().lower() not in ("y", "yes"):
        raise errors.ValidationError("nothing was installed: on='local-k8s' needs the agent there")


def _download(url):
    try:
        with urllib.request.urlopen(url, timeout=120) as answer:  # nosec: the pin checks what it gives
            return answer.read()
    except (urllib.error.URLError, TimeoutError) as err:
        raise errors.ValidationError(f"{url}: {err}\nIt had no network, and what it needed is not here yet: run this once with network.") from None


def _listed(sums, name):
    """The digests SHA256SUMS lists for a file."""
    found = set()
    for line in sums.decode().splitlines():
        digest, _, listed = line.partition(" ")
        if listed.strip().lstrip("*").removeprefix("./") == name:
            found.add(digest)
    return found


def _read_platforms(image):
    docker = shutil.which("docker")
    if not docker:
        return []
    try:
        done = subprocess.run([docker, "buildx", "imagetools", "inspect", "--raw", image], capture_output=True, text=True,
                              timeout=60, check=False)
        if done.returncode != 0:
            return []
        manifests = json.loads(done.stdout).get("manifests")
        if manifests:
            return sorted({entry["platform"]["architecture"] for entry in manifests
                           if entry.get("platform", {}).get("architecture") not in (None, "unknown")})
        done = subprocess.run([docker, "buildx", "imagetools", "inspect", "--format", "{{json .Image}}", image],
                              capture_output=True, text=True, timeout=60, check=False)
        arch = json.loads(done.stdout).get("architecture") if done.returncode == 0 else ""
        return [arch] if arch else []
    except (subprocess.TimeoutExpired, ValueError, KeyError, OSError):
        return []
