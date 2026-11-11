# SPDX-License-Identifier: Apache-2.0

"""The Workspace's machines: zenthros.create, zenthros.delete, and where
f.remote(on=...) runs.

    mi_pc = zenthros.create("local", name="mi-pc", cpus=4, memory="8Gi")
    entrenar.remote(on=mi_pc, params={"ruta": datos})
    entrenar.remote(on="local", params={...})     # a container for this call only
    zenthros.delete(mi_pc)

create("local") starts a container on this computer (zenthros.local) with
the cpus and memory as hard limits and joins it, Private to the Workspace,
by name; it returns only once the machine is ready, and undoes one that
never gets there. A name the Workspace already has is that machine. The
container's image is Python of this process's minor version, with what the
runner needs (cloudpickle, boto3, procps), built here once; a function's
own packages are installed where it runs (requirements=, packages=).

create("k8s:<cluster>") is the same container as a pod of a Kubernetes
backend of the Workspace (by its name or id): a Job there, with cpus and
memory as its requests and limits, that installs the agent and joins with
a token zenthros-api sealed to the cluster's agent, never in the job.

delete retires the machine at the API first (no job reaches it, its tokens
end), then removes its container when it is on this computer, or its pod's
job on its cluster.

attach("my-vm") connects a machine the user already has (zenthros.attaching):
the two lines to paste on it, or with host="ubuntu@address" over ssh, as
root. It joins Private to the Workspace and External: Zenthros never stops
it, and delete only retires it.
"""

import hashlib
import os
import re
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass, field

from zenthros import errors
from zenthros.local import BACKEND_LABEL, PORTS_LABEL, LocalBackend, _api, _docker
from zenthros.view import Backend, Pool

LOCAL = "local"
LOCAL_K8S = "local-k8s"
# create("k8s:<cluster>"): a pod of that Kubernetes backend.
POD = "k8s:"
# The label of a pod machine's job: the backend its pod joined as.
POD_LABEL = "zenthros.ai/machine-backend"
# How long create waits for a machine to be ready before it undoes it: a
# container here, a VM from a template.
READY_TIMEOUT = 600
VM_READY_TIMEOUT = 1200
# PLACED_IDLE is the idle of a machine place() makes for one call: it ends
# that long after its last job, should its caller be gone before it deletes it.
PLACED_IDLE = "30m"
# How long a machine kept up lives unless create(max_lifetime=) says otherwise:
# one nobody remembers bills until then, not for ever.
KEPT_LIFETIME = "24h"
# How long sealing the store's credentials to a machine waits out an API
# that is away (a rollout of zenthros-api takes 2–3 min): the machine was
# made for the call, and a raise deletes it.
SEAL_TIMEOUT = 300
_sleep = time.sleep
# create("local")'s size when none is given; the environment may change it.
LOCAL_CPUS_ENV, LOCAL_MEMORY_ENV = "ZENTHROS_LOCAL_CPUS", "ZENTHROS_LOCAL_MEMORY"
DEFAULT_LOCAL_CPUS, DEFAULT_LOCAL_MEMORY = 2, "4Gi"

_LOCAL_DOCKERFILE = """\
# zenthros-python: what a function needs to run in a container of this
# computer. Python {minor} because cloudpickle needs the caller's minor;
# procps because Spark's scripts call ps.
FROM python:{minor}-slim-bookworm
RUN apt-get update \\
    && apt-get install -y --no-install-recommends procps curl ca-certificates \\
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir {packages}
RUN useradd --create-home --uid 1000 zenthros
USER zenthros
WORKDIR /home/zenthros
ENV PYTHONUNBUFFERED=1
"""


@dataclass
class Machine:
    """One machine of the Workspace, as create() returns it."""

    name: str
    id: str = ""
    target: str = ""
    container: str = ""
    # A machine from a template is a pool of one: jobs reach it by its pool.
    pool: str = ""
    labels: dict = field(default_factory=dict)
    # A pod machine's job on its cluster.
    job: str = ""
    # A machine from a template made with idle: how long it stays up with no
    # job; the next call starts it again.
    idle: str = ""
    # A container here: the ports it publishes to this computer, for serve.
    ports: tuple = ()
    # Made running its own image, which what it asked for completes.
    completes_image: bool = False

    def to_dict(self):
        """This machine as an agent or a script reads it."""
        return {"name": self.name, "id": self.id, "target": self.target, "pool": self.pool, "idle": self.idle,
                "ports": list(self.ports)}

    def __str__(self):
        return self.name


@dataclass
class Placement:
    """Where a call goes: run() options, the machine made for it, and the
    name it goes to (what a call's receipt says)."""

    options: dict
    made: Machine = None
    name: str = ""


def python_minor():
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def local_defaults():
    """create("local")'s cpus and memory when none are given."""
    cpus = os.environ.get(LOCAL_CPUS_ENV) or DEFAULT_LOCAL_CPUS
    return float(cpus) if "." in str(cpus) else int(cpus), os.environ.get(LOCAL_MEMORY_ENV) or DEFAULT_LOCAL_MEMORY


_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(Ti|T|t|Gi|G|g|Mi|M|m)?\s*$")


def docker_memory(value):
    """"8Gi", "512Mi", "4g" or a number of GiB, as docker --memory reads it."""
    match = _SIZE.match(str(value))
    if not match:
        raise errors.ValidationError(f"memory={value!r}: a size such as '8Gi' or '512Mi'")
    number, unit = float(match.group(1)), match.group(2) or "Gi"
    if unit in ("Mi", "M", "m"):
        return f"{int(number)}m"
    if unit in ("Ti", "T", "t"):
        number *= 1024
    return f"{int(number)}g" if number.is_integer() else f"{int(number * 1024)}m"


def bytes_of(value):
    """A size create() takes, in bytes."""
    text = docker_memory(value)
    return int(text[:-1]) << (30 if text.endswith("g") else 20)


def docker_capacity(docker="docker", info_format="{{.NCPU}} {{.MemTotal}}"):
    """(CPUs, memory bytes) the engine has here (info_format asks it, in its
    own words). Raises ValidationError when there is no engine to ask."""
    try:
        code, out = _docker(docker, ["info", "--format", info_format], timeout=30)
    except errors.ValidationError:
        code, out = 127, f"{os.path.basename(docker)} is not installed"
    # Warnings may come before the numbers.
    parts = out.split()[-2:]
    if code != 0 or len(parts) != 2 or not all(p.isdigit() for p in parts):
        name = "Docker" if os.path.basename(docker) == "docker" else os.path.basename(docker)
        raise errors.ValidationError(f"{name} is not running here ({out.strip() or 'its info failed'}): start it, "
                                     "or run on a machine of the Workspace (zenthros.targets())")
    return int(parts[0]), int(parts[1])


def local_image(docker="docker"):
    """The default image of create("local"), built here once per Python
    minor and runner packages."""
    packages = ["boto3"]
    try:
        import cloudpickle

        packages.append(f"cloudpickle=={cloudpickle.__version__}")
    except ImportError:
        packages.append("cloudpickle")
    dockerfile = _LOCAL_DOCKERFILE.format(minor=python_minor(), packages=" ".join(packages))
    tag = f"zenthros-python:{python_minor()}-{hashlib.sha256(dockerfile.encode()).hexdigest()[:12]}"
    code, _ = _docker(docker, ["image", "inspect", tag], timeout=60)
    if code != 0:
        print(f"zenthros: building {tag}, once (Python {python_minor()} and the runner's packages)", file=sys.stderr)
        code, out = _docker(docker, ["build", "-t", tag, "-"], stdin=dockerfile, timeout=1800)
        if code != 0:
            tail = "\n".join(out.strip().splitlines()[-15:])
            raise errors.BackendJoinError(f"docker build {tag}:\n{tail}")
    return tag


def _backends(client):
    return [Backend.from_api(info) for info in client.backends()]


def _find(client, key):
    for backend in _backends(client):
        if key in (backend.name, backend.id):
            return backend
    return None


def _machine(backend, target=""):
    return Machine(name=backend.name or backend.id, id=backend.id, target=target)


def _python_fits(name, template, python):
    from zenthros.targetlist import python_problem

    problem = python_problem(name, (template or {}).get("python"), python)
    if problem:
        raise errors.ValidationError(problem)


def _package_name(requirement):
    """boto3 of "boto3[crt]==1.40.0", as pip compares names."""
    return re.split(r"[\[=<>!~;@ ]", requirement.strip(), maxsplit=1)[0].lower().replace("_", "-")


def _store_of(client):
    """The Workspace's store, "" where the API has none to give (404). An
    outage is raised: read as "no store", a call went without the
    credentials it needed there."""
    try:
        return str(client.store)
    except errors.NotFound:
        return ""


def _runner_fits(client, name, template, python):
    """A call on a template's machine is read from the store and unpickled
    there by its python toolchain, which has only that toolchain's
    packages: one without boto3 (s3://) or cloudpickle is refused here,
    before a machine is made, rather than in the machine. A command (python
    None) reads no call; what the API does not say (no pythonPackages)
    cannot be told."""
    if python is None or not template:
        return
    packages = template.get("pythonPackages")
    if packages is None:
        return
    from zenthros.remote.options import settings

    has = {_package_name(p) for p in packages}
    store = settings().store or _store_of(client)
    needs = (["boto3"] if store.startswith("s3://") else ["google-cloud-storage"] if store.startswith("gs://") else []) + ["cloudpickle"]
    missing = [p for p in needs if p not in has]
    if missing:
        raise errors.ValidationError(
            f"{name}'s Python ({template.get('python')}) has no {', '.join(missing)}, which a function call needs "
            f"there (it has {sorted(packages) or 'no packages'}): ask the hub's administrator to add them, or run a "
            f"command there")


def _needs_sealed_store(client, name, template, python, options):
    """Whether a call there must bring the store's credentials: a function
    call (read from an S3 store) on a template whose machines have no cloud
    identity to read it with (Vast.ai, RunPod), from a caller that brings
    none. On a host a third party runs, only when the caller accepts that
    its owner can read them, as any secret sent there (SEC-6)."""
    if python is None or not template or template.get("cloudIdentity") is not False:
        return False
    from zenthros.core.storage import GCS_KEY_ENV
    from zenthros.remote.options import settings

    if not (settings().store or _store_of(client)).startswith(("s3://", "gs://")):
        return False
    if set((options or {}).get("secrets") or {}) & {"AWS_ACCESS_KEY_ID", GCS_KEY_ENV}:
        return False
    if template.get("untrustedHost") and not settings().untrusted_host_secrets:
        raise errors.ValidationError(
            f"{name}'s machines are hosts a third party runs (Vast.ai), whose owner can read a job's secrets, and a "
            f"function call there needs the store's credentials (an hour's, for this Workspace's calls only). "
            f"zenthros.configure(untrusted_host_secrets=True) sends them; or run a command there "
            f"(zenthros.run('python train.py', on=...)), which needs none")
    return True


def _sealed_store(client, name, pool, options):
    """options with the store's credentials sealed to the machine of pool
    (the hub mints them for a machine it is named, an hour's), added to the
    caller's secrets= and env=."""
    rows = (_answered(client.targets) or {}).get("targets") or []
    machine = next((r.get("id") for r in rows if r.get("pool") == pool), "")
    if not machine:
        raise errors.ValidationError(
            f"{name} has no machine up to seal the store's credentials to (asleep, or still starting): call again "
            f"once zenthros.targets() shows it Ready, or run a command there")
    from zenthros.remote.options import settings

    current = settings()
    if current.store:
        # Your own bucket: minted here and sealed to the machine; the hub never sees it.
        from zenthros import userstore

        env, sources = _answered(lambda: userstore.machine_access(current.store, machine, current.workspace, no_role=True))
    else:
        env, sources = _answered(lambda: client.store_access(machine))
    if not sources:
        raise errors.ValidationError(
            f"the hub minted no credentials of the store for {name}'s machine (it has no role to mint them from): "
            f"bring them (secrets={{'AWS_ACCESS_KEY_ID': ..., 'AWS_SECRET_ACCESS_KEY': ...}}), or run a command there")
    from zenthros.remote.options import Secret

    carried = dict(options.get("secrets") or {})
    for source in sources:
        carried[source.materialization.env_name] = Secret("encrypted-payload", payload=source.encrypted_payload)
    return {"secrets": carried, "env": {**env, **(options.get("env") or {})}}


def _answered(call, timeout=SEAL_TIMEOUT):
    """call's answer, asked again while the API is away, up to timeout:
    then the outage is raised, and nothing goes without its credentials."""
    waited = 0
    while True:
        try:
            return call()
        except errors.TransientError:
            if waited >= timeout:
                raise
            _sleep(5)
            waited += 5


def _template_fits(client, name, template, python, ports, image=""):
    """A call there, before a machine is made. A job in its own image runs
    that image's Python, which the runner completes: the template's says
    nothing about it."""
    if not image:
        _python_fits(name, template, python)
        _runner_fits(client, name, template, python)
    _ports_open(name, template, ports)


def _ports_open(name, template, ports):
    """A service's ports on a template's machine: only those its pool
    exposes (exposedPorts) reach it, so any other is refused here, before a
    machine is made, rather than by the controller after."""
    if not ports:
        return
    exposed = sorted((template or {}).get("ports") or [])
    missing = sorted(set(ports) - set(exposed))
    if not missing:
        return
    where = f"publishes {exposed}" if exposed else "publishes no port"
    there = f"serve on {exposed} there, or " if exposed else ""
    raise errors.ValidationError(
        f"{name} {where}, so a service on {missing} would not be reached: {there}serve on a container here: "
        f"create('local', ports={sorted(ports)})")


def _made(client):
    """(templates, machines) of the Workspace, by name; empty where the API
    has none to give (404: an installation without templates). An outage is
    raised: read as "none", a call on a machine without a cloud identity
    went without its store's credentials."""
    try:
        listed = client.machines()
    except errors.NotFound:
        return {}, {}
    return ({t["name"]: t for t in listed.get("templates") or []},
            {m["name"]: m for m in listed.get("machines") or []})


def _from_template(view):
    return Machine(name=view["name"], target=view.get("template", ""), pool=view.get("pool", ""),
                   idle=view.get("idle", ""))


def _idle(idle):
    """idle as the API reads it: "30m", or a number of seconds."""
    if idle is None or idle == "":
        return ""
    if isinstance(idle, (int, float)) and not isinstance(idle, bool):
        return f"{int(idle)}s"
    return str(idle)


def _max_lifetime(value):
    """max_lifetime as the API reads it: "24h", "2d" or seconds; None is no limit."""
    from zenthros.remote.options import seconds

    return "" if value is None else f"{int(seconds(value, 'max_lifetime'))}s"


def _create_from_template(client, template, name, idle="", boot=False, max_lifetime=None, **image):
    name = name or f"{template}-{secrets.token_hex(2)}"
    view = client.create_machine(template, name, idle, boot=boot, max_lifetime=_max_lifetime(max_lifetime), **image)
    made = _from_template(view)
    made.completes_image = bool(image)
    if idle and not boot:
        # Asleep until its first call: nothing to wait for.
        return made
    waited = 0
    with errors.undone_on_failure((f"delete {made.name}", lambda: client.delete_machine(made.name))):
        while True:
            # The API away for a moment (a rollout) is "not up yet": the
            # boot is waited for until its timeout, never undone for it.
            try:
                current = _made(client)[1].get(made.name) or {}
            except errors.TransientError:
                current = {}
            if int(current.get("ready") or 0) + int(current.get("allocated") or 0) >= 1:
                return made
            if current.get("runtimeFailure"):
                raise errors.ValidationError(f"{template}'s machine cannot prepare what it was asked: {current['runtimeFailure']}")
            if waited >= VM_READY_TIMEOUT:
                raise errors.WaitTimeout(f"machine {name} from {template} not up after {VM_READY_TIMEOUT} s")
            _sleep(10)
            waited += 10


def _cluster(client, target):
    """The Kubernetes backend create("k8s:<cluster>") names, by name or id."""
    key = target[len(POD):]
    clusters = [b for b in _backends(client) if b.type == "k8s"]
    for cluster in clusters:
        if key and key in (cluster.name, cluster.id):
            return cluster
    raise errors.ValidationError(f"create({target!r}): Workspace {client.workspace} has no Kubernetes cluster {key!r}; "
                                 f"it has {sorted(c.name or c.id for c in clusters)}")


def k8s_memory(value):
    """A size create() takes, as Kubernetes reads it."""
    size = bytes_of(value)
    return f"{size >> 30}Gi" if size % (1 << 30) == 0 else f"{size >> 20}Mi"


def _runner_packages():
    """What a function's runner needs where it runs, cloudpickle at this
    process's version."""
    try:
        import cloudpickle

        return ["boto3", f"cloudpickle=={cloudpickle.__version__}"]
    except ImportError:
        return ["boto3", "cloudpickle"]


def pod_script(install):
    """The pod's one process: the runner's packages, the agent (install, no
    secret), the join from the sealed ZENTHROS_JOIN, then sleep, which does
    not keep the token. The agent is not the pod's first process: the pod
    going ends its jobs as a lost machine.

    The pod is a job of the cluster's agent, which gives it its own
    ZENTHROS_* (its report endpoint and token, its job's ID): the machine's
    agent is its join's, so the join sees none of them. Read as its own,
    the endpoint sent its jobs' reports to the cluster's agent, which
    refused their tokens."""
    return "\n".join([
        "set -eu",
        f"pip install --no-cache-dir --quiet --disable-pip-version-check {' '.join(_runner_packages())}",
        install.rstrip("\n"),
        'export PATH="/usr/local/bin:${PATH}"',
        'join="$ZENTHROS_JOIN"',
        "for name in $(env | sed -n 's/^\\(ZENTHROS_[A-Za-z0-9_]*\\)=.*/\\1/p'); do unset \"$name\"; done",
        'printf \'%s\\n\' "$join" | sh',
        "unset join",
        "exec sleep infinity",
    ]) + "\n"


def _pod_job(client, cluster, join, sealed, cpus, memory, image):
    from zenthros.jobs import spec as S
    from zenthros.jobs.http import _sealed_source

    backend_id = str(join.get("backendId") or "")
    return S.Job(
        name=f"machine-{backend_id}",
        labels={POD_LABEL: backend_id},
        spec=S.Spec(
            workspace=client.workspace,
            target=S.Target(backends=[cluster.id]),
            workload=S.Workload(kind="Job", template=S.Template(
                command=["sh", "-c", pod_script(join["installScript"])],
                image=image or f"python:{python_minor()}-slim-bookworm",
                workers=S.Workers(cpu=str(cpus), memory=k8s_memory(memory)),
                secret_sources=[_sealed_source(ref) for ref in sealed],
                # A spent token is not tried again.
                restart_policy=S.RestartPolicy(backoff_limit=0),
            )),
        ),
    )


def _create_pod(client, target, name, cpus, memory, image):
    """A pod of the cluster, joined as a Private machine of the Workspace;
    returned once ready. One that never gets there is retired, then its
    job goes."""
    cluster = _cluster(client, target)
    default_cpus, default_memory = local_defaults()
    cpus = cpus if cpus is not None else default_cpus
    memory = memory if memory is not None else default_memory
    join = client.join_backend(name or "")
    backend_id = str(join.get("backendId") or "")
    job = ""
    # Retired first, then the pod: never a machine nobody holds.
    with errors.undone_on_failure((f"retire {backend_id}", lambda: client.retire_backend(backend_id)),
                                  ("delete its pod", lambda: job and client.delete(job))):
        sealed = client.seal(cluster.id, {"ZENTHROS_JOIN": join["joinScript"]})
        made = _pod_job(client, cluster, join, sealed, cpus, memory, image)
        client.submit(made)
        job = made.name
        LocalBackend(client, backend_id, "", created=False).wait_ready(timeout=READY_TIMEOUT, interval=5)
    return Machine(name=name or backend_id, id=backend_id, target=target, job=job)


def create(target=LOCAL, *, name=None, cpus=None, memory=None, image=None, docker="docker", dry_run=False, idle=None,
           ports=None, max_lifetime=KEPT_LIFETIME):
    """A machine of the Workspace, ready for jobs. target "local" starts a
    container here; the name of a machine the Workspace has returns it.
    idle="30m" (a template's machine only): it ends after that long with no
    job, and the next call on it starts it again; create returns at once and
    the first call starts it. dry_run=True makes nothing: it returns what
    would be made or used (zenthros.targetlist.Plan), its size and price,
    and what is missing. ports=[8000] ("local" only): what a Service on it
    listens on, published to this computer (http://127.0.0.1:8000) and no
    further; a template's machines publish what its pool exposes.
    max_lifetime="24h" (a template's machine only, and its default without
    idle=): it ends that long after it was made, job or no job, with
    MACHINE_LIFETIME for a job on it; max_lifetime=None keeps it until delete."""
    idle = _idle(idle)
    _max_lifetime(max_lifetime)
    if max_lifetime not in (None, KEPT_LIFETIME) and (target == LOCAL or target.startswith(POD)):
        raise errors.ValidationError(f"create({target!r}, max_lifetime=): max_lifetime is for a machine from a template, "
                                     "which zenthros.targets() lists; a container or pod here ends with delete")
    ports = _ports(ports)
    if ports and target != LOCAL:
        raise errors.ValidationError(f"create({target!r}, ports=): ports= is only for create('local'); a template's "
                                     "machines publish the ports the template lists (zenthros.targets())")
    if idle and (target == LOCAL or target.startswith(POD)):
        raise errors.ValidationError(f"create({target!r}, idle=): idle is for a machine from a template, which "
                                     "zenthros.targets() lists; a container or pod here ends with delete")
    if dry_run:
        from zenthros.targetlist import plan_create

        return plan_create(target, name=name, cpus=cpus, memory=memory, docker=docker, idle=idle)
    client = _api()
    if name:
        found = _find(client, name)
        if found is not None:
            _check_published(name, found.id, ports, docker)
            return _machine(found, target)
    if target.startswith(POD):
        return _create_pod(client, target, name, cpus, memory, image)
    if target != LOCAL:
        templates, made = _made(client)
        if name and name in made:
            return _from_template(made[name])
        if target in made:
            return _from_template(made[target])
        found = _find(client, target)
        if found is not None:
            return _machine(found)
        if target in templates:
            # A machine with idle ends by itself: only a limit asked for applies.
            limit = None if idle and max_lifetime == KEPT_LIFETIME else max_lifetime
            return _create_from_template(client, target, name, idle, max_lifetime=limit)
        raise errors.ValidationError(f"create({target!r}): not a target of Workspace {client.workspace}; "
                                     "zenthros.targets() lists them")
    docker_capacity(docker)
    default_cpus, default_memory = local_defaults()
    cpus = cpus if cpus is not None else default_cpus
    memory = memory if memory is not None else default_memory
    image = image or local_image(docker)
    lb = LocalBackend.create(image, cpus=cpus, memory=docker_memory(memory), name=f"zenthros-{name}" if name else "",
                             docker=docker, backend_name=name or "", ports=ports)
    try:
        lb.wait_ready(timeout=READY_TIMEOUT)
    except BaseException:
        # Retired first, then the container: never a machine nobody holds.
        try:
            lb.remove()
        except Exception as exc:  # the raise below says what failed first
            print(f"zenthros: could not undo {lb.container}: {exc}", file=sys.stderr)
        raise
    return Machine(name=name or lb.id, id=lb.id, target=LOCAL, container=lb.container, ports=ports)


# The agent's own ports on a machine (zenthros-agent); a Service never has them.
AGENT_PORTS = (8080, 8081)


def _ports(ports):
    """create's ports=, checked: a list of port numbers, as a tuple."""
    if ports is None:
        return ()
    if not isinstance(ports, (list, tuple)):
        raise errors.ValidationError(f"ports={ports!r}: a list of port numbers, ports=[8000]")
    out = []
    for port in ports:
        if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
            raise errors.ValidationError(f"ports: {port!r} is not a port number (1-65535)")
        if port in AGENT_PORTS:
            raise errors.ValidationError(f"ports: {port} is the agent's own; a Service listens on another")
        out.append(port)
    return tuple(dict.fromkeys(out))


def _published(backend_id, docker):
    """The ports the container here of backend_id publishes to this
    computer, or None when it has no container here (a VM, a pod)."""
    try:
        code, out = _docker(docker, ["ps", "-a", "--filter", f"label={BACKEND_LABEL}={backend_id}",
                                     "--format", 'ports={{.Label "' + PORTS_LABEL + '"}}'], timeout=60)
    except errors.ValidationError:
        return None
    found = [line[len("ports="):] for line in out.splitlines() if line.startswith("ports=")] if code == 0 else []
    if not found:
        return None
    return {int(p) for p in found[0].split(",") if p.strip().isdigit()}


def _check_published(name, backend_id, ports, docker):
    """Refuse a Service on a container here that does not publish its
    ports: its URL would be the container's own address, which this
    computer may not reach. The ports are set when the container is made."""
    if not ports:
        return
    published = _published(backend_id, docker)
    if published is None:
        return
    missing = [p for p in ports if p not in published]
    if missing:
        raise errors.ValidationError(
            f"{name} does not publish port {', '.join(map(str, missing))} to this computer (its container was made "
            f"without it): zenthros.delete({name!r}), then create('local', name={name!r}, ports={sorted(ports)})")


def _ssh(args, stdin, timeout):
    """Run ssh with the script on its standard input. Returns the exit
    code and what it printed, stdout and stderr together."""
    try:
        done = subprocess.run(args, input=stdin, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                              timeout=timeout, check=False)
    except FileNotFoundError:
        raise errors.ValidationError(f"no ssh at {args[0]!r}: install OpenSSH, or attach(name=...) for the command") from None
    except subprocess.TimeoutExpired:
        return 124, f"ssh did not finish in {timeout} s"
    return done.returncode, done.stdout or ""


def attach(name=None, **kwargs):
    """Connect a machine of yours to the Workspace (zenthros.attaching.attach):
    the two lines to paste on it, or, with host=, over ssh."""
    from zenthros.attaching import attach as attach_machine

    return attach_machine(name, **kwargs)


def delete(machine, docker="docker"):
    """Retire the machine at the API, then remove its container when it is
    on this computer. A machine with no container here is only retired."""
    client = _api()
    if isinstance(machine, Machine) and machine.pool:
        client.delete_machine(machine.name)
        return
    if isinstance(machine, Machine) and machine.job:
        client.retire_backend(machine.id)
        client.delete(machine.job)
        return
    key = machine.id if isinstance(machine, (Machine, LocalBackend)) else str(machine)
    if key in _made(client)[1]:
        client.delete_machine(key)
        return
    found = _find(client, key)
    if found is None:
        # A join no machine used yet has tokens and no record, so no list shows
        # it: the refusal of its name gave this id, and the API retires it.
        try:
            client.retire_backend(key)
            return
        except errors.NotFound:
            names = sorted(b.name or b.id for b in _backends(client))
            raise LookupError(f"Workspace {client.workspace} has no machine {key!r}; it has {names}") from None
    try:
        code, out = _docker(docker, ["ps", "-a", "--filter", f"label={BACKEND_LABEL}={found.id}",
                                     "--format", "{{.Names}}"], timeout=60)
    except errors.ValidationError:
        code, out = 1, ""
    containers = out.split() if code == 0 else []
    if containers:
        LocalBackend(client, found.id, containers[0], created=True, docker=docker).remove()
        _say_stopped(found)
        return
    # A pod machine's job, found before the retire: its label names it. A
    # VM is never a pod.
    jobs = [j["metadata"]["name"] for j in client.list_jobs(f"{POD_LABEL}={found.id}")] if found.type == "container" else []
    client.retire_backend(found.id)
    for job in jobs:
        client.delete(job)
    _say_stopped(found)


def _say_stopped(backend):
    """A machine detached with jobs running: they fail now (its access is
    gone), and the person is told so (E-13)."""
    running = int(getattr(backend, "running_jobs", 0) or 0)
    if running:
        from zenthros.logging import create_logger

        name = backend.name or backend.id
        create_logger("zenthros.machines", user=True).warning(
            f"{name} had {running} running job{'s' if running != 1 else ''}: "
            f"{'it fails' if running == 1 else 'they fail'} now that {name} is detached")


def default_on():
    """Where a call with no on= runs: configure(on=) or ZENTHROS_ON, else
    this computer."""
    from zenthros.remote.options import settings

    current = settings()
    if current.on not in (None, ""):
        return current.on
    return LOCAL


def python_image(minor):
    """The image a function runs in on a machine that runs images, when the
    call names none: Python of the call's minor, slim (R4)."""
    return f"python:{minor}-slim-bookworm"


def _image_for(backend, image, python):
    """{"image": ...} for a function sent to a machine whose jobs run in
    images by default, with no image=: the call's Python, so the machine
    needs none of its own (attach's containers, a cluster's pods, which run
    no pod without one). A command (python None), an image named, or a
    machine whose jobs run on it: nothing."""
    if image or python is None or backend is None or backend.default_runtime not in ("containerd", "kubernetes"):
        return {}
    return {"image": python_image(python)}


def _image_fits(name, image, runtimes):
    """image= on a machine: only one that runs images (containerd, a
    cluster's pods) runs the job in it. A machine whose runtimes are not
    known here is left to the controller, which checks them again."""
    if image and runtimes and not {"containerd", "kubernetes"} & set(runtimes):
        raise errors.ValidationError(
            f"{name} runs jobs on the machine itself, not in images. Drop image=, or attach a VM with containerd")


def _store_runner_packages(client):
    """What a function's runner needs on a machine, for this Workspace's store."""
    from zenthros.remote.call import runner_packages
    from zenthros.remote.options import settings

    return runner_packages(settings().store or _store_of(client))


def _with_its_image(client, name, template, image, options, keep, python, ports):
    """A machine of the template running image, for the call or kept. Refused
    before it is made if its hosts cannot read that image."""
    from zenthros import reach

    answer = reach.image(image, template)
    if answer.answer == reach.IMPOSSIBLE:
        raise errors.ValidationError(answer.why)
    _ports_open(name, template, ports)
    sealed = _needs_sealed_store(client, name, template, python, options)
    needs = options.get("env_needs") or {}
    packages = [*(_store_runner_packages(client) if python is not None else ()), *(needs.get("packages") or ())]
    wanted = dict(image=image, python=needs.get("python") or python or "", packages=packages)
    # With packages to add it can say no: waited for here, where that reaches the caller.
    machine = (_create_from_template(client, name, None, max_lifetime=KEPT_LIFETIME, **wanted) if keep
               else _create_from_template(client, name, None, PLACED_IDLE, boot=sealed or bool(packages), **wanted))
    try:
        extra = _sealed_store(client, machine.name, machine.pool, options) if sealed else {}
    except BaseException:
        if not keep:
            delete(machine)
        raise
    return Placement({"pool": machine.pool, **extra}, made=machine)


def who_reads(name, image):
    """Whether the machines of a target can pull image (reach.image)."""
    from zenthros import reach

    templates, made = _made(_api())
    return reach.image(image, templates.get(name) or templates.get(made.get(name, {}).get("template", "")))


def place(on, options, docker="docker", keep=False, python=None, ports=None):
    """_place, after on= is known (default_on() without it), with the name
    it went to."""
    if on is None:
        on = default_on()
    placed = _place(on, options, docker=docker, keep=keep, python=python, ports=ports)
    placed.name = placed.name or str(getattr(on, "name", "") or getattr(on, "id", "") or on)
    return placed


def on_a_new_machine(placed):
    """placed, as run(fresh=True) needs it: a machine no job ran on. Only a
    template or pool has others to make; a machine made for the call already is."""
    if placed.made is None:
        if placed.options.get("pool") != placed.name:
            raise errors.ValidationError(f"fresh=True: on={placed.name!r} is one machine; a new one comes from a template "
                                         "(zenthros.targets())")
        placed.options["acquire"] = ["Create"]
    return placed


def _place(on, options, docker="docker", keep=False, python=None, ports=None):
    """Where on= sends a call: a machine, a name the Workspace has (a
    machine or a pool), "local" for a container made for the call, sized as
    the function asks, or a template: a machine of its pool with nothing to
    do, or a new one, for the call, which the pool ends once idle; with
    keep=True a machine made from it that stays. Without zenthros-api (an administrator's kubeconfig) a
    name is a pool's. python: the minor a
    call needs there; a template, or a machine made from one, whose pinned
    Python differs is refused before any machine is made. ports: a
    Service's; a container here must publish them, and one made for it
    does. image= is refused on a machine that does not run images."""
    from zenthros.remote.options import _through_api, local_client, local_k8s_client

    if on == LOCAL:
        # No hub in it, logged in or not: this computer, the local transport.
        # The job's image is the container's.
        return Placement({"client": local_client()}, name=LOCAL)
    if on == LOCAL_K8S:
        # The same, on the cluster kubectl reaches: each job its pod and image.
        return Placement({"client": local_k8s_client()}, name=LOCAL_K8S)
    if isinstance(on, str) and not _through_api():
        return Placement({"pool": on})
    image = (options or {}).get("image") or ""
    if isinstance(on, Machine) and on.pool:
        if on.target and (python is not None or ports):
            client = _api()
            templates, _ = _made(client)
            template = templates.get(on.target)
            _template_fits(client, on.name, template, python, ports, image)
            if _needs_sealed_store(client, on.name, template, python, options):
                return Placement({"pool": on.pool, **_sealed_store(client, on.name, on.pool, options)})
        return Placement({"pool": on.pool})
    if isinstance(on, (Machine, LocalBackend, Backend)):
        label = getattr(on, "name", "") or on.id
        extra = {}
        if isinstance(on, LocalBackend):
            # A container here runs jobs as its own processes.
            _image_fits(label, image, ["process"])
        elif _through_api() and (image or python is not None):
            backend = on if isinstance(on, Backend) else next((b for b in _backends(_api()) if b.id == on.id), None)
            if image:
                _image_fits(label, image, backend.runtimes if backend is not None else [])
            extra = _image_for(backend, image, python)
        _check_published(label, on.id, ports, docker)
        return Placement({"backend": on.id, **extra})
    name = str(on)
    client = _api()
    found = _find(client, name)
    if found is not None:
        if name in _made(client)[0]:
            raise errors.ValidationError(
                f"on={name!r} is a machine and a template. Rename the machine: zenthros.delete({name!r}), then "
                "attach it with another name")
        _image_fits(name, image, found.runtimes)
        _check_published(name, found.id, ports, docker)
        return Placement({"backend": found.id, **_image_for(found, image, python)})
    templates, made = _made(client)
    if name in made:
        template, pool = templates.get(made[name].get("template", "")), made[name].get("pool", "")
        _template_fits(client, name, template, python, ports, image)
        if _needs_sealed_store(client, name, template, python, options):
            return Placement({"pool": pool, **_sealed_store(client, name, pool, options)})
        return Placement({"pool": pool})
    if name in templates:
        # A Container template's machines are their image: one that runs another
        # is a machine of its own, made from the template, with what completes it.
        if image and templates[name].get("machineKind") == "Container":
            return _with_its_image(client, name, templates[name], image, options, keep, python, ports)
        _template_fits(client, name, templates[name], python, ports, image)
        sealed = _needs_sealed_store(client, name, templates[name], python, options)
        if keep or sealed:
            # A call that brings the store's credentials needs the machine
            # they are sealed to: made first, and gone with the result. Made
            # for the call alone, it boots now and then ends after
            # PLACED_IDLE with no job, so it does not outlive a caller gone
            # before it deleted it (audit 2026-10-16 AUD-041); keep=True is
            # a machine that stays.
            machine = (_create_from_template(client, name, None, max_lifetime=KEPT_LIFETIME) if keep
                       else _create_from_template(client, name, None, PLACED_IDLE, boot=True))
            try:
                extra = _sealed_store(client, machine.name, machine.pool, options) if sealed else {}
            except BaseException:
                if not keep:
                    delete(machine)
                raise
            return Placement({"pool": machine.pool, **extra}, made=machine)
        return Placement({"pool": name})
    pools = [Pool.from_api(info) for info in client.pools()]
    if any(pool.name == name for pool in pools):
        return Placement({"pool": name})
    there = sorted({b.name or b.id for b in _backends(client)} | {p.name for p in pools} | {LOCAL} | set(templates) | set(made))
    raise errors.ValidationError(f"on={name!r}: Workspace {client.workspace} has nothing called that; "
                                 f"it has {there} (zenthros.targets())")
