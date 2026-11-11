# SPDX-License-Identifier: Apache-2.0

"""The fourth transport: no hub. A job goes to an agent in a container of
this computer, through the engine's exec, as the same ZenthrosJob the others
send (operator/agent/local.go takes it from there).

    train.remote(x)                       # no account, no bucket: one container

What the controller keeps is kept here, in .zenthros/local of the project:

    store/      calls, results and outputs (file://, at the same path in the
                container, so a URI means the same on both sides)
    jobs/       <name>.json, each job as the hub would give it; <name>.log

The container has the project's .zenthros/local and the paths a call names,
read-only, and nothing else of this computer. Its limits are hard, and they
are the budget of everything in it. One container per project and image; one
image at a time, since this computer's resources are one budget.
"""

import base64
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path

from zenthros import errors
from zenthros.core.storage import is_carried
from zenthros.jobs import spec as S
from zenthros.jobs.client import JobClient, clip_log, parse_report_body, require_job
from zenthros.jobs.handle import (
    CANCELLED,
    FAILED,
    PENDING,
    SUCCEEDED,
    TERMINAL_PHASES,
    JobHandle,
    seconds_at,
    status_from_object,
)

ENGINE_ENV = "ZENTHROS_ENGINE"
ROOT_ENV = "ZENTHROS_LOCAL_ROOT"
# A built agent to run instead of the one the wheel pins: a checkout's.
AGENT_ENV = "ZENTHROS_LOCAL_AGENT"
NETWORK_ENV = "ZENTHROS_LOCAL_NETWORK"
IDLE_ENV = "ZENTHROS_LOCAL_IDLE_EXIT"

LOCAL_LABEL = "zenthros.ai/local"
CONFIG_LABEL = "zenthros.ai/local-config"
MOUNTS_LABEL = "zenthros.ai/local-mounts"
AGENT_PATH = "/usr/local/bin/zenthros-agent"
STATE_DIR = "/tmp/zenthros"
HOME_DIR = "/home/zenthros"
ENDPOINT_TEMPLATE = "ZENTHROS_ENDPOINT_TEMPLATE={scheme}://127.0.0.1:{port};scope=Private"
AGENT_READY_SECONDS = 60
_OUT_OF_MEMORY = "it ran out of the container's memory, or something killed it: raise memory= (zenthros.configure(local_memory=...))"
# What the store holds that is no job's: code by digest, and later the cache
# and uploaded data.
_NOT_JOBS = {"code", "cache", "data"}
# A job's record stays this long past its end unless it says otherwise.
DEFAULT_KEEP_SECONDS = 7 * 86400


@dataclass(frozen=True)
class Engine:
    """A container engine, as the SDK drives it."""

    name: str
    binary: str
    # What makes the files a job writes this computer's user's.
    user_flags: tuple = ()
    capacity_format: str = "{{.NCPU}} {{.MemTotal}}"
    gpu_flags: tuple = ("--gpus", "all")
    # Whether a name with no registry can be left for the engine to resolve.
    resolves_short_names: bool = True

    def image_name(self, image):
        """The image as this engine must be told: with no terminal to ask on,
        Podman refuses a short name it has no alias for."""
        if self.resolves_short_names:
            return image
        first, _, rest = image.partition("/")
        if rest and ("." in first or ":" in first or first == "localhost"):
            return image
        return f"docker.io/{image}" if rest else f"docker.io/library/{image}"

    def run(self, args, stdin=None, timeout=600):
        """(code, stdout, stderr). No engine is a refusal; one that does not
        answer is the engine's moment, not the job's."""
        try:
            done = subprocess.run([self.binary, *args], input=stdin, capture_output=True, text=True,
                                  timeout=timeout, check=False)
        except FileNotFoundError:
            raise errors.EngineMissing(_no_engine()) from None
        except subprocess.TimeoutExpired:
            raise errors.TransientError(f"{self.name} {args[0]} did not answer in {timeout} s") from None
        return done.returncode, done.stdout or "", done.stderr or ""


# On Windows, this computer's jobs (a container, a local cluster) run in
# WSL2: the SDK installed there sees Docker Desktop's engine and cluster.
WINDOWS_RUNS_IN_WSL = "Zenthros runs here inside WSL2: install the SDK there, with Docker Desktop's WSL integration on"


def _no_engine():
    if platform.system() == "Windows":
        return WINDOWS_RUNS_IN_WSL
    return "no container engine here: install Docker Desktop, OrbStack or Podman, then run this again"


def _user():
    """--user for Docker: the job's files stay this computer's user's."""
    if not hasattr(os, "getuid"):
        return ()
    return ("--user", f"{os.getuid()}:{os.getgid()}")


ENGINES = {
    "docker": lambda binary: Engine("docker", binary, _user()),
    "podman": lambda binary: Engine("podman", binary, ("--userns=keep-id",), capacity_format="{{.Host.CPUs}} {{.Host.MemTotal}}",
                                    gpu_flags=("--device", "nvidia.com/gpu=all"), resolves_short_names=False),
}


def find_engine():
    """The engine to use: ZENTHROS_ENGINE, else docker, else podman."""
    if platform.system() == "Windows":
        raise errors.EngineMissing(_no_engine())
    wanted = os.environ.get(ENGINE_ENV, "")
    for name in ([wanted] if wanted else list(ENGINES)):
        binary = shutil.which(name)
        if binary:
            return ENGINES.get(os.path.basename(name), ENGINES["docker"])(binary)
    raise errors.EngineMissing(_no_engine())


def project_root(start=None):
    """The folder of the project: the nearest above that holds a .zenthros
    or a .git, else where this runs."""
    if os.environ.get(ROOT_ENV):
        return Path(os.environ[ROOT_ENV]).resolve().parent.parent
    here = Path(start or os.getcwd()).resolve()
    for folder in (here, *here.parents):
        if any((folder / ".zenthros" / kept).is_dir() for kept in ("local", "local-k8s", "envs")) or (folder / ".git").exists():
            return folder
    return here


def cache_root():
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "zenthros" / "local"


@dataclass(frozen=True)
class Box:
    """The container a job needs, as creating it fixes it."""

    image: str
    cpus: str = ""
    memory: str = ""
    gpu: bool = False
    ports: tuple = ()
    network: str = ""
    output: str = ""

    def config(self):
        """What, if it changed, needs another container. Mounts go apart: a
        container with more of them serves too."""
        raw = json.dumps([self.image, self.cpus, self.memory, self.gpu, list(self.ports), self.network, self.output])
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


def container_size(capacity=None):
    """The container's cpus and memory: what is configured, else all of the
    engine's CPUs and three quarters of its memory (capacity, if known: its
    cpus and bytes)."""
    from zenthros.machines import LOCAL_CPUS_ENV, LOCAL_MEMORY_ENV, bytes_of, docker_memory

    current = _settings()
    cpus = str(current.local_cpus or os.environ.get(LOCAL_CPUS_ENV) or "")
    memory = str(current.local_memory or os.environ.get(LOCAL_MEMORY_ENV) or "")
    if not cpus or not memory:
        from zenthros.machines import docker_capacity

        engine = None if capacity else find_engine()
        total_cpus, total_memory = capacity or docker_capacity(engine.binary, engine.capacity_format)
        cpus = cpus or str(total_cpus)
        memory = memory or f"{max(1, total_memory * 3 // 4) >> 20}m"
    return cpus, docker_memory(memory), bytes_of(memory)


def _settings():
    from zenthros.remote.options import settings

    return settings()


def _fetch_script():
    return (Path(__file__).parent.parent / "_data" / "container-fetch.sh").read_text()


def agent_pin():
    """The release this wheel goes with: {"install_base", "sha256sums_sha256"}.
    Written by install/build-sdk.sh; absent in a checkout."""
    try:
        return json.loads((Path(__file__).parent.parent / "_data" / "agent-release.json").read_text())
    except OSError:
        return None


class LocalJobClient(JobClient):
    """Jobs in a container of this computer."""

    transport = "local"
    place_name = "local"
    poll_seconds = 0.5
    # A process that failed is sent again from here; a cluster retries its own.
    reruns_here = True
    out_of_memory = _OUT_OF_MEMORY

    def __init__(self, root=None, engine=None):
        self.project = project_root(root)
        self.root = Path(os.environ.get(ROOT_ENV) or self.project / ".zenthros" / "local").resolve()
        self.id = hashlib.sha256(str(self.root).encode()).hexdigest()[:10]
        self._engine = engine
        self._mounts_wanted = {}
        self._to_seal = {}
        self._lock = threading.Lock()
        # One start at a time: another's ensure may make the container again
        # while this one's job has not reached it.
        self._starting = threading.Lock()
        self._collected = False

    # --- where things are -------------------------------------------------

    @property
    def engine(self):
        if self._engine is None:
            self._engine = find_engine()
        return self._engine

    @property
    def store_dir(self):
        return self.root / "store"

    @property
    def store(self):
        return f"file://{self.store_dir}"

    def sees_store(self):
        """Its container has the store: it mounts it."""
        return True

    def output_for(self, name):
        """$ZENTHROS_DATA_OUTPUT of a job: under the store, or under the root
        configure(local_output=) gives. A plain path, the same in the
        container, that a script can open as it is."""
        outputs = _settings().local_output
        folder = Path(outputs).resolve() / name if outputs else self.store_dir / name
        folder.mkdir(parents=True, exist_ok=True)
        return str(folder)

    def _job_file(self, name):
        return self.root / "jobs" / f"{name}.json"

    def _log_file(self, name):
        return self.root / "jobs" / f"{name}.log"

    def _read(self, name):
        try:
            return json.loads(self._job_file(name).read_text())
        except FileNotFoundError:
            raise errors.JobNotFound(f"job {name} not found") from None
        except ValueError as err:
            raise errors.TransientError(f"the record of job {name} does not read: {err}") from None

    def _write(self, name, record):
        # What it keeps here stays out of the project's git, as a venv does.
        ignored = self.root.parent / ".gitignore"
        if not ignored.exists():
            self.root.parent.mkdir(parents=True, exist_ok=True)
            ignored.write_text("*\n")
        path = self._job_file(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}")
        tmp.write_text(json.dumps(record))
        os.replace(tmp, path)

    def knows(self, name):
        return self._job_file(name).exists()

    # --- the container ----------------------------------------------------

    def need_paths(self, paths):
        """The paths a call names: the next job's container has them
        read-only, each at its own path."""
        with self._lock:
            for path in paths:
                self._mounts_wanted[str(path)] = "ro"

    def _container_name(self, box):
        return f"zenthros-local-{self.id}-{hashlib.sha256(box.image.encode()).hexdigest()[:8]}"

    def _inspect(self, name):
        code, out, err = self.engine.run(
            ["inspect", "--format", '{{.State.Status}}|{{index .Config.Labels "' + CONFIG_LABEL +
             '"}}|{{index .Config.Labels "' + MOUNTS_LABEL + '"}}|{{.State.OOMKilled}}', name], timeout=60)
        if code != 0:
            if "No such" in err or "no such" in err:
                return None
            raise errors.TransientError(f"{self.engine.name} inspect {name}: {err.strip()}")
        state, config, mounts, oom = (out.strip().split("|") + ["", "", ""])[:4]
        return {"state": state, "config": config, "mounts": _decode_mounts(mounts), "oom": oom == "true"}

    def _running_jobs(self, container=None):
        """The names of this project's jobs in a container and not finished,
        in container if one is named. One still waiting to start is not in any."""
        found = []
        folder = self.root / "jobs"
        if not folder.is_dir():
            return found
        for path in folder.glob("*.json"):
            try:
                record = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if record.get("final") or not record.get("local") or (container and record.get("container") != container):
                continue
            found.append(path.stem)
        return found

    def ensure(self, box):
        """The container for box, its agent answering. Made, started or made
        again as needed; a container with jobs in it is never touched."""
        name = self._container_name(box)
        with self._lock:
            wanted = dict(self._mounts_wanted)
        self._only_one_image(name)
        found = self._inspect(name)
        if found is not None:
            union = {**found["mounts"], **wanted}
            if found["config"] == box.config() and union == found["mounts"]:
                if found["state"] != "running":
                    self._start(name)
                self._wait_agent(name)
                return name
            wanted = union if found["config"] == box.config() else wanted
            busy = [job for job in self._running_jobs(name) if self._live(job)]
            if busy:
                raise errors.ValidationError(
                    f"the container needs other mounts or limits, and {', '.join(sorted(busy))} still runs in it: "
                    "wait for it, or cancel it")
            self.engine.run(["rm", "-f", name], timeout=120)
        self._create(name, box, wanted)
        self._wait_agent(name)
        return name

    def _only_one_image(self, name):
        """Another image's container of this project: stopped if idle, and a
        refusal if it runs a job (one environment at a time)."""
        code, out, _ = self.engine.run(["ps", "--filter", f"label={LOCAL_LABEL}={self.id}", "--format", "{{.Names}}"], timeout=60)
        if code != 0:
            return
        for other in out.split():
            if other == name:
                continue
            busy = [job for job in self._running_jobs(other) if self._live(job)]
            if busy:
                raise errors.ValidationError(
                    f"{', '.join(sorted(busy))} still runs in another environment, and local runs one at a time: "
                    "wait for it, or use on='local-k8s' for several at once")
            self.engine.run(["stop", "-t", "2", other], timeout=60)

    def _live(self, job):
        try:
            return self.job_object(job).get("status", {}).get("phase") not in TERMINAL_PHASES
        except errors.ZenthrosError:
            return False

    def _start(self, name):
        code, _, err = self.engine.run(["start", name], timeout=120)
        if code != 0:
            raise errors.TransientError(f"{self.engine.name} start {name}: {err.strip()}")

    def _create(self, name, box, mounts):
        agent = self._agent_binary(box.image)
        home = cache_root() / "home" / hashlib.sha256(box.image.encode()).hexdigest()[:12]
        home.mkdir(parents=True, exist_ok=True)
        store = self.root / "store"
        store.mkdir(parents=True, exist_ok=True)
        args = ["run", "-d", "--name", name, *self.engine.user_flags,
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                "--label", f"{LOCAL_LABEL}={self.id}", "--label", f"{CONFIG_LABEL}={box.config()}",
                "--label", f"{MOUNTS_LABEL}={_encode_mounts(mounts)}",
                "-e", "ZENTHROS_LOCAL=1", "-e", "ZENTHROS_AGENT_MODE=container",
                "-e", f"ZENTHROS_STATE_DIR={STATE_DIR}", "-e", f"ZENTHROS_CONTAINER_IMAGE={box.image}",
                "-e", f"HOME={HOME_DIR}", "-e", "PYTHONUNBUFFERED=1",
                "-e", f"ZENTHROS_LOCAL_IDLE_EXIT={os.environ.get(IDLE_ENV, '30m')}",
                "-v", f"{agent}:{AGENT_PATH}:ro", "-v", f"{home}:{HOME_DIR}", "-v", f"{self.root}:{self.root}"]
        if box.output:
            os.makedirs(box.output, exist_ok=True)
            args += ["-v", f"{box.output}:{box.output}"]
        for path in sorted(mounts):
            args += ["-v", f"{path}:{path}:{mounts[path]}"]
        if box.cpus:
            args += ["--cpus", box.cpus]
        if box.memory:
            args += ["--memory", box.memory, "--memory-swap", box.memory]
        if box.gpu:
            if platform.system() == "Darwin":
                raise errors.ValidationError("a container on macOS has no GPU: run this on a machine that has one")
            args += list(self.engine.gpu_flags)
        if box.network:
            args += ["--network", box.network]
        for port in box.ports:
            args += ["-p", f"127.0.0.1:{port}:{port}"]
        if box.ports:
            args += ["-e", ENDPOINT_TEMPLATE]
        code, _, err = self.engine.run([*args, "--entrypoint", AGENT_PATH, box.image], timeout=1800)
        if code != 0:
            self.engine.run(["rm", "-f", name], timeout=60)
            if "Conflict" not in err:
                raise errors.ValidationError(f"{self.engine.name} run {box.image}: {err.strip()}{_offline_hint(err)}")

    def _agent_binary(self, image):
        """The agent the container runs, mounted read-only: a job can never
        change what the next start runs. The first time it comes from the
        release this wheel pins, verified by the same script a machine runs."""
        built = os.environ.get(AGENT_ENV)
        if built:
            return str(Path(built).resolve())
        pin = agent_pin()
        if not pin:
            raise errors.ValidationError(
                f"this SDK comes with no agent release (a checkout?): build one, and set {AGENT_ENV} to it")
        arch = self._image_arch(image)
        folder = cache_root() / "agent" / pin["sha256sums_sha256"][:16] / arch
        binary = folder / "bin" / "zenthros-agent"
        if binary.exists():
            return str(binary)
        folder.mkdir(parents=True, exist_ok=True)
        print(f"zenthros: preparing local ({image} and the agent, once)", file=sys.stderr, flush=True)
        code, _, err = self.engine.run(
            ["run", "--rm", *self.engine.user_flags, "-v", f"{folder}:/fetch", "-e", "ZENTHROS_STATE_DIR=/fetch",
             "-e", f"ZENTHROS_INSTALL_BASE={pin['install_base']}", "-e", f"ZENTHROS_SHA256SUMS_SHA256={pin['sha256sums_sha256']}",
             "-e", "ZENTHROS_BAKED_AGENT=/nonexistent", image, "/bin/sh", "-c", "zenthros_trust_sums=\n" + _fetch_script()],
            timeout=1800)
        if code != 0 or not binary.exists():
            raise errors.ValidationError(f"preparing the agent from {pin['install_base']}: {err.strip()}{_offline_hint(err)}")
        return str(binary)

    def missing_offline(self, image=None):
        """What running image here would still have to download: nothing means
        it works with no network."""
        image = self.engine.image_name(image or _default_image())
        missing = []
        code, out, _ = self.engine.run(["image", "inspect", "--format", "{{.Architecture}}", image], timeout=60)
        if code != 0:
            missing.append(f"the image {image}")
        pin = agent_pin()
        if not os.environ.get(AGENT_ENV):
            arch = out.strip() or platform.machine()
            folder = cache_root() / "agent" / (pin or {}).get("sha256sums_sha256", "")[:16] / arch
            if not pin or not (folder / "bin" / "zenthros-agent").exists():
                missing.append("the agent")
        return missing

    def _image_arch(self, image):
        code, out, err = self.engine.run(["image", "inspect", "--format", "{{.Architecture}}", image], timeout=60)
        if code != 0:
            code, _, err = self.engine.run(["pull", image], timeout=1800)
            if code != 0:
                raise errors.ValidationError(f"{self.engine.name} pull {image}: {err.strip()}{_offline_hint(err)}")
            code, out, err = self.engine.run(["image", "inspect", "--format", "{{.Architecture}}", image], timeout=60)
        return out.strip() or platform.machine()

    def _wait_agent(self, name):
        deadline = time.monotonic() + AGENT_READY_SECONDS
        while True:
            code, out, err = self._exec(name, "info", "--wait", "2s")
            if code == 0:
                return json.loads(out)
            found = self._inspect(name)
            if found is None or found["state"] not in ("running", "created"):
                _, logs, more = self.engine.run(["logs", "--tail", "20", name], timeout=60)
                raise errors.ValidationError(f"the local container stopped as it started: {(logs + more).strip() or err.strip()}")
            if time.monotonic() >= deadline:
                raise errors.TransientError(f"the agent in {name} did not answer in {AGENT_READY_SECONDS} s: {err.strip()}")
            time.sleep(0.2)

    def _exec_args(self, container, *args):
        """The engine's arguments that run a verb of the agent."""
        return ["exec", "-i", container, AGENT_PATH, "local", *args]

    def _exec(self, container, *args, stdin=None, timeout=120):
        return self.engine.run(self._exec_args(container, *args), stdin=stdin, timeout=timeout)

    # --- jobs -------------------------------------------------------------

    def seal_at_start(self, name, secrets):
        """Secrets of the job about to be submitted: in this process's memory
        until its container's agent says its key, so no file here holds them."""
        if secrets:
            self._to_seal[name] = dict(secrets)

    def _sealed_sources(self, container, secrets):
        from zenthros import seal

        code, out, err = self._exec(container, "info")
        if code != 0:
            raise errors.TransientError(f"the agent in {container}: {err.strip()}")
        recipients = json.loads(out).get("ageRecipients") or []
        if not recipients:
            raise errors.ValidationError(f"the agent in {container} has no key to seal secrets to")
        sources = []
        for target, secret in sorted(secrets.items()):
            payload = S.EncryptedPayload(ciphertext=seal.seal(secret.local_value, recipients),
                                         key_id=seal.key_id(recipients[0]), recipients=list(recipients))
            sources.append(replace(secret, payload=payload, local_value=None).source(target))
        return sources

    def _box_for(self, manifest):
        template = manifest["spec"]["workload"]["template"]
        compute = manifest["spec"].get("compute") or {}
        image = self.engine.image_name(template.get("image") or _default_image())
        cpus, memory, memory_bytes = container_size()
        asked = compute.get("memory")
        if asked:
            from zenthros.machines import bytes_of

            if bytes_of(asked) > memory_bytes:
                raise errors.ValidationError(
                    f"memory={asked!r} is more than the local container has ({memory}): raise it with "
                    "zenthros.configure(local_memory=...), or run on a bigger machine")
        ports = tuple(sorted(p["port"] for p in ((template.get("service") or {}).get("ports") or [])))
        outputs = _settings().local_output
        return Box(image=image, cpus=cpus, memory=memory, gpu=bool(compute.get("gpuCount") or compute.get("gpuModel")),
                   ports=ports, network=os.environ.get(NETWORK_ENV, ""),
                   output=str(Path(outputs).resolve()) if outputs else "")

    def submit(self, job):
        job = require_job(job)
        if self.knows(job.name):
            raise errors.Conflict(f"job {job.name} already exists here: name another, or zenthros.get({job.name!r})")
        self._collect_old()
        manifest = job.to_manifest()
        manifest["metadata"]["uid"] = str(uuid.uuid4())
        manifest["metadata"]["creationTimestamp"] = _now()
        manifest["metadata"].pop("namespace", None)
        box = self._box_for(manifest)
        record = {"manifest": manifest, "status": {"phase": PENDING}, "final": False, "pid": os.getpid(),
                  "container": self._container_name(box)}
        depends = list(manifest["spec"].get("dependsOn") or [])
        self._write(job.name, record)
        if depends:
            threading.Thread(target=self._start_after, args=(job.name, box, depends), daemon=True).start()
            return JobHandle(job.name, self)
        try:
            self._start_job(job.name, box)
        except BaseException:
            self._job_file(job.name).unlink(missing_ok=True)
            raise
        return JobHandle(job.name, self)

    def _start_job(self, name, box):
        record = self._read(name)
        manifest = record["manifest"]
        template = manifest["spec"]["workload"]["template"]
        with self._starting:
            container = self.ensure(box)
            held = self._to_seal.pop(name, None)
            if held:
                sealed = [S.to_plain(source) for source in self._sealed_sources(container, held)]
                template["secretSources"] = [*(template.get("secretSources") or []), *sealed]
            code, out, err = self._exec(container, "submit", stdin=json.dumps(self._with_its_cursor(name, manifest)),
                                        timeout=600)
            if code != 0:
                raise errors.Refused(f"job {name}: {err.strip()}")
            record["local"] = json.loads(out)
            record["container"] = container
            record["status"] = {"phase": "Running"}
            # Inline it is the SDK's core, tens of KiB per job: the job has it
            # now, unless a retry will need it again, or it holds the call too.
            if record.get("restarts", 0) >= _backoff(manifest) and not any(map(is_carried, template.get("command") or [])):
                (template.get("source") or {}).pop("inline", None)
            self._write(name, record)

    def _start_after(self, name, box, depends):
        """A job with upstreams starts once they end: this computer is on
        anyway. The pins of what they reported go where a controller puts
        them."""
        try:
            pins = {}
            for upstream in depends:
                while True:
                    obj = self.job_object(upstream)
                    phase = obj.get("status", {}).get("phase")
                    if phase in TERMINAL_PHASES:
                        break
                    time.sleep(self.poll_seconds)
                if phase != SUCCEEDED:
                    self._end(name, FAILED, "UPSTREAM_FAILED", f"{upstream} ended {phase}")
                    return
                pin = self._results_pin(upstream)
                if pin:
                    pins[upstream] = pin
            record = self._read(name)
            if pins:
                record["manifest"].setdefault("status", {})["upstreamResults"] = pins
                self._write(name, record)
            self._start_job(name, box)
        except errors.ZenthrosError as err:
            self._end(name, FAILED, "SUBMIT_FAILED", str(err))

    def _results_pin(self, name):
        """The controller's pin of a job's results (resultdigest.Pin)."""
        from zenthros.remote.call import RESULT_REPORT, results_pin

        digests = []
        for view in self.reports(name):
            entry = view.payload.get(RESULT_REPORT)
            if isinstance(entry, dict) and entry.get("uri") and entry.get("sha256"):
                digests.append((entry["uri"], entry["sha256"]))
        return results_pin(digests) if digests else ""

    def cursor_of(self, job_id):
        """What the agent holds while the job runs; once it ended, or with its
        container gone, what the agent last said, kept on its record."""
        record = self._read(job_id)
        container = record.get("container")
        if record.get("final") or not container or self._gone(container):
            return (record.get("status") or {}).get("progress")
        code, out, err = self._exec(container, "progress", record["manifest"]["metadata"]["uid"], timeout=60)
        if code != 0:
            raise errors.TransientError(f"the checkpoint of {job_id}: {err.strip()}")
        return json.loads(out) if out.strip() else None

    def _with_its_cursor(self, name, manifest):
        """manifest, as the job goes to the agent: with no controller here, this
        client looks at the job it resumes from as the controller would, and the
        same field carries the checkpoint. A job's own cursor is the agent's."""
        source = manifest["spec"].get("resumeFrom")
        if not source:
            return manifest
        try:
            theirs = self._read(source["job"])
        except errors.JobNotFound:
            raise errors.Refused(f"job {name}: resumeFrom: job {source['job']} is gone: run it from the beginning") from None
        if theirs["manifest"]["metadata"]["uid"] != source["uid"]:
            raise errors.Refused(f"job {name}: resumeFrom: {source['job']} is another job now")
        if not theirs.get("final"):
            raise errors.Refused(f"job {name}: resumeFrom: job {source['job']} has not ended")
        cursor = self.cursor_of(source["job"])
        if not cursor:
            return manifest
        return {**manifest, "status": {**(manifest.get("status") or {}), "progress": cursor}}

    def _end(self, name, phase, reason, message):
        record = self._read(name)
        seen = {"progress": record["status"]["progress"]} if (record.get("status") or {}).get("progress") else {}
        record["status"] = {"phase": phase, "message": message, "reasons": [{"code": reason, "detail": message}],
                            "completionTime": _now(), **seen}
        record["final"] = True
        self._write(name, record)

    def job_object(self, job_id):
        record = self._read(job_id)
        if not record.get("final") and record.get("local"):
            record = self._refresh(job_id, record)
        elif not record.get("final") and not _alive(record.get("pid")):
            self._end(job_id, FAILED, "SUBMIT_FAILED", "the process that was waiting for its upstream went")
            record = self._read(job_id)
        return {**record["manifest"], "status": record["status"]}

    def _refresh(self, name, record):
        code, out, err = self._exec(record["container"], "status", stdin=json.dumps(record["local"]), timeout=60)
        if code != 0:
            if self._gone(record["container"]):
                # The kernel may take the agent itself for a job's memory:
                # the job is still what ran the container out of it.
                found = self._inspect(record["container"])
                if found and found["oom"]:
                    self._end(name, FAILED, "OOM_KILLED", self.out_of_memory)
                else:
                    self._end(name, FAILED, "WORKER_LOST", "the local container went away: run it again")
                return self._read(name)
            raise errors.TransientError(f"job {name}: {err.strip()}")
        status = json.loads(out)
        spec = record["manifest"]["spec"]
        deadline = spec.get("activeDeadlineSeconds")
        if deadline and status.get("phase") not in TERMINAL_PHASES and status.get("startTime"):
            if time.time() - seconds_at(status["startTime"]) > int(deadline):
                self._exec(record["container"], "cancel", stdin=json.dumps(record["local"]))
                status = {**status, "phase": CANCELLED}
        # Only the agent's own timer cancels with a record not yet final:
        # cancel() here writes it as it asks.
        if status.get("phase") == CANCELLED:
            status = _past_its_limit(spec, status)
        if status.get("phase") not in TERMINAL_PHASES:
            record["status"] = status
            self._write(name, record)
            return record
        if self.reruns_here and _process_failed(status) and record.get("restarts", 0) < _backoff(record["manifest"]):
            return self._run_again(name, record)
        return self._finish(name, record, status)

    def pull_hint(self, message):
        """What to do about an image that did not come, if the message says it."""
        return _offline_hint(message)

    def _gone(self, container):
        found = self._inspect(container)
        return found is None or found["state"] != "running"

    def _collect(self, name, record):
        """What the agent holds of this attempt comes here: reports and log
        add up over a job's attempts."""
        for task in record["local"]["tasks"]:
            code, out, _ = self._exec(record["container"], "reports", task["id"], timeout=120)
            if code == 0 and out.strip():
                record.setdefault("reports", []).extend(json.loads(out) or [])
        self._log_file(name).parent.mkdir(parents=True, exist_ok=True)
        with self._log_file(name).open("a") as log:
            for task in record["local"]["tasks"]:
                code, out, _ = self._exec(record["container"], "logs", task["handle"], timeout=600)
                if code == 0:
                    log.write(out)

    def _let_go(self, record):
        """Only after what it holds was kept, so nothing is lost in between."""
        local = record["local"]
        self._exec(record["container"], "cancel", stdin=json.dumps(local), timeout=120)
        for task in local["tasks"]:
            self._exec(record["container"], "forget", task["id"], timeout=60)

    def _run_again(self, name, record):
        """A process that failed, with retries left: its next attempt, as the
        controller does on a machine (retryProcess). It runs under its own
        identity (ZENTHROS_ATTEMPT), with the reports keyed by the job."""
        self._collect(name, record)
        self._write(name, record)
        self._let_go(record)
        record["restarts"] = record.get("restarts", 0) + 1
        record["manifest"]["status"] = {**(record["manifest"].get("status") or {}), "runtime": "process",
                                        "restarts": record["restarts"]}
        self._write(name, record)
        self._start_job(name, self._box_for(record["manifest"]))
        return self._read(name)

    def _finish(self, name, record, status):
        """The job ended: what the agent holds of it comes here, for the
        agent to let it go. Its record outlives the container."""
        self._collect(name, record)
        if status.get("phase") == FAILED:
            status = _explained(status, record["container"], self)
        status.setdefault("completionTime", _now())
        record["status"], record["final"] = status, True
        self._write(name, record)
        self._let_go(record)
        return record

    def job_status(self, job_id):
        return status_from_object(self.job_object(job_id))

    def cancel(self, job_id):
        record = self._read(job_id)
        if record.get("final"):
            return
        if record.get("local"):
            cursor = self.cursor_of(job_id)
            code, _, err = self._exec(record["container"], "cancel", stdin=json.dumps(record["local"]), timeout=120)
            if code != 0 and not self._gone(record["container"]):
                raise errors.TransientError(f"cancel {job_id}: {err.strip()}")
            for task in record["local"]["tasks"]:
                self._exec(record["container"], "forget", task["id"], timeout=60)
            if cursor:
                record["status"] = {**record.get("status", {}), "progress": cursor}
        record["status"] = {**record.get("status", {}), "phase": CANCELLED, "message": "cancelled", "completionTime": _now()}
        record["final"] = True
        self._write(job_id, record)

    def delete(self, job_id):
        self.cancel(job_id)
        self._job_file(job_id).unlink(missing_ok=True)
        self._log_file(job_id).unlink(missing_ok=True)

    def logs(self, job_id, tail=None):
        record = self._read(job_id)
        if record.get("final"):
            try:
                return clip_log(self._log_file(job_id).read_bytes(), tail)
            except FileNotFoundError:
                return ""
        text = []
        for task in (record.get("local") or {}).get("tasks", []):
            args = ["logs", *(["--tail", str(int(tail))] if tail is not None else []), task["handle"]]
            code, out, err = self._exec(record["container"], *args, timeout=120)
            if code != 0:
                raise errors.TransientError(f"logs of {job_id}: {err.strip()}")
            text.append(out)
        return clip_log("".join(text).encode("utf-8"), tail)

    def follow_logs(self, job_id):
        """The log as it is written, from a process: its stdout is read until
        the job ends. A map's tasks are read one after the other."""
        record = self._read(job_id)
        tasks = (record.get("local") or {}).get("tasks", [])
        if record.get("final") or len(tasks) != 1:
            return None
        return subprocess.Popen(
            [self.engine.binary, *self._exec_args(record["container"], "logs", "--follow", tasks[0]["handle"])],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

    def reports(self, job_id):
        record = self._read(job_id)
        if record.get("final") or not record.get("local"):
            return parse_report_body(job_id, json.dumps(record.get("reports") or []))
        found = []
        for task in record["local"]["tasks"]:
            code, out, err = self._exec(record["container"], "reports", task["id"], timeout=60)
            if code != 0:
                if self._gone(record["container"]):
                    return found
                raise errors.TransientError(f"reports of {job_id}: {err.strip()}")
            found.extend(parse_report_body(job_id, out))
        return found

    def list_jobs(self, selector=""):
        """Every job here, as stored; selector is k=v on its labels."""
        key, _, value = selector.partition("=")
        out = []
        folder = self.root / "jobs"
        for path in sorted(folder.glob("*.json")) if folder.is_dir() else ():
            try:
                obj = self.job_object(path.stem)
            except errors.NotFound:
                continue
            if not selector or (obj["metadata"].get("labels") or {}).get(key) == value:
                out.append(obj)
        return out

    # --- what ends on its own ---------------------------------------------

    def _collect_old(self, now=None):
        """Records past their keep_for go, once per process: no daemon does
        it for us. What jobs left in the store stays (`zenthros clean`)."""
        if self._collected:
            return
        self._collected = True
        now = now or time.time()
        folder = self.root / "jobs"
        for path in folder.glob("*.json") if folder.is_dir() else ():
            try:
                record = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if not record.get("final"):
                continue
            ended = seconds_at((record.get("status") or {}).get("completionTime"))
            keep = (record["manifest"]["spec"].get("gcPolicy") or {}).get("ttlSecondsAfterFinished") or DEFAULT_KEEP_SECONDS
            if ended and now - ended > int(keep):
                path.unlink(missing_ok=True)
                self._log_file(path.stem).unlink(missing_ok=True)

    def containers(self):
        """This project's containers: name and state."""
        code, out, _ = self.engine.run(["ps", "-a", "--filter", f"label={LOCAL_LABEL}={self.id}", "--format", "{{.Names}} {{.State}}"], timeout=60)
        if code != 0:
            return []
        return [tuple(line.split(None, 1)) for line in out.splitlines() if line.strip()]

    def clean(self, dry_run=False):
        """Removes this project's containers; jobs that still run are not
        touched and keep theirs."""
        removed = []
        for name, _state in self.containers():
            if any(self._live(job) for job in self._running_jobs(name)):
                continue
            if dry_run:
                removed.append(name)
                continue
            code, _, _ = self.engine.run(["rm", "-f", name], timeout=120)
            if code == 0:
                removed.append(name)
        return removed

    def clean_results(self, dry_run=False, outputs=False):
        """Removes from the store what jobs whose record is gone (past keep_for)
        leave besides their outputs; outputs=True, those too. What a good cache
        entry pins stays; a forged one protects nothing. Returns the jobs that
        lost something."""
        from zenthros import cache as entries
        from zenthros.core.storage import FileBlobStore
        from zenthros.remote.call import is_output

        store = self.store_dir
        removed = []
        pinned, _ = entries.referenced(entries.Home(self.store, FileBlobStore()))
        pinned = {uri.removeprefix("file://") for uri in pinned}
        for folder in sorted(store.iterdir()) if store.is_dir() else ():
            if not folder.is_dir() or folder.name in _NOT_JOBS or self.knows(folder.name):
                continue
            going = [path for path in folder.rglob("*") if path.is_file() and str(path) not in pinned and
                     (outputs or not is_output(str(path.relative_to(folder))))]
            if not going:
                continue
            removed.append(folder.name)
            if dry_run:
                continue
            for path in going:
                path.unlink()
            for path in sorted(folder.rglob("*"), reverse=True):
                if path.is_dir() and not any(path.iterdir()):
                    path.rmdir()
            if not any(folder.iterdir()):
                folder.rmdir()
        return removed

    def clean_envs(self, dry_run=False):
        """Removes the cached Python of each image no container of any project
        still has: they are shared, and a container starts with its folder."""
        code, out, err = self.engine.run(["ps", "-a", "--filter", f"label={LOCAL_LABEL}", "--format", "{{.Names}}"], timeout=60)
        if code != 0:
            raise errors.TransientError(f"{self.engine.name} ps: {err.strip()}")
        used = [name.rsplit("-", 1)[-1] for name in out.split()]
        removed = []
        home = cache_root() / "home"
        for folder in sorted(home.iterdir()) if home.is_dir() else ():
            if any(folder.name.startswith(digest) for digest in used):
                continue
            removed.append(folder.name)
            if not dry_run:
                shutil.rmtree(folder)
        return removed


def _past_its_limit(spec, status):
    """A job the agent stopped at its time limit, as the controller says it."""
    stop_after = (spec["workload"]["template"].get("service") or {}).get("stopAfterSeconds")
    if stop_after:
        detail = f"it served for its stop_after ({stop_after} s)"
        return {**status, "reason": "STOP_AFTER", "message": detail, "reasons": [{"code": "STOP_AFTER", "detail": detail}]}
    detail = f"it ran past its timeout ({spec.get('activeDeadlineSeconds')} s)"
    return {**status, "phase": FAILED, "reason": "DEADLINE_EXCEEDED", "message": detail,
            "reasons": [{"code": "DEADLINE_EXCEEDED", "detail": detail}]}


def _backoff(manifest):
    return int((manifest["spec"]["workload"]["template"].get("restartPolicy") or {}).get("backoffLimit") or 0)


def _process_failed(status):
    """A failure of the process, not of what it ran in (processFailed in the
    controller): only that is run again."""
    return status.get("phase") == FAILED and any(
        (reason or {}).get("code") in ("PROCESS_FAILED", "SERVICE_ENDED") for reason in status.get("reasons") or [])


def _encode_mounts(mounts):
    return base64.urlsafe_b64encode(json.dumps(mounts, sort_keys=True).encode()).decode()


def _decode_mounts(text):
    if not text:
        return {}
    try:
        return json.loads(base64.urlsafe_b64decode(text.encode()))
    except ValueError:
        return {}


def _default_image():
    from zenthros.machines import python_image, python_minor

    return python_image(python_minor())


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _offline_hint(text):
    lowered = text.lower()
    if any(word in lowered for word in ("no such host", "temporary failure", "network is unreachable",
                                        "dial tcp", "could not resolve", "connection refused", "i/o timeout")):
        return "\nIt had no network, and what it needed is not here yet: run this once with network."
    return ""


def _explained(status, container, client):
    """A job the container's memory limit killed says so: the agent sees a
    signal, the engine knows why."""
    message = status.get("message", "")
    if "SIGKILL" in message or "signal killed" in message or status.get("exitCode") == 137:
        status = {**status, "reason": "OOM_KILLED", "message": client.out_of_memory,
                  "reasons": [{"code": "OOM_KILLED", "detail": message}]}
    elif client.pull_hint(message):
        status = {**status, "message": f"{message}{client.pull_hint(message)}"}
    else:
        # What the kernel killed may be a child the job only saw fail. The
        # engine's flag lasts as long as the container: it says may.
        found = client._inspect(container)
        if found and found["oom"]:
            status = {**status, "message": f"{message}\nThis container has run out of memory, which may be why: raise "
                                           "memory= (zenthros.configure(local_memory=...))"}
    return status
