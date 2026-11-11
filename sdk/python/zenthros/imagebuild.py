# SPDX-License-Identifier: Apache-2.0

"""How an image is built, loaded into this computer's engine, published and
read back: what an environment (zenthros.env) asks of an engine.

Docker's own builder cannot choose the compression of a layer, so with Docker
this is a builder of ours, as operator/production-images/build-all.sh makes
its own: BuildKit in a container, with a cap on its disk. Podman builds by
itself.

An image has two digests. Its manifest's are the bytes published, and change
with the compression; its config's list the layers uncompressed, and are what
runs: the environment's content, which a recompression keeps.
"""

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from zenthros import errors

BUILDER_ENV = "ZENTHROS_ENV_BUILDER"
BUILDER = "zenthros"
# The registry of the documented cluster (k3d cluster create), which speaks
# plain HTTP, and the same cap the development builder has.
BUILDKITD_CONFIG = """\
[registry."localhost:5111"]
  http = true
  insecure = true

[worker.oci]
  maxUsedSpace = "20GB"
"""
BUILD_SECONDS = 3600
READ_SECONDS = 120
_LOCAL_HOSTS = ("localhost", "127.0.0.1")
# What a registry says when nobody logged in, or the login may not do this.
_DENIED = re.compile(r"denied|unauthorized|authentication required|no basic auth|\b401\b|\b403\b", re.I)
_LIMITED = re.compile(r"\b429\b|too ?many ?requests", re.I)
# The one place that knows what each registry wants for a login.
_LOGIN = (
    (re.compile(r"-docker\.pkg\.dev$"), "gcloud auth configure-docker {host}, and the repository must exist"),
    (re.compile(r"\.gcr\.io$|^gcr\.io$"), "gcloud auth configure-docker {host}"),
    (re.compile(r"\.dkr\.ecr\.[^.]+\.amazonaws\.com$"),
     "aws ecr get-login-password | docker login --username AWS --password-stdin {host}, and the repository must exist"),
    (re.compile(r"^docker\.io$"), "docker login"),
)


def host_of(ref):
    """The registry a reference names; docker.io when it names none."""
    first, _, rest = ref.partition("/")
    return first if rest and ("." in first or ":" in first or first == "localhost") else "docker.io"


def _is_local(ref):
    return host_of(ref).partition(":")[0] in _LOCAL_HOSTS


def _repository_of(ref):
    """ref without its tag or digest."""
    bare = ref.partition("@")[0]
    return bare.rpartition(":")[0] if ":" in bare.rpartition("/")[2] else bare


def login_hint(ref):
    host = host_of(ref)
    for pattern, command in _LOGIN:
        if pattern.search(host):
            return command.format(host=host)
    return f"docker login {host}"


def _refused(what, ref, err):
    """A failed talk with a registry, with the command that fixes a login."""
    text = err.strip()
    if _LIMITED.search(text):
        return errors.ValidationError(f"{what} {ref}: the registry says too many requests. It counts them by address "
                                      f"and allows more once logged in: {login_hint(ref)}; or try again later")
    if _DENIED.search(text):
        return errors.ValidationError(f"{what} {ref}: the registry said no. Log in: {login_hint(ref)}")
    return errors.ValidationError(f"{what} {ref}: {_tail(text)}")


def _tail(text, lines=12):
    return "\n".join(text.splitlines()[-lines:])


@dataclass
class Built:
    """An image's two digests."""

    content: str = ""
    manifest: str = ""


@dataclass
class Seen:
    """An image as a registry or the engine has it."""

    manifest: str = ""
    content: str = ""
    layers: list = field(default_factory=list)
    config: dict = field(default_factory=dict)


# What makes two images with the same layers run differently.
RUN_CONFIG = ("Env", "Entrypoint", "Cmd", "User", "WorkingDir")


def run_config(config):
    return {key: (config or {}).get(key) or None for key in RUN_CONFIG}


class Buildx:
    """Docker: a BuildKit of ours."""

    supports_zstd = True

    def __init__(self, engine):
        self.engine = engine
        self.builder = os.environ.get(BUILDER_ENV) or BUILDER
        self._builder_ready = False

    def _ensure_builder(self):
        if self._builder_ready:
            return
        code, _, _ = self.engine.run(["buildx", "inspect", self.builder], timeout=READ_SECONDS)
        if code != 0:
            with tempfile.TemporaryDirectory() as folder:
                config = Path(folder) / "buildkitd.toml"
                config.write_text(BUILDKITD_CONFIG)
                code, _, err = self.engine.run(
                    ["buildx", "create", "--name", self.builder, "--driver", "docker-container",
                     "--driver-opt", "network=host", "--buildkitd-config", str(config)], timeout=READ_SECONDS)
            if code != 0:
                raise errors.ValidationError(f"could not make the builder {self.builder}: {_tail(err)}")
        self._builder_ready = True

    def _build(self, context, output, platform="", network="", metadata=False):
        """(code, {metadata}, what BuildKit said)."""
        self._ensure_builder()
        args = ["buildx", "build", "--builder", self.builder, "--progress", "plain", "--provenance=false",
                "--output", output]
        if platform:
            args += ["--platform", platform]
        if network:
            args += ["--network", network]
        with tempfile.TemporaryDirectory() as folder:
            written = Path(folder) / "metadata.json"
            if metadata:
                args += ["--metadata-file", str(written)]
            code, _, err = self.engine.run([*args, str(context)], timeout=BUILD_SECONDS)
            return code, json.loads(written.read_text()) if metadata and code == 0 else {}, err

    def export(self, context, dest, network=""):
        """Builds context and leaves its files in dest."""
        return self._build(context, f"type=local,dest={dest}", network=network)[0::2]

    def load(self, context, tag, network=""):
        """Builds context into the engine. Asked for the digests apart: the
        engine's output says only the manifest's."""
        code, _, err = self._build(context, f"type=docker,name={tag}", network=network)
        if code != 0:
            return code, Built(), err
        code, meta, more = self._build(context, f"type=image,name={tag},push=false", network=network, metadata=True)
        return code, _built(meta), err + more

    def would_publish(self, context, ref, platform=""):
        """The digests a publish would give, with nothing sent anywhere."""
        code, meta, err = self._build(context, f"type=image,name={ref},push=false", platform, metadata=True)
        if code != 0:
            raise errors.ValidationError(f"building {ref}: {_tail(err)}")
        return _built(meta)

    def publish(self, context, ref, compression, platform=""):
        output = f"type=image,name={ref},push=true,oci-mediatypes=true,compression={compression}"
        if compression == "zstd":
            output += ",compression-level=3,force-compression=true"
        if _is_local(ref):
            output += ",registry.insecure=true"
        code, meta, err = self._build(context, output, platform, metadata=True)
        if code != 0:
            raise _refused("publishing", ref, err)
        return _built(meta)

    def copy(self, source, dest):
        """Publishes in dest what source has, with no build."""
        self._ensure_builder()
        code, _, err = self.engine.run(
            ["buildx", "imagetools", "create", "--builder", self.builder, "--prefer-index=false", "-t", dest, source],
            timeout=BUILD_SECONDS)
        if code != 0:
            raise _refused("copying to", dest, err)
        return Built()

    def read(self, ref, arch=""):
        """The image a registry has at ref, as it says. None if it has none."""
        self._ensure_builder()
        inspect = ["buildx", "imagetools", "inspect", "--builder", self.builder, ref]
        code, raw, err = self.engine.run([*inspect, "--raw"], timeout=READ_SECONDS)
        if code != 0:
            if "not found" in err.lower():
                return None
            raise _refused("reading", ref, err)
        manifest = json.loads(raw)
        code, image, err = self.engine.run([*inspect, "--format", "{{json .Image}}"], timeout=READ_SECONDS)
        if code != 0:
            raise _refused("reading", ref, err)
        config = json.loads(image)
        if "rootfs" not in config:
            # A list of platforms: this architecture's.
            config = next((value for key, value in config.items() if not arch or key.endswith(f"/{arch}")), {})
        return Seen(manifest="sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
                    content=(manifest.get("config") or {}).get("digest", ""),
                    layers=(config.get("rootfs") or {}).get("diff_ids") or [], config=run_config(config.get("config")))

    def base(self, ref, arch):
        """(digest, user) of a base image, read from its registry."""
        self._ensure_builder()
        inspect = ["buildx", "imagetools", "inspect", "--builder", self.builder, ref]
        code, raw, err = self.engine.run([*inspect, "--raw"], timeout=READ_SECONDS)
        if code != 0:
            raise _refused("reading", ref, err)
        # Only this architecture's: a registry may count each manifest it serves,
        # Docker Hub by address, and an index lists many.
        offered = {entry["platform"]["architecture"]: entry["digest"] for entry in json.loads(raw).get("manifests") or []
                   if entry.get("platform", {}).get("architecture") not in (None, "unknown")}
        if offered and arch not in offered:
            raise errors.ValidationError(f"{ref} is only {', '.join(sorted(offered))}, and this computer is {arch}")
        one = f"{_repository_of(ref)}@{offered[arch]}" if offered else ref
        code, image, err = self.engine.run(
            ["buildx", "imagetools", "inspect", "--builder", self.builder, one, "--format", "{{json .Image}}"],
            timeout=READ_SECONDS)
        if code != 0:
            raise _refused("reading", ref, err)
        config = json.loads(image)
        if not offered and config.get("architecture") != arch:
            raise errors.ValidationError(f"{ref} is only {config.get('architecture')}, and this computer is {arch}")
        return "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), (config.get("config") or {}).get("User") or ""

    def arch(self):
        code, out, err = self.engine.run(["version", "--format", "{{.Server.Arch}}"], timeout=READ_SECONDS)
        if code != 0:
            raise errors.ValidationError(f"{self.engine.name} does not answer: {_tail(err)}")
        return out.strip()

    def clean(self, dry_run=False):
        code, _, _ = self.engine.run(["buildx", "inspect", self.builder], timeout=READ_SECONDS)
        if code != 0:
            return []
        if not dry_run:
            self.engine.run(["buildx", "rm", self.builder], timeout=READ_SECONDS)
        return [f"builder {self.builder}"]


class PodmanBuild:
    """Podman builds by itself. It publishes gzip: zstd is not offered yet."""

    supports_zstd = False

    def __init__(self, engine):
        self.engine = engine

    def _tls(self, ref):
        return ["--tls-verify=false"] if _is_local(ref) else []

    def _args(self, context, network):
        return [*(["--network", network] if network else []), "--layers", "--format", "oci", str(context)]

    def export(self, context, dest, network=""):
        code, _, err = self.engine.run(["build", "--output", f"type=local,dest={dest}", *self._args(context, network)],
                                       timeout=BUILD_SECONDS)
        return code, err

    def load(self, context, tag, network=""):
        with tempfile.TemporaryDirectory() as folder:
            written = Path(folder) / "id"
            code, out, err = self.engine.run(["build", "--iidfile", str(written), "-t", tag,
                                              *self._args(context, network)], timeout=BUILD_SECONDS)
            content = written.read_text().strip() if code == 0 else ""
        return code, Built(content=content), out + err

    def would_publish(self, context, ref, platform=""):
        with tempfile.TemporaryDirectory() as folder:
            written = Path(folder) / "id"
            code, _, err = self.engine.run(
                ["build", "--iidfile", str(written), *(["--platform", platform] if platform else []),
                 *self._args(context, "")], timeout=BUILD_SECONDS)
            if code != 0:
                raise errors.ValidationError(f"building {ref}: {_tail(err)}")
            return Built(content=written.read_text().strip())

    def publish(self, context, ref, compression, platform=""):
        built = self.would_publish(context, ref, platform)
        with tempfile.TemporaryDirectory() as folder:
            written = Path(folder) / "digest"
            code, _, err = self.engine.run(
                ["push", *self._tls(ref), "--digestfile", str(written), "--compression-format", compression,
                 built.content, f"docker://{ref}"], timeout=BUILD_SECONDS)
            if code != 0:
                raise _refused("publishing", ref, err)
            built.manifest = written.read_text().strip()
        return built

    def copy(self, source, dest):
        code, _, err = self.engine.run(["pull", *self._tls(source), source], timeout=BUILD_SECONDS)
        if code != 0:
            raise _refused("copying to", dest, err)
        with tempfile.TemporaryDirectory() as folder:
            written = Path(folder) / "digest"
            code, _, err = self.engine.run(["push", *self._tls(dest), "--digestfile", str(written), source,
                                            f"docker://{dest}"], timeout=BUILD_SECONDS)
            if code != 0:
                raise _refused("copying to", dest, err)
            return Built(manifest=written.read_text().strip())

    def read(self, ref, arch=""):
        # manifest inspect takes only a list of platforms: what the registry has
        # is read by pulling it, by digest, which brings only what is not here.
        code, _, err = self.engine.run(["pull", *self._tls(ref), ref], timeout=BUILD_SECONDS)
        if code != 0:
            if "not found" in err.lower() or "manifest unknown" in err.lower():
                return None
            raise _refused("reading", ref, err)
        code, out, err = self.engine.run(["image", "inspect", ref], timeout=READ_SECONDS)
        if code != 0:
            raise _refused("reading", ref, err)
        found = json.loads(out)[0]
        return Seen(manifest=ref.partition("@")[2], content=f"sha256:{found['Id']}",
                    layers=(found.get("RootFS") or {}).get("Layers") or [], config=run_config(found.get("Config")))

    def base(self, ref, arch):
        code, _, err = self.engine.run(["pull", *self._tls(ref), self.engine.image_name(ref)], timeout=BUILD_SECONDS)
        if code != 0:
            raise _refused("reading", ref, err)
        code, out, err = self.engine.run(
            ["image", "inspect", "--format", "{{.Digest}} {{.Architecture}} {{.Config.User}}", self.engine.image_name(ref)],
            timeout=READ_SECONDS)
        if code != 0:
            raise errors.ValidationError(f"reading {ref}: {_tail(err)}")
        digest, found, *user = out.split()
        if found != arch:
            raise errors.ValidationError(f"{ref} is only {found}, and this computer is {arch}")
        return digest, user[0] if user else ""

    def arch(self):
        code, out, err = self.engine.run(["info", "--format", "{{.Host.Arch}}"], timeout=READ_SECONDS)
        if code != 0:
            raise errors.ValidationError(f"{self.engine.name} does not answer: {_tail(err)}")
        return out.strip()

    def clean(self, dry_run=False):
        return []


def _built(meta):
    return Built(content=meta.get("containerimage.config.digest", ""), manifest=meta.get("containerimage.digest", ""))


def builder_of(engine):
    return PodmanBuild(engine) if engine.name == "podman" else Buildx(engine)


def seen_here(engine, tag):
    """The image as this computer's engine has it, or None."""
    code, out, _ = engine.run(["image", "inspect", tag], timeout=READ_SECONDS)
    if code != 0:
        return None
    found = json.loads(out)[0]
    return Seen(layers=(found.get("RootFS") or {}).get("Layers") or [], config=run_config(found.get("Config")))


def commands_said(text):
    """What the commands inside a build printed, without BuildKit's numbering."""
    return "\n".join(match.group(1) for match in re.finditer(r"^#\d+ \d+\.\d+ (.*)$", text, re.M))
