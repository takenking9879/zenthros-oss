# SPDX-License-Identifier: Apache-2.0

"""An environment: an image made step by step, kept to repeat it, and published
as what was tried.

    zenthros.env.new("train", base="python:3.12-slim-bookworm", registry="...")
    zenthros.env.add("train", "polars", "xgboost==2.1")
    zenthros.env.build("train")       # into this computer's engine
    zenthros.env.use("train")         # what a local call runs in; works with no network
    zenthros.env.push("train")        # what you tried, to the registry you chose

    @zenthros.cloud(env="train")      # or .remote(env="train")

envs/<name>/env.lock.json says all of it and goes in Git. With a Dockerfile in
that folder, that is what builds, by the same builder and cache; without one,
Zenthros writes it. The lock holds the base by digest, the packages asked for
and the exact versions they resolved to, so a build with no cache installs the
same versions. It never holds a credential: a registry's login is the engine's.
"""

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from zenthros import console, errors, ignore, imagebuild

LOCK = "env.lock.json"
DOCKERFILE = "Dockerfile"
ENVS = "envs"
# The same uv the agent's machines use (install/upstream.lock), as an image.
UV_IMAGE = "ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490"
_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
_REQUIREMENT = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)")
_UV_CACHE = "--mount=type=cache,id=zenthros-uv,target=/root/.cache/uv"
_NO_NETWORK = re.compile(r"failed to fetch|dns error|error sending request|connect error|could not connect|"
                         r"temporary failure|network is unreachable|no such host|i/o timeout", re.I)
# Inside the built image: its Python, what it has, and whether the agent can
# run there: it needs glibc.
_LOOK_INSIDE = """\
import glob, importlib.metadata as m, json, sys
print(json.dumps({"python": "%d.%d" % sys.version_info[:2], "musl": bool(glob.glob("/lib/ld-musl-*")),
                  "installed": {d.metadata["Name"]: d.version for d in m.distributions()}}))
"""


@dataclass
class Env:
    """One environment, as its lock says."""

    name: str
    base: str = ""
    base_digest: str = ""
    base_user: str = ""
    registry: str = ""
    packages: list = field(default_factory=list)
    # The packages' exact versions, with what they pull in.
    pins: list = field(default_factory=list)
    resolved_for: str = ""
    python: str = ""
    platform: str = ""
    installed: dict = field(default_factory=dict)
    build: dict = field(default_factory=dict)
    published: list = field(default_factory=list)

    def to_dict(self):
        return {key: getattr(self, key) for key in Env.__dataclass_fields__}

    @property
    def folder(self):
        return _folder(self.name)

    @property
    def advanced(self):
        return (self.folder / DOCKERFILE).exists()

    @property
    def content(self):
        return self.build.get("content", "")


def _root():
    from zenthros.jobs.local import project_root

    return project_root()


def _folder(name):
    return _root() / ENVS / name


def _state(name=""):
    """What is this computer's and not the project's: logs and build contexts."""
    return _root() / ".zenthros" / ENVS / name


def _project_id():
    from zenthros.remote.options import local_client

    return local_client().id


def _engine():
    from zenthros.jobs.local import find_engine

    return find_engine()


def _load(name):
    path = _folder(name) / LOCK
    if not _NAME.match(name) or not path.exists():
        raise errors.ValidationError(f"no environment {name!r} here: zenthros env new {name} --from IMAGE")
    known = set(Env.__dataclass_fields__)
    return Env(**{key: value for key, value in json.loads(path.read_text()).items() if key in known})


def _save(env):
    path = env.folder / LOCK
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(env.to_dict(), indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)
    _state(env.name).mkdir(parents=True, exist_ok=True)


def _name_of(requirement):
    found = _REQUIREMENT.match(requirement)
    return re.sub(r"[-_.]+", "-", found.group(1)).lower() if found else ""


def _digest_of(*parts):
    hashed = hashlib.sha256()
    for part in parts:
        hashed.update(part.encode() if isinstance(part, str) else part)
        hashed.update(b"\0")
    return hashed.hexdigest()


def _base_ref(env):
    bare = env.base.partition("@")[0]
    return f"{bare}@{env.base_digest}"


# --- what a build is made of ---------------------------------------------


def _head(env):
    return f"FROM {_base_ref(env)}\nCOPY --from={UV_IMAGE} /uv /usr/local/bin/uv\n"


def _as_root(env, line):
    """line as root, when the base runs as someone else; the image stays theirs."""
    if env.base_user in ("", "root", "0"):
        return line
    return f"USER root\n{line}USER {env.base_user}\n"


def _resolve_dockerfile(env):
    """A build that only asks uv for the versions, on the base itself: what the
    image has stays as it is, so a heavy base's torch is not downloaded again."""
    python = 'uv pip compile --quiet --no-header --no-annotate --python "$(command -v python3)"'
    return (_head(env) + "COPY requirements.in previous.txt /zenthros-env/\nUSER root\n"
            f"RUN {_UV_CACHE} sh -c 'cd /zenthros-env && uv pip freeze --system > image.txt && "
            f"{python} -c image.txt -c previous.txt requirements.in -o resolved.txt'\n"
            "FROM scratch\nCOPY --from=0 /zenthros-env/resolved.txt /\n")


def _dockerfile(env):
    if not env.pins:
        return _head(env)
    install = f"RUN {_UV_CACHE} uv pip install --system --no-deps --break-system-packages -r /zenthros-env/packages.txt\n"
    return _head(env) + "COPY packages.txt /zenthros-env/packages.txt\n" + _as_root(env, install)


def _context_files(env):
    """{relative path: bytes} of what a build sees. The same lock gives the
    same files, so a later build of it is all cache."""
    if not env.advanced:
        files = {DOCKERFILE: _dockerfile(env).encode()}
        if env.pins:
            files["packages.txt"] = ("\n".join(env.pins) + "\n").encode()
        return files
    files = {}
    for path in sorted(env.folder.rglob("*")):
        relative = path.relative_to(env.folder)
        if path.is_file() and path.name != LOCK and not ignore.is_credential(path):
            files[str(relative)] = path.read_bytes()
    return files


def _input_of(files):
    return _digest_of(*[part for path, data in sorted(files.items()) for part in (path, data)])


def _write_context(files, folder):
    if folder.exists():
        shutil.rmtree(folder)
    for relative, data in files.items():
        path = folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return folder


def _tag(env, input_digest):
    return f"localhost/zenthros-env/{_project_id()}/{env.name}:{input_digest[:12]}"


def _network():
    from zenthros.jobs.local import NETWORK_ENV

    return os.environ.get(NETWORK_ENV, "")


# --- new, add, list -------------------------------------------------------


def new(name, base, registry=""):
    """An environment from base, fixed by its digest. It builds nothing."""
    if not _NAME.match(name or ""):
        raise errors.ValidationError(f"{name!r}: an environment's name is lowercase letters, digits, '.', '-' and '_'")
    if (_folder(name) / LOCK).exists():
        raise errors.ValidationError(f"the environment {name!r} is already here: zenthros env add {name} PACKAGE")
    engine = _engine()
    builder = imagebuild.builder_of(engine)
    arch = builder.arch()
    digest, user = builder.base(base, arch)
    env = Env(name=name, base=base.partition("@")[0], base_digest=digest, base_user=user, registry=registry.rstrip("/"),
              platform=f"linux/{arch}")
    _save(env)
    return env


def _requirements_in(path):
    lines = [line.partition("#")[0].strip() for line in Path(path).read_text().splitlines()]
    return [line for line in lines if line]


def add(name, *packages, requirements=None, upgrade=False):
    """Packages for the next build, which it does not start. upgrade lets every
    version move; else what is pinned stays."""
    env = _load(name)
    if env.advanced:
        raise errors.ValidationError(f"{name} has its own {DOCKERFILE}: add the package there")
    asked = [*packages, *(_requirements_in(requirements) if requirements else [])]
    wrong = [item for item in asked if not _name_of(item)]
    if wrong:
        raise errors.ValidationError(f"not a package: {', '.join(wrong)}")
    named = {_name_of(item) for item in asked}
    env.packages = sorted([item for item in env.packages if _name_of(item) not in named] + asked)
    if upgrade:
        env.pins, env.resolved_for = [], ""
    _save(env)
    return env


def list():  # noqa: A001 - zenthros.env.list
    """The project's environments."""
    root = _root() / ENVS
    return [_load(path.parent.name) for path in sorted(root.glob(f"*/{LOCK}"))]


def to_rows(environments):
    in_use = using()
    return [{"name": env.name, "base": env.base, "packages": len(env.packages), "python": env.python,
             "content": env.content, "in_use": env.name == in_use,
             "published": [entry["ref"] for entry in env.published]} for env in environments]


# --- build ----------------------------------------------------------------


def _wanted(env):
    return _digest_of(env.base_digest, *env.packages)


def _changed(env):
    """Whether the lock or the folder say something other than what was built."""
    if not env.advanced and env.resolved_for != _wanted(env):
        return True
    return _input_of(_context_files(env)) != env.build.get("input")


def _resolve(env, builder):
    """The pins for the packages asked for, called only when those changed:
    with the lock from another computer, the same versions and no network."""
    if not env.packages:
        env.pins, env.resolved_for = [], _wanted(env)
        return
    versioned = {_name_of(item) for item in env.packages if item != _REQUIREMENT.match(item).group(1)}
    previous = [pin for pin in env.pins if _name_of(pin) not in versioned]
    context = _write_context({DOCKERFILE: _resolve_dockerfile(env).encode(),
                              "requirements.in": ("\n".join(env.packages) + "\n").encode(),
                              "previous.txt": ("\n".join(previous) + "\n").encode()},
                             _state(env.name) / "resolve")
    with tempfile.TemporaryDirectory() as dest:
        code, said = builder.export(context, dest, network=_network())
        if code != 0:
            raise errors.ValidationError(_could_not_resolve(env, said))
        resolved = (Path(dest) / "resolved.txt").read_text()
    env.pins = [line.strip() for line in resolved.splitlines() if line.strip()]
    env.resolved_for = _wanted(env)


def _could_not_resolve(env, said):
    inner = imagebuild.commands_said(said) or said
    if _NO_NETWORK.search(inner) or _NO_NETWORK.search(said):
        return (f"{env.name}: no network to find {', '.join(env.packages)}, and the lock does not have them yet. "
                "The image you already built stays as it is, and it still runs; build again with network")
    lines = inner.splitlines()[-12:]
    return (f"{env.name}: these packages do not fit together, or with what {env.base} already has "
            "(a package may not replace what the image has):\n" + "\n".join(lines))


def _build_log(env, said):
    log = _state(env.name) / "build.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(said)
    return log


def build(name, force=False, out=None):
    """The image, in this computer's engine. If nothing that makes it changed
    and the engine has it, the builder is not even called: it needs no network.
    force builds it anyway and takes what comes out as the environment."""
    out = out or sys.stderr
    env = _load(name)
    engine = _engine()
    builder = imagebuild.builder_of(engine)
    if not env.advanced and env.resolved_for != _wanted(env):
        _resolve(env, builder)
        _save(env)
    files = _context_files(env)
    digest = _input_of(files)
    tag = _tag(env, digest)
    if not force and env.build.get("input") == digest and imagebuild.seen_here(engine, tag):
        print(f"zenthros: {name} has not changed", file=out)
        return env
    context = _write_context(files, _state(name) / "context")
    print(f"zenthros: building {name} (its log: {_state(name) / 'build.log'})", file=out, flush=True)
    code, built, said = builder.load(context, tag, network=_network())
    log = _build_log(env, said)
    if code != 0:
        raise errors.ValidationError(f"{name} did not build, and the image you had stays as it is. "
                                     f"Its log is {log}:\n{imagebuild.commands_said(said)[-1500:] or said[-1500:]}")
    _look_inside(env, engine, tag)
    seen = imagebuild.seen_here(engine, tag)
    env.build = {"input": digest, "content": built.content, "manifest": built.manifest,
                 "layers": seen.layers if seen else [], "config": seen.config if seen else {}}
    _save(env)
    return env


def _look_inside(env, engine, tag):
    code, seen, _ = engine.run(["run", "--rm", "--network", "none", "--entrypoint", "python3", tag, "-c", _LOOK_INSIDE],
                               timeout=300)
    if code != 0:
        # A command's image may have no Python; a function will say so.
        env.python, env.installed = "", {}
        return
    found = json.loads(seen.strip().splitlines()[-1])
    if found["musl"]:
        raise errors.ValidationError(f"{env.name}: its base uses musl (Alpine), and the agent needs glibc: "
                                     "use a Debian or Ubuntu image")
    env.python, env.installed = found["python"], found["installed"]


# --- where it runs --------------------------------------------------------


def _usable(env):
    """The tag of the image last built, saying if the lock has moved on: it
    runs what was tried, even with no network to build what was added."""
    if not env.build:
        raise errors.ValidationError(f"{env.name} is not built here: zenthros env build {env.name}")
    if _changed(env):
        print(f"zenthros: {env.name} changed and is not built: this runs the image built before "
              f"(zenthros env build {env.name})", file=sys.stderr)
    return _tag(env, env.build["input"])


def here(name):
    """The image of name on this computer."""
    env = _load(name)
    tag = _usable(env)
    if not imagebuild.seen_here(_engine(), tag):
        raise errors.ValidationError(f"{name} is not built here: zenthros env build {name}")
    return tag


def _fits_python(env, python):
    if python and env.python and python != env.python:
        raise errors.ValidationError(
            f"{env.name} has Python {env.python} and this is {python}: a function needs the same minor on both sides. "
            "Run this with Python " + env.python + ", or run it as a command")


def on_cluster(env, hosting):
    """The image pods can pull: what was built, in the cluster's own registry.
    hosting is its (address from here, address from its nodes). It costs no
    build: the builder's cache has it, and a registry that has it takes nothing."""
    tag = _usable(env)
    if not hosting:
        # Nodes that share the engine's images: Docker Desktop's.
        return tag
    outside, inside = hosting
    repo = f"zenthros-env/{_project_id()}/{env.name}"
    context = _write_context(_context_files(env), _state(env.name) / "context")
    built = imagebuild.builder_of(_engine()).publish(context, f"{outside}/{repo}:{env.build['input'][:12]}", "gzip")
    return f"{inside}/{repo}@{built.manifest}"


def image_for(name, on, python=None):
    """The image of name where on= sends a call."""
    from zenthros.machines import LOCAL, LOCAL_K8S

    env = _load(name)
    _fits_python(env, python)
    if on == LOCAL:
        return here(name)
    if on == LOCAL_K8S:
        from zenthros.remote.options import local_k8s_client

        return local_k8s_client().image_of_env(env)
    if env.published:
        return env.published[-1]["ref"]
    if env.advanced:
        raise errors.ValidationError(f"{name} is built from its own Dockerfile, only on this computer: "
                                     f"publish it with zenthros env push {name}")
    from zenthros import registry

    if registry.anyone_can_read(_base_ref(env)) is False:
        raise errors.ValidationError(f"{name} is only on this computer, and {env.base} needs a login: "
                                     f"publish it with zenthros env push {name}")
    return _base_ref(env)


def packages_for(name, on):
    """What its image lacks where on= sends a call, at the versions the lock has:
    nothing once it is published, its packages on a public base."""
    from zenthros.machines import LOCAL, LOCAL_K8S

    env = _load(name)
    if on in (LOCAL, LOCAL_K8S) or env.published:
        return []
    pins = {_name_of(pin): pin for pin in env.pins}
    return [pins.get(_name_of(package), package) for package in env.packages]


def content_of(name):
    """What an environment built here runs, whatever its compression or
    registry: the identity a cache entry keys on."""
    env = _load(name)
    return env.content or f"not built: {env.base_digest}:{sorted(env.packages)}"


def tag_of(name):
    """The image of an environment as this computer's engine knows it."""
    env = _load(name)
    return env.build.get("tag", "") or env.base


def python_of(name):
    """The minor an environment runs, as its lock has it."""
    return _load(name).python


def using():
    """The environment local calls run in, if one was chosen."""
    path = _state() / "in-use"
    return path.read_text().strip() if path.exists() else ""


def use(name):
    """Local calls run in name from now on, and what they need is made ready
    here, so they work with no network. None: back to the default."""
    path = _state() / "in-use"
    if name is None:
        path.unlink(missing_ok=True)
        return None
    env = _load(name)
    image = here(name)
    from zenthros.machines import LOCAL, python_minor
    from zenthros.remote.call import run

    def ready():
        return True

    # A function's first run makes the runner's Python, and it stays.
    run(ready if env.python == python_minor() else ["true"], on=LOCAL, image=image).result()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(name + "\n")
    return env


# --- push -----------------------------------------------------------------


def _repository(env, dest):
    from zenthros.remote.options import settings

    registry = env.registry or settings().registry.rstrip("/")
    repo = dest or (f"{registry}/{env.name}" if registry else "")
    if not repo:
        raise errors.ValidationError(f"where to? zenthros env push {env.name} REGISTRY/NAME, "
                                     f"or say it once with zenthros env new {env.name} --registry REGISTRY")
    return repo


def _verdict(env, seen, other_platform):
    """What a registry now has, compared with what was tried. It is read, never
    assumed: a registry may change what it receives."""
    if seen is None:
        raise errors.ValidationError("the registry does not have what it just received")
    if seen.content == env.content:
        return "identical" if seen.manifest == env.build.get("manifest") else "same content"
    if other_platform:
        return "same versions, not the same bytes"
    tried = env.build.get("layers") or []
    if seen.layers and seen.layers == tried:
        if seen.config == env.build.get("config"):
            return "same files, only metadata differs"
        raise errors.ValidationError(
            f"the registry has the same files as {env.name} but it would run differently: "
            f"{seen.config} instead of {env.build.get('config')}")
    raise errors.ValidationError(f"the registry has {seen.content}, and what you tried is {env.content}: "
                                 "this is not what you tried")


def push(name, dest=None, zstd=False, platform=None, out=None):
    """What was built and tried, to a registry. Nothing is built again unless
    another architecture is asked for, which is other bytes with the same
    versions. Refused if it is not what you tried."""
    out = out or sys.stderr
    env = _load(name)
    engine = _engine()
    builder = imagebuild.builder_of(engine)
    if zstd and not builder.supports_zstd:
        raise errors.ValidationError(f"{engine.name} publishes gzip here; zstd is not offered yet: drop --zstd")
    if not env.build:
        raise errors.ValidationError(f"{name} is not built: zenthros env build {name}, and try it")
    files = _context_files(env)
    digest = _input_of(files)
    if _changed(env):
        raise errors.ValidationError(f"{name} changed since its build, so this is not what you tried: "
                                     f"zenthros env build {name}")
    repo = _repository(env, dest)
    ref = repo if ":" in repo.rpartition("/")[2] else f"{repo}:{digest[:12]}"
    repo = ref.rpartition(":")[0]
    compression = "zstd" if zstd else "gzip"
    target = platform or env.platform
    other_platform = target != env.platform
    if imagebuild.seen_here(engine, _tag(env, digest)) or other_platform:
        context = _write_context(files, _state(name) / "context")
        if not other_platform:
            built = builder.would_publish(context, ref, target)
            if built.content and built.content != env.content:
                raise errors.ValidationError(
                    f"{name} no longer builds the same (its cache is gone), so this is not what you tried: "
                    f"zenthros env build {name} --force, try it, and push again")
        built = builder.publish(context, ref, compression, target)
    else:
        built = _copy_published(env, ref, compression, builder)
    seen = builder.read(f"{repo}@{built.manifest}" if built.manifest else ref, target.rpartition("/")[2])
    verdict = _verdict(env, seen, other_platform)
    entry = {"ref": f"{repo}@{seen.manifest}", "content": seen.content, "compression": compression,
             "platform": target, "verdict": verdict}
    env.published = [item for item in env.published
                     if (item["ref"].partition("@")[0], item["compression"], item["platform"])
                     != (repo, compression, target)] + [entry]
    _save(env)
    console.show(f"{name} → {entry['ref']} · {verdict} ({env.content[:19]}…) · {target} · {compression}"
                 + ("" if other_platform else " · nothing rebuilt"), out)
    return entry


def _copy_published(env, ref, compression, builder):
    """No build of it on this computer, but it is published: a copy of that,
    which is the same bytes. Only what the lock says was published."""
    source = next((item for item in reversed(env.published)
                   if item["content"] == env.content and item["compression"] == compression), None)
    if source is None:
        raise errors.ValidationError(f"{env.name} is not built on this computer, and nothing published has it as "
                                     f"{compression}: zenthros env build {env.name}, try it, and push again")
    return builder.copy(source["ref"], ref)


# --- clean ----------------------------------------------------------------


def clean_images(dry_run=False):
    """This project's built images, and the builder with its cache, which every
    project of this computer shares. An image a container still uses stays."""
    engine = _engine()
    prefix = f"localhost/zenthros-env/{_project_id()}/"
    code, out, _ = engine.run(["image", "ls", "--format", "{{.Repository}}:{{.Tag}}"], timeout=120)
    removed = []
    for image in (out.split() if code == 0 else ()):
        if not image.startswith(prefix):
            continue
        if dry_run or engine.run(["image", "rm", image], timeout=120)[0] == 0:
            removed.append(image)
    return removed + imagebuild.builder_of(engine).clean(dry_run=dry_run)
