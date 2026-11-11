# SPDX-License-Identifier: Apache-2.0

"""A function's own environment on the machine that runs it.

@zenthros.cloud(requirements="requirements.txt", packages=[...], tools=["java"])
declares what the function needs. The runner builds it before it unpickles
anything of the caller's, then runs again inside it:

  - packages: a venv, cached under ZENTHROS_ENV_CACHE (default
    ~/.cache/zenthros/envs) by the hash of the requirements, the tools, the
    Python minor and the interpreter it was made from, so a machine that is
    kept builds it once. It sees the
    machine's own packages (--system-site-packages, and the packages of the
    venv it was made from: the agent's), with the function's first. uv
    installs when the machine has it, else pip.
  - tools: java, the Temurin build install/upstream.lock pins, checked
    against its SHA-256 before it is unpacked; no root.

Standard library only: this runs before the function's packages exist.
"""

import contextlib
import hashlib
import json
import os
import platform
import shutil
import site
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

from zenthros import errors

CACHE_ENV = "ZENTHROS_ENV_CACHE"
# Set in the runner that runs inside the environment: the key it was built
# for, so it does not build again.
READY_ENV = "ZENTHROS_FUNCTION_ENV"
INSTALL_TIMEOUT = 1800

# install/upstream.lock's java rows (PinsTest keeps them equal).
TOOLS = {
    "java": {
        "version": "25.0.4.1+1",
        "amd64": ("dbb698396d478e7fa2b1e50f4103324b2a99b90569ee27c33f2261f9215cf41e",
                  "https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.4.1%2B1/"
                  "OpenJDK25U-jdk_x64_linux_hotspot_25.0.4.1_1.tar.gz"),
        "arm64": ("69df11a02cfa3ef7d7ca645e03edce6778ec090e100f6ae2b42097865730ac52",
                  "https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.4.1%2B1/"
                  "OpenJDK25U-jdk_aarch64_linux_hotspot_25.0.4.1_1.tar.gz"),
    },
}


class EnvironmentFailed(errors.ZenthrosError, RuntimeError):
    """The function's environment could not be built: the installer's words."""


def _python_minor():
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def key(needs):
    """What the environment is made of, as 16 hex characters."""
    made_of = {"python": _python_minor(), "requirements": needs.get("requirements") or "",
               "tools": sorted(needs.get("tools") or [])}
    return hashlib.sha256(json.dumps(made_of, sort_keys=True).encode()).hexdigest()[:16]


def cache_root():
    root = os.environ.get(CACHE_ENV) or os.path.join(os.path.expanduser("~"), ".cache", "zenthros", "envs")
    try:
        os.makedirs(root, exist_ok=True)
    except OSError:
        root = os.path.join(tempfile.gettempdir(), "zenthros-envs")
        os.makedirs(root, exist_ok=True)
    return root


@contextlib.contextmanager
def _locked(path):
    """One builder per environment on this machine: two jobs that need the
    same one wait for each other instead of installing over each other."""
    import fcntl

    with open(path, "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _arch():
    machine = platform.machine().lower()
    return {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(machine, machine)


def _parent_site():
    """The site-packages of the interpreter the runner was started with."""
    return [p for p in site.getsitepackages() if os.path.isdir(p)]


def _parent_key():
    """The parent's site-packages a venv that sees them is made on, as 8 hex
    characters."""
    return hashlib.sha256("\n".join(_parent_site()).encode()).hexdigest()[:8]


def _download(url, path):
    with urllib.request.urlopen(url, timeout=120) as resp, open(path, "wb") as fh:
        shutil.copyfileobj(resp, fh, 1 << 20)


def _tail(text, lines=20):
    return "\n".join((text or "").strip().splitlines()[-lines:])


def prepare(needs, out=None, run=subprocess.run, fetch=_download):
    """(python, env) to run the function with: the venv's interpreter (this
    one when it declares no packages) and what its tools add to the
    environment. Raises EnvironmentFailed."""
    out = out or sys.stderr
    root = cache_root()
    extra = {}
    for tool in sorted(set(needs.get("tools") or [])):
        if tool not in TOOLS:
            raise EnvironmentFailed(f"unknown tool {tool!r}; Zenthros provides {sorted(TOOLS)}")
        home = _tool(root, tool, out, fetch)
        extra["JAVA_HOME"] = home
        extra["PATH"] = os.path.join(home, "bin") + os.pathsep + extra.get("PATH", os.environ.get("PATH", ""))
    python = sys.executable
    if needs.get("requirements"):
        python = _venv(root, key(needs), needs["requirements"], out, run)
    return python, extra


def runner_env(packages, out=None, run=subprocess.run):
    """The interpreter of a venv with the runner's packages, made once per
    set of them and this Python's minor, and kept."""
    requirements = "".join(p + "\n" for p in sorted(packages))
    name = "runner-" + key({"requirements": requirements})
    return _venv(cache_root(), name, requirements, out or sys.stderr, run,
                 note=f"zenthros: installing what the runner needs on this Python: {' '.join(sorted(packages))}")


def _venv(root, name, requirements, out, run, note="", isolated=False):
    # One that sees the machine's packages holds only what they lack: it is
    # also the interpreter it was made from, and another of the same minor
    # (a second venv, the agent's) gets its own.
    target = os.path.join(root, "py-" + name + ("" if isolated else "-" + _parent_key()))
    python = os.path.join(target, "bin", "python")
    ready = os.path.join(target, ".ready")
    if os.path.exists(ready):
        return python
    with _locked(target + ".lock"):
        if os.path.exists(ready):
            return python
        shutil.rmtree(target, ignore_errors=True)
        print(note or f"zenthros: installing the function's packages into {target}", file=out, flush=True)
        try:
            # An isolated one (the machine's base) sees none of the machine's
            # packages: it holds all it was asked for, which the machine
            # having another version of would otherwise skip.
            made = run([sys.executable, "-m", "venv", *([] if isolated else ["--system-site-packages"]), target],
                       capture_output=True, text=True, timeout=INSTALL_TIMEOUT)
            if made.returncode != 0:
                raise EnvironmentFailed(f"python -m venv: {_tail(made.stdout + made.stderr)}")
            for directory in (os.path.join(lib, "site-packages") for lib in _glob_lib(target)) if not isolated else ():
                with open(os.path.join(directory, "zenthros-parent.pth"), "w") as fh:
                    fh.write("".join(p + "\n" for p in _parent_site()))
            listed = os.path.join(target, "requirements.txt")
            with open(listed, "w") as fh:
                fh.write(requirements)
            uv = shutil.which("uv")
            argv = ([uv, "pip", "install", "--python", python, "-r", listed] if uv else
                    [python, "-m", "pip", "install", "--disable-pip-version-check", "-r", listed])
            done = run(argv, capture_output=True, text=True, timeout=INSTALL_TIMEOUT)
            if done.returncode != 0:
                raise EnvironmentFailed(f"installing the function's packages: {_tail(done.stdout + done.stderr)}")
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise
        with open(ready, "w") as fh:
            fh.write(requirements)
    return python


def _glob_lib(target):
    lib = os.path.join(target, "lib")
    if not os.path.isdir(lib):
        return []
    return [os.path.join(lib, d) for d in sorted(os.listdir(lib)) if d.startswith("python")]


def _tool(root, tool, out, fetch):
    pin = TOOLS[tool]
    arch = _arch()
    if arch not in pin:
        raise EnvironmentFailed(f"{tool} {pin['version']} has no build for {arch}")
    sha256, url = pin[arch]
    home = os.path.join(root, f"{tool}-{pin['version']}")
    ready = os.path.join(home, ".ready")
    if os.path.exists(ready):
        return home
    with _locked(home + ".lock"):
        if os.path.exists(ready):
            return home
        shutil.rmtree(home, ignore_errors=True)
        print(f"zenthros: fetching {tool} {pin['version']} into {home}", file=out, flush=True)
        archive = home + ".tar.gz"
        try:
            fetch(url, archive)
            digest = hashlib.sha256()
            with open(archive, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    digest.update(chunk)
            if digest.hexdigest() != sha256:
                raise EnvironmentFailed(f"{url}: its SHA-256 is {digest.hexdigest()}, not the pinned {sha256}")
            _unpack(archive, home)
        except EnvironmentFailed:
            shutil.rmtree(home, ignore_errors=True)
            raise
        except Exception as exc:
            shutil.rmtree(home, ignore_errors=True)
            raise EnvironmentFailed(f"fetching {tool} from {url}: {exc}") from exc
        finally:
            with contextlib.suppress(OSError):
                os.remove(archive)
        with open(ready, "w") as fh:
            fh.write(sha256)
    return home


def _unpack(archive, home):
    """The archive's one top directory becomes home; nothing outside it."""
    os.makedirs(home)
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            parts = member.name.split("/", 1)
            if len(parts) < 2 or not parts[1]:
                continue
            if member.issym() or member.islnk():
                if os.path.isabs(member.linkname) or ".." in member.linkname.split("/"):
                    continue
            rel = os.path.normpath(parts[1])
            if rel.startswith("..") or os.path.isabs(rel):
                continue
            member.name = rel
            if member.islnk():
                member.linkname = os.path.normpath(member.linkname.split("/", 1)[-1])
            tar.extract(member, home, set_attrs=True, **({"filter": "fully_trusted"} if hasattr(tarfile, "data_filter") else {}))


# The machine's base (zenthros install): a venv per Python minor, under
# base-<minor>, a link swapped at once to a new one when what it holds
# changes. A job that started on the one before keeps it: the last two are
# kept. A function's own packages come before it, and it before the
# machine's.
BASE_PREFIX = "base-"
# What a venv brings itself, never said as installed.
_VENV_OWN = {"pip", "setuptools", "wheel"}


def normal_requirements(text):
    """requirements as the base's hash sees them: one per line, in order,
    without blanks, comments or repeats."""
    lines = {line.split(" #", 1)[0].strip() for line in (text or "").splitlines()}
    return "".join(line + "\n" for line in sorted(lines) if line and not line.startswith("#"))


def _sites(target):
    return [os.path.join(lib, "site-packages") for lib in _glob_lib(target) if os.path.isdir(os.path.join(lib, "site-packages"))]


def _dists(target):
    """{name: version} of what a venv holds, by its .dist-info."""
    found = {}
    for site_dir in _sites(target) if target else []:
        for entry in os.listdir(site_dir):
            if entry.endswith(".dist-info") and "-" in entry:
                name, version = entry[:-len(".dist-info")].rsplit("-", 1)
                name = name.lower().replace("_", "-")
                if name not in _VENV_OWN:
                    found[name] = version
    return found


def base_link(root=None):
    return os.path.join(root or cache_root(), BASE_PREFIX + _python_minor())


def base_site():
    """The site-packages of this machine's base for this Python, or None."""
    link = base_link()
    if not os.path.islink(link):
        return None
    target = os.path.realpath(link)
    if not os.path.exists(os.path.join(target, ".ready")):
        return None
    sites = _sites(target)
    return sites[0] if sites else None


def use_base(path=None):
    """Put the machine's base on path (sys.path) after the environments
    Zenthros made for this call (the function's packages, then the
    runner's) and before the machine's own: the function wins, then the
    runner, then the base. The runner's packages (cloudpickle, the store's
    client and what they import) stay the runner's: a base with six 1.15
    broke boto3 on Python 3.13 (lab, 2026-10-18). Returns the directory, or
    None without a base."""
    path = sys.path if path is None else path
    site_dir = base_site()
    if not site_dir or site_dir in path:
        return None
    made = os.path.realpath(cache_root()) + os.sep + "py-"

    def machines(entry):
        return entry.rstrip(os.sep).endswith(("site-packages", "dist-packages")) and \
            not os.path.realpath(entry).startswith(made)

    index = next((i for i, entry in enumerate(path) if machines(entry)), len(path))
    path.insert(index, site_dir)
    return site_dir


def install_base(requirements, out=None, run=subprocess.run):
    """This machine's base for this Python made to hold requirements (zenthros
    install): nothing when it already does; else built beside the current
    one and swapped in at once. Returns {"python", "up_to_date", "added",
    "removed", "changed"}: (name, version) pairs, changed as (name, old,
    new). Raises EnvironmentFailed."""
    out = out or sys.stderr
    text = normal_requirements(requirements)
    if not text:
        raise EnvironmentFailed("nothing to install")
    # One spelling for every path compared below: the link is read back as
    # its real path, and a base it does not match is removed.
    root = os.path.realpath(cache_root())
    name = BASE_PREFIX + key({"requirements": text})
    target = os.path.join(root, "py-" + name)
    link = base_link(root)
    said = {"python": _python_minor(), "up_to_date": False, "added": [], "removed": [], "changed": []}
    with _locked(link + ".lock"):
        before = os.path.realpath(link) if os.path.islink(link) else None
        if before == target and os.path.exists(os.path.join(target, ".ready")):
            said["up_to_date"] = True
            return said
        print(f"zenthros: installing the machine's packages for Python {_python_minor()}", file=out, flush=True)
        _venv(root, name, text, out, run, note=f"zenthros: installing {name}", isolated=True)
        old, new = _dists(before), _dists(target)
        said["added"] = sorted((n, v) for n, v in new.items() if n not in old)
        said["removed"] = sorted((n, v) for n, v in old.items() if n not in new)
        said["changed"] = sorted((n, old[n], v) for n, v in new.items() if n in old and old[n] != v)
        swap = link + ".new"
        with contextlib.suppress(FileNotFoundError):
            os.remove(swap)
        os.symlink(target, swap)
        os.replace(swap, link)
        # The one before stays for jobs that started on it; another minor's
        # base is that minor's.
        keep = {target, before} | {os.path.realpath(os.path.join(root, e)) for e in os.listdir(root)
                                   if e.startswith(BASE_PREFIX) and os.path.islink(os.path.join(root, e))}
        for entry in os.listdir(root):
            path = os.path.join(root, entry)
            if entry.startswith("py-" + BASE_PREFIX) and not entry.endswith(".lock") and path not in keep:
                shutil.rmtree(path, ignore_errors=True)
    return said
