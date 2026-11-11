# SPDX-License-Identifier: Apache-2.0

"""What travels with a call: the pickled call, the project's files and the
SDK's core, and the versions the machine checks before it unpickles.

The SDK's core rides inline in the job (the runner needs it to start); a
function's project files go to the store once, named by their SHA-256,
and the call pins {uri, sha256}: a job, and each of its fan-out children,
carries a reference instead of the project (audit 2026-10-03 SDK-8).

The project's files are its .py files and the small files beside them (a
run.sh, a conf/train.yaml), without what a .gitignore or .zenthrosignore
in any folder excludes (gitignore syntax, as git reads them), hidden files,
credentials (*.env, keys, kubeconfigs, Terraform variables and state, cloud
credential JSON) unless an ignore file re-includes them with "!", or a data/
directory that is not a Python package.

cloudpickle ties both ends to one Python minor version: the runner refuses a
call pickled by another one before it unpickles anything.
"""

import fnmatch
import functools
import gzip
import hashlib
import io
import os
import sys
import tarfile

from zenthros import errors, ignore

# The controller bounds source.inline at 512 KiB of base64: 384 KiB of tarball.
MAX_CODE_BYTES = 360 << 10
# A function's project goes by reference, through the store: bounded so a
# code_dir that holds data is refused, not uploaded on every run.
MAX_STORED_CODE_BYTES = 64 << 20
# A call this big is data that should have gone by reference.
WARN_CALL_BYTES = 50 << 20
SKIP_DIRS = {"__pycache__", "node_modules", "site-packages", "venv", "env", "build", "dist"}
# A file other than .py travels only under this size: a config or a script,
# not a dataset or a checkpoint (audit 2026-10-03 SDK-5).
MAX_FILE_BYTES = 256 << 10
# Data directories are walked only when they are a Python package (SDK-9).
DATA_DIRS = {"data"}
IGNORE_FILES = (".gitignore", ".zenthrosignore")
# The SDK's core: the runner and the job protocol, without the training and
# data libraries, which come from the machine.
CORE_PACKAGES = ("", "core", "jobs", "remote")
# Not used on the machine (audit 2026-10-03 I.7).
CORE_EXCLUDED = {"zenthros/infra.py", "zenthros/config.py", "zenthros/jobs/fake.py", "zenthros/jobs/local.py",
                 "zenthros/cache.py", "zenthros/pipeline.py", "zenthros/identity.py", "zenthros/reach.py"}
SDK_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def cloudpickle():
    # Imported when a call is made: `import zenthros` stays stdlib-only.
    import cloudpickle as module

    return module


def python_version():
    return "%d.%d" % sys.version_info[:2]


@functools.cache
def _owners():
    """Top-level module -> its distributions, read once per process: a
    package installed after the first call is not named in the caller's
    packages (only the machine's drift note misses it)."""
    from importlib import metadata

    try:
        return metadata.packages_distributions()
    except Exception:  # a broken dist's metadata: nothing to compare with
        return {}


@functools.cache
def _version(dist):
    from importlib import metadata

    try:
        return metadata.version(dist)
    except metadata.PackageNotFoundError:
        return None


def loaded_packages():
    """name -> version of the distributions behind the non-stdlib modules
    loaded now: what the call may need on the other side."""
    owners = _owners()
    stdlib = getattr(sys, "stdlib_module_names", frozenset())
    found = {}
    for module in list(sys.modules):
        top = module.partition(".")[0]
        if not top or top.startswith("_") or top in stdlib:
            continue
        for dist in owners.get(top, []):
            if dist not in found and _version(dist) is not None:
                found[dist] = _version(dist)
    return found


def _installed_roots():
    import site
    import sysconfig

    roots = {sysconfig.get_path(name) for name in ("stdlib", "platstdlib", "purelib", "platlib")}
    roots.update(site.getsitepackages())
    roots.add(site.getusersitepackages())
    return [os.path.realpath(r) for r in roots if r]


def is_project_module(module):
    """A module of the caller's own code: a file outside the interpreter's
    library and site-packages. The machine cannot import it by name unless
    code_dir carried it, so its functions are pickled by value."""
    path = getattr(module, "__file__", None)
    if not path or getattr(module, "__name__", "") == "__main__":
        return False
    path = os.path.realpath(path)
    return not any(path == root or path.startswith(root + os.sep) for root in _installed_roots())


def dumps(fn, value):
    """cloudpickle value. fn's module (a function's, or a Task's class's) goes
    by value when it is the caller's own code, for this pickle only."""
    pickler = cloudpickle()
    module = sys.modules.get(getattr(fn, "__module__", None) or "")
    # A module the caller registered stays theirs: cloudpickle raises nothing
    # for one registered twice, and unregistering it here would undo theirs.
    registered = (module is not None and is_project_module(module)
                  and module.__name__ not in pickler.list_registry_pickle_by_value())
    if registered:
        pickler.register_pickle_by_value(module)
    try:
        return pickler.dumps(value)
    finally:
        if registered:
            pickler.unregister_pickle_by_value(module)


def _ignore_rules(folder, here):
    """[(pattern, negated, directory_only, anchored, here)] from folder's
    ignore files, in order; here is folder's path in the bundle, and its
    rules apply under it alone, as git reads a .gitignore in a folder."""
    rules = []
    for name in IGNORE_FILES:
        try:
            with open(os.path.join(folder, name), encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            negated = line.startswith("!")
            line = line[1:] if negated else line
            directory_only = line.endswith("/")
            line = line.rstrip("/")
            # A slash at the start or in the middle anchors the pattern to
            # the folder; **/ matches at any depth.
            anchored = "/" in line
            while line.startswith("**/"):
                line, anchored = line[3:], "/" in line[3:]
            line = line.lstrip("/")
            if line:
                rules.append((line, negated, directory_only, anchored, here))
    return rules


def _ignored(relative, is_dir, rules):
    """The last rule that matches relative decides."""
    ignored = False
    for pattern, negated, directory_only, anchored, here in rules:
        if (directory_only and not is_dir) or not relative.startswith(here):
            continue
        inside = relative[len(here):]
        if fnmatch.fnmatchcase(inside if anchored else inside.rsplit("/", 1)[-1], pattern):
            ignored = not negated
    return ignored


def _project_files(root):
    """{path in the bundle: file} for the files under root that travel."""
    rules = [(pattern, False, False, False, "") for pattern in ignore.CREDENTIALS]
    found = {}
    for current, dirs, names in os.walk(root):
        here = os.path.relpath(current, root).replace(os.sep, "/")
        here = "" if here == "." else here + "/"
        # A deeper folder's rules come later, so they decide over its parents'.
        rules += _ignore_rules(current, here)
        dirs[:] = sorted(
            d for d in dirs
            if not d.startswith(".") and d not in SKIP_DIRS and not _ignored(here + d, True, rules)
            and (d not in DATA_DIRS or os.path.isfile(os.path.join(current, d, "__init__.py"))))
        for filename in sorted(names):
            path = os.path.join(current, filename)
            if filename.startswith(".") or not os.path.isfile(path) or _ignored(here + filename, False, rules):
                continue
            if os.path.islink(path) and not os.path.realpath(path).startswith(os.path.realpath(root) + os.sep):
                continue  # a link out of the project is not the project's
            if not filename.endswith(".py") and os.path.getsize(path) > MAX_FILE_BYTES:
                continue
            found[here + filename] = path
    return found


def import_roots(root):
    """The caller's sys.path entries inside root, relative to it, in order:
    `python src/train.py` puts src/ on sys.path and train imports its
    siblings as top-level modules. The machine has only the unpacked root on
    sys.path unless the runner puts these back (audit 2026-10-03 SDK-1)."""
    if not root:
        return []
    root = os.path.realpath(root)
    found = []
    for entry in sys.path:
        path = os.path.realpath(entry or os.getcwd())
        if path == root or not path.startswith(root + os.sep) or not os.path.isdir(path):
            continue
        relative = os.path.relpath(path, root).replace(os.sep, "/")
        if relative not in found:
            found.append(relative)
    return found


def sdk_files():
    """The SDK's core as bundle paths."""
    package = os.path.join(SDK_DIR, "zenthros")
    found = {}
    for sub in CORE_PACKAGES:
        folder = os.path.join(package, sub)
        for filename in sorted(os.listdir(folder)):
            name = "/".join(p for p in ("zenthros", sub, filename) if p)
            if filename.endswith(".py") and name not in CORE_EXCLUDED:
                found[name] = os.path.join(folder, filename)
    return found


def project_files(root):
    """{path in the bundle: file} for root's files that travel; {} without root."""
    return _project_files(os.path.abspath(root)) if root else {}


def pack_code(root=None, ship_sdk=True, limit=MAX_CODE_BYTES):
    """The project's files under root, plus the SDK's core under zenthros/,
    as a gzipped tarball. The bytes depend on paths and contents only, so the same
    tree always packs the same.

    The machine runs `python -m zenthros.remote` from the unpacked directory,
    so the SDK shipped here is the one that runs, whatever the machine has
    installed: caller and runner never disagree on the call's format."""
    files = project_files(root)
    if ship_sdk and not any(path.startswith("zenthros/") for path in files):
        files.update(sdk_files())
    return _pack(files, limit, root)


def _pack(files, limit, root, executable=None):
    """files {name: path} as a tar.gz with no dates or owners; executable(name)
    says which go +x (None: none do)."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name in sorted(files):
            with open(files[name], "rb") as handle:
                data = handle.read()
            info = tarfile.TarInfo(name)
            x = executable is not None and executable(name)
            info.size, info.mode, info.mtime = len(data), 0o755 if x else 0o644, 0
            tar.addfile(info, io.BytesIO(data))
    packed = gzip.compress(raw.getvalue(), mtime=0)
    if len(packed) > limit:
        where = "a job carries inline" if limit == MAX_CODE_BYTES else "a call stores"
        raise errors.ValidationError(
            f"the files under {root} pack to {len(packed)} bytes, over the {limit} {where}; pass "
            "code_dir= a smaller directory (list data in .zenthrosignore), or code_dir=None")
    return packed


def _packed_project(root):
    files = project_files(root)
    return _pack(files, MAX_STORED_CODE_BYTES, root) if files else None


def project_digest(root, files=None):
    """The digest store_project would store root's project under, or "". files:
    only these of its files."""
    files = project_files(root) if files is None else files
    return hashlib.sha256(_pack(files, MAX_STORED_CODE_BYTES, root)).hexdigest() if files else ""


def store_project(root, store, base):
    """{uri, sha256} of root's project files, stored at
    <base>/code/<sha256>.tar.gz; None when nothing under root travels. The
    same tree packs the same bytes, so a project this process already
    stored there is not sent again, through whichever store object."""
    packed = _packed_project(root)
    if packed is None:
        return None
    digest = hashlib.sha256(packed).hexdigest()
    uri = f"{base}/code/{digest}.tar.gz"
    # Nothing removes <base>/code/, and the name is the content's digest.
    if store.where(uri) not in _STORED:
        store.put(uri, packed)
        _STORED.add(store.where(uri))
    return {"uri": uri, "sha256": digest}


# Where this process stored a project (BlobStore.where).
_STORED = set()


def extract(packed, into):
    """Unpack a bundle under into: regular files and directories only, none
    outside it."""
    root = os.path.realpath(into)
    with tarfile.open(fileobj=io.BytesIO(packed), mode="r:gz") as tar:
        for member in tar.getmembers():
            target = os.path.realpath(os.path.join(root, member.name))
            if not target.startswith(root + os.sep) or not (member.isfile() or member.isdir()):
                raise errors.ValidationError(f"the code bundle holds {member.name!r}, which may not be unpacked")
            if member.isdir():
                os.makedirs(target, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with tar.extractfile(member) as source, open(target, "wb") as sink:
                sink.write(source.read())
            if member.mode & 0o100:
                os.chmod(target, 0o755)
