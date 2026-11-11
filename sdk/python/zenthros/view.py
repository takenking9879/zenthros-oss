# SPDX-License-Identifier: Apache-2.0

"""What a member sees of its Workspace: who it is, its jobs, its backends
and its pools.

    ws = zenthros.workspace()        # the connection in effect (zenthros.config)
    ws.whoami()                      # who Kubernetes says the token is
    ws.jobs()                        # newest first
    ws.backends()                    # health, capacity, who may use each
    ws.backend("laptop").ready
    ws.pools()                       # where machines come from, and how many are up
    ws.upload("./data")              # local files to the store; a job reads the URI
    ws.ls("salidas/")                # what is in the store: [StoredObject(uri, size, modified)]
    ws.download(uri)                 # an object's bytes; to="./x" for a file or a folder's tree

Everything is read through zenthros-api with the caller's token: what comes
back is what Kubernetes RBAC lets that token see. Standard library only.
"""

import os
import re
from dataclasses import dataclass, field

from zenthros import errors
from zenthros.jobs.handle import JobStatus, status_from_object

# One PUT carries at most 5 GiB (S3's limit for a single upload).
MAX_UPLOAD_BYTES = 5 << 30
# What tools leave beside data and nobody means to upload.
_UPLOAD_SKIPS = {"__pycache__", ".ipynb_checkpoints", ".git", ".DS_Store"}


@dataclass
class JobInfo:
    """One job of the Workspace, as stored."""

    name: str
    phase: str = ""
    kind: str = ""
    created: str = ""
    backends: list = field(default_factory=list)
    status: JobStatus = field(default_factory=JobStatus)

    @classmethod
    def from_api(cls, obj):
        meta, spec = obj.get("metadata") or {}, obj.get("spec") or {}
        status = status_from_object(obj)
        return cls(name=str(meta.get("name") or ""), phase=status.phase,
                   kind=str((spec.get("workload") or {}).get("kind") or ""),
                   created=str(meta.get("creationTimestamp") or ""),
                   backends=list((spec.get("target") or {}).get("backends") or []), status=status)

    def to_dict(self):
        """This job as an agent or a script reads it."""
        return {"name": self.name, "phase": self.phase, "kind": self.kind, "created": self.created, "backends": self.backends,
                "reason": self.status.reason, "message": self.status.message, "exit_code": self.status.exit_code}


@dataclass
class Backend:
    """One backend the Workspace may use, as the Gateway last heard of it."""

    id: str
    type: str = ""
    name: str = ""  # what the Workspace calls it; "" when joined without one
    agent_version: str = ""
    healthy: bool = False
    drained: bool = False
    running_jobs: int = 0
    max_jobs: int = 0  # 0: no limit of its own
    last_heartbeat: int = 0  # unix seconds; 0: never
    ownership: str = ""  # External | Managed
    access_mode: str = ""  # Private | Public
    workspaces: list = field(default_factory=list)
    age_recipients: list = field(default_factory=list)
    cpu: int = 0
    memory_bytes: int = 0
    arch: str = ""
    image: str = ""
    kinds: list = field(default_factory=list)
    # How it runs jobs (systemd, containerd, process, kubernetes): what its
    # agent declares, in any of its workload kinds.
    runtimes: list = field(default_factory=list)
    # The one a job that names none runs under (its Job kind's).
    default_runtime: str = ""

    @classmethod
    def from_api(cls, info):
        host = info.get("host") or {}
        return cls(
            id=str(info.get("backendId") or ""), type=str(info.get("backendType") or ""),
            name=str(info.get("name") or ""),
            agent_version=str(info.get("agentVersion") or ""), healthy=bool(info.get("healthy")),
            drained=bool(info.get("drained")), running_jobs=int(info.get("runningJobs") or 0),
            max_jobs=int(info.get("maxJobs") or 0), last_heartbeat=int(info.get("lastHeartbeatUnix") or 0),
            ownership=str(info.get("ownership") or ""), access_mode=str(info.get("accessMode") or ""),
            workspaces=list(info.get("workspaces") or []), age_recipients=list(info.get("ageRecipients") or []),
            cpu=int(host.get("cpu") or 0), memory_bytes=int(host.get("memoryBytes") or 0),
            arch=str(host.get("arch") or ""), image=str(host.get("containerImage") or ""),
            kinds=[str(c.get("kind") or "") for c in (info.get("capabilities") or []) if isinstance(c, dict)],
            runtimes=sorted({str(r) for c in (info.get("capabilities") or []) if isinstance(c, dict)
                             for r in (c.get("runtimes") or [])}),
            default_runtime=next((str(c.get("defaultRuntime") or "") for c in (info.get("capabilities") or [])
                                  if isinstance(c, dict) and c.get("kind") == "Job"), ""))

    @property
    def ready(self):
        """Heard from in time and open to new jobs. A backend at its
        capacity is still ready: a job sent to it waits its turn."""
        return self.healthy and not self.drained


@dataclass
class Pool:
    """One pool the Workspace may name in a job (zenthros.run(on=...))."""

    name: str
    machine_kind: str = ""
    provider: str = ""
    account: str = ""  # "": the installation's own
    region: str = ""
    zone: str = ""
    purchasing_mode: str = ""  # OnDemand | Spot
    instance_types: list = field(default_factory=list)
    toolchains: list = field(default_factory=list)
    min_ready: int = 0
    max_ready: int = 0
    ready: int = 0
    allocated: int = 0
    provisioning: int = 0
    conditions: list = field(default_factory=list)

    @classmethod
    def from_api(cls, info):
        return cls(
            name=str(info.get("name") or ""), machine_kind=str(info.get("machineKind") or ""),
            provider=str(info.get("provider") or ""), account=str(info.get("account") or ""),
            region=str(info.get("region") or ""), zone=str(info.get("zone") or ""),
            purchasing_mode=str(info.get("purchasingMode") or ""),
            instance_types=list(info.get("instanceTypes") or []), toolchains=list(info.get("toolchains") or []),
            min_ready=int(info.get("minReady") or 0), max_ready=int(info.get("maxReady") or 0),
            ready=int(info.get("ready") or 0), allocated=int(info.get("allocated") or 0),
            provisioning=int(info.get("provisioning") or 0), conditions=list(info.get("conditions") or []))

    @property
    def spot(self):
        return self.purchasing_mode == "Spot"


class Workspace:
    """The Workspace of one zenthros-api connection."""

    def __init__(self, client):
        self._client = client
        self.name = client.workspace
        self.server = client.server

    def whoami(self):
        return self._client.whoami()

    @property
    def store(self):
        """Where a job reads its call and writes its result: your own bucket
        when you gave one (zenthros store s3://...), else the Workspace's
        store on the hub; "" if neither."""
        from zenthros.remote.options import settings

        return settings().store or self._client.store

    def _blobs(self, uri):
        """What reads and writes uri: your credentials in your own bucket,
        URLs the API signs in the hub's store."""
        from zenthros.jobs.http import PresignedBlobStore
        from zenthros.remote.options import _own_bucket, settings

        if _own_bucket(uri, settings(), self._client):
            from zenthros import userstore

            return userstore.blobs()
        return PresignedBlobStore(self._client)

    def jobs(self, selector=""):
        """The Workspace's jobs, newest first."""
        jobs = [JobInfo.from_api(obj) for obj in self._client.list_jobs(selector)]
        # creationTimestamp is RFC 3339 in UTC: it sorts as text.
        return sorted(jobs, key=lambda job: job.created, reverse=True)

    def backends(self):
        return [Backend.from_api(info) for info in self._client.backends()]

    def backend(self, backend_id):
        found = self.backends()
        for backend in found:
            if backend.id == backend_id:
                return backend
        raise LookupError(f"Workspace {self.name} has no backend {backend_id!r}; it has {sorted(b.id for b in found)}")

    def pools(self):
        return [Pool.from_api(info) for info in self._client.pools()]

    def upload(self, path, to=None):
        """Local files to the Workspace's store; returns their URI for a job's
        params. A directory keeps its tree, without __pycache__ and the like.
        `to` is where under the store (default: the path's name). Each file
        is one signed PUT, read from disk as it goes; everything is checked
        before the first is sent."""
        root = self.store
        if not root:
            raise errors.ValidationError("no store to upload to: give your bucket once, "
                                         "`zenthros store s3://your-bucket/zenthros` (zenthros.set_store)")
        path = os.path.abspath(os.fspath(path))
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        name = os.path.basename(path.rstrip(os.sep)) if to is None else to
        if not name or any(part in ("", ".", "..")
                           for part in re.split(r"[/\\]", name.strip("/") if to is None else name)):
            raise errors.ValidationError(f"upload to={to!r}: a relative path inside the Workspace's store, "
                                         "such as 'data' or 'inputs/2026/x.csv'")
        files = []
        if os.path.isdir(path):
            for top, dirs, names in os.walk(path):
                dirs[:] = sorted(d for d in dirs if d not in _UPLOAD_SKIPS)
                for file in sorted(names):
                    if file not in _UPLOAD_SKIPS:
                        full = os.path.join(top, file)
                        files.append((full, os.path.relpath(full, path).replace(os.sep, "/")))
        else:
            files.append((path, ""))
        for full, _ in files:
            if os.path.getsize(full) > MAX_UPLOAD_BYTES:
                raise errors.ValidationError(f"{full} is over {MAX_UPLOAD_BYTES >> 30} GiB, the most one upload "
                                             "carries; split it")
        base = f"{root.rstrip('/')}/{name.strip('/')}"
        store = self._blobs(base)
        for full, rel in files:
            store.put_file(f"{base}/{rel}" if rel else base, full)
        return base

    def _uri(self, name):
        """name as a URI of the store: a URI as is, a path under the store
        joined to it."""
        name = os.fspath(name)
        if name.startswith(("s3://", "gs://")):
            return name
        root = self.store
        if not root:
            raise errors.ValidationError("no store: give your bucket once, `zenthros store s3://your-bucket/zenthros` "
                                         "(zenthros.set_store)")
        if any(part in (".", "..") for part in name.split("/")):
            raise errors.ValidationError(f"{name!r}: a path inside the Workspace's store, such as 'salidas/'")
        return f"{root.rstrip('/')}/{name.lstrip('/')}"

    def ls(self, prefix=""):
        """What is in the Workspace's store under prefix (a URI, or a path
        under the store; default all of it), every page:
        [StoredObject(uri, size, modified)]."""
        uri = self._uri(prefix)
        return self._blobs(uri).ls(uri)

    def download(self, uri, to=None):
        """An object of the Workspace's store back here: its bytes, or with
        `to` a file (into `to` if it is a folder). A folder (what upload of
        a directory gave, or a prefix ending in /) comes back as its tree
        under `to`; returns the files' paths. Every key is checked before the
        first is written; each is written as it arrives."""
        uri = self._uri(uri)
        store = self._blobs(uri)
        if not uri.endswith("/"):
            try:
                if to is None:
                    return store.get(uri)
                path = os.fspath(to)
                if os.path.isdir(path):
                    path = os.path.join(path, uri.rsplit("/", 1)[-1])
                os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
                return store.get_file(uri, path)
            except FileNotFoundError:
                pass
        folder = uri.rstrip("/") + "/"
        # A key ending in / is a folder marker some consoles write: nothing to fetch.
        objects = [o for o in store.ls(folder) if not o.uri.endswith("/")]
        if not objects:
            raise FileNotFoundError(uri)
        if to is None:
            raise errors.ValidationError(f"{uri} is a folder of {len(objects)} objects: "
                                         "ws.download(uri, to='./folder') brings its tree")
        root = os.path.abspath(os.fspath(to))
        plan = []
        for found in objects:
            rel = found.uri[len(folder):]
            parts = rel.split("/")
            if any(part in ("", ".", "..") for part in parts):
                raise errors.ValidationError(f"{found.uri}: its key leaves {folder}; nothing was written")
            plan.append((found.uri, os.path.join(root, *parts)))
        for source, path in plan:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            store.get_file(source, path)
        return [path for _, path in plan]

    @property
    def secrets(self):
        """The Workspace's secrets, by name: ws.secrets["hf-token"] = value
        sets one, ws.secrets["hf-token"] is what a job's secrets= takes,
        del removes it. A value never comes back."""
        return WorkspaceSecrets(self._client)

    @staticmethod
    def _now():
        import datetime

        return datetime.datetime.now(datetime.timezone.utc)

    def clean(self, older_than, dry_run=False, calls=None, code=False, outputs=False):
        """Delete from the store the calls older than older_than ("30d",
        "12h", seconds): in a call's folder (<store>/<job>/, with its
        call-*.pkl) whose every object was last written before then, its
        result or results/, and its checkpoints, which cannot be resumed
        without the job. The rest of the folder is the job's outputs
        ($ZENTHROS_DATA_OUTPUT is that folder): they stay, with the call that
        says the folder is a call's, and the report says how much. outputs=True
        deletes them too, and the call, then or on a later clean. Kept: a call
        whose job is still in the Workspace, code/ (each project once by
        content), and what is no call (what upload put there).
        Each object is a DELETE the API signs (delete zenthrosobjects); the
        call goes last, so a clean cut short finishes on the next one. dry_run
        says what would go and deletes nothing; calls=[job names] looks at
        those calls only. code=True also deletes from code/ the snapshots and
        projects older than that, except those a job still here pins. A call
        that reuses one just then fails on its digest, and runs again.

        What a cache entry with a good signature pins stays (kept_cached): a
        forged one protects nothing, and a hit that loses its bytes runs again."""
        import datetime

        from zenthros import cache as entries
        from zenthros.remote.call import CALL_OBJECT, is_output
        from zenthros.remote.options import seconds
        from zenthros.remote.snapshot import CODE_SHA256

        if older_than is None:
            raise errors.ValidationError('clean(older_than=): how old a call must be to go, such as "30d"')
        cutoff = self._now() - datetime.timedelta(seconds=seconds(older_than, "older_than"))
        root = self.store
        if not root:
            raise errors.ValidationError("no store: give your bucket once, `zenthros store s3://your-bucket/zenthros` "
                                         "(zenthros.set_store)")
        root = root.rstrip("/") + "/"
        if calls is not None:
            if isinstance(calls, str) or not all(isinstance(c, str) and c and c not in (".", "..") and "/" not in c
                                                 for c in calls):
                raise errors.ValidationError(f"clean(calls={calls!r}): a list of calls' job names")
        store = self._blobs(root)
        listed = store.ls(root) if calls is None else [found for c in sorted(set(calls)) for found in store.ls(f"{root}{c}/")]
        folders = {}
        for found in listed:
            folder, _, rest = found.uri[len(root):].partition("/")
            if rest:
                folders.setdefault(folder, []).append((rest, found))
        jobs = self._client.list_jobs()
        live = {(job.get("metadata") or {}).get("name") for job in jobs}
        report = CleanReport(dry_run=bool(dry_run))
        cached, cached_projects = entries.referenced(entries.Home(root.rstrip("/"), store))
        if code:
            pinned = {((job.get("metadata") or {}).get("annotations") or {}).get(CODE_SHA256) for job in jobs}
            pinned |= cached_projects
            for found in store.ls(f"{root}code/"):
                digest = found.uri.rsplit("/", 1)[-1].removesuffix(".tar.gz")
                if found.modified is None or found.modified >= cutoff:
                    continue
                if digest in pinned:
                    report.kept_code.append(digest)
                    continue
                report.code.append(digest)
                report.bytes += found.size
                if not dry_run:
                    store.delete(found.uri)
        for folder in sorted(folders):
            objects = folders[folder]
            calls = [found for rest, found in objects if CALL_OBJECT.fullmatch(rest)]
            if not calls or folder == entries.FOLDER:
                continue
            if any(found.modified is None or found.modified >= cutoff for _, found in objects):
                continue
            if folder in live:
                report.kept_live.append(folder)
                continue
            kept, going = [], []
            for rest, found in objects:
                if found.uri in cached:
                    report.kept_cached.append(found.uri)
                    kept.append(found)
                    continue
                (kept if not outputs and is_output(rest) else going).append(found)
            # The call stays with what stays: without it, the folder is no
            # call's and a later outputs=True could not find them.
            going = [found for found in going if found not in calls or not kept]
            report.kept_outputs += len([found for found in kept if found.uri not in cached])
            report.kept_output_bytes += sum(found.size for found in kept if found.uri not in cached)
            if not going:
                continue
            report.calls.append(folder)
            report.objects += len(going)
            report.bytes += sum(found.size for found in going)
            if dry_run:
                continue
            for found in [found for found in going if found not in calls] + [found for found in going if found in calls]:
                store.delete(found.uri)
        return report

    def pool(self, name):
        found = self.pools()
        for pool in found:
            if pool.name == name:
                return pool
        raise LookupError(f"Workspace {self.name} has no pool {name!r}; it has {sorted(p.name for p in found)}")


@dataclass
class CleanReport:
    """What ws.clean deleted (or, dry_run, would): the calls' folders, their
    objects and bytes, the outputs left in those folders, and the old calls
    kept because their job is still in the Workspace."""

    calls: list = field(default_factory=list)
    objects: int = 0
    bytes: int = 0
    kept_live: list = field(default_factory=list)
    dry_run: bool = False
    # code=True: what went from code/, and what a job here still pins.
    code: list = field(default_factory=list)
    kept_code: list = field(default_factory=list)
    kept_outputs: int = 0
    kept_output_bytes: int = 0
    # What a good cache entry pins, kept wherever it is.
    kept_cached: list = field(default_factory=list)

    def __repr__(self):
        verb = "would delete" if self.dry_run else "deleted"
        out = f"{verb} {len(self.calls)} calls ({self.objects} objects, {self.bytes / (1 << 20):.1f} MiB)"
        if self.kept_outputs:
            out += f"; kept {self.kept_outputs} outputs ({self.kept_output_bytes / (1 << 20):.1f} MiB)"
        if self.kept_live:
            out += f"; kept {len(self.kept_live)} whose job is still here: {', '.join(self.kept_live)}"
        if self.code or self.kept_code:
            out += f"; {verb} {len(self.code)} from code/, kept {len(self.kept_code)} a job here pins"
        return out


class WorkspaceSecrets:
    """ws.secrets: the Workspace's secrets, kept by the hub in Kubernetes and
    sealed by the controller to the machine each job goes to (a template's,
    one that wakes later). A value goes in and never comes back.

        ws.secrets["hf-token"] = os.environ["HF_TOKEN"]
        f.remote(on="aws-spot", secrets={"HF_TOKEN": ws.secrets["hf-token"]})
        del ws.secrets["hf-token"]
    """

    def __init__(self, client):
        self._client = client

    def _listed(self):
        return {item.get("name", ""): item.get("updatedAt", "") for item in self._client.secrets()}

    def __setitem__(self, name, value):
        from zenthros.remote.options import workspace_secret_name

        workspace_secret_name(name)
        if isinstance(value, (bytes, bytearray)):
            try:
                value = bytes(value).decode()
            except UnicodeDecodeError:
                raise errors.ValidationError(f"ws.secrets[{name!r}]: text (UTF-8), not binary bytes") from None
        if not isinstance(value, str) or not value:
            raise errors.ValidationError(f"ws.secrets[{name!r}]: a non-empty string")
        self._client.set_secret(name, value)

    def __getitem__(self, name):
        from zenthros.remote.options import Secret

        if name not in self._listed():
            raise KeyError(f"Workspace {self._client.workspace} has no secret {name!r}: "
                           f"ws.secrets[{name!r}] = value sets it")
        return Secret.workspace(name)

    def __delitem__(self, name):
        try:
            self._client.delete_secret(name)
        except errors.NotFound:
            raise KeyError(f"Workspace {self._client.workspace} has no secret {name!r}") from None

    def __iter__(self):
        return iter(sorted(self._listed()))

    def __len__(self):
        return len(self._listed())

    def __contains__(self, name):
        return name in self._listed()

    def __repr__(self):
        listed = self._listed()
        if not listed:
            return f"Workspace {self._client.workspace} has no secrets"
        width = max(len(n) for n in listed)
        return "\n".join([f"{'name'.ljust(width)}  updated"] +
                         [f"{n.ljust(width)}  {listed[n]}" for n in sorted(listed)])


def workspace():
    """The Workspace of the connection in effect: configure()'s, else the
    environment's, else the zenconfig's current context."""
    from zenthros.jobs.http import HTTPJobClient
    from zenthros.remote import options

    client = options.job_client()
    if not isinstance(client, HTTPJobClient):
        raise errors.ValidationError("no zenthros-api connection: set a server in the zenconfig, ZENTHROS_SERVER "
                                     "or configure(server=)")
    return Workspace(client)


def session():
    """This process's session with its hub: workspace(), on the one client of
    the connection in effect (HTTPJobClient.for_connection), the same one
    login() returns. Every call through it, from any Workspace view of it,
    reuses the thread's kept TLS connection."""
    return workspace()
