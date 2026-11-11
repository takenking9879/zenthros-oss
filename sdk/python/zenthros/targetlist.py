# SPDX-License-Identifier: Apache-2.0

"""zenthros.targets(): where the Workspace can run, before anything runs.

    zenthros.targets()                # a table in Jupyter, text in a terminal
    zenthros.targets()["aws-spot"]    # where each number comes from
    zenthros.targets().to_pandas()

TYPE is what a machine is: vm, container (one here, "local", is one too) or
k8s; a template, which makes machines, is listed apart. STATUS is ready,
busy, starting, asleep (a template's machine with no job; the next call
starts it), draining, offline or unavailable; "available" is something not
made yet that can be, and "full" one that has no room for another.

CPU and MEM are what ONE machine of the target has, or would have from the
catalog when it does not exist yet; they never subtract what is in use.
targets()["name"] adds how many machines can be used or made now, and
where each number comes from. "local" before it exists is create("local")'s
size, and fits as many containers of that size as this computer's Docker
still holds after the Zenthros containers already there.

dry_run=True on create and .remote says what would be made or used, its CPU
and RAM against what the function asks, its price, and what is missing,
and makes nothing.
"""

import html
from dataclasses import dataclass, field

from zenthros import errors

GIB = 1 << 30
COLUMNS = ["NAME", "TYPE", "STATUS", "CPU", "MEM", "GPU", "PRICE"]
TEMPLATE_COLUMNS = ["NAME", "PROVIDER", "STATUS", "CPU", "MEM", "GPU", "PRICE"]
# How a provider is written in PROVIDER.
_PROVIDERS = {"aws": "AWS", "gcp": "GCP", "azure": "Azure", "runpod": "RunPod", "vastai": "Vast.ai"}
# Providers whose machines are containers (a pod someone else runs), not VMs.
_CONTAINER_PROVIDERS = ("runpod", "vastai")
# The API's kinds of row, in the table's order; Offline and Unavailable go last.
_ORDER = {"Local": 0, "VM": 2, "External": 3, "Pool": 4, "Kubernetes": 5}
_GONE = ("Offline", "Unavailable")
# Rows that are one machine (or one cluster): what a call with no on= can go to.
MACHINE_KINDS = ("External", "Kubernetes", "VM")
# Targets with several machines: available is free / all. A cluster whose
# nodes its agent does not report has no count: only whether it takes a job.
_MANY = ("Pool", "Kubernetes")
# STATUS, from the API's: one word a person reads the same way on every row
# (Full is "full" for what makes machines: Docker here, a pool).
_STATUS = {"Ready": "ready", "Busy": "busy", "Full": "busy", "Creating": "starting", "Draining": "draining",
           "Offline": "offline", "Unavailable": "unavailable"}
# A cluster's node has room for a job (its row's AVAILABLE) with this free.
NODE_ROOM = "2 CPUs and 4 GiB free"


def _cpus(milli):
    return str(milli // 1000) if milli % 1000 == 0 else f"{milli / 1000:g}"


def _gib(value):
    gib = value / GIB
    return str(int(gib)) if abs(gib - round(gib)) < 0.05 else f"{gib:.1f}"


def _rate(value):
    return f"{value:.2f}" if value >= 1 else f"{value:.3f}".rstrip("0").rstrip(".")


def cpu_text(milli, high=0):
    if not milli:
        return "-"
    return f"{_cpus(milli)}–{_cpus(high)}" if high else _cpus(milli)


def ram_text(value, high=0):
    if not value:
        return "-"
    return f"{_gib(value)}–{_gib(high)} GiB" if high else f"{_gib(value)} GiB"


def mem_text(value, high=0):
    if not value:
        return "-"
    return f"{_gib(value)}–{_gib(high)}Gi" if high else f"{_gib(value)}Gi"


def price_text(price):
    if not price:
        return "-"
    high = price.get("maxHourlyRate") or 0
    rate = f"{_rate(price['hourlyRate'])}–{_rate(high)}" if high else _rate(price["hourlyRate"])
    return f"${rate}/h" if price.get("currency", "USD") == "USD" else f"{rate} {price['currency']}/h"


@dataclass
class Target:
    """One place the Workspace can run. Its repr says where each number
    comes from."""

    name: str
    # The API's kind of row (External, Kubernetes, VM, Pool, Template) or
    # Local; type is what TYPE says.
    kind: str
    # The API's status; status is what STATUS says.
    state: str
    available: int = 0
    total: int = 0
    milli_cpu: int = 0
    max_milli_cpu: int = 0
    memory_bytes: int = 0
    max_memory_bytes: int = 0
    gpu_model: str = ""
    gpu_count: int = 0
    price: dict = None
    reason: str = ""
    detail: dict = field(default_factory=dict)

    @classmethod
    def from_api(cls, row):
        return cls(name=row["name"], kind=row.get("type", ""), state=row.get("status", ""),
                   available=int(row.get("available") or 0),
                   total=int(row.get("total") or 0), milli_cpu=int(row.get("milliCPU") or 0),
                   max_milli_cpu=int(row.get("maxMilliCPU") or 0), memory_bytes=int(row.get("memoryBytes") or 0),
                   max_memory_bytes=int(row.get("maxMemoryBytes") or 0), gpu_model=row.get("gpuModel", ""),
                   gpu_count=int(row.get("gpuCount") or 0), price=row.get("price"), reason=row.get("reason", ""), detail=row)

    @property
    def template(self):
        return self.kind == "Template"

    @property
    def costs(self):
        """Whether it bills by the hour: what makes machines, or has a price."""
        return bool(self.price) or self.kind in ("Pool", "Template")

    @property
    def untrusted_host(self):
        """A third party runs its machines: their owner can read what runs."""
        return bool(self.detail.get("untrustedHost"))

    def to_dict(self):
        """This row as an agent or a script reads it."""
        price = self.price or {}
        return {"name": self.name, "type": self.type, "status": self.status, "available": self.available, "total": self.total,
                "cpu": self.milli_cpu / 1000, "memory_gib": round(self.memory_bytes / (1 << 30), 1),
                "gpus": self.gpu_count, "gpu_model": self.gpu_model, "price_per_hour": price.get("hourlyRate"),
                "costs": self.costs, "untrusted_host": self.untrusted_host, "reason": self.reason}

    @property
    def type(self):
        """vm, container or k8s; pool or template for what makes machines."""
        if self.kind == "Local":
            return "container"
        if self.kind == "Kubernetes":
            return "k8s"
        if self.kind in ("Pool", "Template"):
            return self.kind.lower()
        if self.kind == "VM":
            return "container" if self.detail.get("provider") in _CONTAINER_PROVIDERS else "vm"
        return {"container": "container", "k8s": "k8s"}.get(self.detail.get("backendType", ""), "vm")

    @property
    def status(self):
        if self.state == "Available":
            # A template's machine with no job sleeps; anything else not made yet can be.
            return "asleep" if self.kind == "VM" else "available"
        if self.state == "Full" and self.kind in ("Local", "Pool"):
            # No more machines fit: a cluster or a machine that is full is busy.
            return "full"
        return _STATUS.get(self.state, self.state.lower())

    @property
    def provider(self):
        """Where a template makes its machines: "AWS Spot", "RunPod"."""
        provider = self.detail.get("provider", "")
        text = _PROVIDERS.get(provider, provider.title() or "-")
        return text + (" Spot" if self.detail.get("purchasingMode") == "Spot" else "")

    @property
    def nodes(self):
        """A cluster's nodes: what each has and what is left of it."""
        return list(self.detail.get("nodes") or [])

    @property
    def gpus(self):
        return f"{self.gpu_count}× {self.gpu_model or 'GPU'}"

    @property
    def cpu(self):
        """CPUs of one machine (1.5 for a container of 1.5); 0: unknown."""
        return self.milli_cpu / 1000

    def _counted(self):
        return self.kind == "Pool" or (self.kind in _MANY and self.total > 0)

    def cells(self):
        gone = self.state == "Offline"
        mem = mem_text(self.memory_bytes, self.max_memory_bytes)
        gpu = (self.gpu_model or "GPU") if self.gpu_count == 1 else self.gpus if self.gpu_count else "-"
        return [self.name, self.provider if self.template else self.type, self.status,
                "-" if gone else cpu_text(self.milli_cpu, self.max_milli_cpu), "-" if gone else mem,
                "-" if gone else gpu, "-" if gone else price_text(self.price)]

    def __repr__(self):
        d = self.detail
        lines = [f"{self.name}: {self.provider + ' template' if self.template else self.type}, {self.status}"]
        if self.reason:
            lines.append(f"  why:        {self.reason}")
        available = f"{self.available} of {self.total} machines free" if self._counted() else f"{self.available}"
        if self.nodes:
            available = f"{self.available} of {self.total} nodes with room for a job ({NODE_ROOM})"
        lines.append(f"  available:  {available}")
        source = {"agent": "what its machine's agent sees", "catalog": "the provider's catalog",
                  "docker": "create('local')'s size; what fits in this computer's Docker"}.get(d.get("capacityFrom"), "unknown")
        if self.nodes:
            source = "its largest node, as its agent sees it"
        lines.append(f"  cpu, ram:   {cpu_text(self.milli_cpu, self.max_milli_cpu)}, "
                     f"{ram_text(self.memory_bytes, self.max_memory_bytes)} (from {source})")
        if self.gpu_count:
            lines.append(f"  gpu:        {self.gpus}")
        if self.nodes:
            total_cpu = sum(n.get("milliCPU", 0) for n in self.nodes)
            total_memory = sum(n.get("memoryBytes", 0) for n in self.nodes)
            lines.append(f"  nodes:      {cpu_text(total_cpu)} CPUs, {ram_text(total_memory)} in {len(self.nodes)} nodes")
            width = max(len(n.get("name", "")) for n in self.nodes)
            for n in self.nodes:
                gpus = f", {n['gpuCount']} GPUs" if n.get("gpuCount") else ""
                lines.append(f"    {n.get('name', '').ljust(width)}  {cpu_text(n.get('milliCPU', 0))} CPUs, "
                             f"{ram_text(n.get('memoryBytes', 0))}{gpus}; free {cpu_text(n.get('freeMilliCPU', 0)) if n.get('freeMilliCPU') else '0'}, "
                             f"{ram_text(n.get('freeMemoryBytes', 0)) if n.get('freeMemoryBytes') else '0 GiB'}")
        if self.price:
            p = self.price
            where = ", ".join(x for x in (p.get("region"), p.get("zone")) if x)
            lines.append(f"  price:      {price_text(p)}, {p.get('instanceType', '')} {p.get('market', '')}"
                         f"{' in ' + where if where else ''}, observed {p.get('observedAt', '')}")
        if d.get("instanceTypes"):
            lines.append(f"  types:      {', '.join(d['instanceTypes'])}")
        if d.get("provider"):
            lines.append(f"  provider:   {d['provider']} {d.get('region', '')} {d.get('purchasingMode', '')}".rstrip())
        if d.get("python"):
            lines.append(f"  python:     {d['python']}")
        if d.get("ports"):
            lines.append(f"  serves on:  {', '.join(str(p) for p in d['ports'])}")
        if self.template:
            lines.append(f"  limit {d.get('limit', 0)} for the Workspace, {d.get('used', 0)} made")
        if d.get("template"):
            lines.append(f"  made from:  {d['template']}")
        if d.get("id"):
            lines.append(f"  backend:    {d['id']} ({d.get('backendType', '')})")
        if d.get("maxJobs"):
            lines.append(f"  jobs:       {d.get('runningJobs', 0)} running of {d['maxJobs']} at once")
        for key in ("dockerCPUs", "dockerMemoryBytes"):
            if key in d:
                lines.append(f"  Docker has: {d['dockerCPUs']} CPUs, {ram_text(d['dockerMemoryBytes'])}; "
                             f"Zenthros containers hold {cpu_text(d.get('heldMilliCPU', 0)) if d.get('heldMilliCPU') else '0'} CPUs, "
                             f"{ram_text(d.get('heldMemoryBytes', 0)) if d.get('heldMemoryBytes') else '0 GiB'}")
                break
        return "\n".join(lines)


def _table(header, rows):
    widths = [max(len(str(c)) for c in column) for column in zip(header, *rows)]
    return "\n".join("   ".join(str(c).ljust(w) for c, w in zip(row, widths)).rstrip() for row in [header, *rows])


class Targets:
    """zenthros.targets()'s answer: a table, [name] for one target's
    detail, to_pandas() to filter."""

    def __init__(self, rows, can=None, docker="docker"):
        self.rows = list(rows)
        self.can = dict(can or {})
        self.docker = docker

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)

    def __contains__(self, name):
        return any(t.name == name for t in self.rows)

    def __getitem__(self, name):
        for target in self.rows:
            if target.name == name:
                return target
        raise KeyError(f"no target {name!r}; there are {[t.name for t in self.rows]}")

    def get(self, name, default=None):
        return next((t for t in self.rows if t.name == name), default)

    def _sections(self):
        """(title, header, rows): the machines, then the templates that make them."""
        machines = [t.cells() for t in self.rows if not t.template]
        templates = [t.cells() for t in self.rows if t.template]
        out = [("", COLUMNS, machines)]
        if templates:
            out.append(("Templates: zenthros.create(name) makes a machine", TEMPLATE_COLUMNS, templates))
        return out

    def __str__(self):
        return "\n\n".join((title + "\n" if title else "") + _table(header, rows)
                           for title, header, rows in self._sections())

    __repr__ = __str__

    def _repr_html_(self):
        parts = []
        for title, header, rows in self._sections():
            head = "".join(f"<th style='text-align:left'>{html.escape(h)}</th>" for h in header)
            body = "".join("<tr>" + "".join(f"<td style='text-align:left'>{html.escape(str(c))}</td>" for c in row)
                           + "</tr>" for row in rows)
            caption = f"<p>{html.escape(title)}</p>" if title else ""
            parts.append(f"{caption}<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>")
        return "".join(parts)

    def to_pandas(self):
        import pandas as pd

        return pd.DataFrame([{
            "name": t.name, "type": t.type, "status": t.status, "available": t.available, "total": t.total,
            "cpu": t.milli_cpu / 1000 or None, "max_cpu": t.max_milli_cpu / 1000 or None,
            "memory_gib": t.memory_bytes / GIB or None, "max_memory_gib": t.max_memory_bytes / GIB or None,
            "gpu": t.gpu_count, "gpu_model": t.gpu_model or None,
            "price_per_hour": (t.price or {}).get("hourlyRate"), "reason": t.reason or None,
        } for t in self.rows])


def _local_containers(docker):
    """{backend id: (milli CPU, memory bytes)} of the Zenthros containers
    running in this computer's Docker."""
    from zenthros.local import BACKEND_LABEL
    from zenthros.machines import _docker

    code, out = _docker(docker, ["ps", "--filter", f"label={BACKEND_LABEL}",
                                 "--format", '{{.ID}} {{.Label "' + BACKEND_LABEL + '"}}'], timeout=60)
    found = [line.split() for line in out.splitlines() if len(line.split()) == 2] if code == 0 else []
    if not found:
        return {}
    code, out = _docker(docker, ["inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}",
                                 *[cid for cid, _ in found]], timeout=60)
    limits = [line.split() for line in out.splitlines()] if code == 0 else []
    held = {}
    for (_, backend), limit in zip(found, limits):
        if len(limit) == 2 and all(x.isdigit() for x in limit):
            held[backend] = (int(limit[0]) // 1_000_000, int(limit[1]))
        else:
            held[backend] = (0, 0)
    return held


def _engine(docker):
    """The engine to ask about this computer, when no one is named."""
    from zenthros.jobs.local import find_engine

    if docker != "docker":
        return docker, "{{.NCPU}} {{.MemTotal}}"
    try:
        engine = find_engine()
    except errors.ValidationError:
        return docker, "{{.NCPU}} {{.MemTotal}}"
    return engine.binary, engine.capacity_format


def local_target(can_join=True, docker="docker", cpus=None, memory=None):
    """The "local" row: the size a job here gets (or the one given), and how
    many of create("local")'s fit in Docker now."""
    from zenthros.machines import bytes_of, docker_capacity, local_defaults

    default_cpus, default_memory = local_defaults()
    milli = int(round(float(cpus if cpus is not None else default_cpus) * 1000))
    memory_bytes = bytes_of(memory if memory is not None else default_memory)
    row = {"name": "local", "type": "Local", "capacityFrom": "docker"}
    target = Target(name="local", kind="Local", state="Available", milli_cpu=milli, memory_bytes=memory_bytes, detail=row)
    docker, info_format = _engine(docker)
    try:
        ncpu, total = docker_capacity(docker, info_format)
    except errors.ValidationError as exc:
        target.state, target.reason = "Unavailable", str(exc)
        return target, {}
    held = _local_containers(docker)
    held_milli = sum(c for c, _ in held.values())
    held_memory = sum(m for _, m in held.values())
    row.update(dockerCPUs=ncpu, dockerMemoryBytes=total, heldMilliCPU=held_milli, heldMemoryBytes=held_memory)
    if cpus is None and memory is None:
        from zenthros.jobs.local import container_size

        size_cpus, _, size_bytes = container_size((ncpu, total))
        target.milli_cpu, target.memory_bytes = int(round(float(size_cpus) * 1000)), size_bytes
    fit = max(0, min((ncpu * 1000 - held_milli) // milli if milli else 0, (total - held_memory) // memory_bytes if memory_bytes else 0))
    if not can_join:
        target.state, target.reason = "Unavailable", "your account cannot attach machines; ask the hub's admin"
    elif fit == 0:
        target.state = "Full"
        target.reason = (f"Docker has {ncpu} CPUs and {ram_text(total)}; Zenthros containers hold "
                         f"{cpu_text(held_milli) if held_milli else 0} and {ram_text(held_memory) if held_memory else '0 GiB'}")
    else:
        target.available = fit
    return target, held


def local_k8s_target():
    """The "local-k8s" row, if kubectl names a cluster: nothing is asked of it,
    nor installed, until a job goes there."""
    from zenthros.jobs.localk8s import TARGET, current_context

    context = current_context()
    if not context:
        return None
    return Target(name=TARGET, kind="Kubernetes", state="Available", detail={"name": TARGET, "type": "Kubernetes", "context": context},
                  reason=f"the cluster of kubectl's context {context}; each job a pod with its own image")


def _sort_key(target):
    if target.name == "local":
        return (0, 0, "")
    if target.name == "local-k8s":
        return (0, 1, "")
    return (1 if target.state in _GONE else 0, _ORDER.get(target.kind, 1), target.name)


def targets(docker="docker"):
    """Where you can run: this computer's Docker as "local", with no account;
    logged in, zenthros-api's targets too."""
    from zenthros.local import _api
    from zenthros.remote.options import _through_api

    cluster = local_k8s_target()
    here = [cluster] if cluster else []
    if not _through_api():
        return Targets([local_target(docker=docker)[0], *here], {}, docker)
    client = _api()
    answer = client.targets()
    can = answer.get("can") or {}
    rows = [Target.from_api(row) for row in answer.get("targets") or []]
    # on="local" asks nothing of the hub; create("local") attaches it, and does.
    local, held = local_target(docker=docker)
    for target in rows:
        if target.detail.get("id") in held:
            target.kind = "Local"
    return Targets(sorted([local, *here, *rows], key=_sort_key), can, docker)


@dataclass
class Plan:
    """What create(dry_run=True) or .remote(dry_run=True) would do. ok is
    False when it would fail; problems say why."""

    action: str  # "create" or "use"
    target: Target
    milli_cpu: int = 0
    memory_bytes: int = 0
    asks_milli_cpu: int = 0
    asks_memory_bytes: int = 0
    name: str = ""
    for_the_call: bool = False
    problems: list = field(default_factory=list)
    # A cluster's nodes the function fits now, of all its nodes.
    nodes_fit: tuple = None
    # create(..., idle=): how long it stays up with no job.
    idle: str = ""
    # For one call: its environment, image, data, cache and the rest
    # (zenthros.remote.call.plan_details).
    call: dict = field(default_factory=dict)

    @property
    def ok(self):
        return not self.problems

    def to_dict(self):
        """This plan as an agent or a script reads it; nothing was created."""
        return {"ok": self.ok, "action": self.action, "target": self.target.to_dict(), "cpu": self.milli_cpu / 1000,
                "memory_gib": round(self.memory_bytes / (1 << 30), 1), "problems": list(self.problems), **self.call}

    def __repr__(self):
        t = self.target
        what = self.name or t.name
        if self.action == "create":
            kind = "a container here" if t.kind == "Local" else f"a machine from {t.name} ({t.provider})"
            if t.kind == "Kubernetes":
                kind = f"a pod of {t.name}"
            if t.price:
                kind += f", {t.price.get('instanceType', '')}"
            head = f"would create {what}: {kind}"
        else:
            head = f"would use {what}: {t.type}, {t.status}"
        lines = [head, f"  cpu, ram:  {cpu_text(self.milli_cpu)}, {ram_text(self.memory_bytes)}"]
        if self.asks_milli_cpu or self.asks_memory_bytes:
            lines.append(f"  asks:      {cpu_text(self.asks_milli_cpu)}, {ram_text(self.asks_memory_bytes)}")
        if self.nodes_fit:
            lines.append(f"  nodes:     fits on {self.nodes_fit[0]} of {self.nodes_fit[1]} nodes now")
        lines.append(f"  price:     {price_text(t.price)}")
        if self.for_the_call:
            lines.append("  removed after the call's result is read (keep=True keeps it)")
        if self.idle:
            lines.append(f"  asleep until its first call; ends after {self.idle} with no job, and the next call starts it")
        if "image" in self.call:
            lines.append(f"  image:     {self.call['image'] or 'the one of the place'}" +
                         (f" (environment {self.call['env']})" if self.call.get("env") else ""))
        for item in self.call.get("data", ()):
            lines.append(f"  data:      {item['path']}: {item['answer']}: {item['why']}")
        if self.call.get("cache", {}).get("enabled"):
            lines.append(f"  cache:     {'reuse' if self.call['cache']['hit'] else 'runs: ' + self.call['cache']['why']}")
        if self.call.get("offline_ready") is not None:
            ready = self.call["offline_ready"]
            lines.append("  no network: " + ("ready" if ready else f"missing {', '.join(self.call['offline_missing'])}"))
        lines += [f"  note:      {w}" for w in self.call.get("warnings", ())]
        lines += [f"  problem:   {p}" for p in self.problems] or ["  nothing in the way"]
        return "\n".join(lines)


def _asks(options):
    """(milli CPU, memory bytes) a function's options ask; 0 where none."""
    from zenthros.machines import bytes_of

    cpu, memory = options.get("cpu"), options.get("memory")
    milli = int(round(float(str(cpu).rstrip("m")) * (1 if str(cpu).endswith("m") else 1000))) if cpu not in (None, "") else 0
    return milli, (bytes_of(memory) if memory not in (None, "") else 0)


def _check_nodes(plan):
    """A cluster that reports its nodes, for a function that asks: it fits
    when one node has what it asks free now, whatever the row's room."""
    t = plan.target
    asks_cpu, asks_memory = plan.asks_milli_cpu, plan.asks_memory_bytes
    big = [n for n in t.nodes if n.get("milliCPU", 0) >= asks_cpu and n.get("memoryBytes", 0) >= asks_memory]
    room = [n for n in big if n.get("freeMilliCPU", 0) >= asks_cpu and n.get("freeMemoryBytes", 0) >= asks_memory]
    plan.nodes_fit = (len(room), len(t.nodes))
    asks = " and ".join(x for x in (f"{cpu_text(asks_cpu)} CPUs" if asks_cpu else "", ram_text(asks_memory) if asks_memory else "") if x)
    if t.state in ("Offline", "Unavailable"):
        plan.problems.append(f"{t.name} is {t.status}" + (f": {t.reason}" if t.reason else ""))
    elif not big:
        plan.problems.append(f"no node of {t.name} has {asks}; its largest has {cpu_text(t.milli_cpu)} CPUs, "
                             f"{ram_text(t.memory_bytes)}")
    elif not room:
        plan.problems.append(f"no node of {t.name} has {asks} free now; {len(big)} of its nodes would, once they free")
    return plan


def _check(plan, current, joins=True):
    t = plan.target
    if t.nodes and (plan.asks_milli_cpu or plan.asks_memory_bytes):
        return _check_nodes(plan)
    container = plan.action == "create" and t.kind == "Local"
    if not container and t.state in ("Offline", "Unavailable", "Full"):
        plan.problems.append(f"{t.name} is {t.status}" + (f": {t.reason}" if t.reason else ""))
    if plan.asks_milli_cpu and plan.milli_cpu and plan.asks_milli_cpu > plan.milli_cpu:
        plan.problems.append(f"asks {cpu_text(plan.asks_milli_cpu)} CPUs; {t.name} has {cpu_text(plan.milli_cpu)}")
    if plan.asks_memory_bytes and plan.memory_bytes and plan.asks_memory_bytes > plan.memory_bytes:
        plan.problems.append(f"asks {ram_text(plan.asks_memory_bytes)}; {t.name} has {ram_text(plan.memory_bytes)}")
    if container and current is not None:
        # A container of this size: does it fit in Docker now?
        local, _ = local_target(not joins or bool(current.can.get("joinBackends")), current.docker,
                                cpus=plan.milli_cpu / 1000, memory=f"{plan.memory_bytes >> 20}Mi")
        if local.state == "Full" or (local.state == "Available" and local.available < 1):
            plan.problems.append(f"a container of {cpu_text(plan.milli_cpu)} CPUs and {ram_text(plan.memory_bytes)} "
                                 f"does not fit in Docker: {local.reason}")
        elif local.state == "Unavailable" and not any(local.reason in p for p in plan.problems):
            plan.problems.append(f"local is Unavailable: {local.reason}")
    return plan


def plan_create(target, *, name=None, cpus=None, memory=None, docker="docker", idle=""):
    """create(..., dry_run=True): what create would make or return."""
    from zenthros.machines import LOCAL, POD, bytes_of, local_defaults

    current = targets(docker)
    if target.startswith(POD):
        key = target[len(POD):]
        clusters = [t for t in current if t.kind == "Kubernetes"]
        found = next((t for t in clusters if key and key in (t.name, t.detail.get("id"))), None)
        if found is None:
            raise errors.ValidationError(f"create({target!r}): no Kubernetes target {key!r}; there are "
                                         f"{[t.name for t in clusters]}")
        default_cpus, default_memory = local_defaults()
        milli = int(round(float(cpus if cpus is not None else default_cpus) * 1000))
        memory_bytes = bytes_of(memory if memory is not None else default_memory)
        return _check(Plan("create", found, milli, memory_bytes, milli, memory_bytes, name=name or ""), None)
    if name and name in current and name != LOCAL:
        found = current[name]
        return _check(Plan("use", found, found.milli_cpu, found.memory_bytes, name=name), None)
    if target == LOCAL:
        local = current[LOCAL]
        milli = int(round(float(cpus) * 1000)) if cpus is not None else local.milli_cpu
        memory_bytes = bytes_of(memory) if memory is not None else local.memory_bytes
        plan = Plan("create", local, milli, memory_bytes, name=name or "")
        if local.state == "Unavailable":
            plan.problems.append(f"local is unavailable: {local.reason}")
            return plan
        return _check(plan, current)
    found = current.get(target)
    if found is None:
        raise errors.ValidationError(f"create({target!r}): not a target of the Workspace; it has {[t.name for t in current]}")
    if not found.template:
        return _check(Plan("use", found, found.milli_cpu, found.memory_bytes), None)
    return _check(Plan("create", found, found.milli_cpu, found.memory_bytes, name=name or "", idle=idle), None)


def python_problem(name, pinned, python):
    """Why a call pickled by Python `python` cannot run where Python
    `pinned` does, or "": the same minor on both ends (cloudpickle). A pin
    without a minor ("3") or no call to unpickle (a command) says nothing."""
    parts = str(pinned or "").split(".")
    if not python or len(parts) < 2 or not all(p.isdigit() for p in parts[:2]):
        return ""
    if ".".join(parts[:2]) == python:
        return ""
    return (f"{name} runs Python {pinned} and this Python is {python}: a call pickled here is unpickled there "
            f"with the same minor; run this from Python {'.'.join(parts[:2])} (a venv), or run a command")


def plan_call(on, options, keep=False, docker="docker", python=None):
    """.remote(..., dry_run=True): where the call would run, against what
    the function asks; python is the minor its call needs there."""
    from zenthros.machines import LOCAL, Machine

    asks_milli, asks_memory = _asks(options)
    current = targets(docker)
    if on is None:
        from zenthros.machines import default_on

        on = default_on()
    name = on.name if isinstance(on, Machine) else str(getattr(on, "name", "") or getattr(on, "id", "") or on)
    if name == LOCAL:
        local = current[LOCAL]
        plan = Plan("create", local, asks_milli or local.milli_cpu, asks_memory or local.memory_bytes,
                    asks_milli, asks_memory, for_the_call=not keep)
        if local.state == "Unavailable":
            plan.problems.append(f"local is unavailable: {local.reason}")
            return plan
        # on="local" asks nothing of the hub: it attaches nothing. With no size
        # asked it takes the container there is, and nothing can fail to fit.
        return _check(plan, current if asks_milli or asks_memory else None, joins=False)
    found = current.get(name)
    if found is None:
        raise errors.ValidationError(f"on={name!r}: the Workspace has nothing called that; it has {[t.name for t in current]}")
    action = "create" if found.template else "use"
    plan = Plan(action, found, found.milli_cpu, found.memory_bytes, asks_milli, asks_memory,
                for_the_call=found.template and not keep)
    plan = _check(plan, None)
    # A job in its own image runs that image's Python, not the template's.
    problem = python_problem(found.name, found.detail.get("python"), python) if not options.get("image") else ""
    if problem:
        plan.problems.append(problem)
    return plan
