# SPDX-License-Identifier: Apache-2.0

"""VMPool, VMFleet and Prewarm as SDK objects, through zctl.

Each spec field of the three CRDs has a place here (test_infra_parity_with_crd
checks it against the generated CRDs). Objects are written as JSON and created
with `zctl create <kind> -f`, which decodes strictly, so a field the API does
not have is refused, never dropped. get() returns the object as stored
(`zctl get <kind> NAME -o json`): a registry's 403 or a provider's quota error
is in its status word for word. A refusal raises what zenthros.zctl maps
zctl's exit code to (ValidationError, WorkspaceForbidden, ZctlError).
"""

import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import Optional

from zenthros.jobs.spec import LabelSelector, to_plain
from zenthros.zctl import Zctl

API_VERSION = "infrastructure.zenthros.io/v1alpha1"


# --- shared ----------------------------------------------------------------


@dataclass
class Access:
    mode: str = ""  # Shared | Restricted
    workspaces: list = field(default_factory=list)


@dataclass
class Toolchain:
    name: str = ""  # python | node | go | java
    version: str = ""
    packages: list = field(default_factory=list)


# --- VMPool ------------------------------------------------------------------


@dataclass
class Identity:
    instance_profile: str = ""
    service_account_name: str = ""


@dataclass
class InstancePolicy:
    allowed_types: list = field(default_factory=list)
    image: str = ""
    # The machine's disk, where the provider's default is too small.
    disk_gb: Optional[int] = field(default=None, metadata={"json": "diskGB"})
    # The least CUDA the host's driver must support, e.g. "12.9".
    min_cuda: str = field(default="", metadata={"json": "minCUDA"})
    # In place of the network's default security group (AWS).
    security_group_ids: list = field(default_factory=list, metadata={"json": "securityGroupIDs"})


@dataclass
class NameRef:
    name: str = ""


@dataclass
class ProviderRef:
    name: str = ""
    # A registered CloudAccount to create the pool's machines in. Empty is the
    # installation's own account. It cannot change once the pool exists.
    account: str = ""


@dataclass
class NetworkZone:
    name: str = ""
    zone: str = ""


@dataclass
class NetworkRef:
    name: str = ""
    # The networks of further availability zones, for a provider whose
    # network lives in one zone (an AWS subnet).
    zones: list = field(default_factory=list, metadata={"of": "NetworkZone"})


@dataclass
class PlacementRef:
    region: str = ""
    zone: str = ""


@dataclass
class ContainerRuntime:
    engine: str = ""  # containerd | none
    gpu: str = ""  # auto | nvidia | none


@dataclass
class PoolRuntime:
    container: Optional[ContainerRuntime] = None
    default_job_runtime: str = ""  # systemd | containerd
    toolchains: list = field(default_factory=list, metadata={"of": "Toolchain"})
    packages: list = field(default_factory=list)


@dataclass
class Scaling:
    min_ready: Optional[int] = None
    max_ready: Optional[int] = None
    idle_grace: str = ""
    idle_ttl: str = field(default="", metadata={"json": "idleTTL"})
    max_lifetime: str = ""
    # A size an external driver asks for (kubectl scale, the cluster
    # autoscaler of a joined pool).
    replicas: Optional[int] = None


@dataclass
class Interruption:
    standby: str = ""  # IdleFirst | NewMachine | None
    # Replace a joined Spot pool's node when its provider says the machine
    # is at elevated risk of interruption.
    capacity_rebalance: Optional[bool] = None


@dataclass
class ExposedPort:
    port: Optional[int] = None
    protocol: str = ""  # HTTP | TCP


@dataclass
class NodeTaint:
    key: str = ""
    value: str = ""
    effect: str = ""  # NoSchedule | PreferNoSchedule | NoExecute


@dataclass
class KubernetesJoin:
    # The enrolled Kubernetes backend whose cluster the machines join.
    backend_id: str = field(default="", metadata={"json": "backendID"})
    join_method: str = ""  # Auto | Kubelet | K3s
    node_labels: dict = field(default_factory=dict)
    node_taints: list = field(default_factory=list, metadata={"of": "NodeTaint"})


@dataclass
class Join:
    # The pool's machines become nodes of a Kubernetes cluster: it serves no
    # job directly, and its size is scaling.replicas.
    kubernetes: Optional[KubernetesJoin] = None


@dataclass
class VMPoolSpec:
    # VM (the default) or Container: a provider that hands out unprivileged
    # containers; instance_policy.image is then the image every job runs in.
    machine_kind: str = ""  # VM | Container
    provider_ref: Optional[ProviderRef] = None
    network_ref: Optional[NetworkRef] = None
    placement_ref: Optional[PlacementRef] = None
    instance_policy: Optional[InstancePolicy] = None
    identity: Optional[Identity] = None
    purchasing_mode: str = ""  # OnDemand | Spot
    ownership: str = ""
    access: Optional[Access] = None
    runtime: Optional[PoolRuntime] = None
    scaling: Optional[Scaling] = None
    interruption: Optional[Interruption] = None
    # The ports every machine publishes, so a Service can be reached on it.
    exposed_ports: list = field(default_factory=list, metadata={"of": "ExposedPort"})
    join: Optional[Join] = None


# --- VMFleet -----------------------------------------------------------------


@dataclass
class FleetMember:
    pool_ref: str = ""
    min: Optional[int] = None
    max: Optional[int] = None
    weight: Optional[int] = None


@dataclass
class FleetTotal:
    min_ready: Optional[int] = None
    max: Optional[int] = None


@dataclass
class FleetConstraint:
    count_by: str = ""  # accelerator | purchasingMode | zone | ...
    value: str = ""
    max: Optional[int] = None
    max_fraction: str = ""  # "0.75"
    max_per_domain: Optional[int] = None


@dataclass
class VMFleetSpec:
    access: Optional[Access] = None
    members: list = field(default_factory=list, metadata={"of": "FleetMember"})
    total: Optional[FleetTotal] = None
    constraints: list = field(default_factory=list, metadata={"of": "FleetConstraint"})


# --- Prewarm -----------------------------------------------------------------


@dataclass
class PrewarmArtifact:
    uri: str = ""
    sha256: str = ""
    path: str = ""


@dataclass
class PrewarmItem:
    image: str = ""
    artifact: Optional[PrewarmArtifact] = None
    toolchain: Optional[Toolchain] = None


@dataclass
class PrewarmTargets:
    backends: list = field(default_factory=list)
    backend_selector: Optional[LabelSelector] = None
    pools: list = field(default_factory=list)
    fleets: list = field(default_factory=list)
    all: Optional[bool] = None


@dataclass
class PrewarmKubernetes:
    node_selector: dict = field(default_factory=dict)
    pvc_name: str = ""
    image: str = ""


@dataclass
class PrewarmSpec:
    targets: PrewarmTargets = field(default_factory=PrewarmTargets)
    items: list = field(default_factory=list, metadata={"of": "PrewarmItem"})
    on_new_members: Optional[bool] = None
    concurrency: Optional[int] = None
    kubernetes: Optional[PrewarmKubernetes] = None


# --- objects -----------------------------------------------------------------


@dataclass
class _Object:
    name: str
    labels: dict = field(default_factory=dict)
    annotations: dict = field(default_factory=dict)

    KIND = ""

    def to_manifest(self):
        meta = {"name": self.name}
        if self.labels:
            meta["labels"] = dict(self.labels)
        if self.annotations:
            meta["annotations"] = dict(self.annotations)
        return {"apiVersion": API_VERSION, "kind": self.KIND, "metadata": meta, "spec": to_plain(self.spec)}

    def to_json(self):
        return json.dumps(self.to_manifest(), indent=2)


@dataclass
class VMPool(_Object):
    spec: VMPoolSpec = field(default_factory=VMPoolSpec)
    KIND = "VMPool"


@dataclass
class VMFleet(_Object):
    spec: VMFleetSpec = field(default_factory=VMFleetSpec)
    KIND = "VMFleet"


@dataclass
class Prewarm(_Object):
    spec: PrewarmSpec = field(default_factory=PrewarmSpec)
    KIND = "Prewarm"


# --- client ------------------------------------------------------------------

_VERB_KIND = {"VMPool": "vmpool", "VMFleet": "vmfleet", "Prewarm": "prewarm"}


class InfraClient:
    """Creates, reads, scales and deletes pools, fleets and Prewarms with zctl."""

    def __init__(self, zctl=None):
        self._run = Zctl(zctl)
        self.zctl = self._run.binary

    def create(self, obj):
        kind = _VERB_KIND[obj.KIND]
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            f.write(obj.to_json())
            path = f.name
        try:
            return self._run("create", kind, "-f", path)
        finally:
            os.unlink(path)

    def get(self, kind, name):
        """The object as the API server holds it, status included."""
        return json.loads(self._run("get", _verb(kind), name, "-o", "json"))

    def delete(self, kind, name, *flags):
        return self._run("delete", _verb(kind), name, *flags)

    def scale_pool(self, name, min_ready=None, max_ready=None):
        args = ["scale", "vmpool", name]
        if min_ready is not None:
            args += ["--min", str(min_ready)]
        if max_ready is not None:
            args += ["--max", str(max_ready)]
        return self._run(*args)

    def scale_fleet(self, name, min_ready=None, max_total=None):
        args = ["scale", "vmfleet", name]
        if min_ready is not None:
            args += ["--min-ready", str(min_ready)]
        if max_total is not None:
            args += ["--max", str(max_total)]
        return self._run(*args)


def _verb(kind):
    if isinstance(kind, type):
        kind = kind.KIND
    return _VERB_KIND.get(kind, kind)
