# SPDX-License-Identifier: Apache-2.0

"""The whole ZenthrosJob spec as dataclasses.

Every field of the CRD's spec has a place here (test_spec_parity_with_crd
checks it against the generated CRD). Field names are the CRD's in
snake_case; to_manifest() writes them back in camelCase and leaves out
what is unset (None, "", [], {}). A bool left None is unset; False is
written.
"""

import json
import os
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Optional

API_VERSION = "zenthros.ai/v1"
KIND = "ZenthrosJob"
# RESOURCE is the CRD's plural: what a Kubernetes Status names a job by.
RESOURCE = "zenthrosjobs"


def _camel(name):
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


def _key(f):
    return f.metadata.get("json") or _camel(f.name)


def _unset(value):
    return value is None or value == "" or value == [] or value == {}


def to_plain(value):
    """A dataclass tree as JSON-ready dicts with the CRD's keys."""
    if is_dataclass(value):
        out = {}
        for f in fields(value):
            item = to_plain(getattr(value, f.name))
            if not _unset(item):
                out[_key(f)] = item
        return out
    if isinstance(value, (list, tuple)):
        return [to_plain(item) for item in value]
    if isinstance(value, dict):
        return {k: to_plain(v) for k, v in value.items()}
    return value


# --- selectors -----------------------------------------------------------


@dataclass
class LabelSelectorRequirement:
    key: str = ""
    operator: str = ""
    values: list = field(default_factory=list)


@dataclass
class LabelSelector:
    match_labels: dict = field(default_factory=dict)
    match_expressions: list = field(default_factory=list, metadata={"of": "LabelSelectorRequirement"})


# --- target, capacity, compute -------------------------------------------


@dataclass
class RayClusterRef:
    name: str = ""
    namespace: str = ""


@dataclass
class Target:
    backends: list = field(default_factory=list)
    ray_cluster: Optional[RayClusterRef] = None


@dataclass
class Capacity:
    acquire: list = field(default_factory=list)  # Backends, Reuse, Create


@dataclass
class Compute:
    architecture: str = ""
    backend_kind: str = ""  # k8s | vm
    cpu: str = ""
    memory: str = ""
    gpu_count: Optional[int] = None
    gpu_model: str = ""
    instance_type: str = ""
    lifespan: str = ""  # Ephemeral | Retain
    max_hourly_price_cents: Optional[int] = None
    purchase_option: str = ""  # OnDemand | Spot
    region: str = ""
    zone: str = ""
    runtime: str = ""  # systemd | containerd | workspace-containers


@dataclass
class GCPolicy:
    cleanup_policy: str = ""  # Delete | Retain
    ttl_seconds_after_finished: Optional[int] = None
    retain_logs_for_seconds: Optional[int] = None
    retain_workdir: Optional[bool] = None


@dataclass
class PreemptNotice:
    signal: str = ""  # SIGTERM | SIGINT | SIGUSR1 | SIGUSR2


@dataclass
class Resilience:
    on_worker_lost: str = ""  # Fail | Reschedule
    replace: list = field(default_factory=list)
    worker_lost_retry_limit: Optional[int] = None
    job_grace: str = ""  # a duration, "45s"
    preempt_notice: Optional[PreemptNotice] = None


# --- placement -----------------------------------------------------------


@dataclass
class PackRule:
    topology_key: str = ""


@dataclass
class SpreadRule:
    topology_key: str = ""
    max_skew: Optional[int] = None
    min_domains: Optional[int] = None
    job_selector: Optional[LabelSelector] = None


@dataclass
class JobAffinityTerm:
    topology_key: str = ""
    job_name: str = ""
    job_selector: Optional[LabelSelector] = None


@dataclass
class Placement:
    pack: Optional[PackRule] = None
    spread: list = field(default_factory=list, metadata={"of": "SpreadRule"})
    separate_machines: Optional[bool] = None
    affinity: list = field(default_factory=list, metadata={"of": "JobAffinityTerm"})
    anti_affinity: list = field(default_factory=list, metadata={"of": "JobAffinityTerm"})
    regions: list = field(default_factory=list)


# --- workload ------------------------------------------------------------


@dataclass
class EnvVar:
    name: str = ""
    value: str = ""


@dataclass
class Workers:
    # Required by the CRD. 0 is the Go zero zctl always wrote; through
    # zenthros-api the JSON reaches the CRD as it is, so it is written here.
    min: int = 0
    max: int = 0
    resource_class: str = ""
    gpus_per_node: Optional[int] = None
    interconnect: str = ""
    cpu: str = ""
    memory: str = ""


@dataclass
class RestartPolicy:
    backoff_limit: Optional[int] = None


@dataclass
class Artifacts:
    input: str = ""
    output: str = ""


@dataclass
class Source:
    host_path: str = ""
    git: str = ""
    git_ref: str = ""
    git_secret_ref: str = ""
    s3: str = ""
    inline: str = ""
    ref: str = ""  # a ConfigMap holding the tarball (zctl run --dir)


@dataclass
class AWSSecretsManager:
    secret_id: str = ""
    region: str = ""
    json_key: str = ""
    version_id: str = ""
    version_stage: str = ""


@dataclass
class EncryptedPayload:
    ciphertext: str = ""
    key_id: str = ""
    recipients: list = field(default_factory=list)


@dataclass
class Materialization:
    mode: str = ""  # env | file
    env_name: str = ""
    path: str = ""


@dataclass
class SecretSource:
    type: str = ""  # k8s-secret | aws-secrets-manager | encrypted-payload
    namespace: str = ""
    name: str = ""
    key: str = ""
    aws_secrets_manager: Optional[AWSSecretsManager] = None
    encrypted_payload: Optional[EncryptedPayload] = None
    materialization: Optional[Materialization] = None


@dataclass
class SparkResources:
    cpu: str = ""
    memory: str = ""


@dataclass
class SparkRestartPolicy:
    type: str = ""
    max_restart_attempts: Optional[int] = None


@dataclass
class Spark:
    main_application_file: str = ""
    main_class: str = ""
    arguments: list = field(default_factory=list)
    driver: Optional[SparkResources] = None
    executor: Optional[SparkResources] = None
    executor_count: Optional[int] = None
    spark_conf: dict = field(default_factory=dict)
    restart_policy: Optional[SparkRestartPolicy] = None


@dataclass
class ConditionRule:
    type: str = ""
    status: str = ""  # True | False | Unknown


@dataclass
class PhasePath:
    path: str = ""
    succeeded: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    running: list = field(default_factory=list)


@dataclass
class EndpointSource:
    # A Service object's URL at path in it ("status.url"), published as name.
    name: str = ""
    path: str = ""


@dataclass
class Manifest:
    # The object as written: the operator that owns it reads it, not Zenthros.
    object: dict = field(default_factory=dict)
    success_when: list = field(default_factory=list, metadata={"of": "ConditionRule"})
    failure_when: list = field(default_factory=list, metadata={"of": "ConditionRule"})
    running_when: list = field(default_factory=list, metadata={"of": "ConditionRule"})
    phase_path: Optional[PhasePath] = None
    log_selector: Optional[LabelSelector] = None
    lifecycle: str = ""  # RunToCompletion | Service
    endpoint_from: list = field(default_factory=list, metadata={"of": "EndpointSource"})


@dataclass
class Distribution:
    mode: str = "Independent"
    count: Optional[int] = None
    tasks_per_node: Optional[int] = None
    failure_policy: str = ""  # FailFast | ContinueOnFailure


@dataclass
class ServicePort:
    name: str = ""
    port: Optional[int] = None
    protocol: str = ""  # HTTP (default) | TCP


@dataclass
class Service:
    # A Job with ports runs until it is cancelled; status.endpoints says
    # where each port is reached. The pool must publish the ports.
    ports: list = field(default_factory=list, metadata={"of": "ServicePort"})
    # The controller stops it (Cancelled, STOP_AFTER) once it has served this long.
    stop_after_seconds: Optional[int] = None


@dataclass
class Template:
    entrypoint: str = ""
    command: list = field(default_factory=list)
    args: list = field(default_factory=list)
    working_dir: str = ""
    image: str = ""
    workers: Optional[Workers] = None
    distribution: Optional[Distribution] = None
    env: list = field(default_factory=list, metadata={"of": "EnvVar"})
    restart_policy: Optional[RestartPolicy] = None
    artifacts: Optional[Artifacts] = None
    storage_class: str = ""
    secret_sources: list = field(default_factory=list, metadata={"of": "SecretSource"})
    source: Optional[Source] = None
    spark: Optional[Spark] = None
    manifest: Optional[Manifest] = None
    service: Optional[Service] = None


@dataclass
class Workload:
    kind: str = "Job"
    template: Template = field(default_factory=Template)


@dataclass
class ResumeFrom:
    """The ended job a new job takes its checkpoint from: by name and UID, never
    a place; the controller reads the cursor."""

    job: str = ""
    uid: str = ""


@dataclass
class Spec:
    workspace: str = ""
    target: Optional[Target] = None
    pool_ref: str = ""
    fleet_ref: str = ""
    compute: Optional[Compute] = None
    capacity: Optional[Capacity] = None
    resilience: Optional[Resilience] = None
    placement: Optional[Placement] = None
    gc_policy: Optional[GCPolicy] = None
    priority: Optional[int] = None
    depends_on: list = field(default_factory=list)
    resume_from: Optional[ResumeFrom] = None
    active_deadline_seconds: Optional[int] = None
    workload: Workload = field(default_factory=Workload)
    # Set by cancel() (zctl cancel), never on create.
    cancel: Optional[bool] = None


@dataclass
class Job:
    """A ZenthrosJob to create: metadata plus the whole spec."""

    name: str
    spec: Spec = field(default_factory=Spec)
    namespace: str = ""
    labels: dict = field(default_factory=dict)
    annotations: dict = field(default_factory=dict)

    def to_manifest(self, namespace=None):
        meta = {"name": self.name}
        # No namespace: zctl places the job (-n, ZENTHROS_NAMESPACE, the zctl
        # context, then tenant-default).
        ns = self.namespace or namespace or os.environ.get("ZENTHROS_NAMESPACE", "")
        if ns:
            meta["namespace"] = ns
        if self.labels:
            meta["labels"] = dict(self.labels)
        if self.annotations:
            meta["annotations"] = dict(self.annotations)
        return {"apiVersion": API_VERSION, "kind": KIND, "metadata": meta, "spec": to_plain(self.spec)}

    def to_json(self, namespace=None):
        """JSON is YAML: zctl create -f reads it as written, no escaping."""
        return json.dumps(self.to_manifest(namespace), indent=2, sort_keys=False)
