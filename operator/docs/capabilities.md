# What each backend runs

A backend says which workload kinds it runs. Nothing else in Zenthros decides
this. Each agent's executor declares its kinds when it connects, and again on
a heartbeat whenever the declaration changes (`capability_hash`). The
controller and the scheduler only read that declaration:

- **Admission**: a job whose kind the chosen backend does not declare fails
  before anything is submitted. The error is `UNSUPPORTED_WORKLOAD_KIND` and
  it lists the kinds the backend does declare.
- **Placement**: the Scheduler leaves out backends that do not declare the
  job's kind.
- **Runtime**: `compute.runtime` must be one of the runtimes declared for the
  kind. When the job names none, it gets the declared default.
- **Lifecycle**: `RunToCompletion` workloads end on their own. `Service`
  workloads (RayCluster) run until they are cancelled or fail. A `Service`
  job never becomes Succeeded, and it refuses `activeDeadlineSeconds`.

`zctl get backends` prints each backend's declaration.

## The table

The table is generated from the executors themselves.
`TestCapabilitiesDocMatchesExecutors` fails when the table and the code
drift apart, and `TestEveryEnumKindHasExecutor` fails when the CRD accepts a
kind no executor runs.

<!-- capabilities:begin -->
| Kind | Machines (VM agent) | Containers (container agent) | Kubernetes (in-cluster agent) | Lifecycle |
|---|---|---|---|---|
| Job | systemd, containerd | process | `batch/v1` jobs | RunToCompletion |
| Manifest | no | no | always | RunToCompletion |
| RayCluster | no | no | `ray.io/v1` rayclusters | Service |
| RayJob | no | no | `ray.io/v1` rayjobs | RunToCompletion |
| SparkApplication | no | no | `spark.apache.org/v1` sparkapplications | RunToCompletion |
<!-- capabilities:end -->

How to read it:

- **Machines.**
  - The VM agent always declares `systemd`: a process in a unit.
  - It declares `containerd` only while containerd is active on the host,
    and it probes again on every heartbeat.
  - The default runtime is the pool's `runtime.defaultJobRuntime` (written
    to `/var/lib/zenthros/runtime-default`), or `systemd`.
- **Containers.**
  - The container agent runs inside one unprivileged container and declares
    `process`: the job is its own child process, in the container's image.
  - There is no runtime to choose, and a job that names a different image
    than the container's is refused (see
    [design/container-agent.md](design/container-agent.md)).
- **Kubernetes.**
  - Every 60 seconds the in-cluster agent asks the API server which of these
    resources it serves.
  - A kind is declared only if its resource is served **and** a
    SelfSubjectAccessReview says the agent may `create` it. Installing
    KubeRay, or granting the agent RBAC, shows up within a minute.
  - `Manifest` is always declared. Whether a given object may be created is
    decided by RBAC at apply time, and a refusal is shown verbatim (see
    [external-operators.md](external-operators.md)).
  - A Manifest with `lifecycle: Service` is a Service.
- **Declared nothing yet** (an agent that has not reached its API server) is
  not the same as "runs nothing". Such a backend is not filtered until it
  declares.

## Host facts

Along with its kinds, an agent reports what it observed about its host:
- architecture and OS;
- CPU, memory and GPUs, including whether the NVIDIA container runtime is
  present;
- private address and network;
- zone, region and provider;
- a bounded summary of its warm cache.

Placement reads the topology from these facts, and a `topology.zenthros.ai/*`
label that the admin sets wins over them (see [placement.md](placement.md)).
The scheduler prefers a host whose cache already holds the job's image, as a
preference and never a requirement (see [prewarm.md](prewarm.md)).
