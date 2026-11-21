# Machines

This file covers where code can run and the machines of a Workspace: `zenthros.targets()` lists them, `zenthros.create` makes one, `zenthros.attach` connects one you already have, and `zenthros.delete` removes one. Read it before you choose `on=`, or before you size This computer.

Sizes, in short:

- `zenthros.create("local", cpus=, memory=)` and `zenthros attach NAME --type local --cpus N --memory M` make a This computer: a container on this computer, joined to the hub. Its CPUs and memory are hard limits. It runs one job at a time. It starts again when Docker does (`--restart unless-stopped`). `zenthros.delete` removes it.
- An existing This computer changes size in place with the same `attach` line (or `zenthros.attach(NAME, cpus=, memory=)`). Its environments stay.
- On a VM or a container, a call's `cpu=`/`memory=` choose a machine; they do not limit it. On Kubernetes they are requests.
- `zenthros targets` shows each machine's CPUs and memory.
- `on="local"` needs no machine and no account: it runs in this computer's own container. `create("local")` is the one that joins the hub, so jobs reach it from anywhere.

## zenthros.targets

```python
zenthros.targets(docker="docker")
```

Lists where you can run, before anything runs. It makes nothing. Without a hub login it has one row, `local`: this computer's Docker, with no account. Logged in, it adds what the hub has: your machines, pools, Kubernetes clusters and templates. It shows each machine's CPUs and memory, and an AGENT column with the agent's version when the machines report one. When a machine's agent is not the hub's release, a line under the table says how to update it (`zenthros attach NAME --type local` for a This computer, `zenthros attach NAME` for a VM, which gives the line to paste).

TYPE is `vm`, `container` (This computer is one too) or `k8s`; a pool says `pool`. Templates, which make machines, come in a second table with their PROVIDER. STATUS is `ready`, `busy`, `starting`, `asleep` (a template's machine with no job; the next call starts it), `draining`, `offline`, `unavailable`, `available` (not made yet, and it can be) or `full` (no room for another). CPU and MEM are what one machine of the target has, or would have from the provider's catalog; they never subtract what is in use. The `local` row is the size of the container `on="local"` uses, and how many containers of that size still fit in Docker.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `docker` | `str` | `"docker"` | The container engine to ask about this computer. With the default, the SDK uses `ZENTHROS_ENGINE`, else `docker`, else `podman`. |

**Returns:** a `Targets`: a table in Jupyter, text in a terminal. `targets["name"]` is one `Target` (`KeyError` if there is none), `targets.get("name")` returns `None` instead, `"name" in targets` tests it, iterating gives every `Target`, and `to_pandas()` gives a DataFrame (needs pandas). A `Target`'s `repr` says where each number comes from.

| `Target` field | What it is |
|---|---|
| `name` | The name to pass as `on=` or to `create`. |
| `type`, `status` | What TYPE and STATUS say. |
| `cpu`, `memory_bytes` | One machine's CPUs and memory (0: unknown). |
| `gpu_count`, `gpu_model` | Its GPUs. |
| `available`, `total` | How many machines can be used or made now, of how many. |
| `price` | The price the hub observed (a `dict` with `hourlyRate`, `instanceType`, `region`...), or `None`. |
| `costs` | Whether it bills by the hour. |
| `untrusted_host` | A third party runs its machines (Vast.ai): their owner can read what runs. |
| `agent_version` | The release of its agent; `""` for what has no agent of its own. |
| `reason` | Why it is unavailable or full. |
| `to_dict()` | The row as a script reads it. |

**Raises:** `zenthros.errors.TransientError` when the hub cannot be reached. Docker that is not running is not an error: the `local` row says `unavailable` and why.

```python
import zenthros

print(zenthros.targets())
row = zenthros.targets()["local"]
print(row.cpu, row.memory_bytes, row.status)
```

## zenthros.create

```python
zenthros.create(target="local", name=None, cpus=None, memory=None, image=None, docker="docker", dry_run=False,
                idle=None, ports=None, max_lifetime="24h")
```

Makes a machine of the Workspace and returns it once it is ready. Every parameter after `target` is keyword-only. A `name` the Workspace already has returns that machine and makes nothing. It needs a hub login, except with `dry_run=True`.

- `"local"`: a container on this computer, joined to the hub as a This computer, Private to the Workspace. Its image is Python of this process's minor version with what a function's runner needs (boto3, cloudpickle, and zstd's backport before 3.14), built here once; `zenthros attach NAME --type local` on one made before brings them with its agent. A function's own packages install where it runs. It waits up to 600 s for the machine, and removes it if it never gets there.
- `"k8s:<cluster>"`: the same container as a pod of a Kubernetes backend of the Workspace (by its name or id). `cpus` and `memory` are its requests and limits. It waits up to 600 s; a pod that never gets there is retired, then its job is deleted.
- A template's name (`zenthros.targets()` lists them): a new machine from it, with the template's size. It waits up to 1200 s, unless `idle=` is given, and deletes a machine that never comes up.
- The name of a machine the Workspace has (attached, or made from a template): that machine, as it is.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `target` | `str` | `"local"` | `"local"`, `"k8s:<cluster>"`, a template's name, or the name of a machine the Workspace has. |
| `name` | `str` | `None` | The machine's name in the Workspace. Without it, a container or pod is named by its backend id, and a template's machine `<template>-<4 hex digits>`. |
| `cpus` | `int` or `float` | `None` | `"local"` and `"k8s:"` only: its CPUs. Default 2, or `ZENTHROS_LOCAL_CPUS`. A template's machines have the template's size: given with a template, it is refused. |
| `memory` | `str` or number | `None` | `"local"` and `"k8s:"` only: `"8Gi"`, `"512Mi"`, `"4g"`, or a number of GiB. Default `"4Gi"`, or `ZENTHROS_LOCAL_MEMORY`. For `"local"` memory and swap are the same, so the limit is hard. |
| `image` | `str` | `None` | `"local"`: the container's image (it needs `sh` and `python3`). `"k8s:"`: the pod's image, default `python:<minor>-slim-bookworm`. Refused with a template, which runs its own. |
| `docker` | `str` | `"docker"` | The docker command for `"local"`. |
| `dry_run` | `bool` | `False` | Make nothing: return what would be made or used, its size and price, and what is missing. |
| `idle` | `str` or number | `None` | A template's machine only: `"30m"` or seconds. It ends after that long with no job, and the next call starts it again. `create` then returns at once, and the first call starts it. |
| `ports` | `list` of `int` | `None` | `"local"` only: the ports a Service on it listens on, published to this computer on `127.0.0.1` and no further. Each is 1–65535, never 8080 or 8081 (the agent's own). Ports are set when the container is made. |
| `max_lifetime` | `str`, number or `None` | `"24h"` | A template's machine only: it ends that long after it was made, job or no job, and a job on it ends with `MACHINE_LIFETIME`. `"24h"`, `"2d"` or seconds. `None` keeps it until `delete`. With `idle=` and this default, no lifetime applies: `idle` ends it. |

**Returns:** a `zenthros.Machine`, once ready (a template's machine with `idle=`: at once, asleep). With `dry_run=True`, a `zenthros.targetlist.Plan`: `action` (`"create"` or `"use"`), `target`, its CPUs, memory and price, `ok`, `problems`, and `to_dict()`.

**Raises:**

- `zenthros.errors.ValidationError` when there is no hub login (`zenthros.login()` first); Docker is not running; a size, port or duration is not one; `idle=`, `max_lifetime=`, `ports=`, or (with a template) `cpus=`, `memory=` or `image=` is given where it does not apply; the Workspace has no such cluster or target; a template's machine cannot prepare what it was asked; or a container of that name exists without the `ports=` asked (delete it, then create it again with them).
- `zenthros.errors.BackendJoinError` when the image build, `docker run` or the join fails.
- `zenthros.errors.WaitTimeout` when the machine is not ready in time. It is undone first.

```python
import zenthros

box = zenthros.create("local", name="caja", cpus=4, memory="8Gi")
print(zenthros.create("gcp-gpu", name="gpu-box", dry_run=True))
gpu = zenthros.create("gcp-gpu", name="gpu-box", idle="30m")
zenthros.delete(box)
```

## zenthros.delete

```python
zenthros.delete(machine, docker="docker")
```

Removes a machine of the Workspace. It retires it at the hub first: no job reaches it, and its tokens end. Then it removes what it ran on: its container on this computer (`docker rm -f`), its pod's job on its cluster, or the template's machine. A VM or container you attached is only retired: Zenthros never stops it. Jobs still running on it fail now, and a warning says how many. A This computer that is stopped is found and removed too.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `machine` | `Machine`, `LocalBackend` or `str` | | What `create()` or `attach()` returned, a `LocalBackend`, or a machine's name or id. |
| `docker` | `str` | `"docker"` | The docker command that removes a container on this computer. |

**Returns:** `None`.

**Raises:** `LookupError` when the Workspace has no machine of that name or id (the message lists the names it has). `zenthros.errors.ValidationError` when there is no hub login.

```python
import zenthros

zenthros.delete("caja")
```

## zenthros.attach

```python
zenthros.attach(name=None, **kwargs)
```

Connects a machine of yours to the Workspace, as `name`. Its keyword arguments go to `zenthros.attaching.attach`:

```python
zenthros.attach(name=None, type=None, runtime=None, host=None, key=None, ssh="ssh", wait=False, test=True,
                timeout=1200, ask=None, out=None, cpus=None, memory=None)
```

The type decides the installer and how jobs run there:

| `type` | What attach does | How its jobs run |
|---|---|---|
| `"vm"` | The two lines to paste on the VM, or with `host=` it runs them over ssh, as root. | `runtime="containerd"`: in images. `runtime="systemd"`: directly on the machine. |
| `"container"` | The lines for a container that already runs (RunPod, Vast.ai). | As processes in it. |
| `"kubernetes"` (or `"k8s"`) | The lines to run where `kubectl` and `helm` reach the cluster. | As pods. |
| `"local"` | A This computer: `zenthros.create("local", name=, cpus=, memory=)`, then a test run. | As processes, one job at a time. |

The order is always the same: checks, then the token, then the two lines, then the wait, then a test run. A check that fails makes no token. The first line is the hub's installer, with no secret. The second is the join, whose token is single use and ends in about an hour. Before anything, attach removes the containers on this computer that the hub no longer has a machine for (older than an hour). A VM attached joins Private to the Workspace and External: Zenthros never stops it.

A name the Workspace already has is that machine, and `type`, `runtime` and `host` are not read:

- A This computer on this computer: if it is stopped, it is started again as it was. If its agent is not the hub's release, it is updated in place, its environments kept. With `cpus=` or `memory=` it is resized in place, its environments kept. An update or a resize is refused while a job runs on it.
- A VM or a container: when its agent is not the hub's release, attach writes to `out` the lines to paste on it again. They update the agent and keep the machine and its jobs.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | `None` | The machine's name in the Workspace. |
| `type` | `str` | `None` | `"vm"`, `"container"`, `"kubernetes"` (or `"k8s"`), `"local"` (or `"this-computer"`). Not given: asked, or with `host=` a VM. |
| `runtime` | `str` | `None` | A VM only: `"containerd"` (jobs in images; it needs 10 GB free) or `"systemd"` (jobs directly; 2 GB). Not given: asked, or with `host=` containerd. |
| `host` | `str` | `None` | `"user@address"`: check the VM over ssh, run the two lines there as root (`sudo -n bash -s`, the script on standard input), and wait. Not for `"kubernetes"` or `"local"`. |
| `key` | `str` | `None` | The ssh private key file for `host=`. |
| `ssh` | `str` | `"ssh"` | The ssh command. |
| `wait` | `bool` | `False` | Without `host=`: wait here until the machine is connected and tested, as the command line does. |
| `test` | `bool` | `True` | Once connected, run a function there and back (a test run). |
| `timeout` | `int` | `1200` | Seconds to wait for the machine to connect. |
| `ask` | callable | `None` | `ask(label, options) -> index`: how a missing `type` or `runtime` is asked. Default: a prompt in the terminal. In a script, give `type=` (and `runtime=` for a VM). |
| `out` | stream | `None` | Where attach writes the lines to paste and its progress, such as `sys.stdout`. `None` writes nothing: then read the lines from the result. |
| `cpus` | `int` or `float` | `None` | This computer only: its CPUs, a hard limit. On a name that exists, its new size. The hub shows whole CPUs. |
| `memory` | `str` | `None` | This computer only: `"8Gi"`, `"512Mi"`, `"4g"`. On a name that exists, its new size. |

**Returns:**

- With no `host=` and `wait=False`: an `AttachCommand`, the two lines to paste. `print(cmd)` shows them.
- With `host=`, `wait=True` or `type="local"`: the `zenthros.Machine`, once connected (and tested, unless `test=False`).
- A name that exists: its `zenthros.Machine`.

| `AttachCommand` | What it is |
|---|---|
| `lines` | The two lines: the installer, then the join. |
| `script()` | Both lines as one script for a root shell. |
| `where()` | Where they run: on the machine, or for a cluster where `kubectl` reaches it. |
| `ends` | When the join ends (`"HH:MM"`). |
| `backend_id` | The machine's id at the hub. |
| `wait(timeout=None, test=True, out=None)` | Wait until the machine is connected and tested; returns the `Machine`. |

**Raises:**

- `zenthros.errors.ValidationError` when there is no hub login; the type or runtime is unknown; `runtime=` is given for something other than a VM; `host=` is given for `"kubernetes"` or `"local"`; `cpus=` or `memory=` is given for something other than This computer; a name that exists is not a container on this computer and `cpus=`/`memory=` were given; a resize or an update would happen while a job runs on it; a VM's checks over ssh fail (no systemd, too little disk, no curl: the message says what to do); or there is no ssh.
- `zenthros.errors.BackendJoinError` when the lines fail over ssh (the token shows as `<token>` in the message), or a This computer does not come back.
- `zenthros.errors.WaitTimeout` when the machine does not connect in `timeout` seconds. Over ssh it is then retired.

```python
import sys
import zenthros

cmd = zenthros.attach("my-vm", type="vm", runtime="containerd")
print(cmd)                      # paste these two lines on my-vm
vm = cmd.wait()                 # the Machine, once connected and tested

gpu = zenthros.attach("gpu-box", host="ubuntu@203.0.113.10", key="~/.ssh/id_ed25519", out=sys.stdout)
here = zenthros.attach("caja", type="local", cpus=4, memory="8Gi", out=sys.stdout)
```

## zenthros.Machine

One machine of the Workspace. `zenthros.create()` and `zenthros.attach()` return it. Pass it as `on=` to a call, or to `zenthros.delete`. `str(machine)` is its name.

| Field | What it is |
|---|---|
| `name` | Its name in the Workspace. |
| `id` | Its backend id at the hub. |
| `target` | What made it: `"local"`, `"k8s:<cluster>"`, a template's name, or `"external"` for one attached. |
| `container` | Its container on this computer, for a This computer. |
| `pool` | A template's machine: its pool, which jobs reach it by. |
| `labels` | Its labels. |
| `job` | A pod machine: its job on the cluster. |
| `idle` | A template's machine made with `idle=`: how long it stays up with no job. |
| `ports` | A This computer: the ports it publishes to this computer. |
| `completes_image` | Made running its own image, which what it asked for completes. |
| `update` | From `attach`: the lines that update its agent where it runs, when it is older than the hub's (a VM, or a provider's container: the machine and its jobs stay). Empty when it is current. With `out=None` nothing is written, so read them here and give them to the person. |

### Machine.to_dict

```python
machine.to_dict()
```

The machine as a script or an agent reads it. It takes no parameters.

**Returns:** a `dict` with `name`, `id`, `target`, `pool`, `idle` and `ports`.

**Raises:** nothing.

```python
import zenthros

box = zenthros.create("local", name="caja")
print(box.to_dict())
```

## zenthros.LocalBackend

A container on this computer joined to the Workspace as a backend. `zenthros.create("local")` makes one and returns a `Machine`. Use `LocalBackend` yourself only to choose the container's name and image, or to join a container that already runs: you get one from `LocalBackend.create(...)` or `LocalBackend.attach(...)`. Both need a hub login.

The container `LocalBackend.create` starts has nothing of the host: no privileges (`--cap-drop ALL`, `no-new-privileges`), no mounts, no devices, the default network. Memory and swap are the same, so a job that needs more dies in the container, not this computer. Its jobs run as processes in the image, as the image's user. It starts again when Docker does (`--restart unless-stopped`), and its agent attaches again with its files and environments. One stopped by hand stays stopped until `zenthros attach NAME` starts it. The join's token goes to the container on standard input, never as an argument: it shows in no process list, no `docker inspect` and no error.

| Attribute | What it is |
|---|---|
| `id` | Its backend id at the hub. |
| `workspace` | The Workspace it joined. |
| `container` | The container's name. |
| `created` | `True` when `create` made the container, `False` when `attach` joined one of yours. |
| `docker` | The docker command it uses. |

### LocalBackend.create

```python
zenthros.LocalBackend.create(image, cpus=2, memory="4g", name="", docker="docker", backend_name="", ports=())
```

Starts a container of `image` with `cpus` and `memory` as hard limits, and joins it to the Workspace. Every parameter after `image` is keyword-only. It does not wait for the machine: call `wait_ready()`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `image` | `str` | | The image. The agent needs `sh` and `python3`; `zenthros.run` also needs cloudpickle and boto3 in it. |
| `cpus` | `int` or `float` | `2` | Its CPUs. |
| `memory` | `str` | `"4g"` | Its memory, as docker reads it (`"4g"`, `"512m"`). |
| `name` | `str` | `""` | The container's name. Default `zenthros-<backend id>`. |
| `docker` | `str` | `"docker"` | The docker command. |
| `backend_name` | `str` | `""` | What the Workspace calls the machine. |
| `ports` | `tuple` of `int` | `()` | Ports a Service of it listens on, published on `127.0.0.1` only. |

**Returns:** the `LocalBackend`, joined.

**Raises:** `zenthros.errors.ValidationError` when there is no hub login or no docker. `zenthros.errors.BackendJoinError` when `docker run` or the join fails; the token is then retired and the container removed.

```python
import zenthros

lb = zenthros.LocalBackend.create("zenthros-pipeline:local", cpus=2, memory="4g", backend_name="caja")
lb.wait_ready()
```

### LocalBackend.attach

```python
zenthros.LocalBackend.attach(container, docker="docker")
```

Joins a container that is already running. It stays yours: `remove()` retires the machine and stops the agent, not the container.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `container` | `str` | | The running container's name or id. |
| `docker` | `str` | `"docker"` | The docker command. Keyword-only. |

**Returns:** the `LocalBackend`, joined.

**Raises:** `zenthros.errors.ValidationError` when the container is not running, or there is no hub login. `zenthros.errors.BackendJoinError` when the join fails; the token is then retired.

```python
import zenthros

lb = zenthros.LocalBackend.attach("my-container")
```

### LocalBackend.status

```python
lb.status()
```

Each check the hub's list of the Workspace's machines says of it. It takes no parameters.

**Returns:** a `Status` with `registered`, `healthy`, `open` (not draining), `accepting` (its agent has its runtime ready) and `private` (Private to this Workspace only); `waiting_for`, the first check that does not hold (`""` when all do); `ready`; and `backend`, the machine as the hub described it.

**Raises:** `zenthros.errors.TransientError` when the hub cannot be reached.

```python
print(lb.status().waiting_for)
```

### LocalBackend.wait_ready

```python
lb.wait_ready(timeout=300, interval=2)
```

Waits until every check of `status()` holds. A hub that is away for a moment counts as "not ready yet".

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `timeout` | `int` | `300` | Seconds to wait. |
| `interval` | `int` | `2` | Seconds between two looks. |

**Returns:** the machine as the hub last described it (a `zenthros.view.Backend`: `healthy`, `running_jobs`, `cpu`, `memory_bytes`, `agent_version`...).

**Raises:** `zenthros.errors.WaitTimeout` with the check it was waiting for.

```python
backend = lb.wait_ready(timeout=600)
```

### LocalBackend.resize

```python
lb.resize(cpus=None, memory=None)
```

Changes the container's limits in place (`docker update`): its files and environments stay, and swap stays equal to memory. The agent says the new size on its next heartbeat. It does not look at running jobs; `zenthros.attach(NAME, cpus=, memory=)` refuses while one runs.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `cpus` | `int` or `float` | `None` | The new CPUs; `None` keeps them. |
| `memory` | `str` | `None` | The new memory, as docker reads it (`"6g"`); `None` keeps it. |

**Returns:** `None`.

**Raises:** `zenthros.errors.ValidationError` when `docker update` fails.

```python
lb.resize(cpus=4, memory="8g")
```

### LocalBackend.revive

```python
lb.revive()
```

Brings the agent of a container `create` made back as it was: it starts the container if it is stopped, then attaches the agent again with its certificate (it waits up to 120 s). It takes no parameters.

**Returns:** `None`. Call `wait_ready()` after it.

**Raises:** `zenthros.errors.BackendJoinError` with what the agent said when it does not come back.

```python
lb.revive()
```

### LocalBackend.update

```python
lb.update(install, packages=())
```

Updates the agent of a container `create` made, in place: it installs `packages` in the container's Python as root, runs `install` in the container, then restarts the agent. Its files and environments stay. `zenthros attach NAME --type local` does this with the hub's installer when the agent is not the hub's release, and with what a function's runner needs when the container runs Zenthros's image.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `install` | `str` | | A join's installer script. It holds no secret. |
| `packages` | `list[str]` | `()` | pip lines installed first, as root (`python3 -m pip`): what the runner needs. Empty installs nothing. |

**Returns:** `None`.

**Raises:** `zenthros.errors.BackendJoinError` when the installer or the restart fails.

```python
lb.update(installer_script)
```

### LocalBackend.remove

```python
lb.remove()
```

Retires the machine at the hub (its access ends), then removes the container `create` made (`docker rm -f`), or stops the agent in a container `attach` joined. A retire the hub refuses raises and leaves everything as it was. It takes no parameters.

**Returns:** `None`.

**Raises:** the hub's error when it refuses the retire.

```python
lb.remove()
```
