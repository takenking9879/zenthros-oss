---
name: zenthros
description: Run code, tests and long jobs on another machine with Zenthros, without stopping work here. Use when a test or a build is long or needs a clean machine, a GPU or a cloud; when several pieces of work should run at once; when asked to run something remotely or to read a job's result; or when writing Python that uses the zenthros SDK (@zenthros.cloud, .remote, .map, upload=).
---

# Zenthros

Zenthros runs code and gives back results. You write the code, and Git keeps the changes. Zenthros is neither a CI nor an IDE.

## With MCP

Add it once: `claude mcp add zenthros -- zenthros mcp`. Six tools:

- `targets`: where it can run, what that costs, and whether you may use it.
- `spend`: what the Workspace's machines spent today and cost per hour now, and each cap the administrator set, with what it leaves for a new machine.
- `run`: starts a command and returns at once. By default it runs a snapshot of the code as it is now, uncommitted changes included, so later edits do not touch it. `dry_run` says what would happen and creates nothing. It never waits: ask `status` later.
- `status`: state, `exit_code`, `reason`, how long it took and what it cost.
- `logs`: only the lines you ask for. Use `grep` first (`--- FAIL`, `FAILED`, `Error`), not the whole log.
- `cancel`: only what your session started.

## With the command line

The same answers, with `--json`:

```console
zenthros targets --json
zenthros spend --json
zenthros run --on NAME --snapshot --detach --json -- make test
zenthros status JOB --json                  # or zenthros jobs JOB; zenthros jobs JOB --delete deletes it
zenthros jobs --today --json                # today's jobs, how long each took and what it cost, and the machines' spend
zenthros logs JOB --grep "FAILED" --json
zenthros cancel JOB --json
```

With no `--on`, it runs on this computer, in a container.

## How to read a result

| You see | It means | What to do |
|---|---|---|
| `exit_code` 1 (or any number) | The test or command failed | Read `logs`, fix it, run again |
| No `exit_code`, `reason` `WORKER_LOST` or `PROVIDER_GONE` | The machine failed, not the code | Run again; nothing is retried in secret |
| `MACHINE_LIFETIME` | The machine reached its `max_lifetime` | Run again, or resume from a checkpoint |
| `OOM_KILLED` | The kernel killed it for memory: its own unit or pod hit the limit | Use less memory (read the data in parts, smaller types), or a machine with more; never run it again unchanged |
| `PROCESS_FAILED`, the message says "probably ran out of memory" | Killed by a signal while the kernel, short of memory, was killing processes on a machine it shares (a container, This computer) | The same; on This computer, `zenthros attach NAME --type local --memory` |
| `PROCESS_FAILED`, `exitCode=137`, on a VM whose jobs run in images (containerd) | Killed by a signal; on such a VM nothing says whether it was memory | Compare what it loads with the machine's memory (`zenthros targets`); read the data in parts, or a machine with more |
| `IMAGE_PULL_FAILED` | The machine could not read the image | Publish it where that machine can read it |
| `QUOTA_EXCEEDED`, job still `Pending` | The cloud account is full for now; it asks again by itself | Wait, or tell the person: the message has the cloud's words |
| `QUOTA_EXCEEDED`, job `Failed` | The quota could never hold this machine | Tell the person: only raising the quota helps |
| `the hub at ... is not answering (it may be restarting)` on stderr | The hub is restarting; the SDK asks again for up to 2 min | Nothing: the call goes on. A `TransientError` after that: tell the person the hub is down |

Exit codes of the command line: the command's own; 1 if a function failed; 2 for bad usage; 125 if Zenthros could not start it; 130 if cancelled.

## Rules

- A target that costs money needs the person's word: `zenthros agent allow TARGET --max-machines 3` is theirs to run. Show them the price `run` answered with, and never run `agent allow` yourself. Only the MCP tools check that word: the SDK and the command line (`zenthros.create`, a call `on=` a template, `zenthros run --on`) make machines without asking, so before one that costs, ask the person yourself.
- Secrets go by name (`secrets=`). Never put a value in a command or a log.
- A host a third party runs (Vast) can read your code and data. `run` warns: tell the person.
- A long job needs no watching while you do other work: ask `status` rarely.

## In Python

```python
import zenthros

@zenthros.cloud
def train(path, depth=4): ...

train(path)                               # here, as usual
train.remote(path)                        # in a container on this computer, with no account
train.remote(path, on="gcp-gpu", env="train", gpu=1)   # the same code, and the environment you tried, elsewhere
```

A `Pipeline` reuses steps with `cache=True`, says what it would do with `plan()`, and goes on from a step's last checkpoint with `resume()`. `zenthros env new/add/build/push` makes and publishes an environment.

A bucket of the person's own needs its client on this computer: `pip install "zenthros[gcs]"` for `gs://`, `"zenthros[s3]"` for `s3://` (`zenthros store` says so when it is missing). A machine installs what the runner needs by itself.

## Writing code with the SDK

Read the reference file of what you use before writing it: each function, each parameter, what it returns and raises.

0. **The same Python minor as the machine.** A function travels pickled. Each job's first log line says the machine's Python. From another minor the job fails at once (`PythonVersionMismatch`): use a venv of that minor (`uv venv --python 3.13`). A command (`zenthros run -- ...`) has no such limit. [run.md](reference/run.md)
1. **Packages.** `packages=` takes pip lines (`--extra-index-url ...` too). Pin the versions the person has here (`pip show`, `importlib.metadata.version`): the same module from another package gives different results (`xgboost` and `xgboost-cpu`). pip brings no system library: `libgomp.so.1: cannot open shared object file` (LightGBM on a plain VM) is the person's `sudo apt-get install -y libgomp1` there, once. [run.md](reference/run.md#packages)
2. **What travels.** `code_dir="."` sends the folder's small files (`.py`, configs, scripts), never notebooks or data, model and output files; `!NAME` in `.zenthrosignore` sends one anyway, and the call says what it left out. From a notebook that imports nothing of the project, `code_dir=None`. [run.md](reference/run.md#what-travels-with-a-call)
3. **Without a bucket**, a call carries at most about 360 KiB of its own files and arguments, and its result at most about 189 KiB, both after compression. Past that the error says what to do. [run.md](reference/run.md#size-limits)
4. **Data.** Defined types (`category`, small integers, `float32` where nothing is lost). Small: as an argument. Big: Parquet with zstd and `upload=`, which needs a bucket (if there is none, tell the person to set one with `zenthros store`). Converting a CSV to Parquet, with its types, before uploading it is the recommendation. [data.md](reference/data.md)
5. **Plots and files.** A plot comes back in the value, as the image's bytes (`savefig` to an `io.BytesIO`). A file a job writes into the folder `$ZENTHROS_DATA_OUTPUT` (with `open()`, `savefig()`, or a shell's `>`, in any language) reaches the bucket before the job ends. [data.md](reference/data.md)
6. **Machine size.** `zenthros.create("local", cpus=, memory=)` or `zenthros attach NAME --type local --cpus N --memory M` size This computer (an existing one changes in place, its environments kept). On a VM or a container, a call's `cpu=`/`memory=` choose a machine with at least that much; they do not limit the job; on Kubernetes they are requests. `zenthros targets` says each machine's CPUs and memory. [machines.md](reference/machines.md)
7. **A machine runs one job at a time** (This computer, a VM, a template's machine; a cluster runs several). Many items go in one `.map`, not in a loop of `.remote()`. A map on one machine runs its tasks one after another, and says so: for many short tasks, group them into fewer items; for them to run at once, give the map several machines (`on=[...]`) or a template. A map on a template the Workspace made several machines of spreads over those. `zenthros targets` says how many machines each template allows (MACHINES): those made with `create` and those the template made for calls count alike, and past the limit a job waits for one. [pipelines.md](reference/pipelines.md)
8. **Each trip costs** the imports and processes the function starts (2–5 s with torch and joblib) plus about 1.5 s of Zenthros: put the heavy work in one trip. Each call prints its job's name (`zenthros: train → zr-train-c91a4b on my-box`): `zenthros.get(name)` and `zenthros logs NAME` take it. [results.md](reference/results.md)
9. **Data larger than the machine.** Measure before loading (the schema and the row count; a Float32 column is 4 bytes a row). Read only the columns used, in 32 bits, with `polars.scan_parquet`; for XGBoost, feed batches of `collect_batches()` to a `QuantileDMatrix`, and if that does not fit, to an `ExtMemQuantileDMatrix` with its pages on disk; if not even that, a machine with more memory. [data.md](reference/data.md)

## Reference

- [reference/cloud.md](reference/cloud.md): `@zenthros.cloud` and its `.remote`, `.submit`, `.map`, `.serve`; its options are each call's defaults, `on=` included.
- [reference/run.md](reference/run.md): `zenthros.run`, functions and commands, every option.
- [reference/results.md](reference/results.md): `RemoteCall` (`result`, `logs`, `status`, `cancel`...), `get`, `gather`, errors.
- [reference/machines.md](reference/machines.md): `targets`, `spend`, `create`, `delete`, `attach`, `Machine`, `LocalBackend`.
- [reference/data.md](reference/data.md): `upload=`, `files`, `parquet`, `Artifact`, `set_store`, `cache`, and data larger than the machine.
- [reference/pipelines.md](reference/pipelines.md): `Pipeline`, `Task`, `map`, `grid`, `spawn`, `children`, `phase`, and work every day or every month (cron, GitHub Actions).
- [reference/progress.md](reference/progress.md): `Reporter`, `report`, `progress`, `Logs`, `transcript`.
- [reference/checkpoints.md](reference/checkpoints.md): `Checkpointer`, `CheckpointPolicy`, `preemption`.
- [reference/services.md](reference/services.md): `serve`, `services`.
- [reference/session.md](reference/session.md): `login`, `logout`, `configure`, `workspace`, `session`, `install`, `Secret`, `JobEnv`.

Examples, in the SDK's repository: `examples/data/notebook_on_a_machine.ipynb` (the heavy part of a notebook on a machine, its plot back in the value) and `examples/data/lazy_training.py` (point 9).
