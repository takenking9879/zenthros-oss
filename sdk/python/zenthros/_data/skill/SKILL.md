---
name: zenthros
description: Run code, tests and long jobs on another machine with Zenthros, without stopping work here. Use when a test or a build is long or needs a clean machine, a GPU or a cloud; when several pieces of work should run at once; when asked to run something remotely or to read a job's result; or when writing Python that uses the zenthros SDK (@zenthros.cloud, .remote, .map, upload=).
---

# Zenthros

Zenthros runs code and gives back results. You write the code, and Git keeps the changes. Zenthros is neither a CI nor an IDE.

## With MCP

Add it once: `claude mcp add zenthros -- zenthros mcp`. Five tools:

- `targets`: where it can run, what that costs, and whether you may use it.
- `run`: starts a command and returns at once. By default it runs a snapshot of the code as it is now, changes not committed included, so later edits do not touch it. `dry_run` says what would happen and creates nothing. Never wait in it.
- `status`: state, `exit_code`, `reason`, how long it took and what it cost.
- `logs`: only the lines you ask for. Use `grep` first (`--- FAIL`, `FAILED`, `Error`), not the whole log.
- `cancel`: only what your session started.

## With the command line

The same answers, with `--json`:

```console
zenthros targets --json
zenthros run --on NAME --snapshot --detach --json -- make test
zenthros status JOB --json
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
| `OOM_KILLED` | The kernel killed it for memory: its own unit or pod hit the limit | Use less memory (read the data in parts, smaller types), or a machine with more |
| `PROCESS_FAILED`, the message says "probably ran out of memory" | Killed while the kernel killed a process of a machine it shares (a container, This computer) for memory | The same; on This computer, `zenthros attach NAME --type local --memory` |
| `PROCESS_FAILED`, `exitCode=137`, on a VM whose jobs run in images (containerd) | Killed by a signal; there nothing says whether it was memory | Compare what it loads with the machine's memory (`zenthros targets`); read the data in parts, or a machine with more |
| `IMAGE_PULL_FAILED` | The machine could not read the image | Publish it where that machine can read it |
| `QUOTA_EXCEEDED`, job still `Pending` | The cloud account is full for now; it asks again by itself | Wait, or tell the person: the message has the cloud's words |
| `QUOTA_EXCEEDED`, job `Failed` | The quota could never hold this machine | Tell the person: only raising the quota helps |

Exit codes of the command line: the command's own; 1 if a function failed; 2 for bad usage; 125 if Zenthros could not start it; 130 if cancelled.

## Rules

- A target that costs money needs the person's word: `zenthros agent allow TARGET --max-machines 3` is theirs to run. Show them the price `run` answered with, and do not run it yourself.
- Secrets go by name (`secrets=`). Never put a value in a command or a log.
- A host a third party runs (Vast) can read your code and data. `run` warns: tell the person.
- Ask `status` rarely and `logs` with a `grep`: a long job costs nothing while you do other work.

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

## Writing code with the SDK

Read the reference file of what you use before writing it: each function, each parameter, what it returns and raises.

0. **The same Python minor as the machine.** A function travels pickled, and the machine unpickles it with its own Python: call it from a Python of the machine's minor (each job's first log line says it, after the machine's host name: `zenthros: train on f79e7fd0612b (2 CPUs, Python 3.13.18)`; a This computer has the minor of the Python that attached it). From another one the job fails at once with `PythonVersionMismatch` and what to do: a venv of that minor (`uv venv --python 3.13`). A command (`zenthros run -- ...`) has no such limit. [run.md](reference/run.md)
1. **Packages.** `packages=` takes pip lines (`--extra-index-url ...` too). Pin the versions the person has here (`pip show`, `importlib.metadata.version`): the same module from another package gives other numbers (`xgboost` and `xgboost-cpu`). pip brings no system library: `libgomp.so.1: cannot open shared object file` (LightGBM on a plain VM) is the person's `sudo apt-get install -y libgomp1` there, once. [run.md](reference/run.md#packages)
2. **What travels.** `code_dir="."` sends the folder's small files (`.py`, configs, scripts), never notebooks or data, model and output files; `!NAME` in `.zenthrosignore` sends one back, and the call says what it left out. From a notebook that imports nothing of the project, `code_dir=None`. [run.md](reference/run.md#what-travels-with-a-call)
3. **Without a bucket**, a call carries about 360 KiB of its own files and arguments, and its result about 189 KiB, both after compression. Past that the error says what to do. [run.md](reference/run.md#size-limits)
4. **Data.** Give it defined types: text to `category`, integers to the smallest size, `float32` where a comparison loses nothing. Without a bucket, a small DataFrame goes as an argument; if it does not fit, to 32 bits where nothing is lost, then again. If it still does not fit, or it is big: Parquet with zstd and `upload=`, which needs a bucket (if there is none, tell the person to set one with `zenthros store`). What you send by `upload=` is always Parquet: convert a CSV, with its types, first; never upload the CSV (`upload=` takes any file: the rule is yours, not the SDK's). [data.md](reference/data.md)
5. **Plots** come back in the value, as the image's bytes (`savefig` to an `io.BytesIO`). [data.md](reference/data.md)
6. **Machine size.** `zenthros.create("local", cpus=, memory=)` or `zenthros attach NAME --type local --cpus N --memory M` size This computer (an existing one changes in place, its environments kept). On a VM or a container, a call's `cpu=`/`memory=` choose a machine, they do not limit it; on Kubernetes they are requests. `zenthros targets` says each machine's CPUs and memory. [machines.md](reference/machines.md)
7. **This computer runs one job at a time.** Many items go in one `.map`, not in a loop of `.remote()`. [pipelines.md](reference/pipelines.md)
8. **Each trip costs** the imports and processes the function starts (2–5 s with torch and joblib) plus about 1.5 s of Zenthros: put the heavy work in one trip. Each call prints its job's name (`zenthros: train → zr-train-c91a4b on caja`): `zenthros.get(name)` and `zenthros logs NAME` take it. [results.md](reference/results.md)
9. **Data larger than the machine.** Measure before loading (the schema and the row count; a Float32 column is 4 bytes a row). Read only the columns used, in 32 bits, with `polars.scan_parquet`; for XGBoost, feed batches of `collect_batches()` to a `QuantileDMatrix`, and if that does not fit, to an `ExtMemQuantileDMatrix` with its pages on disk; if not even that, a machine with more memory. Measured on 26.6 M rows × 26 columns in 2 CPUs and 4 GiB: pandas reading it all ran out of memory; batches into `QuantileDMatrix` fit at the edge (3.9 GiB); `ExtMemQuantileDMatrix` took 1.8 GiB and 1.6 GB of disk, as fast and as good. [data.md](reference/data.md)

A job killed for memory says so: `OOM_KILLED`, or `PROCESS_FAILED` with "probably ran out of memory" on a machine it shares; in Python, the `RemoteError`'s `.reason` and message. On a VM whose jobs run in images (containerd) it does not: `PROCESS_FAILED` with `exitCode=137` and no word on memory. Read the job before trying again (`zenthros jobs`, `zenthros logs NAME`), and change how it reads its data or where it runs; never run it again unchanged.

## Reference

- [reference/cloud.md](reference/cloud.md): `@zenthros.cloud` and its `.remote`, `.submit`, `.map`, `.serve`; its options are each call's defaults, `on=` included.
- [reference/run.md](reference/run.md): `zenthros.run`, functions and commands, every option.
- [reference/results.md](reference/results.md): `RemoteCall` (`result`, `logs`, `status`, `cancel`...), `get`, `gather`, errors.
- [reference/machines.md](reference/machines.md): `targets`, `create`, `delete`, `attach`, `Machine`, `LocalBackend`.
- [reference/data.md](reference/data.md): `upload=`, `files`, `parquet`, `Artifact`, `set_store`, `cache`, and data larger than the machine.
- [reference/pipelines.md](reference/pipelines.md): `Pipeline`, `Task`, `map`, `grid`, `spawn`, `children`, `phase`.
- [reference/progress.md](reference/progress.md): `Reporter`, `report`, `progress`, `Logs`, `transcript`.
- [reference/checkpoints.md](reference/checkpoints.md): `Checkpointer`, `CheckpointPolicy`, `preemption`.
- [reference/services.md](reference/services.md): `serve`, `services`.
- [reference/session.md](reference/session.md): `login`, `logout`, `configure`, `workspace`, `session`, `install`, `Secret`, `JobEnv`.

Examples, in the SDK's repository: `examples/data/notebook_on_a_machine.ipynb` (the heavy part of a notebook on a machine, its plot back in the value) and `examples/data/lazy_training.py` (point 9).
