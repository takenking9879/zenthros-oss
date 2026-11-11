---
name: zenthros
description: Run code, tests and long jobs on another machine with Zenthros, without stopping work here. Use when a test or a build is long or needs a clean machine, a GPU or a cloud; when several pieces of work should run at once; or when asked to run something remotely or to read a job's result.
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
| `OOM_KILLED` | It ran out of memory | Ask for more `memory` |
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
train.remote(uri, on="gcp-gpu", env="train", gpu=1)   # the same code, and the environment you tried, elsewhere
```

A `Pipeline` reuses steps with `cache=True`, says what it would do with `plan()`, and goes on from a step's last checkpoint with `resume()`. `zenthros env new/add/build/push` makes and publishes an environment.
