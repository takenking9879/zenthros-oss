# Checkpoints and the interruption notice

This file covers saving a job's progress so a replacement machine goes on from it: `zenthros.Checkpointer` (one phase's checkpoints), `zenthros.CheckpointPolicy` (when to save), and `zenthros.preemption` (has this machine been told it is being taken back). Read it to write a long loop by hand. `zenthros.Task` and `zenthros.phase` use these pieces for you: see [pipelines.md](pipelines.md).

How it works. A checkpoint is one JSON object, the state, plus the files a library saves (a booster, torch weights), each its own object next to it with its SHA-256. It is written under the job's checkpoint prefix as `<prefix>/jobs/<job>/gen/<generation>/<phase>/<seq>`, its files as `<that>.files/<name>`, and then the controller acknowledges it. Only an acknowledged checkpoint is resumed from. With `resilient=True` (the default of `zenthros.run`), a lost machine is replaced and the replacement is handed the last acknowledged checkpoint of each phase: it never lists the bucket. Outside a job nothing is written, so the same code runs in a notebook.

## zenthros.Checkpointer

```python
zenthros.Checkpointer(name, store=None, agent=None, env=None)
```

One phase's checkpoints. When made, it loads the last acknowledged checkpoint of the phase, if there is one: from the job's environment, or from this process when it saved one since. Then `state` holds the saved state, `files` the saved files, and `completed` whether the phase was marked complete.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | required | The phase's name, the same on every machine. |
| `store` | blob store | `None` | Where to read and write. `None` picks the store of each URI. Leave it out. |
| `agent` | agent client | `None` | The client that asks the machine's agent to acknowledge. `None` makes one from `env`. Leave it out. |
| `env` | `zenthros.JobEnv` | `None` | The job's environment. `None` reads it now (`JobEnv.current()`). |

Properties:

| Property | Type | What it is |
|---|---|---|
| `active` | `bool` | `False` outside a job (no job id and no checkpoint prefix): `save()` writes nothing there. |

Attributes:

| Attribute | Type | What it is |
|---|---|---|
| `state` | JSON value | The last state saved or loaded, or `None`. |
| `files` | `dict` | The files the last checkpoint holds, by name. |
| `seq` | `int` | The number of the last checkpoint, 0 before any. |
| `completed` | `bool` | Whether the last checkpoint marked the phase complete. |
| `phase` | `str` | The phase's name. |

**Returns:** a `Checkpointer`.

**Raises:** `zenthros.errors.CheckpointError` (code `CHECKPOINT_REJECTED`) when the checkpoint to resume from does not match its SHA-256.

```python
import zenthros

def train(rounds):
    ckpt = zenthros.Checkpointer("train")
    policy = zenthros.CheckpointPolicy(every=10)
    start = (ckpt.state or {}).get("round", -1) + 1
    for r in range(start, rounds):
        ...  # one round of work
        if policy.due(ckpt, r):
            ckpt.save({"round": r}, files={"model.json": b"{}"})
    ckpt.complete({"round": rounds - 1})
    return rounds

call = zenthros.run(train, 100, on="caja")
print(call.result())
```

### Checkpointer.notice

```python
Checkpointer.notice()
```

Says whether this machine's interruption notice has just appeared. It is `True` once, the first time it is asked after the notice; then `False` again. Save then: the replacement is already being placed and resumes from the last acknowledged checkpoint. Ask on every step.

No parameters.

**Returns:** `bool`.

**Raises:** nothing.

```python
import zenthros

ckpt = zenthros.Checkpointer("train")
for step in range(1000):
    ...  # one step of work
    if ckpt.notice():
        ckpt.save({"step": step})
```

### Checkpointer.file

```python
Checkpointer.file(name)
```

The bytes of a file the loaded checkpoint holds, checked against the SHA-256 it was saved with.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | required | The file's name, as it was saved. |

**Returns:** `bytes`.

**Raises:** `KeyError` when the checkpoint holds no file of that name. `zenthros.errors.CheckpointError` (code `CHECKPOINT_REJECTED`) when its bytes do not match. `FileNotFoundError` when the object is gone from the store.

```python
import zenthros

ckpt = zenthros.Checkpointer("train")
if "model.json" in ckpt.files:
    model = ckpt.file("model.json")
```

### Checkpointer.file_path

```python
Checkpointer.file_path(name, path=None)
```

Downloads a file the loaded checkpoint holds to a local path, and checks it against its SHA-256. Use it for files a library loads from a path.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | required | The file's name, as it was saved. |
| `path` | `str` | `None` | Where to put it. `None` means a new temporary folder, with the file under its own name. |

**Returns:** the local path, as `str`.

**Raises:** `KeyError` when there is no file of that name. `zenthros.errors.CheckpointError` (code `CHECKPOINT_REJECTED`) when its bytes do not match; the file is removed then.

```python
import zenthros

ckpt = zenthros.Checkpointer("train")
if "weights.pt" in ckpt.files:
    local = ckpt.file_path("weights.pt")
```

### Checkpointer.save

```python
Checkpointer.save(state, complete=False, files=None)
```

Writes the files, then the state, and waits for the controller to acknowledge it: a checkpoint never names a file that is not there yet. A file saved before stays in later checkpoints until one saves that name again. Outside a job it writes nothing: it only keeps `state`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `state` | JSON value | required | The state to resume from. A model goes in `files`. |
| `complete` | `bool` | `False` | `True` marks the phase complete. |
| `files` | `dict` | `None` | `{name: bytes or a local path}` to save next to it. A name is one plain file name. |

**Returns:** a `dict` with `uri`, `seq`, `sha256` and `receipt` (the controller's answer). Outside a job `uri` and `sha256` are `""` and `receipt` is `None`.

**Raises:** `zenthros.errors.ValidationError` when a file name is empty, has a `/`, or is `.` or `..`. `zenthros.errors.CheckpointError` (code `REJECTED`) when the job id or the checkpoint prefix is missing, or the controller refuses it. `zenthros.errors.CheckpointFrozen` when it was not acknowledged: the replacement already took its checkpoint, or this run's lease is over. Stop working then: what follows would be lost. `zenthros.errors.AgentUnanswered` when the agent did not answer for 45 s. A state that is not JSON raises `TypeError`.

```python
import zenthros

ckpt = zenthros.Checkpointer("train")
answer = ckpt.save({"epoch": 3}, files={"weights.bin": b"\x00\x01"})
print(answer["seq"])
```

### Checkpointer.complete

```python
Checkpointer.complete(state=None, files=None)
```

Saves a last checkpoint that marks the phase complete. It is `save(state, complete=True, files=files)`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `state` | JSON value | `None` | The final state. `None` means the last state saved, or `{"complete": True}` when there is none. |
| `files` | `dict` | `None` | Files to save with it, as in `save()`. |

**Returns:** as `save()`.

**Raises:** as `save()`.

```python
import zenthros

ckpt = zenthros.Checkpointer("score")
ckpt.complete({"auc": 0.91})
```

## zenthros.CheckpointPolicy

```python
zenthros.CheckpointPolicy(every=None, on_notice=True)
```

When to save: every `every` steps, and once when the interruption notice appears. `due()` asks for the notice on every step, so it is taken on the step it appears.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `every` | `int` | `None` | Save after every N steps. `None` (or 0) never saves on a cadence. A value below 1 counts as 1. |
| `on_notice` | `bool` | `True` | Save when the interruption notice appears. |

**Returns:** a `CheckpointPolicy`, with the attributes `every` and `on_notice`.

**Raises:** `ValueError` or `TypeError` when `every` is not a number.

```python
import zenthros

class Features(zenthros.Task):
    policy = zenthros.CheckpointPolicy(every=10)

    def step(self, i):
        print(i)
```

### CheckpointPolicy.due

```python
CheckpointPolicy.due(ckpt, step, total=None)
```

Why step `step` should be saved, in this order: `"notice"` when `on_notice` and `ckpt.notice()` is `True`; `"last"` when `total` is given and this is the last step; `"every"` when the step count is a multiple of `every`; else `None`. Because `ckpt.notice()` is `True` only once, `"notice"` comes once.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `ckpt` | `zenthros.Checkpointer` | required | The phase's checkpointer, asked for the notice. |
| `step` | `int` | required | The step just done, 0-based. |
| `total` | `int` | `None` | How many steps there are, to answer `"last"`. |

**Returns:** `"notice"`, `"last"`, `"every"` or `None`.

**Raises:** nothing of its own.

```python
import zenthros

ckpt = zenthros.Checkpointer("train")
policy = zenthros.CheckpointPolicy(every=5)
for step in range(20):
    ...  # one step of work
    if policy.due(ckpt, step, 20):
        ckpt.save({"step": step})
```

## zenthros.preemption

Whether this machine has been told it is being taken back (a Spot reclaim, a host going away). The machine's agent writes the provider's notice as JSON (`source`, `action`, `deadline`, `notedAt`) to the file named by `$ZENTHROS_PREEMPT_FILE` before anything else happens; this module only reads that file. After a notice, the checkpoint acknowledged before the replacement starts is the one it resumes from. Use it as `zenthros.preemption.requested()`, or `from zenthros import preemption`: `import zenthros.preemption` does not work, because the module lives in `zenthros.core`.

### zenthros.preemption.notice

```python
zenthros.preemption.notice(env=None)
```

The notice as a dict, or `None` while there is none. Outside a job (no notice file named) it is always `None`. A notice file that is not a JSON object gives `{}`, which still means a notice.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `env` | `zenthros.JobEnv` | `None` | The job's environment. `None` reads it now. |

**Returns:** a `dict` or `None`.

**Raises:** nothing: a file that cannot be read counts as no notice.

```python
import zenthros

seen = zenthros.preemption.notice()
if seen is not None:
    print("taken back at", seen.get("deadline"))
```

### zenthros.preemption.requested

```python
zenthros.preemption.requested(env=None)
```

`True` once the machine has an interruption notice, and on every later call. Unlike `Checkpointer.notice()`, it does not turn `False` after the first time.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `env` | `zenthros.JobEnv` | `None` | The job's environment. `None` reads it now. |

**Returns:** `bool`.

**Raises:** nothing.

```python
import zenthros

for batch in range(1000):
    if zenthros.preemption.requested():
        break
    ...  # one batch of work
```
