# Pipelines, tasks and fan-out

This file covers work in several pieces: steps in a line (`zenthros.Pipeline`), a job as a class that survives losing its machine (`zenthros.Task`, `zenthros.phase`), one function over many items (`zenthros.map`, `zenthros.grid`), a running job that starts child jobs (`zenthros.spawn`, `zenthros.children`), and work that runs every day or every month ([Every day, every month](#every-day-every-month)). Read it when one call is not enough. How one call runs is in [run.md](run.md); what a call gives back is in [results.md](results.md); checkpoints themselves are in [checkpoints.md](checkpoints.md).

## zenthros.Pipeline

```python
zenthros.Pipeline(steps, name="pipeline")
```

Steps in a line: what one step returns is the first argument of the next. A step is a plain function or a `@zenthros.cloud` function; a cloud function brings its options (`gpu=`, `env=`...) to its step. Each step is one ordinary call (`zenthros.run`), so each place's own rules hold. Without `cache=` the jobs are chained and `run()` returns at once; with `cache=` each step is waited for, because the next step's cache key is made of what the step before gave.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `steps` | `list` of `(name, function)` | required | The steps in order. Each name once. |
| `name` | `str` | `"pipeline"` | The pipeline's name. Lowercased, any other character turned into `-`, at most 40 characters. It names the jobs (`<name>-<step>-<run id>`) and the record of its runs. |

**Returns:** a `Pipeline`.

**Raises:** `zenthros.errors.ValidationError` when a step is not a function, when two steps have the same name, or when there is no step.

Each run is kept on record in `.zenthros/runs/pipelines/<name>/` at the project's root, for `resume()`. Its jobs carry the labels `zenthros.ai/pipeline` and `zenthros.ai/step`.

```python
import zenthros

@zenthros.cloud
def prep(raw):
    ...
    return "s3://my-bucket/zenthros/clean.parquet"

@zenthros.cloud(gpu=1)
def train(data, depth=4):
    ...
    return {"auc": 0.91}

pipe = zenthros.Pipeline([("prep", prep), ("train", train)], name="churn")
print(pipe.plan("s3://my-bucket/zenthros/raw.parquet", on="my-box", cache=True))
run = pipe.run("s3://my-bucket/zenthros/raw.parquet", on="my-box", cache=True, params={"train": {"depth": 8}})
print(run.status())     # prep: reuse · train: run: depth changed
print(run.result())     # what train returned
```

### Pipeline.run

```python
Pipeline.run(*args, on=None, cache=False, params=None, start_at=None, upstream=None, **options)
```

Runs the steps. `args` go to the first step (or to the `start_at` step). Before anything is made, it checks that the first step's place can reach every path or URI in its arguments. A step that fails, or that its place refuses, is recorded, not raised: the steps after it are skipped, and `result()` raises its error. Without `cache=`, when a step runs on a different place from the step before, `run()` waits here for that step's value and passes it on.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `args` | any | none | The first step's arguments. |
| `on` | place, or `dict` | `None` | Where the steps run: one place for all (a machine, its name, a template), or `{"step": place}`. A step left out of the dict runs where a call runs by default. |
| `cache` | `bool` or `"strict"` | `False` | Reuse each step's earlier result when its code, arguments, data and environment are the same. `"strict"` also reads every large file again. With it, each step is waited for. |
| `params` | `dict` | `None` | Keyword arguments per step: `{"train": {"depth": 8}}`. |
| `start_at` | `str` | `None` | The step to start from. The steps before it are skipped. |
| `upstream` | `dict` | `None` | With `start_at=`: what the step right before it left, as `{"prep": value}`. The value is a path, a URI, a `zenthros.get(...)` or a `zenthros.cache.find(...)`; a call is waited for and its value used. |
| `options` | keyword arguments | none | `zenthros.run`'s options (`upload=`, `secrets=`, `timeout=`...). They override a cloud function's own. The pipeline sets `name=` and `labels=` itself. A keyword that is not one of `run()`'s options is refused (`ValidationError`): a step's own keyword arguments go in `params=`. |

**Returns:** a `PipelineRun`. `run.steps` lists each step (`name`, `action`, `why`, `job`, `call`, `error`); `run["train"]` is one of them. `action` is `"run"`, `"reuse"`, `"resume"` or `"skip"`. `run.status()` says each step in one line; `run.result(timeout=None)` waits and returns the last step's value; `run.cancel()` cancels every job; `run.to_dict()` gives it as plain data.

**Raises:** `zenthros.errors.ValidationError` when an option is not one of `zenthros.run`'s; `params=` or a dict `on=` names a step the pipeline does not have; `upstream=` without `start_at=`; `start_at=` with both arguments and `upstream=`; `upstream=` naming a step that is not before `start_at`, or missing the step right before it; or a path the first step's place cannot reach (pass `upload=True` to send it). `zenthros.errors.ValidationError` also when `start_at` is not a step, and (a contract error) when `upstream=` does not fit the start step's `inputs=`.

```python
import zenthros

def prep(raw):
    return raw

def train(data, depth=4):
    return {"depth": depth}

pipe = zenthros.Pipeline([("prep", prep), ("train", train)], name="churn")
earlier = zenthros.get("churn-prep-3f9a1c")
run = pipe.run(on={"prep": "my-box", "train": "my-vm"}, start_at="train", upstream={"prep": earlier})
print(run.result())
```

### Pipeline.resume

```python
Pipeline.resume(*args, on=None, params=None, **options)
```

Runs the last run of this pipeline again, from where it stopped. Arguments, `on=`, `params=` and options you leave out are the last run's (only the ones that could be kept as JSON were kept). A run started with `start_at=` resumes from that step, and with `upstream=` gives it the same value again: a path or URI as it was, a `zenthros.get(...)` or `zenthros.cache.find(...)` by its job's name. If the last run used `cache=`, it runs again with it and every step with a good entry is reused. Without `cache=`, the steps whose job ended well are reused by their job, and it runs on from the first one that did not. A step cut short goes on from its last confirmed checkpoint, in a new job, when its code, inputs and environment are the same; if anything differs it starts over and `why` says why.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `args` | any | the last run's | The first step's arguments. |
| `on` | place, or `dict` | the last run's | As in `run()`. |
| `params` | `dict` | the last run's | As in `run()`. |
| `options` | keyword arguments | the last run's | `zenthros.run`'s options. Give again what is not JSON (a `zenthros.Secret` in `secrets=`, a `Machine` in `on=`). |

**Returns:** a `PipelineRun`. When every step of the last run ended well, nothing runs and every step says `reuse`.

**Raises:** `zenthros.errors.NotFound` when this pipeline has no run on record in this project. `zenthros.errors.ValidationError` when the last run's `upstream=` held a value its record cannot keep (not a path, a URI, a call or JSON): call `run(start_at=..., upstream=...)` again. Whatever `run()` raises.

```python
import zenthros

def prep(raw):
    return raw

def train(data):
    return len(data)

pipe = zenthros.Pipeline([("prep", prep), ("train", train)], name="churn")
print(pipe.plan(resume=True))
again = pipe.resume()
print(again.status())
```

### Pipeline.plan

```python
Pipeline.plan(*args, on=None, cache=False, params=None, start_at=None, upstream=None, probe=False, resume=False, **options)
```

Says what `run()` would do with the same arguments, step by step. It creates nothing and moves no data. For each step it answers four separate questions: is its input there, has it the shape asked (`inputs=` and `outputs=` contracts), are they the bytes expected, and did a job of yours make it. It also lists the data that would move and what cannot be checked until it runs.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `args` | any | none | As in `run()`. |
| `on` | place, or `dict` | `None` | As in `run()`. |
| `cache` | `bool` or `"strict"` | `False` | As in `run()`. With it, a step with a good entry shows as `reuse`. |
| `params` | `dict` | `None` | As in `run()`. |
| `start_at` | `str` | `None` | As in `run()`. |
| `upstream` | `dict` | `None` | As in `run()`. The value is checked against the next step's `inputs=` here. |
| `probe` | `bool` | `False` | Ask a place that is already up (`Available`) to count the objects at the URIs it would read. A place that would have to start is never started for this. |
| `resume` | `bool` | `False` | Plan what `resume()` would do instead. |
| `options` | keyword arguments | none | `zenthros.run`'s options. With `upload=True`, a path the place cannot reach shows as data to send, not as a problem. |

**Returns:** a `PipelinePlan`. `plan.ok` is `True` when no step has a problem; `plan.problems` lists them as `"step: problem"`; `plan.steps` has one row per step (`step`, `action`, `reason`, `available`, `compatible`, `integrity`, `origin`, `transfers`, `unchecked`, `problems`, `target`, `env`, `image`, `resources`, `key`); `print(plan)` shows it as a table; `plan.to_dict()` as plain data.

**Raises:** `zenthros.errors.ValidationError` as `run()` does for its arguments. `zenthros.errors.NotFound` with `resume=True` when there is no run on record.

```python
import zenthros

def prep(raw):
    return raw

def train(data):
    return len(data)

pipe = zenthros.Pipeline([("prep", prep), ("train", train)], name="churn")
plan = pipe.plan("data/raw.parquet", on="my-vm", upload=True)
if not plan.ok:
    print(plan.problems)
print(plan)
```

## zenthros.Task

```python
zenthros.Task()
```

A job as a class that survives losing its machine. You subclass it and write `step(i)`; the SDK's `run()` resumes after the last saved step, saves as the class's `policy` says (every N steps, and at once on the interruption notice), and at the end marks the phase complete, saving `result()` with it. A completed phase returns its result without running `setup()` again. Run it here with `Features().run()`, or on a machine with `zenthros.run(Features(), on=...)`: the same code. Outside a job nothing is written. See [checkpoints.md](checkpoints.md) for where checkpoints go.

It takes no constructor parameters of its own. Set these class attributes:

| Class attribute | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | `None` | The checkpoint phase's name. `None` means the class name, which is stable across machines. |
| `policy` | `zenthros.CheckpointPolicy` | `CheckpointPolicy()` | When to save. The default saves only on the interruption notice and at the end: set `every=` to save on a cadence. |
| `stop_on_notice` | `bool` | `False` | `True` stops right after the step saved at the notice (raises `zenthros.errors.StoppedOnNotice`), so the replacement starts sooner. |

Inside `restore()` and the steps, `self.checkpointer` is the phase's `zenthros.Checkpointer`.

```python
import zenthros

class Features(zenthros.Task):
    policy = zenthros.CheckpointPolicy(every=10)

    def setup(self):
        self.done = []

    def steps(self):
        return 100

    def step(self, i):
        self.done.append(i * i)

    def state(self):
        return {"done": self.done}

    def restore(self, state):
        self.done = state["done"]

    def result(self):
        return len(self.done)

print(Features().run())                         # here
call = zenthros.run(Features(), on="my-box")      # on a machine, the same code
print(call.result())
```

### Task.setup

```python
Task.setup()
```

Opens what the steps need: sessions, clients, config. It runs on the machine, before `restore()`, so what it opens is never pickled. The default does nothing.

No parameters.

**Returns:** nothing.

**Raises:** what your code raises; it ends the run.

```python
import zenthros

class Load(zenthros.Task):
    def setup(self):
        self.rows = []
```

### Task.steps

```python
Task.steps()
```

Says how many steps there are: an `int`, or a sequence whose length is used. The default is `1`.

No parameters.

**Returns:** an `int` or a sequence.

**Raises:** what your code raises.

```python
import zenthros

class Files(zenthros.Task):
    names = ["a.csv", "b.csv", "c.csv"]

    def steps(self):
        return self.names
```

### Task.step

```python
Task.step(i)
```

Does step `i`. It is called with the index, 0-based, even when `steps()` returns a sequence: index the sequence yourself. The default raises `NotImplementedError`, so you must write it.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `i` | `int` | required | The step's index, from 0 to `steps() - 1`. |

**Returns:** nothing (its value is not used).

**Raises:** `NotImplementedError` when not written. What your code raises ends the run.

```python
import zenthros

class Files(zenthros.Task):
    names = ["a.csv", "b.csv"]

    def steps(self):
        return self.names

    def step(self, i):
        print("reading", self.names[i])
```

### Task.state

```python
Task.state()
```

The state to save after a step: JSON-able data that `restore()` takes back. A model or other bytes go in `files()`. The default is `{}`.

No parameters.

**Returns:** a JSON-able value.

**Raises:** what your code raises. A value that is not JSON fails when it is saved.

```python
import zenthros

class Count(zenthros.Task):
    def setup(self):
        self.total = 0

    def state(self):
        return {"total": self.total}
```

### Task.files

```python
Task.files()
```

What a library saves on its own (a model, weights), as `{name: bytes or a local path}`. They are saved next to `state()` at each save, as `Checkpointer.save`'s `files=` ([checkpoints.md](checkpoints.md#checkpointersave)). The default is `{}`.

No parameters.

**Returns:** a `dict` of plain file names (no `/`) to `bytes` or a local path.

**Raises:** `zenthros.errors.ValidationError` when a name is empty, has a `/`, or is `.` or `..` (at save time).

```python
import zenthros

class Fit(zenthros.Task):
    def setup(self):
        self.weights = b""

    def files(self):
        return {"weights.bin": self.weights}
```

### Task.restore

```python
Task.restore(state)
```

Continues from `state`, the last one `state()` returned and was saved. It runs after `setup()`, only when there is a saved step. Read saved files with `self.checkpointer.file(name)` (bytes) or `self.checkpointer.file_path(name)` (a local path). The default does nothing.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `state` | JSON value | required | What `state()` returned at the last save. |

**Returns:** nothing.

**Raises:** what your code raises. `zenthros.errors.CheckpointError` from `checkpointer.file()` when a file's bytes do not match their SHA-256.

```python
import zenthros

class Fit(zenthros.Task):
    def restore(self, state):
        self.epoch = state["epoch"]
        self.weights = self.checkpointer.file("weights.bin")
```

### Task.result

```python
Task.result()
```

What `run()` returns, saved with the completed phase. It must be JSON-able: a completed phase returns it from its checkpoint without running again. The default is `None`.

No parameters.

**Returns:** a JSON-able value.

**Raises:** what your code raises.

```python
import zenthros

class Count(zenthros.Task):
    def result(self):
        return {"rows": 1200}
```

## zenthros.phase

```python
zenthros.phase(name, resume_done=None)
```

The function form of a resumable phase: a decorator. The function is called as `fn(ckpt, *args, **kwargs)` with the phase's `zenthros.Checkpointer`, and you call it without `ckpt`. A phase whose checkpoint is complete returns that checkpoint's state at once, without running. When the function ends without calling `ckpt.complete(...)` in a job, the phase saves as complete its last saved state, or its return value if it saved none.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | required | The phase's name, the same on every machine. |
| `resume_done` | callable | `None` | `resume_done(state)` says whether a complete checkpoint is really done. `False` runs the function again (for example, a checkpoint that only saved rounds and still has to score). |

**Returns:** a decorator. The decorated function returns its own value when it runs, or the saved state when the phase was already complete. Call `ckpt.complete(value)` yourself so both are the same value.

**Raises:** what the function raises. `zenthros.errors.CheckpointError` when a saved checkpoint does not match its SHA-256.

```python
import zenthros

@zenthros.phase("train")
def train(ckpt, rounds):
    policy = zenthros.CheckpointPolicy(every=5)
    start = (ckpt.state or {}).get("round", -1) + 1
    for r in range(start, rounds):
        ...  # one round of work
        if policy.due(ckpt, r):
            ckpt.save({"round": r})
    result = {"rounds": rounds}
    ckpt.complete(result)
    return result

print(train(20))
```

## zenthros.map

```python
zenthros.map(fn, items, on=None, **options)
```

Runs `fn(item)` for each item: one job with a task per item. On one machine the tasks run one after another (a machine runs one job at a time; a cluster runs several), and it says so once. Each item is the function's one argument, a dict too (for a dict as keyword arguments, use a cloud function's `f.map`, in [cloud.md](cloud.md)). It returns at once; `result()` gives the results in the order of the items. `on=` and the options are [zenthros.run](run.md#zenthrosrun)'s machine, environment and failure options, plus `name=`, `retries=`, `retry_on=` and `upload=`. Other options of `run()` (`cache=`, `params=`, `dry_run=`, `keep=`...) are refused.

Inside a Zenthros job, `zenthros.map` spawns the job's children instead (see `zenthros.spawn`). They run where the job runs, and the job's replacement maps onto the same children. Then `fn` is an importable module-level function, `"module:function"`, or a command as a list; items and results travel as JSON; and only `name=` and `env=` apply.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `fn` | function | required | What to run for each item. |
| `items` | iterable | required | The items, at least one. One job takes at most 256. |
| `on` | place | `None` | Where, as in `zenthros.run`. |
| `options` | keyword arguments | none | The options listed above. |

**Returns:** a `RemoteCall` (see [results.md](results.md)); its `result()` is the list of results, in order. Inside a job: a group of children, whose `result()` waits and gives the results in order.

**Raises:** `zenthros.errors.ValidationError` when there are no items, `fn` is not callable, an option is unknown, or there are more than 256 items; inside a job, when an option other than `name=` and `env=` is given, `fn` is not importable, or an item is not JSON (items travel to the children as JSON).

```python
import zenthros

def score(n):
    return n * n

calls = zenthros.map(score, [1, 2, 3], on="my-box", cpu=2)
print(calls.result())   # [1, 4, 9]
```

## zenthros.grid

```python
zenthros.grid(**values)
```

Every combination of some values, as a list of dicts, the first name changing slowest. Give it to a cloud function's `f.map(...)`, where each dict is the function's keyword arguments.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `**values` | lists | required | `name=[value, ...]` for each argument to vary. |

**Returns:** a `list` of `dict`. `zenthros.grid(a=[1, 2], b=["x"])` is `[{"a": 1, "b": "x"}, {"a": 2, "b": "x"}]`.

**Raises:** `zenthros.errors.ValidationError` when no values are given, or a value is not a list, tuple or range (a string is refused).

```python
import zenthros

@zenthros.cloud
def train(learning_rate, leaves):
    return {"lr": learning_rate, "leaves": leaves}

calls = train.map(zenthros.grid(learning_rate=[0.05, 0.1], leaves=[15, 31]), on="my-box")
print(calls.result())
```

## zenthros.spawn

```python
zenthros.spawn(name, count, cmd=None, same=False, env=None, wait=False, timeout=None, poll_seconds=5)
```

Inside a running job, creates `count` child jobs named `<job>-<name>-<i>`. Each child is a job of its own: it gets the job's image, compute, target and placement, and sees `TASK_INDEX` of `TASK_COUNT`, `ZENTHROS_PARENT_JOB_ID` and its own output prefix `<output>/children/<name>/<i>`. Results travel through object storage: a child writes `result.json` under its `ZENTHROS_DATA_OUTPUT`. Spawning the same name again (the job's replacement after a lost machine) creates nothing twice and returns the children that are there.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | required | The group's name. |
| `count` | `int` | required | How many children, at least 1. |
| `cmd` | `list` of `str` | `None` | The children's command (argv). Give `cmd` or `same=True`, not both. |
| `same` | `bool` | `False` | `True` runs this job's own command in each child. |
| `env` | `dict` | `None` | Extra environment variables for the children. |
| `wait` | `bool` | `False` | Wait until every child has ended before returning. |
| `timeout` | seconds | `None` | With `wait=True`, how long to wait. `None` waits for ever. |
| `poll_seconds` | seconds | `5` | With `wait=True`, how often to ask. |

**Returns:** a group: `group.children` (each with `group`, `index`, `name`, `phase`, `message`, `done`), `group.wait(timeout=None, poll_seconds=5)`, `group.results(blob_store=None, strict=False)` (each child's `result.json` as data, in order, `None` where a child wrote none), `group.result(timeout=None, poll_seconds=5)` (wait, then results) and `group.refresh()`.

**Raises:** `zenthros.errors.ValidationError` when both or neither of `cmd` and `same` are given, or `count` is below 1. `zenthros.errors.SpawnRefused` outside a job, or when the controller refuses (a quota, a lease that is not live, a child trying to spawn children of its own), in its words. Waiting raises `zenthros.fanout.ChildFailed` (one line per child that did not succeed) or `zenthros.errors.WaitTimeout`. `results()` raises `zenthros.errors.ValidationError` when the job has no output prefix, or with `strict=True` when a child wrote no result.

```python
import zenthros

group = zenthros.spawn("shards", 4, cmd=["python", "shard.py"])
group.wait(timeout=3600)
print(group.results())
```

## zenthros.children

```python
zenthros.children(name="")
```

Inside a running job, the children of group `name`, or of every group, as the controller last saw them.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | `""` | The group. `""` means every group of this job. |

**Returns:** a `list` of children, each with `group`, `index`, `name`, `phase`, `message` and `done` (`True` once it has ended).

**Raises:** `zenthros.errors.SpawnRefused` outside a job or when the controller refuses. `zenthros.errors.AgentUnanswered` when the machine's agent did not answer for 45 s.

```python
import zenthros

for child in zenthros.children("shards"):
    print(child.name, child.phase)
```

## Every day, every month

Zenthros starts nothing on a clock by itself: a job runs when a script sends it. To run one every day or every month, a scheduler starts the script: cron or a systemd timer on Linux and macOS, Task Scheduler on Windows, or a `schedule:` workflow in GitHub Actions. The computer must be on at that hour; GitHub's runners need none of yours.

With `cache=True`, a step whose code, arguments and data did not change is not run again: its kept result is used ([run.md](run.md)). So "train again when new data arrives" is the same script every day: a day with nothing new costs only the check. `deps=` names what a step reads besides its arguments, such as `{"sales": "2026-10-31"}`.

A crontab line (`crontab -e`), every day at 06:00:

```console
0 6 * * * cd /home/me/project && .venv/bin/python retrain.py >> retrain.log 2>&1
```

GitHub Actions, the first of each month at 06:00 UTC. The key from `zctl user add` goes in the repository's secrets as `ZENTHROS_API_KEY`; the SDK reads it as a whole login ([session.md](session.md)):

```yaml
on:
  schedule:
    - cron: "0 6 1 * *"
jobs:
  retrain:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - run: pip install -r requirements.txt   # zenthros among them, as you installed it
      - run: python retrain.py
        env:
          ZENTHROS_API_KEY: ${{ secrets.ZENTHROS_API_KEY }}
```

`zenthros jobs --today` then says how long each of the day's jobs took and what it cost, beside what the Workspace's machines spent.
