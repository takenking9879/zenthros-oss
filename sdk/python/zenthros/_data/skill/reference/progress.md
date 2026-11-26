# Progress, reports and logs

This file covers what a running job tells you while it works and after: reports of its progress (`zenthros.Reporter`, `zenthros.report`, `zenthros.progress`), a call's whole log kept in a bucket (`zenthros.Logs`), and a copy of everything this process shows (`zenthros.transcript`). Read it to publish numbers from a job, or to keep logs. Reading reports and logs back from a call (`call.reports()`, `call.logs()`) is in [results.md](results.md).

A report is one JSON object, the payload. Each publish sends the whole payload as it is now, never a patch: the control plane keeps the last one and nothing merges. A payload is at most 256 KiB as JSON. In a `zenthros.map`, each task publishes as `task-<i>`.

## zenthros.Reporter

```python
zenthros.Reporter(producer_id=None, strict=True, agent=None, env=None)
```

Builds a payload a key at a time and publishes it to the job's agent. A strict reporter raises outside a job: a report you meant to send is not dropped silently. A lenient one (`strict=False`) drops the snapshot outside a job and logs it at debug level, so the same code runs in a notebook.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `producer_id` | `str` | `None` | Who publishes. `None` means the job's `ZENTHROS_PRODUCER_ID`, or this host's name. |
| `strict` | `bool` | `True` | `True` raises outside a job; `False` drops the report there. |
| `agent` | agent client | `None` | The client that sends to the machine's agent. `None` makes one from `env`. Leave it out. |
| `env` | `zenthros.JobEnv` | `None` | The job's environment. `None` reads it at publish time (`JobEnv.current()`). |

**Returns:** a `Reporter`.

**Raises:** nothing when built.

```python
import zenthros

reporter = zenthros.Reporter(strict=False)
for epoch in range(3):
    reporter.set("epoch", epoch).set("loss", 1.0 / (epoch + 1)).publish()
reporter.publish(final=True)
```

### Reporter.set

```python
Reporter.set(key, value)
```

Sets one key of the payload. Nothing is sent until `publish()`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `key` | `str` | required | The payload's key. |
| `value` | JSON value | required | Its value. It must be JSON when published. |

**Returns:** the same `Reporter`, so calls chain.

**Raises:** nothing.

```python
import zenthros

reporter = zenthros.Reporter(strict=False)
reporter.set("rows", 1200)
```

### Reporter.update

```python
Reporter.update(payload)
```

Sets every key of `payload` at once, over what is there.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `payload` | `dict` | required | The keys and values to set. |

**Returns:** the same `Reporter`.

**Raises:** `zenthros.errors.InvalidSnapshot` when `payload` is not a dict.

```python
import zenthros

reporter = zenthros.Reporter(strict=False)
reporter.update({"epoch": 2, "auc": 0.91})
```

### Reporter.add_artifact

```python
Reporter.add_artifact(uri, name="")
```

Adds a reference to stored bytes (a model, a chart) to the next snapshots. Only the reference travels, not the bytes.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `uri` | `str` | required | Where the bytes are: `s3://my-bucket/zenthros/model.bin`. |
| `name` | `str` | `""` | A name to show for it. |

**Returns:** the same `Reporter`.

**Raises:** nothing.

```python
import zenthros

reporter = zenthros.Reporter(strict=False)
reporter.add_artifact("s3://my-bucket/zenthros/model.bin", name="model")
```

### Reporter.publish

```python
Reporter.publish(final=False)
```

Sends the whole payload, and the artifacts added, as one snapshot to the machine's agent. The agent is asked again for up to 45 s while it restarts or answers that it is busy.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `final` | `bool` | `False` | `True` marks it as the last report of this producer. |

**Returns:** the agent's answer as a `dict`, or `None` for a lenient reporter outside a job.

**Raises:** `zenthros.errors.PayloadTooLarge` when the payload is over 256 KiB as JSON. `zenthros.errors.ReportError` for a strict reporter outside a job (`ZENTHROS_REPORT_ENDPOINT is not set`), or when the controller refuses it. `zenthros.errors.AgentUnanswered` when the agent did not answer for 45 s. A value that is not JSON raises `TypeError`.

```python
import zenthros

reporter = zenthros.Reporter(strict=False)
reporter.set("done", True).publish(final=True)
```

## zenthros.report

```python
zenthros.report(payload, final=False, producer_id=None, strict=True)
```

Publishes one complete payload in one call: `Reporter(producer_id=..., strict=...).update(payload).publish(final=...)`. It is strict by default: outside a job it raises.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `payload` | `dict` | required | The whole payload. |
| `final` | `bool` | `False` | `True` marks it as the last report. |
| `producer_id` | `str` | `None` | Who publishes, as in `Reporter`. |
| `strict` | `bool` | `True` | `False` drops it outside a job instead of raising. |

**Returns:** the agent's answer as a `dict`, or `None` when not strict and outside a job.

**Raises:** as `Reporter.update` and `Reporter.publish`.

```python
import zenthros

def evaluate():
    auc = 0.91
    zenthros.report({"auc": auc, "rows": 1200}, final=True)
    return auc

call = zenthros.run(evaluate, on="my-box")
print(call.result())
```

## zenthros.progress

```python
zenthros.progress(payload, final=False, producer_id=None)
```

Publishes `payload` in a job, and drops it outside one. It is `zenthros.report(..., strict=False)`: use it in loops, so the same code runs here and on a machine.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `payload` | `dict` | required | The whole payload as it is now. |
| `final` | `bool` | `False` | `True` marks it as the last report. |
| `producer_id` | `str` | `None` | Who publishes, as in `Reporter`. |

**Returns:** the agent's answer as a `dict` in a job, `None` outside one.

**Raises:** `zenthros.errors.InvalidSnapshot` when `payload` is not a dict; `zenthros.errors.PayloadTooLarge` over 256 KiB; in a job, `zenthros.errors.ReportError` when refused and `zenthros.errors.AgentUnanswered` when the agent did not answer for 45 s.

```python
import zenthros

def train(epochs):
    for epoch in range(epochs):
        loss = 1.0 / (epoch + 1)
        zenthros.progress({"epoch": epoch, "loss": loss})
    return loss

call = zenthros.run(train, 10, on="my-box")
print(call.result())
```

## zenthros.Logs

```python
zenthros.Logs(destination="", endpoint="", region="", ca="", credentials=None, max_bytes=None)
```

Where a call's whole log goes: `run(..., logs=zenthros.Logs("s3://my-bucket/zenthros/logs"))`. The machine's agent saves it as the job writes it, from the first line. The job never waits for the bucket: a bucket that refuses shows in the job's status as the reason `LOGS_NOT_SAVED`, never as a failure. `call.logs(to="x.log")` reads it back whole, even after the machine is gone. `logs=True`, or `Logs()` with no destination, is your store's `<store>/logs`, written with the credentials your calls already get for it. All parameters after `destination` are keyword-only.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `destination` | `str` | `""` | `s3://bucket/prefix` or `gs://bucket/prefix`. `""` means `<store>/logs`. |
| `endpoint` | `str` | `""` | An `https://` endpoint, for an S3-compatible store outside AWS. |
| `region` | `str` | `""` | The bucket's region. |
| `ca` | `str` | `""` | A PEM certificate, or the path of a file with one, to trust that endpoint. A path is read when the `Logs` is made. |
| `credentials` | `dict` | `None` | `{"AWS_ACCESS_KEY_ID": zenthros.Secret..., "AWS_SECRET_ACCESS_KEY": ...}` (and `AWS_SESSION_TOKEN`), or `GOOGLE_STORE_KEY` for `gs://`. Only the agent opens them, never the job. Without them, a destination of your own is written with the machine's own role (a template's identity: the destination must let it in, as `zenthros store` checks for the bucket). |
| `max_bytes` | `int` | `None` | Cut a longer log there; at least 1 MiB. `None` keeps the agent's limit (1 GiB). |

**Returns:** a `Logs`, to pass as `logs=` to `zenthros.run` (see [run.md](run.md)).

**Raises:** `zenthros.errors.ValidationError` when `destination` is not `s3://bucket/prefix` or `gs://bucket/prefix`, `endpoint` is not `https://`, a credential name is not one of those above, or `max_bytes` is not a whole number of at least 1 MiB. `OSError` when `ca` names a file that cannot be read. When the call is made: `zenthros.errors.ValidationError` for `logs=True` with no store (`zenthros store s3://my-bucket/zenthros`), or when your store is reached over plain http.

```python
import zenthros

def train():
    print("epoch 1")

logs = zenthros.Logs(
    "s3://my-bucket/zenthros/logs",
    region="eu-west-1",
    credentials={"AWS_ACCESS_KEY_ID": zenthros.Secret.workspace("logs-id"),
                 "AWS_SECRET_ACCESS_KEY": zenthros.Secret.workspace("logs-key")},
)
call = zenthros.run(train, on="my-vm", logs=logs)
call.result()
call.logs(to="train.log")
```

## zenthros.transcript

```python
zenthros.transcript(path)
```

A `with` block: everything this process shows while it runs is also appended to `path`. That is its own output (stdout and stderr) and its calls' logs as they arrive, in the order they come, each line whole. The file is opened in append mode, as UTF-8.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `path` | `str` | required | The file to append to. |

**Returns:** a context manager; `as` gives `path`.

**Raises:** `OSError` when the file cannot be opened.

```python
import zenthros

def train(cfg):
    print("training with", cfg)
    return cfg

with zenthros.transcript("session.log"):
    call = zenthros.run(train, {"depth": 4}, on="my-box")
    print(call.result())
```
