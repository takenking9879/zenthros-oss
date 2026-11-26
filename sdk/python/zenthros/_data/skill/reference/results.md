# Results and errors

This file covers what a call gives back: the `RemoteCall` that `zenthros.run`, `.submit()`, `.map()` and `zenthros.get` return, how to wait for it, read its log and reports, stop it, or run it again; `zenthros.gather` and `zenthros.last_failure`; and the errors, `RemoteError` (the code failed) and `JobFailed` (the platform did). Read it when you wait for a call, or when one failed.

## zenthros.RemoteCall

One call: its job, its result, its log and its reports. You do not build one: `zenthros.run()`, `f.submit()`, `f.map()` (one place), `zenthros.serve()` and `zenthros.get(name)` return it. It comes back at once; the job runs on its own, and `.result()` waits for it.

Leaving a `with` block cancels the job if it has not ended (a Service stops serving), and removes the machine made for it. Passed as another call's argument, it chains the calls ([run.md](run.md#chaining-calls)); a Service cannot be passed.

Properties:

| Name | What it is |
|---|---|
| `name` | The job's name, such as `zr-add-c91a4b`: what `zenthros.get`, `zenthros jobs` and `zenthros logs NAME` take. |
| `kind` | `"call"`, `"map"` or `"command"`. |
| `count` | A map's number of items; `0` for a call. |
| `where` | The place `on=` sent it to (the default one, when `on=` was not given). |
| `output` | Where its data ends up: the store URI that `$ZENTHROS_DATA_OUTPUT`, a folder on the machine, is saved to before the job ends (what a job writes there with `open()`, `savefig()` or a shell's `>` is at `f"{call.output}/<path>"`). A folder on this computer for `on="local"`. |
| `log_dir` | The folder of its local record (`.zenthros/runs/<name>`, see `zenthros.last_failure`), or `None` when records are off (`configure(log_dir="off")`, `ZENTHROS_LOG_DIR=off`) or the call came from `zenthros.get`. |
| `timings` | Epoch seconds: `submitted`; once the result is read, `remote_started`, `remote_finished` and `finished`; a Service's `ready` and `reachable`. |
| `machine` | What the runner said of its machine, once the result is read. |
| `attempts` | How many times the function ran (with `retries=`), once the result is read; else `None`. |
| `reschedules` | How many times the job moved to a replacement machine while `.result()` waited. |
| `function` | What it runs: the function's name or the command's words (calls this process started). |
| `service` | A Service's port names, in order; `None` for a call. |
| `made` | The machine made for this call, or `None`. |
| `cached` | `False`. A call answered by `cache=True` without running has `True`. |
| `why` | Why it ran with `cache=`, or why `rerun(resume=True)` started over; else `""`. |
| `cache_key` | Its cache key with `cache=`, else `""`. |

```python
import zenthros

def add(a, b):
    return a + b

call = zenthros.run(add, 2, 3, on="my-box")    # stderr: zenthros: add → zr-add-c91a4b on my-box
print(call.name, call.status())
print(call.result())                          # 5

with zenthros.run(["python", "long_job.py"], on="my-vm") as job:
    job.result(timeout=3600)
# leaving the block, by an error or a timeout too, cancels the job if it has not ended
```

### RemoteCall.result

```python
call.result(timeout=None, stream_logs=True, cancel_on_interrupt=True)
```

Waits for the job and returns its result: a function's value, a map's list (in the order of its items), a command's `0`. Only a job that ended Succeeded returns. While it waits it prints the job's log here, and a line each time the code reports with `zenthros.report`; on success it prints one line such as `✓ done in 9s on my-box · $0.0012`. The value is read only when its bytes match the SHA-256 the job reported. The outcome is kept: calling it again returns the same value, or raises the same error, without asking again.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `timeout` | number or `str` | `None` | How long to wait: seconds, or `"30m"`, `"2h"` as `run(timeout=)` takes them. `None`: until the job ends. Past it, the job keeps running. |
| `stream_logs` | `bool` | `True` | Print the log, the report lines and the closing line while it waits. |
| `cancel_on_interrupt` | `bool` | `True` | Ctrl-C while it waits cancels the job and removes the machine made for it. `False` leaves the job running and says how to come back to it (`zenthros.get("<name>")`). |

**Returns:** the result.

**Raises:**

- `zenthros.RemoteError`: the code failed. The function raised (with its traceback), the command exited non-zero (with the end of its log), the job was killed for memory, the result was over the no-bucket limit (`remote_type` `"ResultTooLarge"`), or its bytes were not the ones the job reported.
- `zenthros.JobFailed`: the platform failed it, with `.reason` (`IMAGE_PULL_FAILED`, `WORKER_LOST`, `DEADLINE_EXCEEDED`...).
- `zenthros.errors.JobCancelled`: the job was cancelled.
- `zenthros.errors.WaitTimeout`: `timeout` passed; the job still runs.
- `zenthros.errors.JobNotFound`: the job is gone (its `keep_for` ended, or it was deleted). Its result is not read without the job's digest.
- `zenthros.errors.TransientError`: the SHA-256s the job reported could not be read; the job and its result are kept, and `zenthros.get(name).result()` reads them later.
- `TypeError`: it is a Service, which has no result.
- `KeyboardInterrupt`: Ctrl-C, after the job was cancelled (unless `cancel_on_interrupt=False`).

```python
import zenthros

def slow(n):
    return n

call = zenthros.run(slow, 3, on="my-vm")
try:
    value = call.result(timeout=600, stream_logs=False)
except zenthros.errors.WaitTimeout:
    print(call.name, "still runs")
```

### RemoteCall.outcomes

```python
call.outcomes(timeout=None, stream_logs=True, cancel_on_interrupt=True)
```

Waits like `result()`, then gives each item's outcome, so one failed item of a map does not hide the others' values. A call is one item.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `timeout` | number | `None` | Seconds to wait, as in `result()`. |
| `stream_logs` | `bool` | `True` | As in `result()`. |
| `cancel_on_interrupt` | `bool` | `True` | As in `result()`. |

**Returns:** a list of `(True, value)` or `(False, error)`, in the order of the items.

**Raises:** what `result()` raises when the job has not ended (`zenthros.errors.WaitTimeout`, `KeyboardInterrupt`); a job that ended never raises here.

```python
import zenthros

def inverse(x):
    return 1 / x

calls = zenthros.map(inverse, [1, 0, 4], on="my-box")
for ok, value in calls.outcomes():
    print(ok, value)
```

### RemoteCall.status

```python
call.status()
```

The job's phase now, asked once.

No parameters.

**Returns:** a string: `"Queued"`, `"Pending"`, `"Running"`, `"Succeeded"`, `"Failed"` or `"Cancelled"`.

**Raises:** `zenthros.errors.JobNotFound` when the job is gone; `zenthros.errors.TransientError` when it cannot be asked.

### RemoteCall.logs

```python
call.logs(tail=None, follow=False, to=None)
```

The end of the job's log as text: its last 512 KiB from the start of a line, or its last `tail` lines; what it leaves out is said on a line. Once the machine is gone, a job with `logs=` is read from its bucket.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `tail` | `int` | `None` | Only the last lines, this many. |
| `follow` | `bool` | `False` | `True`: that, then each line as it is written until the job ends, to iterate. `close()` or a `with` block stops it. `result()` goes on from where it got. |
| `to` | path | `None` | Writes the whole log into this file and returns its path: with `logs=`, from its bucket and the machine; without, all the machine keeps (50 MiB). |

**Returns:** a string; with `follow=True` an iterable of lines; with `to=` the file's path.

**Raises:** `ValueError` when `to=` comes with `tail=` or `follow=`. The client's errors when there is no log to read (the machine and its log gone, and no bucket).

```python
import zenthros

call = zenthros.run(["pytest", "-q"], on="my-vm")
print(call.logs(tail=50))
with call.logs(follow=True) as lines:
    for line in lines:
        print(line, end="")
call.logs(to="pytest.log")
```

### RemoteCall.reports

```python
call.reports()
```

What the job's code published with `zenthros.report` ([progress.md](progress.md)), oldest first. Zenthros' own reports (each result's SHA-256) are not among them.

No parameters.

**Returns:** a list of reports, each with `producer_id`, `payload` (a dict), `sequence`, `produced_at`, `final` and `artifacts`.

**Raises:** the client's errors, such as `zenthros.errors.TransientError`.

### RemoteCall.info

```python
call.info()
```

The job as the controller holds it, and what the machine said.

No parameters.

**Returns:** a dict with `name`, `kind`, `output`, `phase`, `reason`, `message`, `restarts`, `reschedules`, `attempts`, `cleanup_pending` (cancelled, but its machine may still run it), `machine`, `timings`, and `endpoints` (a Service's URLs, else `None`).

**Raises:** `zenthros.errors.JobNotFound` when the job is gone.

### RemoteCall.cost

```python
call.cost()
```

What the call cost so far: its running time times the price the hub last saw for its machine's offer.

No parameters.

**Returns:** a `Cost` with `cost` (`None` where there is no price: your own machine, a cluster's pod, a connection with no hub), `currency`, `seconds`, `running`, `hourly_rate`, `instance_type`, `market`, `region`, `zone`, `backend`, `observed_at` and `basis` (what was counted, or why nothing was).

**Raises:** the client's errors when the hub cannot be asked.

### RemoteCall.overhead

```python
call.overhead()
```

Where the time went, once `result()` read a function's result.

No parameters.

**Returns:** `{"queue_and_start": s, "compute": s, "total": s}` in seconds, or `None` before the result is read (and for a command, which reports no timings).

**Raises:** nothing.

### RemoteCall.cancel

```python
call.cancel()
```

Stops the job; it stays, as Cancelled. The machine made for the call (a template's with `keep=False`) goes too: it bills until it is gone.

No parameters.

**Returns:** `None`.

**Raises:** the client's errors, such as `zenthros.errors.JobNotFound` or `zenthros.errors.TransientError`. The machine made for it is removed even then.

### RemoteCall.delete

```python
call.delete()
```

Deletes the job (its workload is cancelled first), and the machine made for it goes too. Its log and result can no longer be read through it.

No parameters.

**Returns:** `None`.

**Raises:** the client's errors, as `cancel()`.

### RemoteCall.release

```python
call.release()
```

Removes the machine made for this call, once. `result()`, `cancel()`, `delete()` and a `with` block already do it. A call with `logs=` keeps the machine up to 60 s more, until its whole log is in its bucket.

No parameters.

**Returns:** `None`.

**Raises:** what deleting the machine raises.

### RemoteCall.rerun

```python
call.rerun(on=None, resume=False, upload=False, name=None, **changes)
```

Runs this call again in a new job, with the code it ran and what it was asked. The call is read back through its job, checked by its digest.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `on` | place | `None` | Where the new job runs. `None`: where this one ran. |
| `resume` | `bool` | `False` | Go on from this job's last confirmed checkpoint, if it has ended and the new place runs the same image and the same store. If not, it starts over and `.why` says why. |
| `upload` | `bool` | `False` | As `zenthros.run`'s: a path only this computer has goes to the store first. |
| `name` | `str` | `None` | The new job's name. `None`: this one's with `-r<4 hex digits>`. |
| `**changes` | | | [zenthros.run](run.md#zenthrosrun)'s options to change. Secrets were sealed for the first machine: give them again with `secrets=`. |

**Returns:** the new `RemoteCall`.

**Raises:** `zenthros.errors.ValidationError` for a map or a Service (send them again), or when the job used secrets and `secrets=` is not given; `zenthros.errors.JobNotFound` when the job is gone; what `zenthros.run` raises.

```python
import zenthros

call = zenthros.get("zr-train-5d1e0a")
again = call.rerun(on="gpu-box", resume=True, gpu="A10G")
print(again.why)
```

### RemoteCall.url

```python
call.url(name=None, timeout=600, poll_seconds=2.0, health=None)
```

A Service's URL for one of its ports, once every port it declares accepts connections. Read it again after a restart: each run, and each replacement machine, publishes its own URL. See [services.md](services.md).

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | `None` | The port's name. `None`: the first one declared. |
| `timeout` | number | `600` | Seconds to wait. `None`: no limit. |
| `poll_seconds` | number | `2.0` | Seconds between two looks. |
| `health` | `str` | `None` | A path such as `"/health"`: also wait for a 2xx answer there, asked from here, through whatever is in front of the app. |

**Returns:** the URL, a string.

**Raises:** `TypeError` when the call is not a Service; `zenthros.errors.ValidationError` for an unknown port name or a `health` that does not start with `/`; `zenthros.errors.WaitTimeout` past `timeout`, with why there is no URL yet; `zenthros.JobFailed` or `zenthros.errors.JobCancelled` when the Service ended.

### RemoteCall.endpoints

```python
call.endpoints()
```

Where a Service answers now, read from its status.

No parameters.

**Returns:** a list of endpoints, each with `name`, `url`, `port`, `scope` (`Public`, `Private` or `Cluster`), `backend`, `attempt` and `ready_at`. Empty until every port it declares accepts connections, and after it stops.

**Raises:** `zenthros.errors.JobNotFound` when the job is gone.

### RemoteCall.replace

```python
call.replace(new, timeout=600, poll_seconds=2.0, health=None, keep_failed=False)
```

Swaps this Service for `new`, another `serve()` already started: waits until `new` answers (`new.url(health=)`), then cancels this one. Both run side by side meanwhile. A Service holds its machine alone, so `new` must run on another machine.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `new` | `RemoteCall` | required | The new Service's call. |
| `timeout` | number | `600` | Seconds to wait for `new` to answer. |
| `poll_seconds` | number | `2.0` | Seconds between two looks. |
| `health` | `str` | `None` | A path that must answer 2xx on `new`, as in `url()`. |
| `keep_failed` | `bool` | `False` | When `new` fails or does not answer in time, keep it running to look at it, instead of cancelling it. |

**Returns:** `new`.

**Raises:** `TypeError` when this call is not a Service; `zenthros.errors.ValidationError` when `new` is not another started Service, or runs on this one's machine; what `new.url()` raises (this one keeps serving).

## zenthros.get

```python
zenthros.get(name)
```

The call named `name`, from any process that reaches its job: its status, log and result after the process that started it is gone. The call is read through the job's spec, whose SHA-256 pins it: a call in the store with other bytes is never unpickled. A job is kept 7 days after it ends, unless `keep_for=` said otherwise.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | required | The job's name, as the call said it on stderr (`zr-add-c91a4b`), or a spread map's `name=`. |

**Returns:** a `RemoteCall` (a command's job is one of kind `"command"`); for the name of `f.map(..., on=[...], name=...)`, that map of several jobs.

**Raises:** `zenthros.errors.JobNotFound` when there is no such job (never made, or collected after its `keep_for`); `zenthros.errors.ValidationError` when the call the job runs is not in the store; `zenthros.RemoteError` when the stored call's SHA-256 is not the one the job's spec pins.

```python
import zenthros

call = zenthros.get("zr-add-c91a4b")
print(call.status())
print(call.result())
```

## zenthros.gather

```python
zenthros.gather(*calls, return_exceptions=False)
```

The results of several calls, in order: `gather(a, b)` or `gather([a, b])`. It waits for each one in turn with `.result()`. The first failure raises at once, and the calls after it are not waited for (they keep running).

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `*calls` | `RemoteCall`, or one list of them | | The calls to wait for. |
| `return_exceptions` | `bool` | `False` | `True`: a failed call's error goes in its place in the list instead of being raised. |

**Returns:** a list of results, in the order of `calls`.

**Raises:** the first error a call's `.result()` raises, unless `return_exceptions=True`.

```python
import zenthros

def square(x):
    return x * x

calls = [zenthros.run(square, n, on="my-box") for n in range(3)]
print(zenthros.gather(calls))                             # [0, 1, 4]
print(zenthros.gather(*calls, return_exceptions=True))
```

## zenthros.last_failure

```python
zenthros.last_failure(current=None)
```

The folder of the last call made from here that failed, was cancelled or was interrupted, from the local record (`.zenthros/runs/index.jsonl`). Its `error.txt` has the error's type, message and remote traceback; `logs.txt` the log as the call followed it; `call.json` what was asked; `reports.jsonl` the code's reports.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `current` | settings | `None` | The SDK's settings to find the records with. Leave it out: `None` is what `configure()` set (`log_dir=`, else `ZENTHROS_LOG_DIR`, else `./.zenthros/runs`). |

**Returns:** the folder's path, a string; `None` when no call failed, or records are off.

**Raises:** nothing.

```python
import os
import zenthros

folder = zenthros.last_failure()
if folder:
    print(open(os.path.join(folder, "error.txt")).read())
```

## zenthros.RemoteError

Raised by `.result()`, `.remote()` and `zenthros.get()` when the code failed on the machine; you do not build one. It is a `zenthros.ZenthrosError` and a `RuntimeError`.

- The function raised: the message is `<job>: <Type>: <message>`, then the remote traceback. `__cause__` is the function's own exception, rebuilt here when its class can be imported here.
- The command exited non-zero: the message ends with the last 30 lines of its log. `__cause__` is the job's `JobFailed`, and `reason` its code (`PROCESS_FAILED`).
- The job was killed for memory: that is the code's too. The message ends with the log, `__cause__` is the `JobFailed`, and `reason` its code: `OOM_KILLED` when the job's own unit or pod hit the limit; on a shared container (This computer, `on="local"`) `PROCESS_FAILED`, whose message says it "probably ran out of memory"; on a VM whose jobs run in images (containerd) `PROCESS_FAILED` with `exitCode=137` and no word on memory. Use less memory, or a machine with more.
- The result is not one the job vouched for (written over in the store), or the job succeeded without writing it.

Properties:

| Name | What it is |
|---|---|
| `remote_type` | The exception's class name on the machine (`"KeyError"`, `"ResultTooLarge"`), or `""`. |
| `remote_traceback` | The traceback the machine printed, or `""`. |
| `job` | The job's name. |
| `reason` | A code: the runner's (`"OUTPUT_CONTRACT"`, `"INPUT_CONTRACT"`), or the job's when it ended with no result (`"OOM_KILLED"`, `"PROCESS_FAILED"`), or `""`. |

```python
import zenthros

def parse(path):
    return open(path).read()

try:
    zenthros.run(parse, "/data/missing.txt", on="my-vm").result()
except zenthros.RemoteError as err:
    print(err.remote_type, err.job)
    print(err.remote_traceback)
    if err.reason == "OOM_KILLED" or "probably ran out of memory" in str(err):
        print("killed for memory")
```

## zenthros.JobFailed

Raised by `.result()` and `.url()` when the platform failed the job, not the code: the image, a quota, admission, the machine lost past its retries, the job past its `timeout`. You do not build one. It is a `zenthros.ZenthrosError` and a `RuntimeError`. `str(err)` is the job's status message, word for word. A failure of the job's own process is the code's and reaches you as a `RemoteError`: a command's non-zero exit or a kill for memory (`PROCESS_FAILED`, `OOM_KILLED`) has this `JobFailed` as its `__cause__`.

Properties:

| Name | What it is |
|---|---|
| `reason` | The first reason code, or `""`: `IMAGE_PULL_FAILED`, `WORKER_LOST`, `DEADLINE_EXCEEDED`, `QUOTA_EXCEEDED`, `PROVIDER_GONE`, `MACHINE_LIFETIME`...; `OOM_KILLED` or `PROCESS_FAILED` for a job killed for memory (see `zenthros.RemoteError` above). |
| `reasons` | Every reason, as dicts with `code` and `detail`. |
| `message` | The job's status message, as `str(err)`. |
| `status` | The job's last status (`phase`, `reason`, `message`, `exit_code`...), or `None`. |

```python
import zenthros

try:
    zenthros.run(["python", "train.py"], on="gpu-box", image="ghcr.io/me/train:1").result()
except zenthros.JobFailed as err:
    print(err.reason, err)
    if err.reason == "IMAGE_PULL_FAILED":
        print("publish the image where gpu-box can read it")
```

## zenthros.ZenthrosError

What the SDK raises; you do not build one. Every error of the SDK is a `ZenthrosError` and also the built-in exception it is, so `except zenthros.ZenthrosError` catches what Zenthros said, and code that catches the built-in ones keeps working. Its kinds are in `zenthros.errors`:

| Error | Also a | When |
|---|---|---|
| `zenthros.RemoteError` | `RuntimeError` | The code failed on the machine. |
| `zenthros.JobFailed` | `RuntimeError` | The platform failed the job. |
| `zenthros.errors.ValidationError` | `ValueError` | A call or option that cannot work; do not send it again unchanged. |
| `zenthros.errors.JobCancelled` | `RuntimeError` | The job was cancelled. |
| `zenthros.errors.WaitTimeout` | `TimeoutError` | A wait ran out of time; the job still runs. |
| `zenthros.errors.JobNotFound` | `LookupError` | No such job: never made, deleted, or collected after its `keep_for`. |
| `zenthros.errors.Conflict` | `RuntimeError` | The name is taken, or a limit is reached. |
| `zenthros.errors.TransientError` | `RuntimeError` | The store, the network or the hub failed for now; ask again later. |
| `zenthros.errors.LoginRequired` | `PermissionError` | There is no session: `zenthros.login()` starts one. |

A few checks raise plain Python errors instead: `TypeError` from `.result()` on a Service, and `ValueError` from `.logs(to=)` with `tail=` or `follow=`.

```python
import zenthros

def add(a, b):
    return a + b

try:
    zenthros.run(add, 1, 2, on="my-box", timeout="soon").result()
except zenthros.errors.ValidationError as err:
    print("fix the call:", err)
except zenthros.ZenthrosError as err:
    print("zenthros said:", err)
```
