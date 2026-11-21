# Services

This file covers long-running services: `zenthros.serve` starts one behind a URL (an API, a model server), and `zenthros.services` lists what is serving now and what it costs. Read it before you start a process that should answer requests rather than return a value.

## zenthros.serve

```python
zenthros.serve(target, *args, port=None, ports=None, retries=2, on=None, keep=False, stop_after=None, params=None,
               **options)
```

Runs `target` as a Service: it runs until it is cancelled, or until `stop_after`, and `.url()` says where it answers. It returns a `RemoteCall` at once. `target` is a command (a string run by `sh -c`, or a list) or a function that serves (such as `uvicorn.run`), called as `target(*args, **params)`. A process that exits is run again, up to `retries` times; a lost machine is replaced. Each run publishes its own URL: read `.url()` again after a restart. The URL may be public: with no `secrets=`, `serve` warns, and the service should check a key of its own.

Its `**options` are [zenthros.run](run.md#zenthrosrun)'s machine and environment options: `cpu`, `memory`, `gpu`, `instance_type`, `spot`, `image`, `python`, `code_dir`, `env`, `secrets`, `logs`, `after`, `resilient`, `labels`, `requirements`, `packages`, `tools`, `keep_for`, and `name`. `timeout=` and `retry_on=` are refused: a Service runs until it is cancelled, and `retries=` re-runs its process. The other options of `zenthros.run` (`cache`, `dry_run`, `snapshot`, `fresh`...) are refused as unknown.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `target` | `str`, `list` or callable | | The command, or the function that serves. A command takes its arguments inside it. |
| `port` | `int` | `None` | One port it listens on, named `"http"`. Give `port` or `ports`, not both. |
| `ports` | `dict` or `list` | `None` | Several ports: `{"api": 8000, "metrics": (9090, "TCP")}`, or `[8000, 9090]` (named `port-8000`, `port-9090`). The protocol is `"HTTP"` (the default) or `"TCP"`. Names are lowercase letters, digits and `-`. |
| `retries` | `int` | `2` | How many times a process that exits is run again. |
| `on` | `Machine` or `str` | `None` | Where, as `zenthros.run`'s `on=`: a machine, its name, or a template. Default: `configure(on=)`, else `ZENTHROS_ON`, else this computer's container. |
| `keep` | `bool` | `False` | A machine made from a template for this Service stays after it. Without it, that machine goes when the Service is cancelled. |
| `params` | `dict` | `None` | A function's keyword arguments. A command takes its arguments inside it, so it refuses them. |
| `stop_after` | `str` or number | `None` | `"4h"`, `"30m"` or seconds: the controller stops it (`Cancelled`, `STOP_AFTER`) once it has served that long, whether this process is still open or not. `None`: only a cancel stops it. |

The machine must publish the ports. A container on this computer publishes only the ports `zenthros.create("local", ports=[...])` gave it, on `127.0.0.1`. A template's machines publish what their pool exposes. Any other port is refused before a machine is made.

**Returns:** a `RemoteCall` ([results.md](results.md)). `.url(name=None, timeout=600, poll_seconds=2.0, health=None)` waits until every port answers, then returns the URL of port `name` (default: the first); `health="/health"` also waits for a 2xx from that path. `.cancel()` stops it. Another process finds it again with `zenthros.get(name)`.

**Raises:** `zenthros.errors.ValidationError` when neither or both of `port` and `ports` are given; a port is not 1–65535, its protocol is not HTTP or TCP, or its name is not a lowercase name; a port or a name comes twice; `timeout=` or `retry_on=` is given; an option is unknown; `retries` is below 0; a command gets `*args` or `params=`; the command is empty; `target` is neither a command nor a function; `stop_after` is not a duration; or the machine does not publish a port.

```python
import zenthros

llm = zenthros.serve("vllm serve Qwen/Qwen3-8B --port 8000", on="gpu-box", port=8000,
                     secrets={"VLLM_API_KEY": zenthros.Secret.workspace("vllm-key")}, stop_after="4h")
print(llm.url(health="/health"))
llm.cancel()

box = zenthros.create("local", name="caja", ports=[8000])
api = zenthros.serve("uvicorn app:app --host 0.0.0.0 --port 8000", on=box, port=8000)
print(api.url())                 # http://127.0.0.1:8000
```

## zenthros.services

```python
zenthros.services()
```

The Workspace's Services that have not ended: their URLs, how long each has served, when `stop_after` stops it, and what it has cost so far. It reads the connection in effect: the hub when you are logged in, else this computer's jobs. It takes no parameters. To stop one, `zenthros.get(name).cancel()`.

**Returns:** a list of `ServiceInfo`, shown as a table (`name`, `state`, `url`, `serving`, `stops in`, `cost`). Each has `name`, `phase`, `urls`, `serving_seconds` (from its first start), `stops_in_seconds` (`None`: only a cancel ends it) and `cost` (`None` where it cannot be read: no hub, or the hub did not answer).

**Raises:** the connection's error when the jobs cannot be listed, such as `zenthros.errors.TransientError` when the hub is away. A cost that cannot be read is `None`, never an error.

```python
import zenthros

print(zenthros.services())
zenthros.get("zr-api-1a2b3c").cancel()
```
