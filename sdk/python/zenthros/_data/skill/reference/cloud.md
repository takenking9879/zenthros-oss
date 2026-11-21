# Cloud functions

This file covers `@zenthros.cloud` and the `CloudFunction` it makes: one function you call here as usual, and also on another machine with `.remote()`, `.submit()`, `.map()` or `.serve()`. Read it when the same code should run here and elsewhere. The options themselves are `zenthros.run`'s ([run.md](run.md#zenthrosrun)).

## zenthros.cloud

```python
zenthros.cloud(fn=None, **options)
```

Marks `fn` as runnable on Zenthros and returns a `CloudFunction`. Use it bare (`@zenthros.cloud`) or with options (`@zenthros.cloud(gpu="A10G", packages=[...])`). The options are [zenthros.run](run.md#zenthrosrun)'s, the defaults of every remote call of the function; a call's own keywords win over them. `on=` here is where every call runs unless the call names another place, and `params=` here are the function's keyword arguments under each call's.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `fn` | function | `None` | The function, when the decorator is used without parentheses. |
| `**options` | | | [zenthros.run](run.md#zenthrosrun)'s options: `on`, `cpu`, `memory`, `gpu`, `image`, `env`, `secrets`, `packages`, `requirements`, `code_dir`, `timeout`, `retries`, `retry_on`, `cache`, `params`... (not `dry_run`, which is a call's). |

**Returns:** a `CloudFunction` when `fn` is given; else a decorator that makes one.

**Raises:** `zenthros.errors.ValidationError` when a name is not one of `zenthros.run`'s options (a typo such as `gpux=`): the function's own keyword arguments go in `params=`. The values are checked when a call starts.

```python
import zenthros

@zenthros.cloud
def add(a, b):
    return a + b

@zenthros.cloud(cpu="8", memory="32Gi", packages=["polars==1.44.2"])
def rows(path):
    import polars
    return polars.read_parquet(path).height

add(2, 3)                                              # 5, here
add.remote(2, 3, on="caja")                            # 5, on caja
rows.remote("gs://my-bucket/zenthros/sales.parquet", on="my-vm")

@zenthros.cloud(on="my-vm", packages=["polars==1.44.2"])
def count(path):
    import polars
    return polars.read_parquet(path).height

count.remote("gs://my-bucket/zenthros/sales.parquet")                 # on my-vm
count.remote("gs://my-bucket/zenthros/sales.parquet", on="caja")      # on caja: the call's on= wins
```

## zenthros.CloudFunction

`@zenthros.cloud` makes it; you do not build one. Calling it, `f(...)`, runs the function here, unchanged. Its methods run it elsewhere. It keeps the function's `__name__`, `__qualname__`, `__doc__` and `__module__`, and the function itself as `__wrapped__`.

Properties:

| Name | What it is |
|---|---|
| `fn` | The function it wraps. |
| `options` | The options given to `@zenthros.cloud`, as a dict. |

### CloudFunction.local

```python
f.local(*args, **kwargs)
```

Runs the function here, in this process: the same as `f(*args, **kwargs)`. Nothing is sent.

No parameters of its own: `*args` and `**kwargs` are the function's.

**Returns:** what the function returns.

**Raises:** what the function raises.

```python
import zenthros

@zenthros.cloud
def add(a, b):
    return a + b

add.local(2, 3)     # 5
```

### CloudFunction.submit

```python
f.submit(*args, on=None, params=None, keep=None, dry_run=False, **kwargs)
```

Starts the function on Zenthros and returns its `RemoteCall` at once, without waiting. It is `zenthros.run(f.fn, *args, ...)` with the decorator's options under the call's own. `**kwargs` holds both [zenthros.run](run.md#zenthrosrun)'s options and the function's keyword arguments: a name that is an option of `zenthros.run` is taken as the option, so pass such an argument in `params=`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `*args` | any | | The function's positional arguments. |
| `on` | `str`, `Machine` or template name | `None` | Where it runs, as `zenthros.run`'s `on=`: a machine, its name, a template, or `"local"`. Without it: the decorator's `on=`, else `configure(on=)`, else `ZENTHROS_ON`, else `"local"`. |
| `params` | `dict` | `None` | The function's keyword arguments, whatever their names (`memory=`, `timeout=`...), over the decorator's `params=`. |
| `keep` | `bool` | `None` | With `on=` a template: the machine made from it for the call stays after it. Without it, the decorator's `keep=`, else `False`. |
| `dry_run` | `bool` | `False` | Sends nothing and makes nothing: returns a `Plan` of where the call would run, against what the function asks (`.ok`, `.problems`, `.to_dict()`). |
| `**kwargs` | | | [zenthros.run](run.md#zenthrosrun)'s options, and the function's keyword arguments. |

**Returns:** a `RemoteCall` ([results.md](results.md#zenthrosremotecall)); a `Plan` with `dry_run=True`; a cached call with `cache=True` and a kept result.

**Raises:** what `zenthros.run` raises ([run.md](run.md#zenthrosrun)), such as `zenthros.errors.ValidationError` for an option that does not apply.

```python
import zenthros

@zenthros.cloud(cpu="4")
def train(path, depth=4):
    return {"path": path, "depth": depth}

call = train.submit("gs://my-bucket/zenthros/train.parquet", on="my-vm", params={"depth": 6})
print(call.name)            # zr-train-...
model = call.result()
plan = train.submit("gs://my-bucket/zenthros/train.parquet", on="my-vm", dry_run=True)
print(plan.ok, plan.problems)
```

### CloudFunction.remote

```python
f.remote(*args, on=None, params=None, keep=False, dry_run=False, **kwargs)
```

Runs the function on Zenthros and waits for it: `f.submit(...).result()`. It streams the job's log here while it waits. If the wait stops with an error before the job ended, the job is cancelled when a machine was made for it, so nothing is left billing.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `*args` | any | | The function's positional arguments. |
| `on` | `str`, `Machine` or template name | `None` | Where it runs, as in `submit`. |
| `params` | `dict` | `None` | The function's keyword arguments, whatever their names. |
| `keep` | `bool` | `False` | With `on=` a template: the machine made from it stays. |
| `dry_run` | `bool` | `False` | Returns the `Plan` instead, as in `submit`. |
| `**kwargs` | | | [zenthros.run](run.md#zenthrosrun)'s options, and the function's keyword arguments. |

**Returns:** the function's value; a `Plan` with `dry_run=True`.

**Raises:** what `zenthros.run` raises before it starts, then what `RemoteCall.result` raises: `zenthros.RemoteError` when the code failed, `zenthros.JobFailed` when the platform did ([results.md](results.md#remotecallresult)).

```python
import zenthros

@zenthros.cloud(gpu="A10G", packages=["torch==2.9.0"])
def embed(texts):
    return [len(t) for t in texts]

embed.remote(["a", "bb"], on="gpu-box")
embed.remote(["a", "bb"], on="gpu-box", timeout="20m", retries=2, retry_on=(ConnectionError,))
```

### CloudFunction.map

```python
f.map(items, on=None, shared=None, dry_run=False, **kwargs)
```

Runs the function once per item, a task each, in one job. A dict item is the function's keyword arguments, over `shared`'s; any other item is its one argument. `on=` is one place, as in `submit`, or a list of places: then the items spread over them, first where `zenthros.targets()` says there is room, one job per place. One job holds at most 256 items: split a longer list into several maps.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `items` | iterable | required | One task per item. A dict is keyword arguments; anything else is the one argument. `zenthros.grid(...)` makes a list of dicts. |
| `on` | place, or a list of places | `None` | Where the tasks run. A list spreads them, each place once. |
| `shared` | `dict` | `None` | Keyword arguments every dict item gets. A dict item that gives the same name is refused. |
| `dry_run` | `bool` | `False` | Makes nothing: returns how many items each place would take, and its plan (`.counts`, `.ok`, `.problems`). |
| `**kwargs` | | | [zenthros.run](run.md#zenthrosrun)'s machine, environment and failure options: `cpu`, `memory`, `gpu`, `instance_type`, `spot`, `image`, `python`, `code_dir`, `env`, `secrets`, `logs`, `timeout`, `after`, `resilient`, `labels`, `requirements`, `packages`, `tools`, `keep_for`; and `name`, `retries`, `retry_on`, `upload`. |

**Returns:** with one place, a `RemoteCall` whose `.result()` is the list of results in the order of `items`, and whose `.outcomes()` gives each item's value or error. With a list of places, a map of several jobs (named `<name>-0`, `<name>-1`...): `.result(timeout=None, return_exceptions=False)` in the order of `items`, `.where` (the place of each item), `.calls`, `.reports()`, `.cost()`, `.cancel()`, `.delete()`. `zenthros.get(name)` brings it back. With `dry_run=True`, the plan.

**Raises:** `zenthros.errors.ValidationError` before anything starts when `items` is empty, `shared` is not a dict or clashes with an item, `on=[]` or a place is named twice, the map's `name` is not lowercase letters, digits and `-` (at most 60) with a list of places, a job would hold more than 256 items, or an option is not one of the above. A map is not cached, kept or snapshotted: `cache=`, `keep=`, `snapshot=`, `fresh=`, `deps=`, `inputs=` and `outputs=` are refused by name, also when they came from `@zenthros.cloud`, and the message says how to call it without them (`f.with_options(cache=False).map(...)`). The decorator's `on=` is the map's place unless `on=` is given, and its `params=` are every task's keyword arguments, as `shared`'s. With a list of places, either every job starts or none is left running.

```python
import zenthros

@zenthros.cloud
def square(x):
    return x * x

square.map([1, 2, 3, 4], on="caja").result()        # [1, 4, 9, 16]

@zenthros.cloud
def fit(data, learning_rate, leaves):
    return learning_rate * leaves

configs = zenthros.grid(learning_rate=[0.05, 0.1], leaves=[15, 31])
calls = fit.map(configs, on=["my-vm", "caja"], shared={"data": "gs://my-bucket/zenthros/train.parquet"},
                name="sweep")
print(calls.result(), calls.where)
```

### CloudFunction.serve

```python
f.serve(*args, **kwargs)
```

Runs the function as a Service: it runs until cancelled, and `.url()` says where it answers. It is `zenthros.serve(f.fn, *args, ...)` with the decorator's options under the call's own. Its keywords are `zenthros.serve`'s: `port=` or `ports=`, `on=`, `keep=`, `retries=`, `stop_after=`, `params=` (the function's keyword arguments), and run's machine and environment options ([services.md](services.md)). A decorator option a Service cannot honor (`cache=`, `snapshot=`, `upload=`...) is refused by name, with how to call it without them; a decorator `timeout=` or `retry_on=` is refused by `zenthros.serve`.

No parameters of its own: `*args` are the function's, `**kwargs` are `zenthros.serve`'s.

**Returns:** a `RemoteCall` of a Service: `.url()`, `.logs()`, `.cancel()` ([results.md](results.md#remotecallurl)).

**Raises:** what `zenthros.serve` raises: `zenthros.errors.ValidationError` with no `port=`/`ports=` or both, a bad port, `timeout=` or `retry_on=`, or an unknown option.

```python
import zenthros

@zenthros.cloud(cpu="2")
def api():
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
```

```python
service = api.serve(port=8000, on="my-vm")
print(service.url(health="/health"))
service.cancel()
```

### CloudFunction.with_options

```python
f.with_options(**overrides)
```

A new `CloudFunction` of the same function, with the decorator's options and `overrides` over them. The original one does not change.

No parameters of its own: `**overrides` are [zenthros.run](run.md#zenthrosrun)'s options.

**Returns:** a `CloudFunction`.

**Raises:** nothing; the options are checked when a call starts.

```python
import zenthros

@zenthros.cloud(cpu="4")
def train(path):
    return path

big = train.with_options(cpu="16", memory="64Gi")
big.remote("gs://my-bucket/zenthros/train.parquet", on="my-vm")
```
