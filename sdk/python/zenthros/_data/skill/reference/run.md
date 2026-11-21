# Running code

This file covers `zenthros.run`: one function or one command on another machine, and every option it takes. The same options go to `@zenthros.cloud` functions ([cloud.md](cloud.md)). Read it before you send code somewhere, or when a call refuses an option, leaves a file out, or is too large. What you get back, and its errors, are in [results.md](results.md).

## zenthros.run

```python
zenthros.run(target, *args, on=None, cpu=None, memory=None, gpu=None, instance_type="", spot=None,
             image="", python=None, code_dir=".", env=None, secrets=None, logs=None, retries=0, retry_on=(),
             timeout=None, after=(), resilient=True, name=None, labels=None, requirements=None, packages=None,
             tools=(), params=None, keep=False, dry_run=False, keep_for=None, snapshot=False, cache=False,
             deps=None, inputs=None, outputs=None, upload=False, fresh=False, **kwargs)
```

Starts `target` on a machine and returns a `RemoteCall` at once. It does not wait: `.result()` waits ([results.md](results.md#zenthrosremotecall)). A function, a `zenthros.Task` or any callable runs as `target(*args, **kwargs)` and gives back its value. A list (`["python", "train.py"]`) or a string (run by `sh -c`) is a command: it runs in `code_dir`'s files, its data goes to `$ZENTHROS_DATA_OUTPUT` (`call.output`), and its result is `0` when it exits 0.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `target` | function, `zenthros.Task`, callable, `list` or `str` | required | What runs. A list is a command's words (argv); a string is one shell line. |
| `*args` | any | | The function's positional arguments. A command takes none: its arguments go inside it. |
| `on` | `str`, `Machine` or template name | `None` | Where it runs: a machine, its name, a template (`zenthros.targets()` lists them, [machines.md](machines.md)), or `"local"`. With no `on=`: `configure(on=)`, else `ZENTHROS_ON`, else `"local"`, a container on this computer that needs no account. A template gives one of its machines that has nothing to do, or a new one. |
| `cpu` | `str` or `int` | `None` | The CPUs the machine must have (`"4"`, `8`, `"500m"`). On a VM or a container it chooses the machine; it does not limit the job. On Kubernetes it is the pod's request. |
| `memory` | `str` or number | `None` | The memory the machine must have: `"16Gi"`, `"512Mi"`, `"4g"`, or a number of GiB. Chooses, does not limit, as `cpu`. |
| `gpu` | `str` or `int` | `None` | A GPU model (`"A10G"`: one of it) or a count (`2`). |
| `instance_type` | `str` | `""` | The cloud's machine type, such as `"e2-standard-8"`. |
| `spot` | `bool` | `None` | `True`: a Spot machine. `False`: on demand. `None`: what the place says. |
| `image` | `str` | `""` | Run the job in this container image. Only a machine that runs images (containerd, a Kubernetes cluster) takes it; another one refuses it before anything is made. A function sent with no `image=` to a machine whose jobs run in images runs in `python:<this minor>-slim-bookworm`. |
| `python` | `str` | `None` | The interpreter that runs the call on the machine. `None`: `configure(python=)`, else `ZENTHROS_REMOTE_PYTHON`, else `python3`. A function needs the same Python minor version there as here (cloudpickle): a template whose Python differs is refused before a machine is made. |
| `code_dir` | `str` or `None` | `"."` | The folder whose files go with the call (see [What travels with a call](#what-travels-with-a-call)). `None` sends no project files. |
| `env` | `dict` or `str` | `None` | A dict: plain environment variables, `{"MODE": "fast"}`. A string: the name of an environment made with `zenthros env`, whose image the call runs in. Never a secret value. |
| `secrets` | `dict` | `None` | `{NAME or "/path": zenthros.Secret}`. Each one becomes that environment variable, or a file at that path, on the machine. Its value never travels in the call. |
| `logs` | `bool`, `str` or `zenthros.Logs` | `None` | `True`: the machine saves the whole log in your store (`<store>/logs`). `"s3://my-bucket/logs"` or `zenthros.Logs(...)`: in that bucket. `None`: `configure(logs=)`. `call.logs(to="x.log")` reads it back whole, also once the machine is gone. A call on `"local"` keeps its whole log on this computer anyway. |
| `retries` | `int` | `0` | A function: how many times the exceptions in `retry_on` are retried in place; it needs `retry_on`. A command: how many times it runs again after it fails. |
| `retry_on` | exception class, or a tuple of them | `()` | The function's exceptions to retry, such as `(ConnectionError,)`. A bug is not retried, and a lost machine is `resilient`'s. Not for a command. |
| `timeout` | `int`, `float` or `str` | `None` | Ends the job after this long: `90`, `"90s"`, `"30m"`, `"2h"`, `"1d"`. The job fails with `DEADLINE_EXCEEDED`. |
| `after` | list of `RemoteCall` or job names | `()` | Calls that must succeed before this one starts. A Service never succeeds, so it cannot be waited for. |
| `resilient` | `bool` | `True` | `True`: a lost machine (Spot reclaim, a dead agent) moves the job to another machine, where `zenthros.Task` and `zenthros.phase` go on from their last checkpoint ([checkpoints.md](checkpoints.md)). `False`: the job fails with its machine. |
| `name` | `str` | `None` | The job's name. `None`: `zr-<function>-<6 hex digits>`. A name already taken is refused (`zenthros.errors.Conflict`). |
| `labels` | `dict` | `None` | Labels put on the job. |
| `requirements` | path | `None` | A requirements file (relative to where this runs) to install on the machine. A function's only. |
| `packages` | `list` or `str` | `None` | pip lines to install on the machine (see [Packages](#packages)). A function's only. |
| `tools` | `list` | `()` | Tools to add on the machine. Zenthros provides `"java"`. A function's only. |
| `params` | `dict` | `None` | The function's keyword arguments, whatever their names: use it for an argument called like an option (`memory=`, `timeout=`, `name=`). |
| `keep` | `bool` | `False` | With `on=` a template: a machine is made from it for this call and stays after it (for up to 24 h). Without it, the template's pool gives a machine and ends it once it is idle; a machine the SDK made for the call goes once the result is read, or when the call is cancelled. |
| `dry_run` | `bool` | `False` | Sends nothing and makes nothing. Returns a `Plan` of where it would run, its image, packages, data and cache, and every problem: `.ok`, `.problems`, `.to_dict()`. |
| `keep_for` | `str` or `int` | `None` | How long the ended job is kept, with its status, its log and the digest that vouches for its result. `None` is `"7d"`; at most `"90d"`. After that, `zenthros.get(name)` and `.result()` find no job. |
| `snapshot` | `bool` | `False` | A command's only. Runs `code_dir`'s git worktree as it is now (changes not committed included), stored by its digest: later edits change nothing that runs. Needs git here and Python on the machine. |
| `cache` | `bool` or `"strict"` | `False` | A function's only. `True`: a later call with the same code, arguments, data and environment returns the kept result without running, and says why when it does run. `"strict"` also reads every large file and output again. The entries: [data.md](data.md#zenthroscache); steps that reuse them: [pipelines.md](pipelines.md). |
| `deps` | list or `dict` | `None` | For `cache=`: what the function reads without taking it as an argument: paths, URIs, or `{"sales": "2026-10-31"}`, a version you give. |
| `inputs` | contract | `None` | The function's input contract (`zenthros.contracts`), checked where it runs; what this computer can already see is checked here first. |
| `outputs` | contract | `None` | The function's output contract, checked where it runs. A result that breaks it fails with reason `OUTPUT_CONTRACT`. |
| `upload` | `bool` | `False` | A path argument that only this computer has goes to your store first, instead of stopping the call. Not for `on="local"`, which reads your files in place. See [data.md](data.md). |
| `fresh` | `bool` | `False` | Always a new machine from the template, for a run nothing else may have touched. With `on=` one machine, it is refused. |
| `**kwargs` | any | | The function's keyword arguments. One named like an option of `run` is taken as the option: pass it in `params=`. |

**Returns:** a `RemoteCall`, at once ([results.md](results.md#zenthrosremotecall)). With `dry_run=True`, a `Plan`. With `cache=True` and a kept result, a cached call: `.cached` is `True` and `.result()` returns the kept value, with no job.

**Raises:** `zenthros.errors.ValidationError` before anything is made when:

- `snapshot=True` is given for a function, or `cache=`, `deps=`, `inputs=` or `outputs=` for a command;
- a command gets `*args`, keyword arguments, `params=` or `retry_on=`, or is empty;
- `target` is not callable and not a command;
- `retries` is below 0, or given for a function without `retry_on`, or `retry_on` holds something that is not an exception class;
- `packages=`, `requirements=` or `tools=` are given for a command, a requirement line names a file of this computer, the requirements file cannot be read, or a tool is unknown;
- `timeout` or `keep_for` cannot be read, or `keep_for` is over 90 days;
- `on=` names nothing the Workspace has, or `image=` goes to a machine that does not run images;
- an argument names a path only this computer has, and `upload=True` is not given;
- a path argument is a credential (a `.env`, a key, a kubeconfig);
- the call's files are over the size limit (see [Size limits](#size-limits));
- `after=` names a Service.

`zenthros.errors.Conflict` when `name=` is taken.

```python
import zenthros

def add(a, b):
    return a + b

call = zenthros.run(add, 2, 3, on="caja")
print(call.result())        # 5

test = zenthros.run("make test", on="my-vm", snapshot=True, timeout="30m")
test.result()               # 0, or RemoteError with the end of the log
```

### The job's name

Each call says its job's name on stderr when it starts:

```text
zenthros: add → zr-add-c91a4b on caja
zenthros: add ×4 → zr-add-c91a4b on caja
```

The second line is a map of 4 items. That name is `call.name`. It is what `zenthros.get(name)`, `zenthros jobs` and `zenthros logs NAME` take. A call also writes a local record under `.zenthros/runs/<name>/` (`call.json`, `logs.txt`, `reports.jsonl`, `error.txt`); `call.log_dir` is that folder, and `zenthros.last_failure()` gives the last failed one ([results.md](results.md#zenthroslast_failure)).

### What travels with a call

With the default `code_dir="."`, the call takes the folder's `.py` files and the small files beside them (`.yaml`, `.json`, `.toml`, `.sql`, `.sh`...). A file that is not `.py` stays home when it is over 256 KiB.

These never travel in the code:

- notebooks (`*.ipynb`);
- data, model and output files: `.csv`, `.tsv`, `.parquet`, `.feather`, `.arrow`, `.pkl`, `.pickle`, `.joblib`, `.npy`, `.npz`, `.pt`, `.pth`, `.ckpt`, `.safetensors`, `.onnx`, `.h5`, `.hdf5`, `.png`, `.jpg`, `.jpeg`, `.gif`, `.xlsx`, `.xls`, `.zip`, `.tar`, `.gz`;
- hidden files and folders, `__pycache__`, `node_modules`, `site-packages`, `venv`, `env`, `build`, `dist`, and a `data/` folder that is not a Python package;
- what a `.gitignore` or `.zenthrosignore` in any folder excludes (gitignore syntax);
- credentials: `*.env`, `.env`, `*.pem`, `*.key`, `*.p12`, `*.pfx`, `id_rsa*`, `id_ed25519*`, `kubeconfig`, `*.tfvars`, `*.tfstate`, `*credentials*.json`, `*service-account*.json`, `client_secret*.json`. A path to one given as an argument is refused: pass it with `secrets=`.

A line `!NAME` in `.zenthrosignore` sends one of them back. The first call from a folder in a process says once what it left out:

```text
zenthros: add: left out of the code: sales.csv, explore.ipynb (notebooks and data; !NAME in .zenthrosignore sends one)
```

`code_dir=None` sends no project files: use it from a notebook whose function imports nothing of the project (the function travels pickled). A function's project goes to your store once, named by its SHA-256. A command's files go inside the job; with `snapshot=True` its git worktree goes to the store by its digest instead. Data goes by URI, by `upload=`, or as a path `on="local"` reads in place ([data.md](data.md)).

### Size limits

Without a bucket (no store), a call carries its own files and arguments up to about 360 KiB, and brings back a result up to about 189 KiB, both measured after compression (zstd). Past the first, `run` raises `ValidationError`, which says to give the Workspace a bucket (`zenthros store gs://my-bucket/zenthros`), to pass a smaller `code_dir`, or to pass large data by URI. Past the second, `.result()` raises `RemoteError` (`remote_type` `"ResultTooLarge"`), which says to give a bucket, return less, or write the data where it runs and return its path. Without a bucket, a `RemoteCall` cannot be another call's argument: pass its `.result()`.

A command's own files always go inside the job, up to the same 360 KiB, unless `snapshot=True`.

With a bucket, a function's project may be up to 64 MiB packed. An argument whose pickle passes 1 MiB is uploaded once, by its SHA-256, and the call carries a reference ([data.md](data.md)). A call that still pickles to more than 50 MiB says so on stderr: pass large data by URI.

### Packages

`packages=` takes pip lines: `"polars==1.44.2"`, `"torch>=2.4"`, `"git+https://..."`, and option lines such as `"--extra-index-url https://download.pytorch.org/whl/cpu"`. `requirements=` takes a file of the same lines. They are installed on the machine in a venv cached by their hash, so a kept machine builds it once; the venv also sees the machine's own packages.

- Pin the versions this computer uses. A bare name (`"polars"`) is pinned to the version installed here, without its local label (`2.9.0+cpu` becomes `==2.9.0`). A name not installed here, or installed here from a URL or a folder, stays as written, and the machine takes what pip finds.
- The same module name can come from another package with other numbers: `import xgboost` here may be the `xgboost-cpu` package, and `packages=["xgboost"]` then installs another one there. Name the package as `pip list` shows it here.
- A line that names a file of this computer is refused: `-r`, `-c`, `-e`, `./path`, `/path`, `~`, `file:`.
- They are a function's: a command runs in its image as it is, and refuses them.
- pip installs Python packages, not the system's libraries. A wheel that loads one fails at import with `OSError: libNAME.so.N: cannot open shared object file`. LightGBM is the common one: it needs OpenMP (`libgomp.so.1`), which a plain Ubuntu VM does not have. On a VM, the person installs it there once (`sudo apt-get install -y libgomp1`, by ssh): it is their machine, and Zenthros does not install system packages. In a container, choose an `image=` that has it. XGBoost and scikit-learn bring their own OpenMP in the wheel.

```python
import zenthros

def fit(path):
    import polars
    return polars.read_parquet(path).height

call = zenthros.run(fit, "gs://my-bucket/zenthros/sales.parquet", on="my-vm",
                    packages=["polars==1.44.2"], cpu="8", memory="32Gi")
```

### Where it runs, and how big

- `on="local"` (the default) runs in a container on this computer, with no account.
- This computer (a machine attached with `zenthros attach NAME --type local`) runs one job at a time. Its size is set when it is attached ([machines.md](machines.md)).
- On a VM or a container, `cpu=` and `memory=` choose a machine that has that much; they do not limit the job. On Kubernetes they are the pod's requests.
- A template gives one of its machines with nothing to do, or a new one. `fresh=True` always makes a new one. `keep=True` keeps the one made for the call.

### When it fails

A failure of the code is `RemoteError`: the function raised (its traceback), or the command exited non-zero (the end of its log). A failure of the platform is `JobFailed`, with `.reason` (`IMAGE_PULL_FAILED`, `WORKER_LOST`, `DEADLINE_EXCEEDED`...). A job killed for memory is the code's, a `RemoteError` whose `__cause__` is the `JobFailed`: `OOM_KILLED` when the job's own unit or pod hit the limit; on a shared container (This computer, `on="local"`) `PROCESS_FAILED`, whose message says it "probably ran out of memory". Details in [results.md](results.md#zenthrosremoteerror).

### Chaining calls

A `RemoteCall` passed as another call's argument travels as a reference to its result: the second job waits for the first, and its machine reads the first one's result, checked against its SHA-256, before the function runs. This needs a bucket. `after=[call]` waits for a call without taking its value.

```python
import zenthros

def clean(path):
    return path.replace(".csv", ".parquet")

def train(path, depth=4):
    return {"path": path, "depth": depth}

cleaned = zenthros.run(clean, "gs://my-bucket/zenthros/raw.csv", on="my-vm")
model = zenthros.run(train, cleaned, on="my-vm", params={"depth": 6})
print(model.result())
```
