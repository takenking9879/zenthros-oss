# Session and settings

This file covers how a process reaches its hub and what it carries there: logging in and out, the process's settings, the Workspace view, packages kept on a machine, secrets, and what a job knows about itself. Read it before the first call to a hub, when a job needs a secret, or when code must tell whether it runs inside a job.

Where the login is kept: the zenconfig is `ZENTHROS_CONFIG`, else `~/.config/zenthros/config.yaml` (JSON, which zctl reads too). An OIDC session goes to the system keyring when the `keyring` package works, else to `tokens/<context>.json` beside the zenconfig, readable by its owner only. Nothing here prints a token. Each connection value comes from the first of: `configure()`, the environment (`ZENTHROS_SERVER`, `ZENTHROS_TOKEN`, `ZENTHROS_TOKEN_FILE`, `ZENTHROS_CA_FILE`, `ZENTHROS_WORKSPACE`, `ZENTHROS_CONTEXT`, `ZENTHROS_API_KEY`), the zenconfig's context.

## zenthros.login

```python
zenthros.login(context="", key="", server="", workspace="", token="", ca_file="", issuer="", client_id="",
               scopes=None, google=False, prompt=None, timeout=None)
```

Logs this user in to a context of the zenconfig and makes it the current one. Every parameter after `context` is keyword-only. The ways in:

- `key=`: an API key (`zen_...`) an administrator made with `zctl user add`. It is a whole context: server, Workspace and token. It becomes the current context, named `context` or the key's Workspace.
- `server=` with no Workspace (none given, none in the context) and no `token` or `issuer`, or `google=True`: sign in with Google in the browser, on a hub that takes sign-ups, for a Workspace of your own. `google=True` with no `server` uses the hub of the context.
- `token=`: the context carries that bearer token.
- Otherwise the context's OIDC issuer (its `auth.oidc`, or `issuer=` and `client_id=`) is asked for a device login: `prompt` shows a URL and a code, the person approves in a browser, and `login` waits. The context is written first, so a zenconfig that cannot be written fails before anyone opens a browser.

A context that already holds a token logs in with no question.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `context` | `str` | `""` | The zenconfig context to log in to. Default: the current one. A name not there yet is written. |
| `key` | `str` | `""` | An API key from `zctl user add`. Read it from a file or standard input, never from code. |
| `server` | `str` | `""` | The hub's URL (`https://...`). |
| `workspace` | `str` | `""` | The Workspace. |
| `token` | `str` | `""` | A bearer token for the hub. |
| `ca_file` | `str` | `""` | A PEM CA file that verifies the hub. |
| `issuer` | `str` | `""` | An OIDC issuer's URL: `https://`, or `http://` on this machine only. |
| `client_id` | `str` | `""` | The OIDC client id at that issuer. |
| `scopes` | `list` of `str` | `None` | The OIDC scopes. Default `openid` and `offline_access`. |
| `google` | `bool` | `False` | Sign in with Google on a hub that takes sign-ups. |
| `prompt` | callable | `None` | Shows where to approve. OIDC: `prompt(url, code, complete_url)`. Google: `prompt(url)`. Default: print it. |
| `timeout` | `int` | `None` | Seconds to wait for the approval. Default: as long as the code lives (what the issuer says, else 600). |

**Returns:** the Workspace of that context, as `zenthros.workspace()` returns it. Clients this process made before are dropped.

**Raises:**

- `zenthros.errors.ValidationError` when the key is not one; the context has no server or no Workspace; there is neither a token nor an issuer to ask; the server or issuer is not a URL that may carry a token; `ca_file` holds no PEM certificate; or the zenconfig is not JSON.
- `zenthros.errors.LoginRequired` when the issuer refuses the device login, or nobody approves it in time.
- `zenthros.errors.TransientError` when the issuer cannot be reached.

```python
import zenthros

ws = zenthros.login(server="https://hub.example.com")     # Google, in the browser
print(ws.name)

with open("zenthros.key") as fh:                          # a key an administrator gave you
    zenthros.login(key=fh.read().strip())
```

## zenthros.logout

```python
zenthros.logout(context="")
```

Logs out of a context. Its kept OIDC session is dropped, and a token or key that `login()` wrote into the zenconfig is removed from it. The context stays, with its server and Workspace: `login()` logs in again. A key removed here still works until `zctl user revoke`. Clients kept in this process are dropped, so the next call reads the zenconfig, logged out.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `context` | `str` | `""` | The context to log out of. Default: the current one. |

**Returns:** `None`.

**Raises:** `zenthros.errors.ValidationError` when the credential in use is in the environment (`ZENTHROS_TOKEN`, `ZENTHROS_TOKEN_FILE`, or `ZENTHROS_API_KEY` with no `context` given), which `logout()` cannot drop: unset it. Nothing changes then. Also when the zenconfig is not JSON or has no such context.

```python
import zenthros

zenthros.logout()
```

## zenthros.configure

```python
zenthros.configure(**changes)
```

Sets where Zenthros is, and how calls go, for this process. Each call changes only the settings it names; the others keep their value. A setting never configured comes from the environment. What a call runs and needs (`on`, `cpu`, `image`, `secrets`...) goes to [zenthros.run](run.md#zenthrosrun); this is for what every call shares.

| Setting | Type | Default | What it is |
|---|---|---|---|
| `store` | `str` | `ZENTHROS_REMOTE_STORE`, else the bucket `zenthros store` saved with your login | The `s3://` or `gs://` prefix calls and results go to, such as `s3://my-bucket/zenthros`. |
| `on` | `str` or `Machine` | `ZENTHROS_ON`, else `"local"` | Where a call with no `on=` runs. |
| `workspace` | `str` | `ZENTHROS_WORKSPACE`, else the context's | The Workspace. |
| `server` | `str` | `ZENTHROS_SERVER`, else the context's | The hub's URL. |
| `token` | `str` | `ZENTHROS_TOKEN`, else the context's | The bearer token for the hub. Prefer `login()`: a token in code is a secret in code. |
| `context` | `str` | `ZENTHROS_CONTEXT`, else the current one | The zenconfig context. |
| `python` | `str` | `ZENTHROS_REMOTE_PYTHON`, else `"python3"` | The interpreter on the machine. |
| `env` | `dict` | `ZENTHROS_REMOTE_ENV` (a JSON object) | Variables every job gets, merged over the environment's. Never a secret: use `secrets=`. |
| `local_cpus` | `int` or `str` | `ZENTHROS_LOCAL_CPUS`, else all the engine's CPUs | The CPUs of the container `on="local"` runs in. |
| `local_memory` | `str` | `ZENTHROS_LOCAL_MEMORY`, else 3/4 of the engine's memory | Its memory (`"8Gi"`). A call's `memory=` above it is refused. |
| `local_output` | `str` | `""`: `.zenthros/local/store` of the project | Where `on="local"` jobs write their outputs: `<local_output>/<job>`. |
| `registry` | `str` | `""` | Where `zenthros env push` sends an environment that names no registry. |
| `log_dir` | `str` | `ZENTHROS_LOG_DIR`, else `./.zenthros/runs` | Where each call's local record goes (what was asked, its log, its reports, why it failed). `"off"` writes none. |
| `logs` | `True`, `str` or `zenthros.Logs` | `None` | Every call's `logs=` unless it gives its own: `True` keeps the whole log in your store, a bucket URI keeps it there, or a [zenthros.Logs](progress.md). |

`local_cpus` and `local_memory` size `on="local"` only. `create("local")` takes its own `cpus=` and `memory=` ([machines.md](machines.md)).

**Returns:** a context manager. In a `with` block, the settings go back to what they were on exit.

**Raises:** `zenthros.errors.ValidationError` for a setting it does not know.

```python
import zenthros

zenthros.configure(store="s3://my-bucket/zenthros", on="my-box")
with zenthros.configure(local_cpus=4, local_memory="8Gi", log_dir="off"):
    pass
```

## zenthros.workspace

```python
zenthros.workspace()
```

The Workspace of the connection in effect: `configure()`'s, else the environment's, else the zenconfig's current context. Everything is read through the hub with your token: what comes back is what that token may see. It takes no parameters.

**Returns:** a `Workspace`:

| Member | What it is |
|---|---|
| `name`, `server` | The Workspace's name and the hub's URL. |
| `store` | Where a job reads its call and writes its result: your own bucket when you gave one (`zenthros store`), else the hub's store; `""` if neither. |
| `whoami()` | Who the hub says the token is. |
| `jobs(selector="")` | The Workspace's jobs, newest first: each with `name`, `phase`, `kind`, `created`, `backends`, `status` and `to_dict()`. |
| `backends()`, `backend(backend_id)` | Its machines as the hub last heard of them: `healthy`, `ready`, `running_jobs`, `cpu`, `memory_bytes`, `agent_version`... `backend()` raises `LookupError` for an id it does not have. |
| `pools()`, `pool(name)` | Its pools: provider, region, instance types, how many machines are ready. `pool()` raises `LookupError`. |
| `upload(path, to=None)`, `ls(prefix="")`, `download(uri, to=None)` | Files in the store ([data.md](data.md)). |
| `clean(older_than, dry_run=False, calls=None, code=False, outputs=False)` | Deletes from the store the calls older than `older_than` (`"30d"`) whose job is gone. `dry_run=True` only says what would go. |
| `secrets` | The Workspace's secrets, by name. `ws.secrets["hf-token"] = value` sets one, `ws.secrets["hf-token"]` is the `zenthros.Secret` for `secrets=`, `del ws.secrets["hf-token"]` removes it. A value never comes back. |

**Raises:** `zenthros.errors.ValidationError` when there is no hub connection (no server in the zenconfig, `ZENTHROS_SERVER` or `configure(server=)`).

```python
import os
import zenthros

ws = zenthros.workspace()
print(ws.name, ws.store)
ws.secrets["hf-token"] = os.environ["HF_TOKEN"]
```

## zenthros.session

```python
zenthros.session()
```

This process's session with its hub: the same as `zenthros.workspace()`, on the one client of the connection in effect, which `login()` returns too. Every call through it reuses the thread's kept TLS connection. It takes no parameters.

**Returns:** the `Workspace`, as `zenthros.workspace()` returns it.

**Raises:** `zenthros.errors.ValidationError` when there is no hub connection.

```python
import zenthros

print(zenthros.session().whoami())
```

## zenthros.install

```python
zenthros.install(name, requirements=None, packages=None, out=None, timeout=1800)
```

Installs packages on a machine for every function it runs. What the runner itself needs (the bucket's client, cloudpickle) needs no install: a machine installs it by itself. The machine keeps them as its base for this process's Python minor version. The same list again does nothing ("up to date"). Another list is built beside the current one and swapped in at once, so a job running on the old one keeps it. A function's own `packages=` come before the base and win where they differ. It runs as a job on the machine and prints one line: what changed.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | | A machine the Workspace has now, by name or id. Not a template and not a Kubernetes cluster. |
| `requirements` | `str` | `None` | A `requirements.txt` file here. |
| `packages` | `list` of `str`, or `str` | `None` | pip lines (`"xgboost==3.0.5"`), after the file's. |
| `out` | stream | `None` | Where the line goes. Default: standard output. |
| `timeout` | `int` | `1800` | Seconds to wait for the install. |

**Returns:** a `dict`: `python` (the minor), `up_to_date`, `added` (`[name, version]` pairs), `removed`, and `changed` (`[name, old, new]`).

**Raises:** `zenthros.errors.ValidationError` when there is nothing to install; the requirements file cannot be read; the Workspace has no machine of that name; the machine is a cluster (its pods start fresh each time: give the function `packages=` or `image=`); or there is no hub login. A failed or slow install raises as a call's `result()` does ([results.md](results.md)).

```python
import zenthros

zenthros.install("my-vm", requirements="requirements.txt")
zenthros.install("my-vm", packages=["xgboost==3.0.5", "scikit-learn==1.7.2"])
```

## zenthros.Secret

A reference to a secret, not its value: the machine where the job runs looks the value up. Pass it as `secrets={NAME: secret}` to a call; `NAME` is the environment variable it becomes, or an absolute path (`"/run/secrets/gcp.json"`) for a file. You get one from its class methods (`Secret.workspace(...)`, `Secret.encrypted(...)`, `Secret.aws(...)`, `Secret.k8s(...)`), or from `ws.secrets["name"]`. A plain string in `secrets=` is refused: it would sit in the job's spec in clear text (use `env=` for what is not secret). Secrets go by name: never put a value in a command, an argument or a log.

| Class method | Use it when |
|---|---|
| `Secret.workspace(name)` | The secret is the Workspace's, set once with `ws.secrets[name] = value`. The hub seals it to whatever machine the job lands on, a template's, or one made later. |
| `Secret.encrypted(...)` | The value is here, and the job runs on one machine that is up now, or on `"local"`. |
| `Secret.aws(...)` | It is in AWS Secrets Manager, read where the job runs. |
| `Secret.k8s(...)` | It is a key of a Kubernetes Secret. |

### Secret.aws

```python
zenthros.Secret.aws(secret_id, key="", region="", version="")
```

An AWS Secrets Manager secret. The spec names it; it is resolved where the job runs.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `secret_id` | `str` | | The secret's name or ARN. |
| `key` | `str` | `""` | One field of a JSON secret. Default: the whole value. |
| `region` | `str` | `""` | Its region. |
| `version` | `str` | `""` | A version id. Default: the current one. |

**Returns:** a `Secret`.

**Raises:** nothing: it is resolved where the job runs.

```python
import zenthros

db = zenthros.Secret.aws("prod/db", key="password", region="us-east-1")
```

### Secret.k8s

```python
zenthros.Secret.k8s(name, key, namespace="")
```

A key of a Kubernetes Secret. The spec names it; it is resolved where the job runs.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | | The Secret's name. |
| `key` | `str` | | The key in it. |
| `namespace` | `str` | `""` | Its namespace. |

**Returns:** a `Secret`.

**Raises:** nothing: it is resolved where the job runs.

```python
import zenthros

token = zenthros.Secret.k8s("hf", "token")
```

### Secret.workspace

```python
zenthros.Secret.workspace(name)
```

A secret of the Workspace, by name. The spec carries the name and nothing more. The hub keeps the value and seals it to the machine the job goes to as it sends it, so it reaches a template's machine or one made later, which no secret sealed here can.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `name` | `str` | | The secret's name: lowercase letters, digits and `-`, at most 47 characters, such as `"hf-token"`. |

**Returns:** a `Secret`.

**Raises:** `zenthros.errors.ValidationError` when the name is not one the hub can keep.

```python
import zenthros

hf = zenthros.Secret.workspace("hf-token")
zenthros.run("python train.py", on="gcp-gpu", secrets={"HF_TOKEN": hf})
```

### Secret.encrypted

```python
zenthros.Secret.encrypted(value=None, from_env="", from_file="", on=None, recipients=(), zctl=None)
```

A value sealed here to the age keys of the machines that read it. It is the only kind a container backend, or a RunPod or Vast.ai machine, resolves. The spec carries ciphertext only, which only those machines' agents can open. The keys are those of the machine `on=` names now: a machine made later has its own and cannot open it, so seal it after the machine is ready, and run on that machine. For a template, a machine made with `idle=` (the one it starts next has other keys) or one a template made for a call, use `Secret.workspace`: sealing to one of them is refused, and the refusal says so. `on="local"` is this computer's container: the value stays in this process's memory and is sealed as the job starts there. The SDK seals it itself (with `cryptography`: `pip install 'zenthros[seal]'`) when it knows the keys: from `recipients=`, or the machine's keys, fetched through the hub. Otherwise zctl seals it on this computer, the value given through its environment, never its arguments. Every parameter after `value` is keyword-only.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `value` | `str` or `bytes` | `None` | The value. Give exactly one of `value`, `from_env` and `from_file`. |
| `from_env` | `str` | `""` | The name of an environment variable here that holds the value. |
| `from_file` | `str` | `""` | A file here that holds the value. |
| `on` | `Machine` or `str` | `None` | The machine whose keys seal it, or `"local"`. Give exactly one of `on` and `recipients`. Without a hub, a name is a pool's. |
| `recipients` | `list` of `str` | `()` | `age1...` public keys to seal it to. |
| `zctl` | `zenthros.zctl.Zctl` | `None` | The zctl to seal with when the SDK cannot. Default: the `zctl` on the PATH. |

**Returns:** a `Secret`.

**Raises:** `zenthros.errors.ValidationError` when not exactly one of `value`, `from_env` and `from_file` is given, or not exactly one of `on` and `recipients`; `on="local"` without `cryptography`; the variable of `from_env` is empty; `on` names no machine of the Workspace, or a template's machine; or the machine has no key yet (wait until it is ready). `zenthros.errors.ZctlError` when zctl answers something unexpected.

```python
import zenthros

box = zenthros.create("local", name="my-box")
key = zenthros.Secret.encrypted(from_env="API_KEY", on=box)
zenthros.run("python serve.py", on=box, secrets={"API_KEY": key})
```

## zenthros.JobEnv

What the agent tells a job about itself, read from its `ZENTHROS_*` variables in one place. Outside a job (a notebook, a test, a call run here) they are unset: checkpoints and reports are then skipped, so the same code runs everywhere. You get one with `zenthros.JobEnv.current()`. It is frozen.

| Field | Variable | What it is |
|---|---|---|
| `job_id` | `ZENTHROS_JOB_ID` | The job's name; `""` outside a job. |
| `generation` | `ZENTHROS_GENERATION` | The job's generation, which its checkpoints carry; 0 when unset. |
| `checkpoint_prefix` | `ZENTHROS_CHECKPOINT_PREFIX` | Where its checkpoints go. |
| `data_output` | `ZENTHROS_DATA_OUTPUT` | Where it writes its outputs: a folder on its machine, which its agent saves to `data_output_uri` once the job ends; on a cluster's pod, the URI itself. |
| `data_output_uri` | `ZENTHROS_DATA_OUTPUT_URI` | Where its outputs end up (a bucket's URI): what the next step reads. |
| `data_input` | `ZENTHROS_DATA_INPUT` | Where its inputs are. |
| `endpoint` | `ZENTHROS_REPORT_ENDPOINT` | Where its reports go. |
| `token` | `ZENTHROS_REPORT_TOKEN` | Its token for reports. Never print it. |
| `producer_id` | `ZENTHROS_PRODUCER_ID` | Its id as the writer of its reports. |
| `preempt_file` | `ZENTHROS_PREEMPT_FILE` | The file a notice to stop appears in. |
| `local_root` | `ZENTHROS_CHECKPOINT_LOCAL_ROOT` | The local folder of its checkpoints. |
| `parent_job_id` | `ZENTHROS_PARENT_JOB_ID` | The job that spawned it. |
| `map_items` | `ZENTHROS_MAP_ITEMS` | A spawned map's child: the URI of the items; its own is item `task_index`. |
| `task_index`, `task_count` | `TASK_INDEX`, `TASK_COUNT` | Its task's index (`-1` when unset) and how many tasks there are. |
| `cursors` | `ZENTHROS_CHECKPOINT_CURSORS` | Each checkpointed phase's acked cursor, by phase. |

| Property | What it is |
|---|---|
| `prefix` | Where checkpoints go: `checkpoint_prefix`, else `data_output_uri`. |
| `output_folder` | The folder on this machine its agent saves to `data_output_uri`; `""` where `data_output` is the URI itself. |
| `in_job` | `True` inside a job that has somewhere to checkpoint (`job_id` and `prefix`). |

### JobEnv.current

```python
zenthros.JobEnv.current(environ=None)
```

Reads the variables now. It is not cached.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `environ` | `dict` | `None` | A mapping to read instead of `os.environ`. |

**Returns:** a `JobEnv`.

**Raises:** nothing. A `ZENTHROS_CHECKPOINT_CURSORS` that is not JSON is warned about, and only the latest cursor is kept.

```python
import zenthros

env = zenthros.JobEnv.current()
if env.in_job:
    print(env.job_id, env.data_output)
```

### JobEnv.cursor

```python
env.cursor(phase)
```

The acked checkpoint of a phase ([checkpoints.md](checkpoints.md)).

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `phase` | `str` | | The phase's name. |

**Returns:** a `Cursor` (`seq`, `uri`, `sha256`, `complete`), or `None` when the phase has none.

**Raises:** nothing.

```python
import zenthros

cursor = zenthros.JobEnv.current().cursor("train")
```
