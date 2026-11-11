# zenthros

Run a Python function on a machine of yours (a VM in any cloud, a GPU pod,
your other computer) from a notebook, and get its value back.

## 1. Try it, with no account

All you need is Docker Desktop, OrbStack or Podman (on Windows, inside WSL2):

```bash
pip install "zenthros[seal] @ https://github.com/takenking9879/zenthros-oss/releases/download/v0.1.1/zenthros-0.1.0-py3-none-any.whl"
```

```python
import zenthros

@zenthros.cloud
def suma(a, b):
    return a + b

print(suma.remote(2, 3))   # 5
```

The first call downloads the image and the agent, verified against this wheel, and
takes a moment. After that it takes seconds, and needs no network if the job
does not. Jobs run in one container per project, with all your CPUs and 3/4 of
your memory unless `zenthros.configure(local_cpus=, local_memory=)` says otherwise.
A path you pass as an argument is mounted read-only at the same path, and nothing
else of your computer is in there. `.env` and keys are refused.

```console
$ zenthros run train.py            # a command, in the same container
$ zenthros jobs                    # what ran; zenthros logs NAME, zenthros status NAME
$ zenthros clean                   # its containers; --results and --envs say what else
```

A secret reaches only the job, sealed: `secrets={"API_KEY":
zenthros.Secret.encrypted(from_env="API_KEY", on="local")}`. Big outputs go to
`$ZENTHROS_DATA_OUTPUT`, a folder under `.zenthros/local` or wherever
`configure(local_output=)` says.

The rest of this page is for running on your other machines: that needs a hub.

## 2. Log in

```python
import zenthros

zenthros.login(key="zen_...")   # the key your hub gave you; saved for next time
```

No key, on a hub that takes sign-ups: sign in with Google for a Workspace of
your own (a page opens in your browser; when the key ends, `zenthros login
--google`). Signing in accepts the hub's terms and privacy notice: login
prints where they are before the page opens.

```python
zenthros.login(server="https://api.zenthros.app")
```

A hub that keeps no data of yours (a Workspace from a Google sign-in) runs on
your bucket: give it once, with the AWS credentials you already have here.

```python
zenthros.set_store("gs://your-bucket/zenthros")   # or s3://; or: zenthros store gs://your-bucket/zenthros
```

It is checked (a small object written, read and removed) and saved with your
login. Your code and results go there straight from this computer; a job's
machine gets credentials limited to that prefix, sealed to that machine: for 12
hours on `s3://`, and on `gs://` the key of an account made for that prefix, which
`zenthros.set_store(None)` ends. A Google Cloud machine uses its own role. The hub
never sees them.

## 3. Attach a machine

Create a VM wherever you like (AWS, GCP, a server at home), then:

```console
$ zenthros attach my-vm
Machine type   › VM
Run jobs in    › containers (containerd)

On my-vm, run:
f="$(mktemp)" && curl -fsSL … && sudo env … bash "$f" --container-engine containerd …
sudo zenadm join 'hub.example:9443,hub.example:443' --token 'zt1_…'   # single use, expires 14:05

Waiting for my-vm…
✓ connected (2 CPU, 8Gi)
✓ test run 9s
```

The first line installs the agent (no secret in it) after checking the machine; the
second joins it and ends once it is connected. The machine needs no Python of its own.
With ssh there is nothing to paste: `zenthros attach my-vm --ssh ubuntu@203.0.113.7 --key ~/.ssh/aws.pem`.

Jobs never run as root on your machine: each Workspace's jobs run as a user of their own
(`zj-…`), with no way to gain privileges, and write only their own directories, `/tmp` and
`/var/tmp`. They cannot read root's files or the agent's key, and cannot `apt-get` or `sudo`:
tools come with `tools=` and `zenthros install`. To give jobs a folder of the machine (a
dataset, say), add `--folder /data` to the join line, and give that folder's permissions to
them (`sudo chmod o+rwx /data`, or an ACL).
From a notebook it is the same call:

```python
cmd = zenthros.attach("my-vm", type="vm", runtime="containerd")
cmd          # the two lines to paste
cmd.wait()   # returns once my-vm is connected and a test run came back
```

```python
zenthros.targets()
```

```text
NAME    TYPE        STATUS      CPU   MEM   GPU   PRICE
local   container   available   4     8Gi   -     -
my-vm   vm          ready       2     8Gi   -     -
```

## 4. Run

```python
@zenthros.cloud(packages=["xgboost", "scikit-learn"])
def train(depth):
    ...
    return {"auc": auc}

train(6)           # runs here, as always
train.remote(6, on="my-vm")    # runs on my-vm and returns its value
```

```text
✓ done in 1m26s on my-vm
{'auc': 0.884}
```

A package named without a version (`"xgboost"`) gets the one this computer
has (`xgboost==2.1.3`), so the function runs there with what it ran with
here; write a version (`"xgboost>=2"`) to choose another.

Packages every function on a machine should have go on once:

```console
$ zenthros install my-vm -r requirements.txt      # or: zenthros.install("my-vm", packages=["xgboost"])
my-vm (py3.12): + xgboost 2.1.3  + scikit-learn 1.6.1   ✓ 41s
```

The same list again says `up to date`; another one is swapped in at once, and a
job already running keeps what it started with. A function's own `packages=`
win over the machine's. A cluster's pods start fresh, so there give the function
its packages or an `image=`.

With no `on=`, a call runs in a container on this computer (`on="local"`), and
with `on="local-k8s"` as a pod of the cluster `kubectl` reaches, each with its
own `image=`. Still no account: see below. Name the machine to run it there: `train.remote(6, on="my-vm")`, or set a default
once with `zenthros.configure(on="my-vm")`.

## Several environments on your own Kubernetes

With no account either. Zenthros uses the cluster of `kubectl`'s current context
and installs nothing but its agent, in the namespace `zenthros-local`, once you
say yes (`ZENTHROS_LOCAL_K8S_INSTALL=1` where no terminal can ask). It needs
`kubectl` and `helm`. For a cluster of your own, on Docker:

```console
$ k3d cluster create zenthros --registry-create zenthros-registry:0.0.0.0:5111 \
    -v "$HOME/.zenthros/k8s:$HOME/.zenthros/k8s@all"
```

```python
@zenthros.cloud(image="python:3.13-slim-bookworm", memory="2g")
def prep(n): ...

@zenthros.cloud(image="python:3.13-slim-trixie")
def train(up): ...

train.remote(prep.submit(10, on="local-k8s"), on="local-k8s")   # two pods, two images
```

A pod asks for the memory and CPU you gave, and Kubernetes places it; one that
fits nowhere stays `Unschedulable` and says why. What pods read and write is
`~/.zenthros/k8s`, at the same path on every node: put your data there. A path
anywhere else is refused before anything starts, and so is a cluster without that
folder. `zenthros clean --k8s` takes the agent and its namespace off the cluster.

## Your own environment, step by step

Your packages in an image, built once, kept, and published as what you tried.
You write no Dockerfile.

```console
$ zenthros env new train --from python:3.13-slim-bookworm --registry us-central1-docker.pkg.dev/my-project/zenthros
$ zenthros env add train polars xgboost==2.1
$ zenthros env build train       # into your Docker or Podman
$ zenthros env use train         # local calls run in it, and work with no network
$ zenthros run entrenar.py
$ zenthros env add train lightgbm && zenthros env build train   # downloads only lightgbm
$ zenthros env push train        # what you tried, to that registry; nothing is rebuilt
```

```python
@zenthros.cloud(env="train")     # or f.remote(env="train"), zenthros run --env train
def entrenar(datos): ...
```

`envs/train/env.lock.json` says it all, with the base by digest and each version,
and goes in Git: another computer builds the same versions. If nothing changed,
`env build` does not even call the builder. With your own `Dockerfile` in
`envs/train/`, that is what builds, by the same builder and cache; credentials in
that folder stay out.

`env push` reads back what the registry has and refuses what is not what you
tried: a change not built, or a cache gone that makes other bytes. `--zstd`
compresses it with zstd: other bytes published, the same content; check your
destination reads zstd. `--platform linux/arm64` builds for that architecture:
the same versions, other bytes. Another registry is only another destination:
`zenthros env push train docker.io/me/train`. Your login is your engine's
(`docker login`, `gcloud auth configure-docker ...`), and Zenthros never sees it.

On `on="local-k8s"`, an environment goes by itself to the cluster's own registry,
the one the command above made. `zenthros clean --images` takes the images and
the builder with its cache.

## Not doing the same work twice

`cache=True` keeps a function's result once checked. The same call, with the same
code, arguments, data and environment, returns it without running, and when it does
run it says why. A `Pipeline` is steps in a line, each taking what the one above
left:

```python
pipe = zenthros.Pipeline([("prep", prep), ("features", features), ("train", train)], name="churn")
print(pipe.plan("/data/raw", cache=True))             # nothing is created, no data moves

for depth in (4, 6, 8):
    run = pipe.run("/data/raw", cache=True, params={"train": {"depth": depth}})
    print(depth, run.result(), run.status())          # prep: reuse · features: reuse · train: run: depth changed
```

- **What makes an entry another:** the function's code, the project's files, the
  arguments, `deps=` (what it reads without taking it as an argument), the image's
  content, its packages, its Python and its GPU. A file goes by its content, and a
  bucket's object by its version. Where it ran and the job's name do not count, so
  changing a file of the project runs every step again.
- **20 GB are not read each time:** a large file is known by its size, dates, inode
  and device, as git does, and `cache="strict"` reads it all again.
- **Only what you checked gets in:** the entry is a manifest in the store next to the
  bytes, signed with a key that stays on this computer (`zenthros cache key export`
  takes it to another of yours). It outlives the job and the cluster that made it. A
  job can write the store, so one that is not signed is never opened.
- **A step that does not hold its promise stops there:**
  `@zenthros.cloud(outputs=zenthros.parquet(columns={"score": "double"}))` ends it
  with `OUTPUT_CONTRACT`, and neither the next step nor the cache get its output.
  Formats are `parquet`, `json`, `file` and `dir`; columns need `pyarrow`.
- **Starting further along:** `pipe.run(start_at="train", upstream={"features":
  "./features.parquet"})` takes a file of yours, `zenthros.get(...)` or
  `zenthros.cache.find(...)`.
- **Picking up:** `pipe.resume()` runs what did not end well. A step cut short with a
  confirmed checkpoint and the same code, inputs and environment goes on from that
  checkpoint in a new job; if anything changed it starts over and says why. That works
  while the job that was cut short exists: 7 days unless you pass another `keep_for`.

`zenthros cache ls|show|rm|sync` looks after the entries. `zenthros clean --results`
keeps what a good entry or a live job needs, and what jobs left in their output
unless you add `--outputs`; `--cache --older 180d` forgets old entries.

## From here to another machine

The same function, with the environment you tried, in another place:

```python
train.remote(feats, env="train")                                   # here, small data
train.remote("gs://your-bucket/feats/", on="gcp-gpu", env="train", gpu=1)
train.remote("gs://your-bucket/feats/", on="runpod-cpu", requirements="requirements.txt")
```

- **`env="train"` elsewhere** runs what `env push` published, by digest. Not published
  and with a public base, it runs that base with the same versions, and `plan` says it
  is not the same image. A machine made from a template takes that image, and a template
  per image is not needed. A package that would replace one in the image is refused: the
  call says which, and its machine goes.
- **What it cannot reach stops it before anything is made:** a path only on this computer
  says to upload it. `upload=True` sends what the call names to your bucket, by content,
  so the same file never goes twice, and `plan` says how much it would send. The function
  gets a path of the same name.
- **Who can read the image:** a Google Cloud VM reads its own Artifact Registry by its
  role. RunPod and Vast read public images only.
- **Another try:** `zenthros.get(name).rerun(on="other")` sends the same call with the code
  it ran; `resume=True` goes on from its last confirmed checkpoint. Secrets were sealed for
  its machine, so give them again.
- **Leave and come back:** a job on a machine that is not this computer goes on without it.
  `zenthros jobs`, `zenthros logs` and `zenthros.get(name).result()` read it from any
  computer of yours, for 7 days (`keep_for=`).
- **No bucket:** a small call and its result ride with the job, up to about 190 KiB of
  result, and past that it says what to do. `zenthros.get(name)` reads it back while the
  job exists.
- **A machine kept up** with `zenthros.create("gcp-gpu")` ends after 24 hours unless
  `max_lifetime=` says otherwise (`None`: until you delete it). A job on it gets the same
  notice as a cloud's, and ends with `MACHINE_LIFETIME` or moves.
- **`resilient=False`** ends a job with its machine (`WORKER_LOST`), with no replacement.

## Looking at it, and letting an agent run it

`zenthros ui` opens a page on this computer with your jobs and their logs, your targets,
environments and cache. It says what `--json` says, from the same functions, and keeps
nothing of its own. It only reads, and answers to the address it prints, on this computer
only.

Every answer also comes as data: `zenthros targets --json`, `jobs`, `status`, `logs`,
`plan`, `env list`, `cache ls`, `clean --dry-run`. Exit codes: the command's own; 1 if a
function failed; 2 for bad usage; 125 if Zenthros could not start it; 130 if cancelled.

An agent uses five tools (`targets`, `run`, `status`, `logs`, `cancel`) and goes on working
while the test runs on a snapshot of the code, changes not committed included:

```console
$ claude mcp add zenthros -- zenthros mcp
$ zenthros agent skill                              # .claude/skills/zenthros: when to use it, and how to read a result
$ zenthros agent allow gcp-lab --max-machines 3     # what costs money needs your word
```

## More ways to run

```python
call = train.submit(6)                      # start it; call.result(), call.logs(), call.cost()
zenthros.run("python train.py", on="my-vm") # a script you already have
zenthros.map(train, [4, 6, 8])              # one task per item
api = zenthros.serve("uvicorn app:app --port 8000", on="my-vm", port=8000)   # a service with a URL
```

## Inside a job

The same package reports progress and checkpoints from code that runs on a
Zenthros machine, with any framework: a checkpoint is your state (JSON) and
your files (a model, an optimizer), resumed on a replacement after a lost
machine. The bytes go to your store; Zenthros keeps only where they are.

```python
ckpt = zenthros.Checkpointer("train")
start = ckpt.state["epoch"] + 1 if ckpt.state else 0
if ckpt.state:
    model.load(ckpt.file_path("model.bin"))
for epoch in range(start, epochs):
    ...
    model.save("/tmp/model.bin")
    ckpt.save({"epoch": epoch}, files={"model.bin": "/tmp/model.bin"}, complete=epoch + 1 == epochs)
```

Recipes for XGBoost, PyTorch and scikit-learn: [examples/checkpoints](examples/checkpoints).
Extras: `zenthros[s3]` (boto3), `zenthros[gcs]`, `zenthros[seal]` (secrets sealed here).

## Development

One environment for every test of the SDK: a venv in `.venv`, made by
[uv](https://docs.astral.sh/uv/) with the Python of `.python-version` and the
pins of `requirements-dev.txt` (CI installs the same file).

```bash
make env          # make or update .venv (ENV_ML=1 adds torch CPU and Ray)
make test lint    # the unit suite, each skip by its reason, then ruff
make wheel-check  # the wheel installed alone, outside the repository
```

`make test` fails when a package of `requirements-dev.txt` is missing or at
another version (`tests/test_dev_env.py`): no test skips for a package the
environment should have. An editable install hides code the wheel does not
carry, so a change to what ships runs `make wheel-check` too.

### Linux, macOS and Windows

CI (`.github/workflows/sdk-os.yml`) runs the unit suite on Linux, macOS and
Windows with Python 3.11 and 3.14, and then the wheel as a person installs it
(`scripts/os_smoke.py`): `zenthros --help`, `zenthros login --key`, a call
through the API, and the key's file readable only by its owner (on Windows,
by its ACL). A test of the machine's side, which runs on Linux, skips on the
others and says why.

- **Windows:** this computer's jobs (`on="local"`, `"local-k8s"`) run in WSL2,
  with the SDK installed there; everything else runs on Windows itself.
- **Not tested yet:** the login with Google in a browser of macOS or Windows
  (a key's login is), and this computer's jobs on a real Mac.

## More

- [REMOTE_API.md](REMOTE_API.md): the whole surface and its decisions.
- [operator/docs/api.md](../../operator/docs/api.md): zenthros-api, keys,
  logins and the zenconfig.
