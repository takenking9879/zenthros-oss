# Data: sending it, storing it, reading it back

This file covers how data gets to a machine and back: local files sent with `upload=`, large arguments, the size limits without a bucket, what form to send data in, the store's files by URI (`zenthros.files`), Parquet contracts (`zenthros.parquet`), references to stored bytes (`zenthros.Artifact`), your bucket (`zenthros.set_store`) and the result cache (`zenthros.cache`). Read it before you pass a file, a DataFrame or a folder to a call. The call itself is in [run.md](run.md); its result in [results.md](results.md).

## Sending data with upload=

`upload=True` is an option of `zenthros.run`, of a cloud function's `f.remote(...)`, of `call.rerun(...)` and of a `Pipeline`'s `run()`. Without it, a call whose argument names a path only this computer has stops before anything is made, and says so. With it, each such file or folder goes to your store first, at `<root>/data/<sha256>`, and the function receives a local path on the machine: a file (or folder) of the same name, in a temporary folder, each byte checked against its SHA-256 before the function starts. So the same code runs here and there.

- It sends by content: a file already there, of the same size and less than 15 days old, is not sent again, however many calls name it. A folder goes as its files plus a list of them, written last.
- It needs a bucket (or a hub that keeps a store of its own). Without one the call raises `zenthros.errors.ValidationError` ("this hub keeps none of your data"): tell the person to set one, `zenthros store s3://my-bucket/zenthros` (or `gs://my-bucket/zenthros`, or `zenthros.set_store(...)`).
- A path that looks like a credential is refused (`zenthros.errors.ValidationError`): pass it with `secrets=` instead.
- With `on="local"` (a container made for the call on this computer), files need no upload: they go into it read-only, at their own path. An attached This computer (`zenthros attach NAME --type local`) has no mounts of this computer: it needs `upload=`, as any machine.
- `zenthros.map` and `f.map` take `upload=` too: each item, and each `shared` value, that names a path is sent. `Pipeline.plan(..., upload=True)` lists what would be sent and how big it is, and sends nothing.
- Nothing is deleted by Zenthros. A blob that has been in the store 15 days or more is sent again by the next call that names it, so a job never finds it gone. The bucket's own lifecycle rule deletes `<root>/data/` after 30 days: `zenthros store` prints the `gcloud` or `aws` line that sets that rule, for the person to run. Zenthros never changes the bucket's settings itself. Calls and results are outside that rule.

```python
import zenthros

@zenthros.cloud
def train(path, depth=4):
    import pandas as pd
    df = pd.read_parquet(path)   # a local path on the machine
    return {"rows": len(df), "depth": depth}

call = train.remote("data/sales.parquet", on="my-vm", upload=True)
print(call.result())
```

## Large arguments and size limits

- With a bucket, an argument whose pickle passes 1 MiB is stored once, by its SHA-256, under `<root>/data/`. The call carries only a reference, and the machine checks the bytes against that SHA-256 before it opens them. The same DataFrame passed twice is uploaded once. The same 15-day and 30-day rules as `upload=` apply.
- Without a bucket, a call carries its own files and arguments up to about 360 KiB, and brings its result back up to about 189 KiB, both after compression. Past the first, the call raises `zenthros.errors.ValidationError` before anything is made, and asks for a bucket or for less data. Past the second, `.result()` raises `zenthros.RemoteError` with `remote_type` `"ResultTooLarge"`: give the Workspace a bucket, return less, or write the data where it runs and return its path.

## What to send

- Give data defined types first: text to `category`, integers to the smallest size that holds them, `float32` where comparing loses nothing.
- Small data goes as an argument.
- If it does not fit, use 32-bit types where nothing is lost.
- If it still does not fit, or it is big: write Parquet with zstd and send it with `upload=`.
- What you send by `upload=` is always Parquet. `upload=` takes any file, so this is your rule, not a refusal of the SDK: convert a CSV to Parquet with its types first (smaller, and read with its types); never upload the CSV.

```python
import pandas as pd

df = pd.read_csv("sales.csv", dtype={"store": "category", "units": "int16", "price": "float32"})
df.to_parquet("data/sales.parquet", compression="zstd")
```

Plots come back in the value, as the image's bytes: `savefig` to an `io.BytesIO` and return its bytes.

```python
import io
import zenthros

@zenthros.cloud
def chart(path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    df = pd.read_parquet(path)
    fig, ax = plt.subplots()
    ax.plot(df["day"], df["units"])
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    return buffer.getvalue()

png = chart.remote("data/sales.parquet", on="my-vm", upload=True).result()
with open("sales.png", "wb") as fh:
    fh.write(png)
```

## zenthros.files

Files of the store by URI, the same here and in a job. In a job it reads and writes the store as the machine reaches it, with the access the job was given. Here, your own bucket is reached with your own credentials, and the hub's store through URLs zenthros-api signs, so no cloud keys are needed on this side. A path that is not an `s3://` or `gs://` URI is a local file, so a function that uses `zenthros.files` also runs here on local data.

```python
import zenthros

out = "s3://my-bucket/zenthros/reports"
zenthros.files.write_bytes(f"{out}/summary.txt", b"rows: 1200")
print(zenthros.files.read_bytes(f"{out}/summary.txt"))
for item in zenthros.files.ls(out):
    print(item.uri, item.size, item.modified)
```

### zenthros.files.read_bytes

```python
zenthros.files.read_bytes(uri)
```

The bytes of an object, or of a local file.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `uri` | `str` or path | required | `s3://...`, `gs://...` or a local path. |

**Returns:** `bytes`.

**Raises:** `FileNotFoundError` when there is no such object or file.

```python
import zenthros

raw = zenthros.files.read_bytes("gs://my-bucket/zenthros/model.bin")
```

### zenthros.files.write_bytes

```python
zenthros.files.write_bytes(uri, data)
```

Writes `data` to `uri` whole. For a local path, its folders are made as needed.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `uri` | `str` or path | required | `s3://...`, `gs://...` or a local path. |
| `data` | `bytes` | required | What to write. |

**Returns:** `uri`.

**Raises:** the store's own error when it refuses the write.

```python
import pickle
import zenthros

model = {"depth": 4}
zenthros.files.write_bytes("s3://my-bucket/zenthros/model.pkl", pickle.dumps(model))
```

### zenthros.files.ls

```python
zenthros.files.ls(prefix)
```

Every object whose URI starts with `prefix`. A folder lists its whole tree.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `prefix` | `str` or path | required | A URI prefix or a local path. |

**Returns:** a `list` of `StoredObject`, each with `uri`, `size` (bytes), `modified` (UTC) and `version`.

**Raises:** the store's own error when the listing fails.

```python
import zenthros

total = sum(item.size for item in zenthros.files.ls("s3://my-bucket/zenthros/reports/"))
```

### zenthros.files.open

```python
zenthros.files.open(uri, mode="rb", encoding="utf-8")
```

A file object for `uri`. `"rb"` and `"r"` read the whole object into memory first. `"wb"` and `"w"` send what was written when the file is closed, and send nothing when the `with` block raised. A local path is opened as it is (its folders made for writing).

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `uri` | `str` or path | required | `s3://...`, `gs://...` or a local path. |
| `mode` | `str` | `"rb"` | `"rb"`, `"r"`, `"wb"` or `"w"`. |
| `encoding` | `str` | `"utf-8"` | The text encoding, for `"r"` and `"w"`. |

**Returns:** a file object.

**Raises:** `ValueError` for any other mode. `FileNotFoundError` when reading what is not there.

```python
import pandas as pd
import zenthros

df = pd.read_parquet(zenthros.files.open("s3://my-bucket/zenthros/clean.parquet"))
with zenthros.files.open("s3://my-bucket/zenthros/report.txt", "w") as fh:
    fh.write(f"rows: {len(df)}\n")
```

## zenthros.parquet

```python
zenthros.parquet(columns=None)
```

A contract for `inputs=` or `outputs=` of a call: the value is a Parquet file (or a folder of them), with these columns at least. Only the file's footer is read, never its data. A contract travels with the function, so it holds in a `Pipeline` or out of one, and `Pipeline.plan()` compares one step's `outputs=` with the next one's `inputs=`. An output that fails ends the call with the reason `OUTPUT_CONTRACT`, and its value reaches neither the next step nor the cache. An input that fails is refused with `INPUT_CONTRACT`: here, before a job exists, when the value is visible here; else where it runs. Other contracts are the strings `"json"`, `"file"` and `"dir"`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `columns` | `dict` | `None` | `{name: type}`, the type as pyarrow writes it (`"int64"`, `"float32"`, `"string"`). Other columns may be there. With columns, pyarrow is needed where it is checked. |

**Returns:** a contract.

**Raises:** nothing when made. When checked: `zenthros.contracts.ContractError` (a `zenthros.errors.ValidationError`) for an output that is not Parquet, lacks a column or has another type, and `zenthros.contracts.InputContractError` for an input.

```python
import zenthros

@zenthros.cloud(outputs=zenthros.parquet(columns={"user_id": "int64", "score": "float32"}))
def score(path):
    out = "s3://my-bucket/zenthros/scores.parquet"
    ...
    return out
```

## zenthros.Artifact

```python
zenthros.Artifact(uri, sha256="", size=None, version="")
```

A reference to stored bytes: where they are and the SHA-256 they must hash to. Only the reference travels; the bytes stay in the store or the path. As a call's argument, its `uri` is checked for reach like any path or URI, and the cache keys it by its `sha256` when given, without reading the bytes. The function receives the `Artifact` itself: read it with `zenthros.files.open(artifact.uri)` (`str(artifact)` is its URI too). `os.fspath(artifact)` gives the path of a local or `file://` one, and raises `TypeError` for a bucket's. A cache entry gives its result and outputs this way (`zenthros.cache.find(...).artifact`, `.outs()`).

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `uri` | `str` | required | `s3://...`, `gs://...`, `file://...` or a local path. |
| `sha256` | `str` | `""` | The SHA-256 the bytes must hash to, as hex. |
| `size` | `int` | `None` | Their size in bytes. |
| `version` | `str` | `""` | What the store said its version was when last seen (a generation, an ETag), for what is too large to read each time. |

**Returns:** an `Artifact`. It cannot be changed once made.

**Raises:** nothing when made.

```python
import zenthros

@zenthros.cloud
def rows(data):
    import pandas as pd
    return len(pd.read_parquet(zenthros.files.open(data.uri)))

data = zenthros.Artifact("s3://my-bucket/zenthros/clean.parquet", sha256="9f2c1e...", size=48213)
print(rows.remote(data, on="caja").result())
```

## zenthros.set_store

```python
zenthros.set_store(uri, out=None)
```

Keeps `uri` as your bucket, where your calls and results go on a hub that keeps none of them. First it checks that the bucket answers: a small object is written, read back and removed, with your own credentials. Then it is kept with your login (the current context of your config), and the line that sets the bucket's 30-day rule for `<store>/data/` is printed for the person to run. `uri=None` forgets it. The command line does the same: `zenthros store s3://my-bucket/zenthros`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `uri` | `str` | required | `s3://bucket/prefix` or `gs://bucket/prefix`. `None` forgets the bucket kept. |
| `out` | text stream | `None` | Where to print what it says. `None` is standard output. |

**Returns:** the store as kept (`"s3://my-bucket/zenthros"`), or `""` after `uri=None`.

**Raises:** `zenthros.errors.ValidationError` when `uri` is not `s3://bucket/prefix` or `gs://bucket/prefix`; when the bucket does not answer to your credentials here; or when the library it needs is missing (`pip install "zenthros[s3,seal]"` for `s3://`, `"zenthros[gcs,seal]"` for `gs://`).

```python
import zenthros

zenthros.set_store("gs://my-bucket/zenthros")
```

## zenthros.cache

The result cache that `cache=True` uses (see [run.md](run.md)): not doing again what already ran. An entry is one signed manifest at `<store>/cache/<key>.json`, next to the bytes it pins. The key is made of the function's code, the project's files, its arguments (a path by its content, a URI by the object's version), its `deps=`, image, packages, Python, GPU, environment variable values, secret names and contracts; not where or when it ran. Only the SDK writes an entry, after it checked a result against the digests its job reported, and signs it with your key (`cache.key` next to your config). An entry with no valid signature is never opened. The functions without `homes=` look in this project's local store and, when you have one, your bucket. `zenthros cache ls|show|rm|sync|key` and `zenthros clean --cache --older 180d` are the same from the command line.

### zenthros.cache.find

```python
zenthros.cache.find(fn, *args, on=None, code_dir=".", deps=None, **kwargs)
```

The good entry of `fn(*args, **kwargs)`, as a call that gives its value, or `None`. Use it to give a `Pipeline` an `upstream=` value without running anything. A cloud function brings its options to the key (`code_dir=`, `deps=`, packages, contracts).

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `fn` | function | required | The function, or a `@zenthros.cloud` function with its options. |
| `args` | any | none | Its positional arguments, as the call would get them. |
| `on` | place | `None` | Where the call would run: it picks the stores looked in and the image that counts. |
| `code_dir` | `str` | `"."` | The project folder whose files count, unless the cloud function says another. |
| `deps` | `list` or `dict` | `None` | What the call reads without taking it as an argument, as in `zenthros.run`. |
| `kwargs` | keyword arguments | none | Its keyword arguments. |

**Returns:** a cached call or `None`. The cached call has `result()`, `artifact` (its result as a `zenthros.Artifact`), `outs()`, `name` (the job that made it), `where`, and `status()` (`"reused"`).

**Raises:** `zenthros.cache.CannotKey` when a URI among the arguments holds nothing, or its store says no version. `zenthros.errors.ValidationError` when `deps=` as a list names a path that is not here.

```python
import zenthros

@zenthros.cloud
def prep(raw):
    return "s3://my-bucket/zenthros/clean.parquet"

found = zenthros.cache.find(prep, "s3://my-bucket/zenthros/raw.parquet", on="caja")
if found is not None:
    print(found.result())
```

### zenthros.cache.ls

```python
zenthros.cache.ls(homes=None)
```

Every entry of the stores, good or not.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `homes` | `list` | `None` | The stores to look in. `None` is this project's local store and your bucket. Leave it out. |

**Returns:** a `list` of `dict`, each with `key`, `uri`, `valid`, `fn`, `job`, `where`, `at`, `took_s`, and either `result`, `outs` and `parts` (a good entry) or `problem` (why it is not good).

**Raises:** the store's own error when a listing fails: an incomplete answer is never shown as an empty cache.

```python
import zenthros

for entry in zenthros.cache.ls():
    print(entry["key"][:12], entry["fn"], entry["valid"])
```

### zenthros.cache.show

```python
zenthros.cache.show(key, homes=None)
```

One entry, named by the start of its key.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `key` | `str` | required | The key, or its first characters. |
| `homes` | `list` | `None` | As in `ls()`. |

**Returns:** a `dict`, as one row of `ls()`.

**Raises:** `zenthros.errors.NotFound` when no entry starts with `key`. `zenthros.errors.ValidationError` when it names several entries.

```python
import zenthros

print(zenthros.cache.show("3fa9c2"))
```

### zenthros.cache.rm

```python
zenthros.cache.rm(key, homes=None)
```

Removes an entry: its manifest, in every store that has it. Its bytes stay until the next `clean()`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `key` | `str` | required | The key, or its first characters. |
| `homes` | `list` | `None` | As in `ls()`. |

**Returns:** a `list` of the keys removed, one per store it was in.

**Raises:** `zenthros.errors.NotFound` when no entry starts with `key`. `zenthros.errors.ValidationError` when it names several entries.

```python
import zenthros

zenthros.cache.rm("3fa9c2")
```

### zenthros.cache.clean

```python
zenthros.cache.clean(older_than, homes=None, dry_run=False, now=None)
```

Removes the entries older than `older_than`, and the copies of results under `<store>/cache/blobs/` that only they held and that are as old. The results themselves go with `zenthros clean --results`.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `older_than` | seconds or `str` | required | How old an entry must be to go: a number of seconds, or `"90m"`, `"12h"`, `"180d"`. |
| `homes` | `list` | `None` | As in `ls()`. |
| `dry_run` | `bool` | `False` | Say what would go, and remove nothing. |
| `now` | `datetime` | `None` | The time to count from, with a time zone. `None` is now. |

**Returns:** a `list` of the keys removed, or that would be with `dry_run=True`.

**Raises:** `zenthros.errors.ValidationError` when `older_than` is not a positive duration. The store's own error when a listing fails.

```python
import zenthros

print(zenthros.cache.clean("180d", dry_run=True))
```

### zenthros.cache.sync

```python
zenthros.cache.sync()
```

Enters in the cache the jobs you sent with `cache=` that ended well and that nobody read since: each is read and checked as `result()` does. It looks at this project's jobs and your Workspace's. A job that does not check out stays out, and a line says why.

No parameters.

**Returns:** a `list` of the job names entered.

**Raises:** the job client's errors when the jobs cannot be listed.

```python
import zenthros

print(zenthros.cache.sync())
```

### zenthros.cache.export_key

```python
zenthros.cache.export_key()
```

The key that signs your entries, as text, for another computer of the same person. It is made at its first use. Treat it as a secret: never print it in a log or a command; pipe it straight to `zenthros cache key import` on the other computer.

No parameters.

**Returns:** the key, as base64 text.

**Raises:** `OSError` when the key file cannot be read or made.

```python
import zenthros

key = zenthros.cache.export_key()
```

### zenthros.cache.import_key

```python
zenthros.cache.import_key(text)
```

Makes `text` this computer's signing key, so it signs and reads the entries of that key. It replaces the key there (file mode 0600). Entries signed with the old key are no longer good here.

| Parameter | Type | Default | What it is |
|---|---|---|---|
| `text` | `str` | required | The key as `export_key()` gives it. |

**Returns:** `None`.

**Raises:** `zenthros.errors.ValidationError` when it is not 32 bytes once decoded. `binascii.Error` (a `ValueError`) when it is not base64.

```python
import sys
import zenthros

zenthros.cache.import_key(sys.stdin.read())
```

## Data larger than the machine

When a table does not fit in the memory of the machine that trains, read
less of it, read it in parts, or use a machine with more memory. Reading in
parts does not always fit: measure first. In this order:

**1. Measure before loading.** `zenthros targets` says each machine's
memory. A Parquet file's schema and row count cost nothing to read, and in
memory a `Float64` column takes 8 bytes a row, a `Float32` 4, an `Int16` 2.

```python
import polars as pl

lf = pl.scan_parquet("train.parquet")
rows = lf.select(pl.len()).collect().item()      # from the file's metadata
sample = lf.head(100_000).collect()
gib = sample.estimated_size() / sample.height * rows / 2**30   # in memory, as these types
```

A machine needs room for the table, for what the model builds from it,
and for a copy or two in between: pandas' `read_parquet` and then
`drop(columns=...)` hold the table twice.

**2. Read only what is used.** A file sent with `upload=` is a local path on
the machine, so `pl.scan_parquet(path)` reads nothing until it is
collected. Before that, `.select()` the columns used, `.filter()` the rows,
and `.cast()` to `Float32` or small integers (a cast that would lose a value
fails, it does not cut it); then `.collect(engine="streaming")` processes it
in parts.

```python
frame = (pl.scan_parquet(path)
         .select(["distance", "hour", "fare"])
         .filter(pl.col("distance") > 0)
         .with_columns(pl.col("distance", "fare").cast(pl.Float32), pl.col("hour").cast(pl.Int8))
         .collect(engine="streaming"))
```

**3. If the model's matrix still does not fit, train from batches (XGBoost).**
An `xgboost.DataIter` whose `reset()` opens `lf.collect_batches()` again:
each pass reads the file once, in order. Never `lf.slice(i, n).collect()` in
a loop, which may read from the start for every batch.

- First `xgboost.QuantileDMatrix(iterator)`: it keeps the quantized matrix in
  memory, about one byte a value with `max_bin` 256 or less, plus labels,
  gradients and predictions.
- If that does not fit, `xgboost.ExtMemQuantileDMatrix(iterator)` with
  `DataIter(cache_prefix=<a folder of the job>)`: its pages go to disk and are
  read again on each round. Less memory, for disk and a little time.

```python
import xgboost as xgb

class Batches(xgb.DataIter):
    def __init__(self, path, columns, target, cache_prefix=None):
        self.path, self.columns, self.target, self.batches = path, columns, target, None
        super().__init__(cache_prefix=cache_prefix)

    def reset(self):
        lf = pl.scan_parquet(self.path).select([pl.col(c).cast(pl.Float32) for c in [*self.columns, self.target]])
        self.batches = iter(lf.collect_batches(chunk_size=1_000_000))

    def next(self, input_data):
        frame = next(self.batches, None)
        if frame is None:
            return False
        input_data(data=frame.select(self.columns).to_numpy(), label=frame[self.target].to_numpy())
        return True

matrix = xgb.QuantileDMatrix(Batches(path, columns, "fare"), max_bin=256)
# or, with its pages on disk:
matrix = xgb.ExtMemQuantileDMatrix(Batches(path, columns, "fare", cache_prefix="/tmp/xgb/train"), max_bin=256)
booster = xgb.train({"tree_method": "hist", "max_depth": 8}, matrix, num_boost_round=100)
```

LightGBM and CatBoost do not take batches this simply: train them on a
sample, or on a machine with more memory.

**4. If not even that fits, a machine with more memory:**
`zenthros attach caja --type local --memory 8g` resizes This computer in
place, or the call goes to a bigger VM (`on=`).

**5.** `polars` and `xgboost` go in `packages=` at the versions the person
has, as everything the function imports.

The whole example, with synthetic data, is `examples/data/lazy_training.py`
in the SDK's repository.

**Measured** (2026-11-20, a This-computer-sized container: 2 CPUs, 4 GiB, no
swap; XGBoost 3.0.5, Polars 2.1.0; the NYC taxi train, 26.6 M rows, 26
features read as Float32, a 2.2 GB Parquet; 100 rounds, depth 8, `hist`):

| How it reads the data | Peak memory (not file cache) | Time | Written to disk | Validation RMSE |
|---|---|---|---|---|
| pandas `read_parquet` of it all, then `XGBRegressor.fit` (the table twice, then the matrix) | Past 4 GiB: killed, "probably ran out of memory" | | | |
| Batches into `QuantileDMatrix` | 3.9 GiB, at the edge of 4 | 4 min 27 s | 0 | 1.797 |
| Batches into `ExtMemQuantileDMatrix` | 1.8 GiB | 4 min 37 s | 1.6 GB | 1.797 |
