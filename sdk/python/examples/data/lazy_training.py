# SPDX-License-Identifier: Apache-2.0

"""XGBoost on data larger than the machine's memory: read with Polars,
lazily and in batches, into a QuantileDMatrix (it keeps only the quantized
matrix, about one byte a value), or into an ExtMemQuantileDMatrix, whose
pages go to disk.

    python lazy_training.py                    # here, on synthetic data
    python lazy_training.py --on caja          # on a machine of yours
    python lazy_training.py --on caja --external

The order, as the skill's reference/data.md says: measure before loading,
read only the columns used, in 32 bits, then batches into QuantileDMatrix;
if that still does not fit, ExtMemQuantileDMatrix; if not even that, a
machine with more memory.
"""

import argparse
import os
import tempfile

import zenthros

# The caller's versions, pinned: the same module name from another package
# (xgboost-cpu) gives other numbers.
PACKAGES = ["polars==2.1.0", "xgboost==3.0.5", "numpy==2.3.2"]
CHUNK_ROWS = 1_000_000


def synthetic(folder, rows=200_000, features=20, seed=0):
    """A train and a validation Parquet (zstd) of a linear target with
    noise: (train path, val path)."""
    import numpy as np
    import polars as pl

    rng = np.random.default_rng(seed)
    weights = rng.normal(size=features).astype(np.float32)
    paths = []
    for name, n in (("train", rows), ("val", rows // 4)):
        x = rng.normal(size=(n, features)).astype(np.float32)
        y = x @ weights + np.sin(x[:, 0] * 3) + rng.normal(scale=0.1, size=n).astype(np.float32)
        frame = pl.DataFrame({f"x{i}": x[:, i] for i in range(features)} | {"y": y})
        paths.append(os.path.join(folder, f"{name}.parquet"))
        frame.write_parquet(paths[-1], compression="zstd")
    return tuple(paths)


def lazy(path, columns, target):
    """The Parquet at path as a LazyFrame of columns and target in Float32:
    nothing is read until it is collected."""
    import polars as pl

    return pl.scan_parquet(path).select([pl.col(c).cast(pl.Float32) for c in [*columns, target]])


def batches(path, columns, target, chunk_rows=CHUNK_ROWS, cache_prefix=None):
    """An xgboost.DataIter over the Parquet at path. Each pass (reset) opens
    the file again and reads it once, in order, chunk_rows at a time: never
    lf.slice(i, n).collect() in a loop, which may read from the start for
    every batch. cache_prefix: where ExtMemQuantileDMatrix keeps its pages."""
    import xgboost as xgb

    class Batches(xgb.DataIter):
        def __init__(self):
            self.passes, self.rows_per_pass, self._open = 0, [], None
            super().__init__(cache_prefix=cache_prefix)

        def reset(self):
            self._open = iter(lazy(path, columns, target).collect_batches(chunk_size=chunk_rows))
            self.passes += 1
            self.rows_per_pass.append(0)

        def next(self, input_data):
            frame = next(self._open, None)
            if frame is None:
                return False
            self.rows_per_pass[-1] += frame.height
            input_data(data=frame.select(columns).to_numpy(), label=frame[target].to_numpy())
            return True

    return Batches()


@zenthros.cloud(packages=PACKAGES, code_dir=None)
def train(train_path, val_path, target="y", rounds=100, external=False, chunk_rows=CHUNK_ROWS):
    """XGBoost trained from batches of train_path; its RMSE on val_path, read
    in batches too, and how many passes it made over the file."""
    import numpy as np
    import polars as pl
    import xgboost as xgb

    # Measure before loading: the schema costs nothing, and a Float32 column
    # is 4 bytes a row in memory.
    columns = [c for c in pl.scan_parquet(train_path).collect_schema().names() if c != target]
    if external:
        cache = tempfile.mkdtemp(prefix="xgb-pages-")
        data = batches(train_path, columns, target, chunk_rows, cache_prefix=os.path.join(cache, "train"))
        matrix = xgb.ExtMemQuantileDMatrix(data, max_bin=256)
    else:
        data = batches(train_path, columns, target, chunk_rows)
        matrix = xgb.QuantileDMatrix(data, max_bin=256)
    params = {"tree_method": "hist", "max_depth": 8, "learning_rate": 0.1, "objective": "reg:squarederror", "seed": 0}
    booster = xgb.train(params, matrix, num_boost_round=rounds)
    squared, n = 0.0, 0
    for frame in lazy(val_path, columns, target).collect_batches(chunk_size=chunk_rows):
        predicted = booster.inplace_predict(frame.select(columns).to_numpy()).astype(np.float64)
        squared += float(np.sum((predicted - frame[target].to_numpy()) ** 2))
        n += frame.height
    return {"rmse": (squared / n) ** 0.5, "passes": data.passes, "rows_per_pass": data.rows_per_pass}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--on", help="a machine of yours (zenthros targets); here if not given")
    parser.add_argument("--external", action="store_true", help="ExtMemQuantileDMatrix: its pages on disk")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as folder:
        train_path, val_path = synthetic(folder)
        if args.on:
            # A path given as an argument is uploaded once (upload=, a bucket
            # needed) and is a local path on the machine.
            print(train.remote(train_path, val_path, external=args.external, on=args.on, upload=True))
        else:
            print(train(train_path, val_path, external=args.external))
