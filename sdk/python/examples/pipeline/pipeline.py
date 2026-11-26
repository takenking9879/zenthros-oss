# SPDX-License-Identifier: Apache-2.0

"""The two steps flow.ipynb sends to a machine, one engine each:

    pipeline.models(data, out, profile)   features with Polars, then three scikit-learn models
    pipeline.tables(data, out, profile)   the same features and summary with Spark

data is where gen_data wrote the source tables (a folder here, or the
s3:// URI ws.upload gave it); out is where the step leaves what it made:

    <out>/features     the feature table
    <out>/summary      customers, orders and spend by segment and country
    <out>/result.json
    <out>/_SUCCESS     written last: without it the step did not end

A step that ended is not run again: a call sent twice, or retried on another
machine after its Spot machine was taken, returns what the first attempt
left. A step without its marker starts over from an empty folder.
"""

import time

import limits
import profiles
import store

SUCCESS = "_SUCCESS"
RESULT = "result.json"
# A call is a fresh process: polars must start with the profile's threads.
STRICT_THREADS = True


def models(data, out, profile=""):
    """Features with Polars and the three models; returns their scores (also
    in result.json)."""
    return _once("polars", out, profile, lambda chosen: _models(data, out, chosen))


def tables(data, out, profile=""):
    """Features and summary with Spark; returns their rows (also in
    result.json)."""
    return _once("spark", out, profile, lambda chosen: _tables(data, out, chosen))


def _once(engine, out, profile, step):
    chosen = profiles.get(profile)
    if store.read_json(store.join(out, SUCCESS)) is not None:
        return store.read_json(store.join(out, RESULT))
    store.remove_dir(out)
    started = time.perf_counter()
    result = {"engine": engine, "profile": chosen.name, **step(chosen)}
    result["seconds"]["total"] = round(time.perf_counter() - started, 3)
    result["limits"] = {"cpus": limits.cpus(chosen.cpus), "memory_bytes": limits.memory_bytes(chosen.memory_gb << 30)}
    store.write_json(store.join(out, RESULT), result)
    store.write_json(store.join(out, SUCCESS), {"engine": engine})
    return result


def _models(data, out, chosen):
    import pipeline_polars
    import train

    started = time.perf_counter()
    written = pipeline_polars.build(data, out, chosen, strict=STRICT_THREADS)
    built = time.perf_counter()
    features = store.join(out, "features")
    scores = train.fit(features, chosen)
    return {
        # Counted from the files' footers: nothing is computed again.
        "rows": {"features": store.parquet_rows(features), "summary": store.parquet_rows(store.join(out, "summary")),
                 "training": scores["rows"]},
        "models": scores["models"], "baselines": scores["baselines"],
        "seconds": {"features": round(built - started, 3), "train": round(time.perf_counter() - built, 3)},
        "engine_threads": written["threads"],
    }


def _tables(data, out, chosen):
    import pipeline_spark

    started = time.perf_counter()
    written = pipeline_spark.build(data, out, chosen)
    return {
        "rows": {"features": store.parquet_rows(store.join(out, "features")),
                 "summary": store.parquet_rows(store.join(out, "summary"))},
        "seconds": {"features": round(time.perf_counter() - started, 3)},
        "engine_threads": written["threads"],
    }
