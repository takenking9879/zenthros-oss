# SPDX-License-Identifier: Apache-2.0

"""Three models on the feature table, whichever engine wrote it.

    linear regression          the baseline a model must beat
    random forest              the slow one: it is what uses the CPUs
    histogram gradient boosting

The target is what a customer spends the month after. The models are scored
on the last months of the data, which they did not see: a forecast is
judged on the future.

load() is the one place in the pipeline where a table becomes memory, so it
is where the profile's row cap applies. The cap is a filter the Parquet
reader applies as it goes (whole customers, chosen by their id), never a
cut on a table already read.
"""

import time

import limits
import numpy as np
import profiles
import pyarrow.compute as pc
import pyarrow.dataset as ds
import queries
import store
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from threadpoolctl import threadpool_limits

COLUMNS = [*queries.CATEGORICAL, *queries.NUMERIC]
# The months held out to score on: the last ones.
TEST_MONTHS = 2
# Customers are sampled in 1024ths, by a scramble of their id.
_BUCKETS = 1024
_SCRAMBLE = 40503


def _head(dataset, sample, rows):
    """At most `rows` rows of the dataset that pass `sample`."""
    return ds.Scanner.from_dataset(dataset, filter=sample).head(rows)


def load(features_uri, cap):
    """The feature table as a DataFrame of at most cap rows. Over the cap it
    is a sample of whole customers, so each one's months stay together."""
    filesystem_, _ = store.filesystem(features_uri)
    dataset = ds.dataset(store.parquet_files(features_uri), filesystem=filesystem_, format="parquet")
    total = store.parquet_rows(features_uri)
    if total <= cap:
        return dataset.to_table().to_pandas()
    # Aim a little under the cap: customers differ in how many months they have.
    keep = max(1, int(_BUCKETS * 0.9 * cap / total))
    bucket = pc.bit_wise_and(pc.multiply(pc.field("customer_id"), _SCRAMBLE), _BUCKETS - 1)
    return _head(dataset, bucket < keep, cap).to_pandas()


def split(frame):
    """(train, test): the last TEST_MONTHS months are the test."""
    first_test = int(frame["month_idx"].max()) - TEST_MONTHS + 1
    return frame[frame["month_idx"] < first_test], frame[frame["month_idx"] >= first_test]


def models(profile):
    """The three models, in the order they are fitted. Threads are the
    profile's, at most the container's."""
    threads = limits.cpus(profile.cpus)
    scaled = ColumnTransformer([("categorical", OneHotEncoder(handle_unknown="ignore"), list(queries.CATEGORICAL)),
                                ("numeric", StandardScaler(), queries.NUMERIC)])

    def plain():
        return ColumnTransformer([("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                                   list(queries.CATEGORICAL)), ("numeric", "passthrough", queries.NUMERIC)])

    return {
        "linear_regression": Pipeline([("columns", scaled), ("model", LinearRegression())]),
        "random_forest": Pipeline([("columns", plain()), ("model", RandomForestRegressor(
            n_estimators=profile.forest_trees, min_samples_leaf=5, max_features=0.5, n_jobs=threads, random_state=0))]),
        "hist_gradient_boosting": Pipeline([("columns", plain()), ("model", HistGradientBoostingRegressor(
            max_iter=200, learning_rate=0.1, random_state=0))]),
    }


def _scores(actual, predicted):
    return {"rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
            "mae": float(mean_absolute_error(actual, predicted)), "r2": float(r2_score(actual, predicted))}


def fit(features_uri, profile):
    """Fit and score the three models. Returns their scores, the rows used
    and two baselines to read the scores against."""
    if profile.months < profiles.MIN_MONTHS:
        raise ValueError(f"profile {profile.name}: {profile.months} months of data; training needs "
                         f"{profiles.MIN_MONTHS} (profiles.MIN_MONTHS says why)")
    frame = load(features_uri, profile.train_rows)
    fitted, held_out = split(frame)
    actual = held_out[queries.TARGET]
    threads = limits.cpus(profile.cpus)
    scores = {}
    # BLAS and OpenMP size their pools from the machine too.
    with threadpool_limits(limits=threads):
        for name, model in models(profile).items():
            started = time.perf_counter()
            model.fit(fitted[COLUMNS], fitted[queries.TARGET])
            seconds = time.perf_counter() - started
            scores[name] = {**_scores(actual, model.predict(held_out[COLUMNS])), "fit_seconds": round(seconds, 3)}
    baselines = {"mean": _scores(actual, np.full(len(actual), fitted[queries.TARGET].mean())),
                 "previous_month": _scores(actual, held_out["prev_spend"])}
    return {"rows": {"table": store.parquet_rows(features_uri), "loaded": len(frame), "train": len(fitted),
                     "test": len(held_out)},
            "threads": threads, "models": scores, "baselines": baselines}
