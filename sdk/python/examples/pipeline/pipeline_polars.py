# SPDX-License-Identifier: Apache-2.0

"""The pipeline on Polars: lazy from the scan to the file.

    scan_parquet (three tables)  ->  SQL (join, aggregate)  ->  features (windows)  ->  sink_parquet

Nothing here returns a table to Python. scan_parquet reads only a schema,
the SQL and the expressions add to a plan, and sink_parquet runs the plan in
the streaming engine, which reads the files in pieces and writes as it goes.
What bounds memory is then the engine and the container's limit, not the
size of the data. Polars has no memory limit of its own to set.

Its thread pool is sized once, when polars is first imported: this module
imports it only inside threads(), after setting POLARS_MAX_THREADS.
"""

import math
import os
import sys

import limits
import queries
import store

# Each table is one file: the streaming sink writes it as it goes.
PART = "part-000.parquet"


def threads(profile, strict=False):
    """Start polars with the profile's threads, at most the container's,
    and return how many it has. If something imported polars before, its
    pool is already sized; strict refuses a pool larger than asked."""
    wanted = limits.cpus(profile.cpus)
    if "polars" not in sys.modules:
        os.environ["POLARS_MAX_THREADS"] = str(wanted)
    import polars as pl

    have = pl.thread_pool_size()
    if strict and have > wanted:
        raise RuntimeError(f"polars already runs {have} threads and this profile allows {wanted}: set "
                           f"POLARS_MAX_THREADS={wanted} before polars is imported")
    return have


def scan(base):
    """The three source tables, as plans."""
    import polars as pl

    return {name: pl.scan_parquet(store.join(base, "raw", name, "*.parquet"), storage_options=store.polars_options(base))
            for name in ("customers", "orders", "order_lines")}


def monthly(tables):
    """One row per customer and month it bought in: the SQL."""
    import polars as pl

    return pl.SQLContext(tables).execute(queries.MONTHLY_SQL)


def summary(monthly_frame):
    import polars as pl

    counted = pl.SQLContext(monthly=monthly_frame).execute(queries.SUMMARY_SQL)
    return counted.select(_typed(pl, queries.SUMMARY_SCHEMA))


def _typed(pl, schema):
    kinds = {"int64": pl.Int64, "int32": pl.Int32, "float64": pl.Float64, "string": pl.String}
    return [pl.col(name).cast(kinds[kind]) for name, kind in schema.items()]


def features(monthly_frame, months):
    """The feature table (queries.SCHEMA) from the monthly one. months is
    how many the data spans: the last has no month after it, so no target."""
    import polars as pl

    col = pl.col
    by_month = {"partition_by": "customer_id", "order_by": "month_idx"}

    def earlier(name, rows):
        """The value `rows` purchases ago (negative: ahead)."""
        return col(name).shift(rows).over(**by_month)

    def if_within(gap, value, months_):
        # A first purchase has no gap: null is not within, so it adds 0.
        return pl.when(gap <= months_).then(value).otherwise(0)

    gap_1 = col("month_idx") - earlier("month_idx", 1)
    gap_2 = col("month_idx") - earlier("month_idx", 2)
    gap_next = earlier("month_idx", -1) - col("month_idx")
    angle = (col("month_idx") % 12) * (2 * math.pi / 12)
    shares = {f"share_{name}": pl.when(col("spend") > 0).then(col(f"spend_{name}") / col("spend")).otherwise(0.0)
              for name in queries.SHARE_CATEGORIES}
    frame = monthly_frame.with_columns(
        avg_order_value=col("spend") / col("n_orders"),
        avg_line_value=col("spend") / col("n_lines"),
        prev_spend=if_within(gap_1, earlier("spend", 1), 1),
        prev_orders=if_within(gap_1, earlier("n_orders", 1), 1),
        spend_3m=col("spend") + if_within(gap_1, earlier("spend", 1), 2) + if_within(gap_2, earlier("spend", 2), 2),
        orders_3m=col("n_orders") + if_within(gap_1, earlier("n_orders", 1), 2) + if_within(gap_2, earlier("n_orders", 2), 2),
        spend_cum=col("spend").cum_sum().over(**by_month),
        orders_cum=col("n_orders").cum_sum().over(**by_month),
        months_since_prev=gap_1.fill_null(-1),
        tenure_months=col("month_idx") - col("month_idx").min().over("customer_id"),
        month_sin=angle.sin(),
        month_cos=angle.cos(),
        **shares,
        **{queries.TARGET: if_within(gap_next, earlier("spend", -1), 1)},
    )
    # After the windows: a month's target is read from the month this drops.
    return frame.filter(col("month_idx") < months - 1).select(_typed(pl, queries.SCHEMA))


def _sink(frame, uri):
    """The plan that writes frame to uri; nothing runs yet."""
    if not uri.startswith(store.S3):
        os.makedirs(os.path.dirname(os.path.abspath(uri)), exist_ok=True)
    return frame.sink_parquet(uri, compression="zstd", storage_options=store.polars_options(uri), lazy=True)


def build(base, out, profile, strict=False):
    """Read <base>/raw and write the tables <out>/features and
    <out>/summary. Returns where, and the threads polars ran."""
    running = threads(profile, strict)
    import polars as pl

    monthly_frame = monthly(scan(base))
    written = {"features": store.join(out, "features"), "summary": store.join(out, "summary")}
    # One run for both files: the join and the aggregation they share are
    # done once.
    pl.collect_all([_sink(features(monthly_frame, profile.months), store.join(written["features"], PART)),
                    _sink(summary(monthly_frame), store.join(written["summary"], PART))], engine="streaming")
    return {**written, "threads": running}
