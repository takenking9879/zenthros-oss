# SPDX-License-Identifier: Apache-2.0

"""The two sizes of the example pipeline, as configuration.

    small   2 CPUs, 4 GiB   the container on your own computer
    full    4 CPUs, 8 GiB   the Spot machine

A profile says how much data there is and how much of the box each engine
may take. Nothing else in the pipeline holds a size: to try another, add a
profile here. PIPELINE_PROFILE picks one; a job gets it through its env.
"""

import os
from dataclasses import dataclass

PROFILE_ENV = "PIPELINE_PROFILE"
# The first month of the data: month 0 of every month index.
START_YEAR = 2024
# The fewest months a profile may span: a year and the months held out to
# score on, plus the one their target is read from. With less, the models
# are asked about a season they never saw and lose to guessing the average
# (measured: with 8 to 14 months R2 went from -0.37 to 0.37 by the luck of
# which months were held out; from 15 on it stayed between 0.3 and 0.5).
MIN_MONTHS = 15


@dataclass(frozen=True)
class Profile:
    name: str
    # The box the profile is sized for. limits.py takes the smaller of this
    # and what the container really has.
    cpus: int
    memory_gb: int
    # The data: customers, their orders over `months` months, and each
    # order's lines. Each table is written and read as `files` Parquet files.
    customers: int
    orders: int
    lines_per_order: int
    months: int
    files: int
    # Training reads the feature table into memory: at most this many rows.
    train_rows: int
    forest_trees: int
    # Spark runs in this process's machine (local mode): its JVM heap, the
    # partitions of a shuffle, and the most it may hand back to Python.
    spark_driver_memory: str
    spark_shuffle_partitions: int
    spark_max_result: str


PROFILES = {
    # 4.5 million order lines, 30 MB of Parquet.
    "small": Profile(name="small", cpus=2, memory_gb=4, customers=50_000, orders=1_500_000, lines_per_order=3,
                     months=18, files=8, train_rows=100_000, forest_trees=100,
                     spark_driver_memory="1536m", spark_shuffle_partitions=8, spark_max_result="256m"),
    # 24 million order lines, 150 MB of Parquet.
    "full": Profile(name="full", cpus=4, memory_gb=8, customers=200_000, orders=8_000_000, lines_per_order=3,
                    months=36, files=16, train_rows=400_000, forest_trees=200,
                    spark_driver_memory="3g", spark_shuffle_partitions=16, spark_max_result="512m"),
}


def get(name=""):
    """The named profile, else PIPELINE_PROFILE's, else small."""
    name = name or os.environ.get(PROFILE_ENV, "") or "small"
    if name not in PROFILES:
        raise ValueError(f"profile {name!r}: one of {sorted(PROFILES)}")
    return PROFILES[name]


def megabytes(size):
    """A JVM size ("1536m", "3g") in MiB."""
    number, unit = int(size[:-1]), size[-1].lower()
    if unit not in ("m", "g"):
        raise ValueError(f"size {size!r}: a number and m or g")
    return number * (1024 if unit == "g" else 1)
