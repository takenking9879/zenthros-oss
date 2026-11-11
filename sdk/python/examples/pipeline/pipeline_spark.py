# SPDX-License-Identifier: Apache-2.0

"""The pipeline on Spark: PySpark in local mode, lazy from the read to the write.

    read.parquet (three tables)  ->  SQL (join, aggregate)  ->  features (windows)  ->  write.parquet

Local mode is Spark inside this job's own machine: one JVM, as many task
threads as the profile allows. It is not a SparkApplication, which Zenthros
runs on Kubernetes only; here the job is an ordinary one and the machine
needs Java.

Nothing here returns rows to Python: the only actions are the two writes.
Memory is bounded by settings, all from the profile and the container: the
JVM heap (never more than half the box), the task threads, the partitions of
a shuffle and the most Spark may hand to the driver.

build() runs Spark in a process of its own. Stopping a Spark session does
not end its JVM, which would keep its heap while the models train; a process
that exits gives everything back.
"""

import contextlib
import dataclasses
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import tempfile

import limits
import profiles
import queries
import spark_jars
import store

_SPARK_TYPES = {"int64": "bigint", "int32": "int", "float64": "double", "string": "string"}


def conf(profile, base, jars=()):
    """Spark's settings for this profile in this container. jars: the S3
    connector's, when pyspark does not carry them (connector_jars)."""
    threads = limits.cpus(profile.cpus)
    half_the_box = limits.memory_bytes(profile.memory_gb << 30) // 2 >> 20
    settings = {
        "spark.master": f"local[{threads}]",
        "spark.app.name": "zenthros-pipeline",
        "spark.driver.memory": f"{min(profiles.megabytes(profile.spark_driver_memory), half_the_box)}m",
        "spark.driver.maxResultSize": profile.spark_max_result,
        "spark.sql.shuffle.partitions": str(profile.spark_shuffle_partitions),
        "spark.default.parallelism": str(threads),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.parquet.compression.codec": "zstd",
        "spark.ui.enabled": "false",
        # One machine: nothing listens beyond it.
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
    }
    if base.startswith(store.S3):
        # Credentials are never settings: S3A takes them from the process's
        # environment or the machine's role, as every other library here.
        endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or ""
        if endpoint:
            settings["spark.hadoop.fs.s3a.endpoint"] = endpoint
            settings["spark.hadoop.fs.s3a.path.style.access"] = "true"
            settings["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(endpoint.startswith("https://")).lower()
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if region:
            settings["spark.hadoop.fs.s3a.endpoint.region"] = region
    if jars:
        # Set before the JVM starts: local mode's driver is its executor.
        settings["spark.driver.extraClassPath"] = ":".join(jars)
    return settings


def connector_jars(base):
    """The S3 connector's jars to name, downloaded if pyspark lacks them
    (a pool's toolchain): only for s3://."""
    return spark_jars.for_s3() if base.startswith(store.S3) else []


def session(profile, base):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in conf(profile, base, connector_jars(base)).items():
        builder = builder.config(key, value)
    spark = builder.config("spark.local.dir", tempfile.mkdtemp(prefix="spark-")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def monthly(spark, base):
    """One row per customer and month it bought in: the SQL."""
    for name in ("customers", "orders", "order_lines"):
        spark.read.parquet(store.spark_uri(store.join(base, "raw", name))).createOrReplaceTempView(name)
    return spark.sql(queries.MONTHLY_SQL)


def summary(spark, monthly_frame):
    monthly_frame.createOrReplaceTempView("monthly")
    return spark.sql(queries.SUMMARY_SQL).selectExpr(*_typed(queries.SUMMARY_SCHEMA))


def _typed(schema):
    return [f"CAST(`{name}` AS {_SPARK_TYPES[kind]}) AS `{name}`" for name, kind in schema.items()]


def features(monthly_frame, months):
    """The feature table (queries.SCHEMA) from the monthly one. months is
    how many the data spans: the last has no month after it, so no target."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    col = F.col
    by_month = Window.partitionBy("customer_id").orderBy("month_idx")
    to_date = by_month.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    def earlier(name, rows):
        """The value `rows` purchases ago (negative: ahead)."""
        return F.lag(name, rows).over(by_month)

    def if_within(gap, value, months_):
        # A first purchase has no gap: null is not within, so it adds 0.
        return F.when(gap <= months_, value).otherwise(F.lit(0))

    gap_1 = col("month_idx") - earlier("month_idx", 1)
    gap_2 = col("month_idx") - earlier("month_idx", 2)
    gap_next = earlier("month_idx", -1) - col("month_idx")
    angle = (col("month_idx") % 12) * (2 * math.pi / 12)
    columns = {
        "avg_order_value": col("spend") / col("n_orders"),
        "avg_line_value": col("spend") / col("n_lines"),
        **{f"share_{name}": F.when(col("spend") > 0, col(f"spend_{name}") / col("spend")).otherwise(F.lit(0.0))
           for name in queries.SHARE_CATEGORIES},
        "prev_spend": if_within(gap_1, earlier("spend", 1), 1),
        "prev_orders": if_within(gap_1, earlier("n_orders", 1), 1),
        "spend_3m": col("spend") + if_within(gap_1, earlier("spend", 1), 2) + if_within(gap_2, earlier("spend", 2), 2),
        "orders_3m": col("n_orders") + if_within(gap_1, earlier("n_orders", 1), 2) + if_within(gap_2, earlier("n_orders", 2), 2),
        "spend_cum": F.sum("spend").over(to_date),
        "orders_cum": F.sum("n_orders").over(to_date),
        "months_since_prev": F.coalesce(gap_1, F.lit(-1)),
        "tenure_months": col("month_idx") - F.min("month_idx").over(Window.partitionBy("customer_id")),
        "month_sin": F.sin(angle),
        "month_cos": F.cos(angle),
        queries.TARGET: if_within(gap_next, earlier("spend", -1), 1),
    }
    frame = monthly_frame.withColumns(columns)
    # After the windows: a month's target is read from the month this drops.
    return frame.where(col("month_idx") < months - 1).selectExpr(*_typed(queries.SCHEMA))


def build_here(base, out, profile):
    """build(), in this process: its JVM stays until the process ends."""
    from pyspark import StorageLevel

    spark = session(profile, base)
    try:
        # Both tables read the monthly one: kept on disk, the join and the
        # aggregation are done once and hold no memory.
        monthly_frame = monthly(spark, base).persist(StorageLevel.DISK_ONLY)
        written = {"features": store.join(out, "features"), "summary": store.join(out, "summary")}
        features(monthly_frame, profile.months).write.mode("overwrite").parquet(store.spark_uri(written["features"]))
        summary(spark, monthly_frame).write.mode("overwrite").parquet(store.spark_uri(written["summary"]))
        return {**written, "threads": limits.cpus(profile.cpus)}
    finally:
        spark.stop()


def build(base, out, profile):
    """Read <base>/raw and write the tables <out>/features and
    <out>/summary, in a process that ends with its JVM. Returns where, and
    the task threads Spark ran."""
    command = [sys.executable, os.path.abspath(__file__), base, out, json.dumps(dataclasses.asdict(profile))]
    # Files, not pipes: a JVM that outlived its stage would hold a pipe
    # open, and reading it would wait for that JVM instead of ending it.
    with tempfile.TemporaryFile("w+") as printed, tempfile.TemporaryFile("w+") as logged:
        stage = subprocess.Popen(command, stdout=printed, stderr=logged, start_new_session=True)
        try:
            stage.wait()
        finally:
            # A stage that was killed could not end its JVM: nothing of its
            # process group outlives it.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(stage.pid, signal.SIGKILL)
        printed.seek(0)
        logged.seek(0)
        stdout, stderr = printed.read(), logged.read()
    if stage.returncode != 0:
        # The stage prints its reason last; a Java stack trace above it is
        # hundreds of lines.
        lines = stderr.strip().splitlines()
        reason = lines[-1] if lines else f"no output ({_free_disk()})"
        raise RuntimeError(f"the Spark stage exited {stage.returncode}: {reason}\n" + "\n".join(lines[-40:-1]))
    return json.loads(stdout.splitlines()[-1])


def _free_disk():
    """What is left where the stage writes (its output, Spark's spill, the
    S3 connector's jars): a stage that cannot write cannot say why either
    (Python exits 120), and on an 8 GB disk that happened."""
    where = tempfile.gettempdir()
    return f"{shutil.disk_usage(where).free >> 20} MiB free in {where}"


def _end_jvm():
    """End the JVM a stopped session leaves running, and wait for it."""
    from pyspark import SparkContext

    gateway = SparkContext._gateway
    if gateway is None:
        return
    jvm = getattr(gateway, "proc", None)
    gateway.shutdown()
    if jvm is None:
        return
    if jvm.stdin:
        jvm.stdin.close()
    try:
        jvm.wait(timeout=30)
    except subprocess.TimeoutExpired:
        jvm.kill()
        jvm.wait()


def main(argv):
    """The stage build() starts: base, out and the profile as JSON."""
    try:
        written = build_here(argv[0], argv[1], profiles.Profile(**json.loads(argv[2])))
    except Exception as failure:  # whatever it was, the caller is told why, in one line
        # Before the JVM ends: a Java failure pyspark does not convert (S3A's)
        # asks the JVM for its message.
        words = " ".join(str(failure).split("JVM stacktrace:")[0].split("\n\tat ")[0].split())
        _end_jvm()
        sys.exit(f"{type(failure).__name__}: {words[:2000]}")
    _end_jvm()
    print(json.dumps(written))


if __name__ == "__main__":
    main(sys.argv[1:])
