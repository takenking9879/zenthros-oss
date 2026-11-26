#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The example pipeline on Spark (examples/pipeline/pipeline_spark.py).

Three things are pinned. Spark's settings come from the profile and the
container, never from the machine. The pipeline is lazy: the only actions
are the two writes. And Spark writes the same table as Polars, column for
column and value for value, from the same files.

Spark needs Java and pyspark: without them (this machine) only the settings
and the source are checked. The whole file runs inside the pipeline's image,
with an init as PID 1 (as LocalBackend runs it): a stage's orphans are
reaped, or a zombie java would count as a JVM left behind:

    docker run --rm --init --cpus 2 --memory 4g --memory-swap 4g -v "$PWD":/src:ro -w /src \\
        zenthros-pipeline:local python -m unittest discover -s examples/pipeline/tests -t examples/pipeline/tests
"""
import ast
import dataclasses
import glob
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

PIPELINE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE))

import limits  # noqa: E402
import profiles  # noqa: E402

try:
    import pipeline_spark
    import spark_jars
except ImportError:  # pragma: no cover - the example's requirements are not installed
    pipeline_spark = None

try:
    import pyspark  # noqa: F401

    HAS_SPARK = shutil.which("java") is not None
except ImportError:
    HAS_SPARK = False

GiB = 1 << 30
SMALL = profiles.get("small")
TINY = dataclasses.replace(SMALL, name="tiny", customers=500, orders=3000, months=6, files=3, train_rows=2000, forest_trees=10)


# hadoop-aws's POM and hadoop-project's, cut to what spark_jars reads
# (from Maven Central, Hadoop 3.5.0: what pyspark 4.1 carries).
AWS_POM = """<project><parent><artifactId>hadoop-project</artifactId></parent><dependencies>
<dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-common</artifactId><scope>provided</scope></dependency>
<dependency><groupId>software.amazon.awssdk</groupId><artifactId>bundle</artifactId><scope>compile</scope></dependency>
<dependency><groupId>software.amazon.encryption.s3</groupId><artifactId>amazon-s3-encryption-client-java</artifactId>
  <scope>provided</scope></dependency>
<dependency><groupId>software.amazon.s3.analyticsaccelerator</groupId><artifactId>analyticsaccelerator-s3</artifactId>
  <scope>compile</scope></dependency>
<dependency><groupId>org.wildfly.openssl</groupId><artifactId>wildfly-openssl</artifactId><scope>runtime</scope></dependency>
<dependency><groupId>org.assertj</groupId><artifactId>assertj-core</artifactId><scope>test</scope></dependency>
<dependency><groupId>com.example</groupId><artifactId>optional-thing</artifactId><optional>true</optional></dependency>
</dependencies></project>"""
PROJECT_POM = """<project><properties>
<aws-java-sdk-v2.version>2.35.4</aws-java-sdk-v2.version>
<amazon-s3-analyticsaccelerator-s3.version>1.3.1</amazon-s3-analyticsaccelerator-s3.version>
</properties><dependencyManagement><dependencies>
<dependency><groupId>software.amazon.awssdk</groupId><artifactId>bundle</artifactId>
  <version>${aws-java-sdk-v2.version}</version></dependency>
<dependency><groupId>software.amazon.s3.analyticsaccelerator</groupId><artifactId>analyticsaccelerator-s3</artifactId>
  <version>${amazon-s3-analyticsaccelerator-s3.version}</version></dependency>
<dependency><groupId>com.example</groupId><artifactId>optional-thing</artifactId><version>9</version></dependency>
</dependencies></dependencyManagement></project>"""


@unittest.skipIf(pipeline_spark is None, "pip install -r examples/pipeline/requirements.txt")
class SparkSettingsTest(unittest.TestCase):
    def conf(self, profile=SMALL, base="/data", cpus=2, memory=4 * GiB, env=None):
        clean = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        with mock.patch.object(limits, "cpus", return_value=cpus) as asked, \
                mock.patch.object(limits, "memory_bytes", return_value=memory), \
                mock.patch.dict(os.environ, {**clean, **(env or {})}, clear=True):
            settings = pipeline_spark.conf(profile, base)
        asked.assert_called_with(profile.cpus)
        return settings

    def test_a_stage_that_dies_with_nothing_written_says_how_much_disk_was_left(self):
        # Seen on AWS's 8 GB disk: the stage exited 120 (Python could not
        # write its own traceback) and the job said "no output". The disk it
        # writes to is the first suspect, so the error names what is free.
        with tempfile.TemporaryDirectory() as folder:
            silent = os.path.join(folder, "silent-stage")
            with open(silent, "w") as fh:
                fh.write("#!/bin/sh\nexit 120\n")
            os.chmod(silent, 0o755)
            usage = shutil.disk_usage(folder)._replace(free=37 << 20)
            with mock.patch.object(pipeline_spark.sys, "executable", silent), \
                    mock.patch.object(pipeline_spark.shutil, "disk_usage", return_value=usage), \
                    mock.patch.object(pipeline_spark.tempfile, "gettempdir", return_value=folder):
                with self.assertRaises(RuntimeError) as caught:
                    pipeline_spark.build("/data", os.path.join(folder, "out"), SMALL)
        self.assertEqual(str(caught.exception).splitlines()[0],
                         f"the Spark stage exited 120: no output ({37} MiB free in {folder})")

    def test_task_threads_are_the_containers_not_the_machines(self):
        self.assertEqual(self.conf(cpus=2)["spark.master"], "local[2]")
        self.assertEqual(self.conf(cpus=1)["spark.master"], "local[1]")
        self.assertNotIn("local[*]", str(self.conf()))

    def test_the_heap_is_the_profiles_and_never_more_than_half_the_box(self):
        self.assertEqual(self.conf()["spark.driver.memory"], "1536m")
        self.assertEqual(self.conf(memory=2 * GiB)["spark.driver.memory"], "1024m", "a 2 GiB container")
        self.assertEqual(self.conf(profiles.get("full"), memory=8 * GiB)["spark.driver.memory"], "3072m")

    def test_what_bounds_memory_is_set_from_the_profile(self):
        settings = self.conf()
        self.assertEqual((settings["spark.sql.shuffle.partitions"], settings["spark.driver.maxResultSize"]),
                         (str(SMALL.spark_shuffle_partitions), SMALL.spark_max_result))
        self.assertEqual(settings["spark.ui.enabled"], "false")
        self.assertEqual((settings["spark.driver.bindAddress"], settings["spark.driver.host"]), ("127.0.0.1", "127.0.0.1"))

    def test_s3_settings_only_for_s3_and_never_a_credential(self):
        env = {"AWS_ENDPOINT_URL": "http://floci:4566", "AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "AKIAEXAMPLE",
               "AWS_SECRET_ACCESS_KEY": "secret-example", "AWS_SESSION_TOKEN": "token-example"}
        self.assertFalse([key for key in self.conf(env=env) if "s3a" in key], "a local path needs none, whatever the environment")
        settings = self.conf(base="s3://bucket/pipeline", env=env)
        self.assertEqual(settings["spark.hadoop.fs.s3a.endpoint"], "http://floci:4566")
        self.assertEqual(settings["spark.hadoop.fs.s3a.path.style.access"], "true")
        self.assertEqual(settings["spark.hadoop.fs.s3a.connection.ssl.enabled"], "false")
        self.assertEqual(settings["spark.hadoop.fs.s3a.endpoint.region"], "us-east-1")
        for value in ("AKIAEXAMPLE", "secret-example", "token-example"):
            self.assertNotIn(value, str(settings), "settings are logged and shown; the environment is not")
        real = self.conf(base="s3://bucket/pipeline", env={"AWS_REGION": "us-east-2"})
        self.assertNotIn("spark.hadoop.fs.s3a.endpoint", real, "real S3 needs no endpoint")
        self.assertNotIn("spark.hadoop.fs.s3a.path.style.access", real)

    def test_jars_from_elsewhere_go_on_the_drivers_class_path(self):
        # On a VM pyspark comes from the pool's python toolchain, without
        # the S3 connector: its jars are downloaded apart and named here.
        settings = pipeline_spark.conf(SMALL, "s3://bucket/x", jars=["/c/hadoop-aws-3.5.0.jar", "/c/bundle-2.35.4.jar"])
        self.assertEqual(settings["spark.driver.extraClassPath"], "/c/hadoop-aws-3.5.0.jar:/c/bundle-2.35.4.jar")
        self.assertNotIn("spark.driver.extraClassPath", pipeline_spark.conf(SMALL, "s3://bucket/x"))

    def test_only_s3_asks_for_the_connector(self):
        with mock.patch.object(spark_jars, "for_s3", return_value=["/c/a.jar"]) as asked:
            self.assertEqual(pipeline_spark.connector_jars("/data"), [])
            asked.assert_not_called()
            self.assertEqual(pipeline_spark.connector_jars("s3://bucket/x"), ["/c/a.jar"])

    def test_the_session_starts_with_the_connectors_jars(self):
        given = {}

        class Builder:
            def config(self, key, value):
                given[key] = value
                return self

            def getOrCreate(self):
                return mock.MagicMock()

        fake = mock.MagicMock()
        fake.sql.SparkSession.builder = Builder()
        with mock.patch.dict(sys.modules, {"pyspark": fake, "pyspark.sql": fake.sql}), \
                mock.patch.object(pipeline_spark, "connector_jars", return_value=["/c/hadoop-aws.jar"]) as asked:
            pipeline_spark.session(TINY, "s3://bucket/x")
        asked.assert_called_once_with("s3://bucket/x")
        self.assertEqual(given["spark.driver.extraClassPath"], "/c/hadoop-aws.jar")

    def test_the_connector_is_downloaded_only_where_pyspark_lacks_it(self):
        with tempfile.TemporaryDirectory() as jars, tempfile.TemporaryDirectory() as cache:
            Path(jars, "hadoop-client-api-3.5.0.jar").touch()
            calls = []

            def ensure(directory=None, into=None):
                calls.append((directory, into))
                return [os.path.join(into, "hadoop-aws-3.5.0.jar")]

            with mock.patch.object(spark_jars, "ensure", ensure):
                self.assertEqual(spark_jars.for_s3(jars, cache), [os.path.join(cache, "hadoop-aws-3.5.0.jar")])
                self.assertEqual(calls, [(jars, cache)], "Hadoop's version from pyspark's jars, the download to the cache")
                Path(jars, "hadoop-aws-3.5.0.jar").touch()  # the image's: put there when it was built
                self.assertEqual(spark_jars.for_s3(jars, cache), [])
                self.assertEqual(len(calls), 1)
        with mock.patch.dict(os.environ, {"PIPELINE_JARS_DIR": "/somewhere"}):
            self.assertEqual(spark_jars.cache_dir(), "/somewhere")
        with mock.patch.dict(os.environ, {k: v for k, v in os.environ.items() if k != "PIPELINE_JARS_DIR"}, clear=True):
            self.assertEqual(spark_jars.cache_dir(), os.path.join(tempfile.gettempdir(), "zenthros-pipeline-jars"))

    def test_ensure_reads_hadoop_in_one_place_and_writes_in_another(self):
        poms = {f"{spark_jars.MAVEN}/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.pom": AWS_POM,
                f"{spark_jars.MAVEN}/org/apache/hadoop/hadoop-project/3.5.0/hadoop-project-3.5.0.pom": PROJECT_POM}
        downloaded = []
        with tempfile.TemporaryDirectory() as jars, tempfile.TemporaryDirectory() as root:
            Path(jars, "hadoop-client-api-3.5.0.jar").touch()
            cache = os.path.join(root, "not-yet")
            with mock.patch.object(spark_jars, "_fetch", lambda url: poms[url].encode()), \
                    mock.patch.object(spark_jars, "_download", lambda url, path: downloaded.append(path)):
                paths = spark_jars.ensure(jars, into=cache)
            self.assertEqual(paths, downloaded)
            self.assertEqual({os.path.dirname(p) for p in paths}, {cache})
            self.assertTrue(os.path.isdir(cache))

    def test_two_downloads_of_one_jar_do_not_share_a_part_file(self):
        parts = []
        real_open = open

        def recording_open(path, *args, **kwargs):
            parts.append(path)
            return real_open(path, *args, **kwargs)

        class Body:
            def __init__(self, data):
                self.data = data

            def read(self, size=-1):
                data, self.data = self.data, b""
                return data

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        import hashlib

        def opened(url, timeout=None):
            if url.endswith(".sha1"):
                return Body(hashlib.sha1(b"jar bytes").hexdigest().encode())
            return Body(b"jar bytes")

        with tempfile.TemporaryDirectory() as jars:
            target = os.path.join(jars, "x.jar")
            with mock.patch.object(spark_jars.urllib.request, "urlopen", opened), \
                    mock.patch("builtins.open", recording_open):
                spark_jars._download("https://repo/x.jar", target)
            self.assertEqual(os.listdir(jars), ["x.jar"])
        self.assertIn(str(os.getpid()), os.path.basename(parts[0]), "the part file is this process's own")

    def test_the_stage_reads_why_it_failed_before_its_jvm_ends(self):
        # What pyspark does not turn into a Python exception (S3A's) keeps
        # its message in the JVM: read after the JVM ended, it was
        # "Connection refused" (the E2E's Spark stage on the container).
        ended = []

        class JavaFailure(Exception):
            def __str__(self):
                if ended:
                    raise ConnectionRefusedError(111, "Connection refused")
                return "An error occurred while calling o42.parquet.\n: java.nio.file.AccessDeniedException: s3a://b/raw\n\tat org.apache"

        def fail(*args):
            raise JavaFailure()

        with mock.patch.object(pipeline_spark, "build_here", fail), \
                mock.patch.object(pipeline_spark, "_end_jvm", lambda: ended.append(True)), \
                self.assertRaises(SystemExit) as caught:
            pipeline_spark.main(["s3://b", "s3://b/out", json.dumps(dataclasses.asdict(TINY))])
        self.assertEqual(ended, [True], "the JVM still ends")
        self.assertEqual(caught.exception.code, "JavaFailure: An error occurred while calling o42.parquet. "
                                                ": java.nio.file.AccessDeniedException: s3a://b/raw")

    def test_the_source_has_one_way_out_and_it_is_the_write(self):
        tree = ast.parse((PIPELINE / "pipeline_spark.py").read_text())
        called = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
        actions = called & {"collect", "count", "toPandas", "show", "take", "first", "head", "tail", "toLocalIterator",
                            "foreach", "foreachPartition", "isEmpty", "toArrow", "cache", "checkpoint", "describe"}
        self.assertEqual(actions, set(), "these run the plan and bring rows to the driver or keep them in memory")
        self.assertIn("parquet", called)

    def test_the_s3_connector_is_the_hadoop_that_pyspark_carries(self):
        with tempfile.TemporaryDirectory() as jars:
            Path(jars, "hadoop-client-api-3.4.1.jar").touch()
            Path(jars, "hadoop-client-runtime-3.4.1.jar").touch()
            self.assertEqual(spark_jars.hadoop_version(jars), "3.4.1")
            Path(jars, "hadoop-client-api-3.3.6.jar").touch()
            with self.assertRaises(RuntimeError):
                spark_jars.hadoop_version(jars)

    def test_the_connector_brings_every_jar_it_is_built_against(self):
        # hadoop-aws 3.5.0 needs the AWS SDK bundle and the analytics
        # accelerator; with the SDK alone, the first s3a:// read was
        # NoClassDefFoundError .../analyticsaccelerator/request/ObjectClient.
        self.assertEqual(spark_jars.dependencies(AWS_POM, PROJECT_POM), [
            ("software.amazon.awssdk", "bundle", "2.35.4"),
            ("software.amazon.s3.analyticsaccelerator", "analyticsaccelerator-s3", "1.3.1"),
        ])
        unmanaged = AWS_POM.replace("<artifactId>analyticsaccelerator-s3</artifactId>",
                                    "<artifactId>not-in-the-project</artifactId>")
        with self.assertRaises(RuntimeError):
            spark_jars.dependencies(unmanaged, PROJECT_POM)
        unpinned = PROJECT_POM.replace("<aws-java-sdk-v2.version>2.35.4</aws-java-sdk-v2.version>", "")
        with self.assertRaises(RuntimeError):
            spark_jars.dependencies(AWS_POM, unpinned)

    def test_ensure_downloads_the_connector_and_what_it_needs(self):
        poms = {f"{spark_jars.MAVEN}/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.pom": AWS_POM,
                f"{spark_jars.MAVEN}/org/apache/hadoop/hadoop-project/3.5.0/hadoop-project-3.5.0.pom": PROJECT_POM}
        downloaded = []
        with tempfile.TemporaryDirectory() as jars:
            Path(jars, "hadoop-client-api-3.5.0.jar").touch()
            Path(jars, "bundle-2.35.4.jar").touch()  # there already: not again
            with mock.patch.object(spark_jars, "_fetch", lambda url: poms[url].encode()), \
                    mock.patch.object(spark_jars, "_download", lambda url, path: downloaded.append((url, path))):
                paths = spark_jars.ensure(jars)
            self.assertEqual([os.path.basename(p) for p in paths],
                             ["hadoop-aws-3.5.0.jar", "bundle-2.35.4.jar", "analyticsaccelerator-s3-1.3.1.jar"])
            self.assertEqual(downloaded, [
                (f"{spark_jars.MAVEN}/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar",
                 os.path.join(jars, "hadoop-aws-3.5.0.jar")),
                (f"{spark_jars.MAVEN}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/"
                 "analyticsaccelerator-s3-1.3.1.jar", os.path.join(jars, "analyticsaccelerator-s3-1.3.1.jar")),
            ])

    def test_a_jar_that_is_not_what_maven_says_is_not_kept(self):
        with tempfile.TemporaryDirectory() as jars:
            target = os.path.join(jars, "x.jar")

            class Body:
                def __init__(self, data):
                    self.data = data

                def read(self, size=-1):
                    data, self.data = self.data, b""
                    return data

                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            def opened(url, timeout=None):
                return Body(b"0000000000000000000000000000000000000000  x.jar\n" if url.endswith(".sha1") else b"jar bytes")

            with mock.patch.object(spark_jars.urllib.request, "urlopen", opened):
                with self.assertRaises(RuntimeError) as caught:
                    spark_jars._download("https://repo/x.jar", target)
            self.assertIn("SHA-1", str(caught.exception))
            self.assertEqual(os.listdir(jars), [], "neither the jar nor a part of it")


@unittest.skipUnless(HAS_SPARK, "needs Java and pyspark: run inside the pipeline's image")
class SparkPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import gen_data
        import pipeline_polars
        from pyspark.sql import DataFrame

        cls.dir = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.dir.cleanup)
        cls.base = os.path.join(cls.dir.name, "data")
        gen_data.generate(cls.base, TINY)
        cls.polars = pipeline_polars.build(cls.base, os.path.join(cls.dir.name, "polars"), TINY)
        cls.actions = []
        patchers = []
        for name in ("collect", "count", "toPandas", "show", "take", "first", "head", "tail", "toLocalIterator"):
            original = getattr(DataFrame, name)

            def counted(self, *args, _name=name, _original=original, **kwargs):
                cls.actions.append(_name)
                return _original(self, *args, **kwargs)

            patchers.append(mock.patch.object(DataFrame, name, counted))
        for patcher in patchers:
            patcher.start()
        try:
            cls.spark = pipeline_spark.build_here(cls.base, os.path.join(cls.dir.name, "spark"), TINY)
        finally:
            for patcher in patchers:
                patcher.stop()

    def table(self, written, name, keys):
        import pyarrow.parquet as pq

        return pq.read_table(written[name]).to_pandas().sort_values(keys).reset_index(drop=True)

    def test_no_row_reaches_python_on_the_way_to_the_files(self):
        self.assertEqual(self.actions, [])

    def test_both_engines_write_the_same_columns_of_the_same_kinds(self):
        import queries
        import store

        for name, schema in (("features", queries.SCHEMA), ("summary", queries.SUMMARY_SCHEMA)):
            self.assertEqual(store.parquet_kinds(self.spark[name]), schema, name)
            self.assertEqual(store.parquet_kinds(self.spark[name]), store.parquet_kinds(self.polars[name]), name)

    def test_both_engines_count_the_same_rows_from_the_footers(self):
        import store

        for name in ("features", "summary"):
            rows = store.parquet_rows(self.spark[name])
            self.assertGreater(rows, 0, name)
            self.assertEqual(rows, store.parquet_rows(self.polars[name]), name)

    def test_both_engines_compute_the_same_values(self):
        import numpy as np
        import queries

        for name, keys, schema in (("features", list(queries.KEYS), queries.SCHEMA),
                                   ("summary", ["segment", "country"], queries.SUMMARY_SCHEMA)):
            ours, theirs = self.table(self.spark, name, keys), self.table(self.polars, name, keys)
            self.assertEqual(list(ours.columns), list(theirs.columns), name)
            for column, kind in schema.items():
                if kind == "float64":
                    np.testing.assert_allclose(ours[column], theirs[column], rtol=1e-9, atol=1e-9, err_msg=f"{name}.{column}")
                else:
                    self.assertTrue((ours[column] == theirs[column]).all(), f"{name}.{column}")

    def test_s3a_starts_with_every_class_it_needs(self):
        # The image's jars, without an S3: starting S3A builds its client and
        # loads what the first read needs (NoClassDefFoundError there before).
        # Elsewhere the connector's jars would join a JVM this class already
        # started, which takes no more (ClassNotFoundException in CI).
        if not glob.glob(os.path.join(spark_jars.jars_dir(), "hadoop-aws-*.jar")):
            self.skipTest("needs the image's pyspark, which carries the S3 connector: run inside the pipeline's image")
        env = {"AWS_ENDPOINT_URL": "http://127.0.0.1:9", "AWS_REGION": "us-east-1",
               "AWS_ACCESS_KEY_ID": "unused", "AWS_SECRET_ACCESS_KEY": "unused"}
        with mock.patch.dict(os.environ, env):
            spark = pipeline_spark.session(TINY, "s3://zenthros-no-bucket/data")
        try:
            jvm = spark._jvm
            fs = jvm.org.apache.hadoop.fs.FileSystem.newInstance(jvm.java.net.URI("s3a://zenthros-no-bucket/"),
                                                                  spark._jsc.hadoopConfiguration())
            self.assertEqual(fs.getClass().getName(), "org.apache.hadoop.fs.s3a.S3AFileSystem")
            jvm.java.lang.Class.forName("software.amazon.s3.analyticsaccelerator.request.ObjectClient")
            fs.close()
        finally:
            spark.stop()

    def test_the_spark_stage_runs_in_a_process_that_ends_with_its_jvm(self):
        import store

        out = os.path.join(self.dir.name, "spark-subprocess")
        before = _jvms()
        self.assertTrue(before, "this process's own session was stopped and its JVM is still here: stop() does not end it")
        written = pipeline_spark.build(self.base, out, TINY)
        self.assertEqual(written["threads"], limits.cpus(TINY.cpus))
        self.assertEqual(store.parquet_rows(written["features"]), store.parquet_rows(self.polars["features"]))
        self.assertEqual(_jvms(zombies=True) - before, set(),
                         "no JVM is left holding its heap while the models train, and the stage reaped its own")

    def test_a_failed_spark_stage_says_why_and_leaves_no_jvm(self):
        before = _jvms()
        with self.assertRaises(RuntimeError) as caught:
            pipeline_spark.build(os.path.join(self.dir.name, "no-such-data"), os.path.join(self.dir.name, "x"), TINY)
        self.assertIn("exited", str(caught.exception))
        self.assertIn("no-such-data", str(caught.exception).splitlines()[0], "the reason first, not a Java stack trace")
        self.assertEqual(_jvms(zombies=True) - before, set())

    def test_a_stage_that_dies_without_ending_its_jvm_does_not_leave_it_running(self):
        # A stage killed for memory cannot tidy up: what it started goes with it.
        fake_java = os.path.join(self.dir.name, "java-left-behind")
        os.symlink(shutil.which("sleep"), fake_java)
        dying = os.path.join(self.dir.name, "dying-stage")
        with open(dying, "w") as fh:
            fh.write(f"#!/bin/sh\n{fake_java} 300 &\necho killed >&2\nexit 137\n")
        os.chmod(dying, 0o755)
        before = _jvms()
        started = time.monotonic()
        with mock.patch.object(pipeline_spark.sys, "executable", dying):
            with self.assertRaises(RuntimeError) as caught:
                pipeline_spark.build(self.base, os.path.join(self.dir.name, "y"), TINY)
        self.assertLess(time.monotonic() - started, 30, "it did not wait for what the stage left running")
        self.assertIn("exited 137: killed", str(caught.exception))
        deadline = time.monotonic() + 5
        while _jvms() - before and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertEqual(_jvms() - before, set())

    def test_the_spark_step_leaves_its_tables_and_an_end_marker(self):
        import pipeline
        import store

        out = os.path.join(self.dir.name, "step")
        with mock.patch.dict(profiles.PROFILES, {"tiny": TINY}):
            result = pipeline.tables(self.base, out, "tiny")
        self.assertEqual(sorted(os.listdir(out)), ["_SUCCESS", "features", "result.json", "summary"])
        self.assertEqual((result["engine"], result["profile"]), ("spark", "tiny"))
        self.assertEqual(result["rows"]["features"], store.parquet_rows(os.path.join(out, "features")))
        self.assertGreater(result["rows"]["summary"], 0)


def _jvms(zombies=False):
    """The JVMs running now. A zombie is not running (it holds no memory)
    and counts only when asked: it is a JVM nobody waited for."""
    found = set()
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            with open(f"/proc/{pid}/status") as fh:
                status = dict(line.split(":\t", 1) for line in fh.read().splitlines() if ":\t" in line)
        except OSError:
            continue
        if status.get("Name", "").startswith("java") and (zombies or not status.get("State", "").startswith("Z")):
            found.add(pid)
    return found


if __name__ == "__main__":
    unittest.main()
