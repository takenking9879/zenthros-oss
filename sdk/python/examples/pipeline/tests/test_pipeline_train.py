#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The example pipeline's training and its steps (examples/pipeline/
train.py, pipeline.py).

Training is the one place where the feature table becomes memory, so it is
where the row cap and the thread count are enforced. A step is what a call
executes: it leaves its folder with an end marker written last.
"""
import dataclasses
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PIPELINE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE))

try:
    import gen_data
    import limits
    import pipeline
    import pipeline_polars
    import polars  # noqa: F401 - run() is strict about threads only in a fresh process
    import profiles
    import pyarrow.parquet as pq
    import queries
    import store
    import train
except ImportError:  # pragma: no cover - the example's requirements are not installed
    train = None

TINY = None if train is None else dataclasses.replace(
    profiles.get("small"), name="tiny", customers=600, orders=16000, months=16, files=3, train_rows=8000, forest_trees=20)


@unittest.skipIf(train is None, "pip install -r examples/pipeline/requirements.txt")
class TrainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.dir.cleanup)
        cls.base = os.path.join(cls.dir.name, "data")
        gen_data.generate(cls.base, TINY)
        written = pipeline_polars.build(cls.base, os.path.join(cls.dir.name, "out"), TINY)
        cls.features = written["features"]
        cls.total = store.parquet_rows(cls.features)
        cls.result = train.fit(cls.features, TINY)

    def test_three_models_are_scored_on_months_they_did_not_see(self):
        self.assertEqual(list(self.result["models"]), ["linear_regression", "random_forest", "hist_gradient_boosting"])
        frame = train.load(self.features, TINY.train_rows)
        fitted, held_out = train.split(frame)
        self.assertLess(fitted["month_idx"].max(), held_out["month_idx"].min(), "the test months come after")
        self.assertEqual(self.result["rows"], {"table": self.total, "loaded": len(frame), "train": len(fitted),
                                               "test": len(held_out)})
        self.assertGreater(len(held_out), 100)

    def test_each_model_beats_guessing_the_average(self):
        for name, scores in self.result["models"].items():
            self.assertEqual(sorted(scores), ["fit_seconds", "mae", "r2", "rmse"], name)
            self.assertGreater(scores["r2"], 0.2, name)
            for baseline in ("mean", "previous_month"):
                self.assertLess(scores["rmse"], self.result["baselines"][baseline]["rmse"], (name, baseline))

    def test_a_profile_too_short_to_show_a_year_is_refused(self):
        short = dataclasses.replace(TINY, months=profiles.MIN_MONTHS - 1)
        with self.assertRaises(ValueError) as caught:
            train.fit(self.features, short)
        self.assertIn("months", str(caught.exception))

    def test_the_same_table_gives_the_same_scores(self):
        again = train.fit(self.features, TINY)
        for name, scores in self.result["models"].items():
            for metric in ("rmse", "mae", "r2"):
                self.assertAlmostEqual(scores[metric], again["models"][name][metric], places=9, msg=(name, metric))

    def test_no_more_rows_than_the_cap_become_memory(self):
        self.assertEqual(len(train.load(self.features, TINY.train_rows)), self.total, "a table under the cap is read whole")
        capped = train.load(self.features, 500)
        self.assertLessEqual(len(capped), 500)
        self.assertGreater(len(capped), 250, "about the cap, not a sliver")
        # Whole customers: a customer's months stay together.
        whole = train.load(self.features, TINY.train_rows)
        kept = set(capped["customer_id"])
        self.assertGreater(len(kept), 20)
        self.assertEqual(len(whole[whole["customer_id"].isin(kept)]), len(capped))

    def test_the_cap_reaches_the_reader_not_only_the_result(self):
        # A table read whole and cut afterwards would hold everything first.
        with mock.patch.object(train, "_head", wraps=train._head) as head:
            train.load(self.features, 500)
        head.assert_called_once()
        _, sample, rows = head.call_args.args
        self.assertIsNotNone(sample, "the sample is a filter the reader applies file by file")
        self.assertEqual(rows, 500, "and the reader stops at the cap")

    def test_threads_are_the_containers(self):
        with mock.patch.object(limits, "cpus", return_value=1) as asked:
            models = train.models(TINY)
        asked.assert_called_with(TINY.cpus)
        self.assertEqual(models["random_forest"].named_steps["model"].n_jobs, 1)
        self.assertEqual(models["random_forest"].named_steps["model"].n_estimators, TINY.forest_trees)

    def test_it_trains_on_the_features_and_never_on_the_target_or_the_keys(self):
        used = set(queries.NUMERIC) | set(queries.CATEGORICAL)
        self.assertEqual(set(train.COLUMNS), used)
        self.assertFalse({queries.TARGET, "customer_id"} & used)


@unittest.skipIf(train is None, "pip install -r examples/pipeline/requirements.txt")
class RunTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.base = self.dir.name
        # A cap under the table's size: what was written and what was
        # trained on are then two different numbers.
        patcher = mock.patch.dict(profiles.PROFILES, {"tiny": dataclasses.replace(TINY, train_rows=2000)})
        patcher.start()
        self.addCleanup(patcher.stop)
        # This process imported polars with the machine's threads; a job is
        # a fresh process (test_pipeline_polars pins that).
        patcher = mock.patch.object(pipeline, "STRICT_THREADS", False)
        patcher.start()
        self.addCleanup(patcher.stop)
        # The source tables, as the notebook makes them before ws.upload.
        gen_data.generate(self.base, profiles.get("tiny"))

    def folder(self):
        return os.path.join(self.dir.name, "out", "polars")

    def models(self):
        return pipeline.models(self.base, self.folder(), "tiny")

    def test_a_step_leaves_its_tables_its_scores_and_an_end_marker(self):
        result = self.models()
        self.assertEqual(sorted(os.listdir(self.folder())), ["_SUCCESS", "features", "result.json", "summary"])
        self.assertEqual(store.read_json(os.path.join(self.folder(), "result.json")), result)
        self.assertEqual((result["engine"], result["profile"]), ("polars", "tiny"))
        self.assertEqual(result["rows"]["features"], pq.read_table(os.path.join(self.folder(), "features")).num_rows)
        self.assertEqual(result["rows"]["summary"], pq.read_table(os.path.join(self.folder(), "summary")).num_rows)
        self.assertLessEqual(result["rows"]["training"]["loaded"], 2000)
        self.assertGreater(result["rows"]["features"], 2000, "the table's rows, not the rows that were trained on")
        self.assertEqual(len(result["models"]), 3)
        self.assertEqual(sorted(result["seconds"]), ["features", "total", "train"])
        self.assertEqual(result["limits"], {"cpus": limits.cpus(TINY.cpus), "memory_bytes": limits.memory_bytes(TINY.memory_gb << 30)})

    def test_the_end_marker_is_written_after_everything_else(self):
        written = []
        original = store.write_json
        with mock.patch.object(store, "write_json", lambda uri, value: written.append(os.path.basename(uri)) or original(uri, value)):
            self.models()
        self.assertEqual(written[-1], "_SUCCESS")
        self.assertEqual(written.count("_SUCCESS"), 1)

    def test_a_step_that_failed_leaves_no_marker_and_is_done_again(self):
        with mock.patch.object(train, "fit", side_effect=MemoryError("killed")):
            with self.assertRaises(MemoryError):
                self.models()
        self.assertFalse(os.path.exists(os.path.join(self.folder(), "_SUCCESS")))
        stale = os.path.join(self.folder(), "features", "stale.parquet")
        os.rename(os.path.join(self.folder(), "features", "part-000.parquet"), stale)
        result = self.models()
        self.assertFalse(os.path.exists(stale), "what the failed attempt wrote is gone")
        self.assertEqual(result["rows"]["features"], store.parquet_rows(os.path.join(self.folder(), "features")))

    def test_a_step_that_ended_is_not_run_twice(self):
        first = self.models()
        with mock.patch.object(pipeline_polars, "build", side_effect=AssertionError("ran again")):
            self.assertEqual(self.models(), first)

    def test_a_profile_it_does_not_have_is_refused_by_name(self):
        with self.assertRaises(ValueError) as caught:
            pipeline.models(self.base, self.folder(), "huge")
        self.assertIn("small", str(caught.exception))
        self.assertFalse(os.path.exists(self.folder()))


if __name__ == "__main__":
    unittest.main()
