#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The example pipeline on Polars (examples/pipeline/pipeline_polars.py).

Two things are pinned here. The numbers: every feature is checked against
the same thing computed the slow, obvious way with pandas. And that the
pipeline is lazy: from the scan to the written file nothing is brought into
Python, so what bounds its memory is the engine, not the size of the data.
"""
import ast
import dataclasses
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PIPELINE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE))

try:
    import gen_data
    import numpy as np
    import pandas as pd
    import pipeline_polars
    import polars as pl
    import profiles
    import pyarrow.parquet as pq
    import queries
    import store
except ImportError:  # pragma: no cover - the example's requirements are not installed
    pl = None

TINY = None if pl is None else dataclasses.replace(
    profiles.get("small"), name="tiny", customers=500, orders=3000, months=6, files=3, train_rows=2000, forest_trees=10)


def reference(base, months):
    """Customer-month spend and orders, the obvious way."""
    raw = os.path.join(base, "raw")
    customers, orders, lines = (pq.read_table(os.path.join(raw, name)).to_pandas() for name in gen_data.TABLES)
    orders = orders[orders["status"] == "completed"].merge(customers, on="customer_id")
    lines = lines.merge(orders, on="order_id")
    lines["amount"] = lines["quantity"] * lines["unit_price"] * (1 - lines["discount"])
    dates = pd.to_datetime(lines["order_date"])
    lines["month_idx"] = (dates.dt.year - queries.START_YEAR) * 12 + dates.dt.month - 1
    monthly = lines.groupby(["customer_id", "month_idx"]).agg(
        spend=("amount", "sum"), n_orders=("order_id", "nunique"), n_lines=("order_id", "size"),
        units=("quantity", "sum"),
        electronics=("amount", lambda a: a[lines.loc[a.index, "category"] == "electronics"].sum()))
    return monthly, orders


@unittest.skipIf(pl is None, "pip install -r examples/pipeline/requirements.txt")
class PolarsPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.dir.cleanup)
        cls.base = os.path.join(cls.dir.name, "data")
        cls.out = os.path.join(cls.dir.name, "out")
        gen_data.generate(cls.base, TINY)
        cls.collects, cls.reads = [], []
        collect, read = pl.LazyFrame.collect, pl.read_parquet
        with mock.patch.object(pl.LazyFrame, "collect", lambda self, *a, **k: cls.collects.append(1) or collect(self, *a, **k)), \
                mock.patch.object(pl, "read_parquet", lambda *a, **k: cls.reads.append(1) or read(*a, **k)):
            cls.written = pipeline_polars.build(cls.base, cls.out, TINY)
        cls.features = pq.read_table(cls.written["features"]).to_pandas().set_index(["customer_id", "month_idx"]).sort_index()
        cls.monthly, cls.orders = reference(cls.base, TINY.months)

    def spend(self, customer, month):
        return float(self.monthly["spend"].get((customer, month), 0.0))

    def test_nothing_is_brought_into_python_on_the_way_to_the_file(self):
        self.assertEqual((self.collects, self.reads), ([], []))

    def test_the_source_has_one_way_out_and_it_is_the_sink(self):
        tree = ast.parse((PIPELINE / "pipeline_polars.py").read_text())
        called = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
        eager = called & {"collect", "collect_async", "fetch", "read_parquet", "read_csv", "to_pandas", "to_numpy",
                          "to_arrow", "to_dicts", "rows", "item", "describe"}
        self.assertEqual(eager, set(), "these bring a table into Python")
        self.assertIn("sink_parquet", called)

    def test_the_table_is_the_columns_and_types_both_engines_agree_on(self):
        kinds = store.parquet_kinds(self.written["features"])
        self.assertEqual(list(kinds), queries.OUTPUT_COLUMNS)
        self.assertEqual(kinds, queries.SCHEMA)
        self.assertEqual(store.parquet_kinds(self.written["summary"]), queries.SUMMARY_SCHEMA)

    def test_a_row_is_a_month_a_customer_bought_in_except_the_last(self):
        expected = {key for key in self.monthly.index if key[1] < TINY.months - 1}
        self.assertEqual(set(self.features.index), expected)
        self.assertEqual(len(self.features), len(expected), "one row each")
        self.assertEqual(store.parquet_rows(self.written["features"]), len(expected),
                         "and the file's footer says so without reading it")

    def test_the_months_aggregates_are_the_joins(self):
        for column, theirs in (("spend", "spend"), ("n_orders", "n_orders"), ("n_lines", "n_lines"), ("units", "units")):
            ours = self.features[column]
            np.testing.assert_allclose(ours.to_numpy(), self.monthly.loc[ours.index, theirs].to_numpy(), rtol=1e-9,
                                       err_msg=column)
        cancelled = set(pq.read_table(os.path.join(self.base, "raw", "orders")).to_pandas()
                        .query("status != 'completed'")["order_id"])
        self.assertTrue(cancelled, "the data has orders that must not count")
        self.assertEqual(int(self.features["n_orders"].sum()),
                         int(self.monthly.query(f"month_idx < {TINY.months - 1}")["n_orders"].sum()))

    def test_shares_and_averages(self):
        row = self.features
        np.testing.assert_allclose(row["avg_order_value"], row["spend"] / row["n_orders"], rtol=1e-9)
        np.testing.assert_allclose(row["avg_line_value"], row["spend"] / row["n_lines"], rtol=1e-9)
        np.testing.assert_allclose(row["share_electronics"],
                                   self.monthly.loc[row.index, "electronics"].to_numpy() / row["spend"], rtol=1e-9, atol=1e-12)
        shares = row[[f"share_{name}" for name in queries.SHARE_CATEGORIES]]
        self.assertTrue(((shares >= 0) & (shares <= 1 + 1e-9)).all().all())
        self.assertTrue((shares.sum(axis=1) <= 1 + 1e-9).all())

    def test_the_past_is_the_calendars_not_the_previous_row(self):
        # A customer that skipped a month has no "previous month" spend,
        # and its three months are three months, not its last three rows.
        skipped = 0
        for (customer, month), row in self.features.iterrows():
            self.assertAlmostEqual(row["prev_spend"], self.spend(customer, month - 1), places=6)
            self.assertAlmostEqual(row["spend_3m"], sum(self.spend(customer, month - k) for k in range(3)), places=6)
            self.assertAlmostEqual(row["spend_cum"], sum(self.spend(customer, m) for m in range(month + 1)), places=6)
            skipped += month > 0 and self.spend(customer, month - 1) == 0 and row["spend_cum"] > row["spend"] + 1e-9
        self.assertGreater(skipped, 20, "the data has customers that skip months")

    def test_recency_and_tenure(self):
        for (customer, month), row in self.features.iterrows():
            before = [m for m in range(month) if self.spend(customer, m) > 0]
            self.assertEqual(row["months_since_prev"], month - before[-1] if before else -1)
            self.assertEqual(row["tenure_months"], month - before[0] if before else 0)
            self.assertEqual(row["orders_cum"], sum(int(self.monthly["n_orders"].get((customer, m), 0)) for m in range(month + 1)))

    def test_the_target_is_next_months_spend_and_zero_when_there_is_none(self):
        zeros = 0
        for (customer, month), row in self.features.iterrows():
            self.assertAlmostEqual(row[queries.TARGET], self.spend(customer, month + 1), places=6)
            zeros += row[queries.TARGET] == 0
        self.assertGreater(zeros, 20, "a month with no purchase is a zero, not a missing row")

    def test_the_season_is_a_point_on_a_circle(self):
        for (_, month), row in self.features.iterrows():
            self.assertAlmostEqual(row["month_sin"], math.sin(2 * math.pi * (month % 12) / 12), places=9)
            self.assertAlmostEqual(row["month_cos"], math.cos(2 * math.pi * (month % 12) / 12), places=9)

    def test_no_feature_is_missing(self):
        self.assertFalse(self.features.isna().any().any())

    def test_the_summary_counts_what_the_features_hold(self):
        summary = pq.read_table(self.written["summary"]).to_pandas()
        self.assertEqual(list(summary.columns), queries.SUMMARY_COLUMNS)
        self.assertEqual(int(summary["orders"].sum()), int(self.monthly["n_orders"].sum()))
        self.assertAlmostEqual(summary["spend"].sum(), self.monthly["spend"].sum(), places=4)
        buyers = self.orders.groupby(["segment", "country"])["customer_id"].nunique()
        for row in summary.itertuples():
            self.assertEqual(row.customers, buyers[(row.segment, row.country)], (row.segment, row.country))


@unittest.skipIf(pl is None, "pip install -r examples/pipeline/requirements.txt")
class PolarsThreadsTest(unittest.TestCase):
    def test_a_fresh_process_gets_the_profiles_threads_not_the_machines(self):
        with tempfile.TemporaryDirectory() as scratch:
            code = ("import dataclasses, json, sys, gen_data, pipeline_polars, profiles\n"
                    "tiny = dataclasses.replace(profiles.get('small'), customers=50, orders=200, months=3, files=1)\n"
                    "gen_data.generate(sys.argv[1], tiny)\n"
                    "print(json.dumps(pipeline_polars.build(sys.argv[1], sys.argv[1] + '/out', tiny)))\n")
            done = subprocess.run([sys.executable, "-c", code, scratch], cwd=PIPELINE, capture_output=True, text=True,
                                  env={k: v for k, v in os.environ.items() if k != "POLARS_MAX_THREADS"})
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(json.loads(done.stdout.splitlines()[-1])["threads"], min(2, len(os.sched_getaffinity(0))))

    @unittest.skipIf(pl is not None and pl.thread_pool_size() <= 1, "this machine has one thread")
    def test_an_engine_already_started_with_more_threads_is_refused_when_asked_to_be_strict(self):
        one = dataclasses.replace(TINY, cpus=1)
        with self.assertRaises(RuntimeError) as caught:
            pipeline_polars.threads(one, strict=True)
        self.assertIn("POLARS_MAX_THREADS", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
