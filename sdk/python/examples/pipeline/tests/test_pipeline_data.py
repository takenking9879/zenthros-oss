#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The example pipeline's source tables (examples/pipeline/gen_data.py).

Three tables a pipeline can join: customers, their orders, each order's
lines. They are generated one file at a time, so making them never holds
more than a file in memory, and from a seed, so two runs (or two machines)
make the same bytes and both engines read the same data.
"""
import dataclasses
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import gen_data
    import numpy as np
    import profiles
    import pyarrow.parquet as pq
    import store
except ImportError:  # pragma: no cover - the example's requirements are not installed
    gen_data = None

TINY = None if gen_data is None else dataclasses.replace(
    profiles.get("small"), name="tiny", customers=500, orders=3000, months=6, files=3, train_rows=2000, forest_trees=10)


def table(base, name):
    return pq.read_table(os.path.join(base, "raw", name))


@unittest.skipIf(gen_data is None, "pip install -r examples/pipeline/requirements.txt")
class GenDataTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.dir.cleanup)
        cls.base = os.path.join(cls.dir.name, "a")
        cls.manifest = gen_data.generate(cls.base, TINY)

    def test_each_table_is_as_many_files_as_the_profile_says(self):
        for name in gen_data.TABLES:
            files = sorted(os.listdir(os.path.join(self.base, "raw", name)))
            self.assertEqual(files, [f"part-{i:03d}.parquet" for i in range(TINY.files)], name)

    def test_the_sizes_are_the_profiles_and_the_manifest_counts_what_was_written(self):
        customers, orders, lines = (table(self.base, name) for name in gen_data.TABLES)
        self.assertEqual((customers.num_rows, orders.num_rows), (TINY.customers, TINY.orders))
        self.assertGreater(lines.num_rows, orders.num_rows, "an order has one line or more")
        self.assertEqual(self.manifest["rows"], {"customers": customers.num_rows, "orders": orders.num_rows,
                                                 "order_lines": lines.num_rows})
        # The same numbers, from the files' footers alone.
        for name in gen_data.TABLES:
            self.assertEqual(store.parquet_rows(store.join(self.base, "raw", name)), self.manifest["rows"][name])

    def test_the_keys_join(self):
        customers, orders, lines = (table(self.base, name).to_pandas() for name in gen_data.TABLES)
        self.assertTrue(customers["customer_id"].is_unique)
        self.assertTrue(orders["order_id"].is_unique)
        self.assertTrue(orders["customer_id"].isin(customers["customer_id"]).all())
        self.assertEqual(set(lines["order_id"]), set(orders["order_id"]), "every order has lines; no line is an orphan")
        self.assertFalse(lines.duplicated(["order_id", "line_no"]).any())

    def test_orders_fall_inside_the_profiles_months(self):
        dates = table(self.base, "orders").to_pandas()["order_date"]
        months = {(d.year - gen_data.START_YEAR) * 12 + d.month - 1 for d in dates}
        self.assertEqual(months, set(range(TINY.months)))

    def test_some_customers_buy_more_than_others(self):
        # Without a lasting difference between customers there is nothing
        # to learn: the past of one would say nothing of its future.
        orders, lines = table(self.base, "orders").to_pandas(), table(self.base, "order_lines").to_pandas()
        per_customer = orders.groupby("customer_id").size()
        taste = gen_data.loyalty(per_customer.index.to_numpy())
        self.assertGreater(np.corrcoef(taste, per_customer.to_numpy())[0, 1], 0.5, "loyal customers order more often")
        units = lines.merge(orders[["order_id", "customer_id"]], on="order_id")
        self.assertGreater(np.corrcoef(gen_data.loyalty(units["customer_id"].to_numpy()), units["quantity"])[0, 1], 0.2,
                           "and more of each thing")

    def test_the_same_seed_makes_the_same_tables(self):
        other = os.path.join(self.dir.name, "b")
        gen_data.generate(other, TINY)
        for name in gen_data.TABLES:
            self.assertTrue(table(self.base, name).equals(table(other, name)), name)
        different = os.path.join(self.dir.name, "c")
        gen_data.generate(different, TINY, seed=8)
        for name in gen_data.TABLES:
            self.assertFalse(table(self.base, name).equals(table(different, name)), name)

    def test_a_file_is_made_alone_as_it_is_made_among_the_others(self):
        # One file at a time is the memory bound: file 2 does not need 0 and 1.
        orders, lines = gen_data.orders_file(TINY, 2, seed=gen_data.SEED)
        written = pq.read_table(os.path.join(self.base, "raw", "orders", "part-002.parquet"))
        self.assertTrue(orders.equals(written))
        self.assertLessEqual(orders.num_rows, -(-TINY.orders // TINY.files))
        self.assertEqual(set(lines.column("order_id").to_pylist()), set(orders.column("order_id").to_pylist()))

    def test_data_that_is_already_there_is_kept(self):
        marker = os.path.join(self.base, "raw", "orders", "part-000.parquet")
        before = os.stat(marker).st_mtime_ns
        again = gen_data.generate(self.base, TINY)
        self.assertEqual(again, self.manifest)
        self.assertEqual(os.stat(marker).st_mtime_ns, before, "nothing was written again")

    def test_another_profiles_data_is_not_mistaken_for_this_one(self):
        bigger = dataclasses.replace(TINY, orders=4000)
        with self.assertRaises(ValueError) as caught:
            gen_data.generate(self.base, bigger)
        self.assertIn("force", str(caught.exception))

    def test_replacing_the_tables_leaves_no_file_of_the_old_ones(self):
        base = os.path.join(self.dir.name, "replaced")
        gen_data.generate(base, TINY)
        fewer = dataclasses.replace(TINY, files=2)
        manifest = gen_data.generate(base, fewer, force=True)
        for name in gen_data.TABLES:
            self.assertEqual(len(os.listdir(os.path.join(base, "raw", name))), 2, name)
            self.assertEqual(store.parquet_rows(store.join(base, "raw", name)), manifest["rows"][name])

    def test_tables_without_their_manifest_are_not_finished(self):
        half = os.path.join(self.dir.name, "half")
        gen_data.generate(half, TINY)
        os.unlink(os.path.join(half, "raw", gen_data.MANIFEST))
        marker = os.path.join(half, "raw", "orders", "part-000.parquet")
        before = os.stat(marker).st_mtime_ns
        gen_data.generate(half, TINY)
        self.assertNotEqual(os.stat(marker).st_mtime_ns, before, "a generation that did not end is done again")


if __name__ == "__main__":
    unittest.main()
