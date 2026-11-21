# SPDX-License-Identifier: Apache-2.0

"""examples/data/lazy_training.py, the skill's way to train on data larger
than the machine (reference/data.md): read lazily in batches, it is as good
as reading all at once, and each pass reads the file once, in order."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "data" / "lazy_training.py"
HAS = all(importlib.util.find_spec(name) for name in ("polars", "xgboost", "numpy"))


def _example():
    spec = importlib.util.spec_from_file_location("lazy_training", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(HAS, "polars, xgboost and numpy: the example's own packages")
class LazyTrainingTest(unittest.TestCase):
    ROWS, CHUNK = 40_000, 7_000

    @classmethod
    def setUpClass(cls):
        cls.example = _example()
        cls.folder = tempfile.TemporaryDirectory()
        cls.train_path, cls.val_path = cls.example.synthetic(cls.folder.name, rows=cls.ROWS, features=8)

    @classmethod
    def tearDownClass(cls):
        cls.folder.cleanup()

    def all_at_once(self):
        """The RMSE of the same model on the data read whole."""
        import numpy as np
        import polars as pl
        import xgboost as xgb

        train, val = pl.read_parquet(self.train_path), pl.read_parquet(self.val_path)
        columns = [c for c in train.columns if c != "y"]
        matrix = xgb.QuantileDMatrix(train.select(columns).to_numpy(), label=train["y"].to_numpy(), max_bin=256)
        params = {"tree_method": "hist", "max_depth": 8, "learning_rate": 0.1, "objective": "reg:squarederror", "seed": 0}
        booster = xgb.train(params, matrix, num_boost_round=30)
        predicted = booster.inplace_predict(val.select(columns).to_numpy()).astype(np.float64)
        return float(np.sqrt(np.mean((predicted - val["y"].to_numpy()) ** 2)))

    def test_in_batches_it_is_within_1_percent_of_all_at_once(self):
        whole = self.all_at_once()
        for external in (False, True):
            with self.subTest(external=external):
                got = self.example.train(self.train_path, self.val_path, rounds=30, external=external, chunk_rows=self.CHUNK)
                self.assertLess(abs(got["rmse"] - whole) / whole, 0.01, (got, whole))

    def test_each_pass_reads_the_file_once_in_order(self):
        import xgboost as xgb

        columns = [f"x{i}" for i in range(8)]
        data = self.example.batches(self.train_path, columns, "y", self.CHUNK)
        xgb.QuantileDMatrix(data, max_bin=256)
        # XGBoost resets once more when it is done, and reads nothing then:
        # opening the LazyFrame reads nothing until a batch is asked for.
        read = [rows for rows in data.rows_per_pass if rows]
        self.assertTrue(read and data.rows_per_pass[-1] == 0, data.rows_per_pass)
        self.assertEqual(read, [self.ROWS] * len(read), "every row once a pass, none twice")


if __name__ == "__main__":
    unittest.main()
