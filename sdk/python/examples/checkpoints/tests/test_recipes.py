# SPDX-License-Identifier: Apache-2.0

"""The checkpoint recipes (examples/checkpoints), without a cluster: the
machine is lost mid-run, a replacement resumes from the acked checkpoint,
and the model is the one an uninterrupted run gives.

Pinned (xgboost_spot on Floci showed it, 2026-10-19): the model goes as a
file of the checkpoint, stored before the checkpoint's JSON and acked after
it; the replacement resumes at the round after the acked one, not at 0;
the predictions match an uninterrupted run. Each recipe's test skips
without its library.
"""

import importlib.util
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))

from zenthros.core.checkpoint import forget_acked  # noqa: E402
from zenthros.core.storage import MemoryBlobStore  # noqa: E402


class Lost(Exception):
    """The machine went away: nothing after this ack runs."""


class Agent:
    """Acks checkpoints; after `lose_after` acks the machine is lost."""

    def __init__(self, lose_after=None):
        self.acked = []
        self.lose_after = lose_after

    def ack_checkpoint(self, commit, timeout=10):
        self.acked.append(commit)
        if self.lose_after is not None and len(self.acked) == self.lose_after:
            raise Lost()
        return {"accepted": True, "seq": commit["seq"]}


@contextmanager
def job(store, agent, env=None):
    forget_acked()
    values = {"ZENTHROS_JOB_ID": "uid-1", "ZENTHROS_GENERATION": "1", "ZENTHROS_CHECKPOINT_PREFIX": "memory://out",
              **(env or {})}
    with mock.patch.dict(os.environ, values), \
            mock.patch("zenthros.core.checkpoint.store_for", lambda uri="", env=None: store), \
            mock.patch("zenthros.core.checkpoint.AgentClient", lambda env=None: agent):
        yield


def replacement(commit):
    """The env the controller gives the replacement: the last acked cursor."""
    return {"ZENTHROS_GENERATION": "2", "ZENTHROS_CHECKPOINT_PHASE": commit["phase"],
            "ZENTHROS_CHECKPOINT_URI": commit["uri"], "ZENTHROS_CHECKPOINT_SEQ": str(commit["seq"]),
            "ZENTHROS_CHECKPOINT_SHA256": commit["sha256"]}


def lost_then_resumed(run, lose_after):
    """run() on a machine lost at its `lose_after`-th ack, then on a
    replacement; (what the replacement returned, the store, the acks)."""
    store, first = MemoryBlobStore(), Agent(lose_after=lose_after)
    with job(store, first):
        try:
            run()
        except Lost:
            pass
        else:
            raise AssertionError("the machine was never lost")
    second = Agent()
    with job(store, second, env=replacement(first.acked[-1])):
        return run(), store, first.acked + second.acked


@unittest.skipUnless(importlib.util.find_spec("xgboost") and importlib.util.find_spec("numpy"), "xgboost required")
class XGBoostResumeTest(unittest.TestCase):
    def test_the_replacement_trains_the_rounds_left_from_the_saved_booster(self):
        import numpy as np
        import xgboost as xgb
        import xgboost_resume

        rng = np.random.RandomState(0)
        X = rng.rand(300, 4)
        y = X @ [1.0, -2.0, 0.5, 3.0] + rng.normal(0, 0.1, 300)
        params = {"max_depth": 3, "eta": 0.3, "nthread": 1}
        whole = xgboost_resume.train(params, X, y, rounds=20, every=5)

        rounds = []
        real = xgb.train

        def counted(*args, **kwargs):
            rounds.append(kwargs["num_boost_round"])
            return real(*args, **kwargs)

        with mock.patch.object(xgb, "train", counted):
            got, store, acked = lost_then_resumed(lambda: xgboost_resume.train(params, X, y, rounds=20, every=5),
                                                  lose_after=2)
        # Lost after the round-9 save: the replacement trains rounds 10–19.
        self.assertEqual(rounds, [20, 10])
        self.assertTrue(acked[-1]["complete"])
        # The booster is a file next to the JSON the ack names, stored first.
        self.assertTrue(store.get(f"{acked[0]['uri']}.files/{xgboost_resume.MODEL}"))
        dtest = xgb.DMatrix(X[:50])
        np.testing.assert_array_equal(got.predict(dtest), whole.predict(dtest))


@unittest.skipUnless(importlib.util.find_spec("sklearn"), "scikit-learn required")
class SklearnResumeTest(unittest.TestCase):
    def test_the_replacement_fits_the_chunks_left(self):
        import numpy as np
        import sklearn_resume

        rng = np.random.RandomState(1)
        X = rng.rand(400, 3)
        y = (X[:, 0] > 0.5).astype(int)
        seen = []

        def load_chunk(i):
            seen.append(i)
            return X[i * 20:(i + 1) * 20], y[i * 20:(i + 1) * 20]

        whole = sklearn_resume.Fit(load_chunk, chunks=20, classes=[0, 1]).run()
        seen.clear()
        got, _, _ = lost_then_resumed(lambda: sklearn_resume.Fit(load_chunk, chunks=20, classes=[0, 1]).run(),
                                      lose_after=2)
        # every=5: saved after chunks 4 and 9, lost at the second; the
        # replacement reads chunks 10–19 only.
        self.assertEqual(seen, list(range(10)) + list(range(10, 20)))
        np.testing.assert_array_equal(got.coef_, whole.coef_)

    def test_a_completed_fit_returns_its_model_without_fitting_again(self):
        # result() is kept as JSON: a model there failed the last save.
        import numpy as np
        import sklearn_resume

        X, y = np.eye(4)[[0, 1, 2, 3] * 5], np.array([0, 1, 0, 1] * 5)
        store, agent = MemoryBlobStore(), Agent()
        with job(store, agent):
            first = sklearn_resume.Fit(lambda i: (X, y), chunks=3, classes=[0, 1]).run()
        with job(store, Agent(), env=replacement(agent.acked[-1])):
            again = sklearn_resume.Fit(lambda i: self.fail("fitted again"), chunks=3, classes=[0, 1]).run()
        np.testing.assert_array_equal(again.coef_, first.coef_)


@unittest.skipUnless(importlib.util.find_spec("torch"), "torch required")
class TorchResumeTest(unittest.TestCase):
    def test_the_replacement_runs_the_epochs_left_from_the_saved_weights(self):
        import torch
        import torch_resume

        torch.manual_seed(0)
        x, y = torch.rand(64, 3), torch.rand(64, 1)

        def make_model():
            torch.manual_seed(1)
            return torch.nn.Linear(3, 1)

        epochs = []

        def batches():
            epochs.append(1)
            yield x, y

        whole = torch_resume.train(make_model, batches, epochs=6)
        epochs.clear()
        got, _, _ = lost_then_resumed(lambda: torch_resume.train(make_model, batches, epochs=6), lose_after=3)
        # Saved every epoch, lost at the third ack (epoch 2): 3 + 3 epochs, none twice.
        self.assertEqual(len(epochs), 6)
        for name in whole:
            torch.testing.assert_close(got[name], whole[name])


if __name__ == "__main__":
    unittest.main()
