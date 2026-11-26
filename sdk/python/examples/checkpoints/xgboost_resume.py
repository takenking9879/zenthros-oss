# SPDX-License-Identifier: Apache-2.0

"""XGBoost that resumes where a lost machine stopped, on Zenthros checkpoints.

The booster is a file of the checkpoint: saved every `every` rounds, and at
once on the interruption notice, straight to your store. Zenthros keeps only
the pointer to it. The replacement loads it and trains the rounds left.

    booster = zenthros.run(train, params, X, y, rounds=200, on="aws-spot")

Outside a job nothing is saved and it is plain xgboost.train.
"""

import os
import tempfile

import zenthros

MODEL = "model.ubj"


def train(params, X, y, rounds, every=10, phase="xgboost"):
    import xgboost as xgb

    ckpt = zenthros.Checkpointer(phase)
    policy = zenthros.CheckpointPolicy(every=every)
    start, booster = 0, None
    if ckpt.state:
        start = ckpt.state["round"] + 1
        booster = xgb.Booster(model_file=ckpt.file_path(MODEL))
    if start >= rounds:
        return booster
    folder = tempfile.mkdtemp()

    class Save(xgb.callback.TrainingCallback):
        def after_iteration(self, model, epoch, evals_log):
            done = start + epoch  # after a resume xgboost counts epochs from 0 again
            if policy.due(ckpt, done, rounds):
                path = os.path.join(folder, MODEL)
                model.save_model(path)
                ckpt.save({"round": done}, files={MODEL: path}, complete=done + 1 >= rounds)
            return False

    return xgb.train(params, xgb.DMatrix(X, label=y), num_boost_round=rounds - start,
                     xgb_model=booster, callbacks=[Save()])
