# SPDX-License-Identifier: Apache-2.0

"""scikit-learn's partial_fit over chunks, as a zenthros.Task.

A Task saves state() and files() together on its policy; restore() gets the
state back and reads its files through self.checkpointer. The model is a
pickle file of the checkpoint, kept in your store, never in Zenthros; run()
returns it.

    model = zenthros.run(Fit(load_chunk, chunks=100, classes=[0, 1]), on="aws-spot")

load_chunk(i) returns (X, y) for chunk i.
"""

import pickle

import zenthros

MODEL = "model.pkl"


class Fit(zenthros.Task):
    policy = zenthros.CheckpointPolicy(every=5)

    def __init__(self, load_chunk, chunks, classes):
        self.load_chunk = load_chunk
        self.chunks = chunks
        self.classes = classes

    def setup(self):
        from sklearn.linear_model import SGDClassifier

        self.model = SGDClassifier(random_state=0)

    def steps(self):
        return self.chunks

    def step(self, i):
        X, y = self.load_chunk(i)
        self.model.partial_fit(X, y, classes=self.classes)

    def files(self):
        return {MODEL: pickle.dumps(self.model)}

    def restore(self, state):
        self.model = pickle.loads(self.checkpointer.file(MODEL))

    def run(self):
        """The model: this run's, or, when the phase already completed, the
        one in its last checkpoint. result() is kept as JSON, so a model
        goes as the file."""
        self.model = None
        super().run()
        return self.model if self.model is not None else pickle.loads(self.checkpointer.file(MODEL))
