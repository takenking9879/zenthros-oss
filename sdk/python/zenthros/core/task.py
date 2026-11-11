# SPDX-License-Identifier: Apache-2.0

"""zenthros.Task: a job as a class that survives losing its machine.

    class Features(zenthros.Task):
        policy = zenthros.CheckpointPolicy(every=10)

        def setup(self):             # sessions, config, clients: not pickled
            self.store = connect()
            self.done = []

        def steps(self):
            return 100               # or a sequence; step() gets the index

        def step(self, i):
            self.done.append(build_chunk(self.store, i))

        def state(self):
            return {"done": self.done}

        def restore(self, state):
            self.done = state["done"]

        def result(self):
            return len(self.done)

    Features().run()                           # here
    zenthros.run(Features(), on="aws-spot")    # on a machine; same code

files() is what a library saves on its own (a model, weights), as
{name: bytes or a local path}: kept with each saved step next to state(),
and read back in restore() with self.checkpointer.file(name) or
.file_path(name).

run() is the SDK's: it resumes after the last saved step, saves on the
policy (every N steps, and at once on the interruption notice), and marks
the phase complete with result(). A completed phase returns its result
without running setup again. Outside a job nothing is written.

stop_on_notice = True stops at the notice, right after saving that step
(StoppedOnNotice), instead of working until the machine goes: the
replacement starts sooner, from the step just saved.
"""

from zenthros.core.checkpoint import Checkpointer, CheckpointPolicy
from zenthros.errors import StoppedOnNotice
from zenthros.logging import create_logger

logger = create_logger("zenthros.core.task")


class Task:
    # The checkpoint phase. Default: the class name, stable across machines.
    name = None
    policy = CheckpointPolicy()
    stop_on_notice = False

    def setup(self):
        """Open what the steps need. Runs on the machine, before restore()."""

    def steps(self):
        """How many steps: an int, or a sequence (its length)."""
        return 1

    def step(self, i):
        raise NotImplementedError(f"{type(self).__name__}.step(i)")

    def state(self):
        """JSON-able state that restore() takes back."""
        return {}

    def files(self):
        """{name: bytes or a local path} saved with state(); restore() reads
        them with self.checkpointer.file(name)."""
        return {}

    def restore(self, state):
        """Continue from state, the last one state() returned and was saved."""

    def result(self):
        """What run() returns. JSON-able: a completed phase returns it from
        its checkpoint without running again; a model goes in files()."""
        return None

    @property
    def phase_name(self):
        return self.name or type(self).__name__

    def __call__(self):
        return self.run()

    def run(self):
        ckpt = Checkpointer(self.phase_name)
        self.checkpointer = ckpt
        saved = ckpt.state if isinstance(ckpt.state, dict) else {}
        if ckpt.completed and "result" in saved:
            logger.info(f"task {self.phase_name} already complete at seq={ckpt.seq}")
            return saved["result"]
        ckpt.completed = False
        self.setup()
        total = _count(self.steps())
        start = 0
        if "step" in saved:
            self.restore(saved.get("state"))
            start = int(saved["step"]) + 1
            logger.info(f"task {self.phase_name}: resuming at step {start} of {total}")
        for i in range(start, total):
            self.step(i)
            # The last step is saved by complete(), right after.
            reason = self.policy.due(ckpt, i) if i < total - 1 else None
            if reason:
                ckpt.save({"step": i, "state": self.state()}, files=self.files())
            if reason == "notice" and self.stop_on_notice:
                raise StoppedOnNotice(f"task {self.phase_name}: stopped after step {i} at the interruption notice")
        result = self.result()
        ckpt.complete({"step": total - 1, "state": self.state(), "result": result}, files=self.files())
        return result


def _count(steps):
    return int(steps) if isinstance(steps, int) else len(steps)
