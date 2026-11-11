#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.Task: the order run() keeps, what it saves, and where it resumes."""

import json
import os
import tempfile
import unittest
from unittest import mock

import zenthros
from zenthros.core.checkpoint import CheckpointPolicy, write_blob
from zenthros.core.storage import MemoryBlobStore

from support import job, no_job


class Counter(zenthros.Task):
    """Adds step indexes; records every call in order."""

    policy = CheckpointPolicy(every=3)

    def __init__(self, n=10, notice_at=None, notice=None):
        self.n = n
        self.notice_at = notice_at
        self.notice = notice
        self.calls = []

    def setup(self):
        self.calls.append("setup")
        self.total = 0

    def steps(self):
        return self.n

    def step(self, i):
        self.calls.append(("step", i))
        self.total += i
        if i == self.notice_at:
            self.notice()

    def state(self):
        return {"total": self.total}

    def restore(self, state):
        self.calls.append(("restore", state["total"]))
        self.total = state["total"]

    def result(self):
        return self.total


def resumed_env(store, name, state, seq=1, complete=False):
    uri = f"memory://out/jobs/uid-1/gen/1/{name}/{seq}"
    _, digest = write_blob(uri, state, store=store)
    cursor = {name: {"seq": seq, "uri": uri, "sha256": digest, "complete": complete}}
    return {"ZENTHROS_CHECKPOINT_CURSORS": json.dumps(cursor)}


class TaskTest(unittest.TestCase):
    def test_saves_on_the_policy_and_completes_after_the_last_step(self):
        store = MemoryBlobStore()
        with job(store) as agent:
            self.assertEqual(Counter(10).run(), 45)
        saved = [(c["seq"], json.loads(store.get(c["uri"]))["state"]["step"], c["complete"]) for c in agent.acked]
        # every=3: after steps 2, 5, 8; the last step (9) is the complete one.
        self.assertEqual(saved, [(1, 2, False), (2, 5, False), (3, 8, False), (4, 9, True)])
        self.assertEqual(json.loads(store.get(agent.acked[-1]["uri"]))["state"]["result"], 45)

    def test_a_last_step_on_the_cadence_is_saved_once(self):
        store = MemoryBlobStore()
        with job(store) as agent:
            Counter(9).run()
        saved = [(json.loads(store.get(c["uri"]))["state"]["step"], c["complete"]) for c in agent.acked]
        self.assertEqual(saved, [(2, False), (5, False), (8, True)])

    def test_setup_then_restore_then_the_step_after_the_saved_one(self):
        store = MemoryBlobStore()
        env = resumed_env(store, "Counter", {"step": 5, "state": {"total": 15}})
        task = Counter(10)
        with job(store, env=env):
            self.assertEqual(task.run(), 45)
        self.assertEqual(task.calls[:3], ["setup", ("restore", 15), ("step", 6)])
        self.assertNotIn(("step", 5), task.calls)

    def test_a_completed_task_returns_its_result_without_setup(self):
        store = MemoryBlobStore()
        env = resumed_env(store, "Counter", {"step": 9, "state": {"total": 45}, "result": 45}, complete=True)
        task = Counter(10)
        with job(store, env=env) as agent:
            self.assertEqual(task.run(), 45)
        self.assertEqual(task.calls, [])
        self.assertEqual(agent.acked, [])

    def test_the_notice_saves_the_step_it_appears_on(self):
        store = MemoryBlobStore()
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "notice.json")

            def notice():
                with open(path, "w") as f:
                    f.write("{}")

            with job(store, env={"ZENTHROS_PREEMPT_FILE": path}) as agent:
                Counter(10, notice_at=3, notice=notice).run()
        steps = [json.loads(store.get(c["uri"]))["state"]["step"] for c in agent.acked]
        # 3 is off the cadence; the notice is taken once, then every=3 goes on.
        self.assertEqual(steps, [2, 3, 5, 8, 9])

    def test_stop_on_notice_saves_that_step_then_stops(self):
        store = MemoryBlobStore()
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "notice.json")

            def notice():
                with open(path, "w") as f:
                    f.write("{}")

            task = Counter(10, notice_at=3, notice=notice)
            task.stop_on_notice = True
            with job(store, env={"ZENTHROS_PREEMPT_FILE": path}) as agent, \
                    self.assertRaises(zenthros.errors.StoppedOnNotice):
                task.run()
        self.assertEqual([json.loads(store.get(c["uri"]))["state"]["step"] for c in agent.acked], [2, 3])
        self.assertFalse(agent.acked[-1]["complete"])
        self.assertEqual(task.calls[-1], ("step", 3))

    def test_run_again_in_the_same_process_continues_from_its_own_save(self):
        # The runner's retry_on runs the Task again in this process. The job's
        # env still holds the cursor it started with; the controller has
        # acked past it, and a save from there would be refused as behind.
        store = MemoryBlobStore()
        with job(store) as agent:
            first = Counter(10, notice_at=4, notice=self._raise)
            with self.assertRaises(ConnectionError):
                first.run()
            again = Counter(10)
            self.assertEqual(again.run(), 45)
        self.assertEqual(again.calls[:2], ["setup", ("restore", 3)])
        seqs = [c["seq"] for c in agent.acked]
        self.assertEqual(seqs, sorted(set(seqs)), "every seq is new: none goes back")

    @staticmethod
    def _raise():
        raise ConnectionError("the database went away")

    def test_outside_a_job_nothing_is_written(self):
        with no_job(), mock.patch("zenthros.core.checkpoint.write_blob") as write:
            self.assertEqual(Counter(4).run(), 6)
        write.assert_not_called()

    def test_a_task_is_its_own_callable(self):
        # zenthros.run(task) ships the instance and calls it on the machine.
        with no_job():
            self.assertEqual(Counter(3)(), 3)

    def test_the_phase_is_the_class_name_unless_named(self):
        class Named(Counter):
            name = "features"

        self.assertEqual(Counter().phase_name, "Counter")
        self.assertEqual(Named().phase_name, "features")


class Notices:
    def __init__(self, at=()):
        self.at, self.asked = set(at), []

    def notice(self):
        self.asked.append(len(self.asked))
        return len(self.asked) - 1 in self.at


class PolicyTest(unittest.TestCase):
    def test_every_n_and_last(self):
        policy = CheckpointPolicy(every=5)
        ckpt = Notices()
        reasons = [policy.due(ckpt, step, total=12) for step in range(12)]
        self.assertEqual([i for i, r in enumerate(reasons) if r], [4, 9, 11])
        self.assertEqual(reasons[11], "last")

    def test_the_notice_is_asked_on_every_step(self):
        # Also on a step that saves anyway: the notice is not left for later.
        policy = CheckpointPolicy(every=2)
        ckpt = Notices(at={1})
        self.assertEqual([policy.due(ckpt, s) for s in range(4)], [None, "notice", None, "every"])
        self.assertEqual(len(ckpt.asked), 4)

    def test_no_cadence_saves_only_on_notice(self):
        policy = CheckpointPolicy()
        self.assertEqual([policy.due(Notices(at={7}), s) for s in range(3)], [None, None, None])
        ckpt = Notices(at={2})
        self.assertEqual([policy.due(ckpt, s) for s in range(4)], [None, None, "notice", None])

    def test_on_notice_off_never_asks(self):
        ckpt = Notices(at={0})
        CheckpointPolicy(every=10, on_notice=False).due(ckpt, 0)
        self.assertEqual(ckpt.asked, [])


if __name__ == "__main__":
    unittest.main()
