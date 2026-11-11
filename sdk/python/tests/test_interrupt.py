#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Ctrl-C while a cell waits for a call: the job is cancelled and the
machine made for it released, so nothing keeps running (and costing) after
the user stopped; the interrupt still stops the cell, and says which job it
was. result(cancel_on_interrupt=False) leaves the job running and says how
to come back to it. A map over several destinations cancels all its jobs."""
import contextlib
import io
import unittest

from zenthros.remote.call import RemoteCall
from zenthros.remote.spread import MapCalls


class Handle:
    def __init__(self, interrupt=True):
        self.interrupt = interrupt
        self.cancelled = 0

    def wait(self, timeout=None, on_reschedule=None):
        if self.interrupt:
            raise KeyboardInterrupt
        raise AssertionError("an uninterrupted job: not this test's")

    def cancel(self):
        self.cancelled += 1


def call(name, interrupt=True):
    made = RemoteCall(name, "s3://b/p", handle=Handle(interrupt), client=object(), blob_store=None)
    made.released = 0

    def release():
        made.released += 1

    made._release = release
    return made


class InterruptTest(unittest.TestCase):
    def test_ctrl_c_cancels_the_job_and_releases_its_machine(self):
        c = call("zr-entrenar-1")
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(KeyboardInterrupt):
            c.result(stream_logs=False)
        self.assertEqual((c.handle.cancelled, c.released), (1, 1))
        self.assertIn("zr-entrenar-1", err.getvalue())
        self.assertIn("cancelled", err.getvalue())

    def test_ctrl_c_can_leave_it_running(self):
        c = call("zr-entrenar-2")
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(KeyboardInterrupt):
            c.result(stream_logs=False, cancel_on_interrupt=False)
        self.assertEqual((c.handle.cancelled, c.released), (0, 0))
        self.assertIn('zenthros.get("zr-entrenar-2")', err.getvalue())

    def test_a_failing_cancel_still_stops_the_cell(self):
        c = call("zr-entrenar-3")

        def broken():
            raise OSError("the API is down")

        c.handle.cancel = broken
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(KeyboardInterrupt):
            c.result(stream_logs=False)
        self.assertEqual(c.released, 1)
        self.assertIn("the API is down", err.getvalue())

    def test_a_map_over_destinations_cancels_every_job(self):
        first, second = call("zr-variant-a"), call("zr-variant-b", interrupt=False)
        calls = MapCalls([{"w": 1}, {"w": 2}], [("aws-spot", [0], first), ("mi-pc", [1], second)])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(KeyboardInterrupt):
            calls.result(stream_logs=False)
        self.assertEqual([c.handle.cancelled for c in calls.calls], [1, 1])
        self.assertEqual([c.released for c in calls.calls], [1, 1])


if __name__ == "__main__":
    unittest.main()
