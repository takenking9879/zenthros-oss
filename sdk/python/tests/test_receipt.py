#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""The receipt: one line once a call succeeded (usability audit 2026-10-17,
U-14).

Pinned: it says how long the call took and where it ran, by the name on=
gave or the default chose, and what it cost when its machine has a price;
it comes once, after the result, only from a result() that streams (the
interactive default), never for a failure, and a price that cannot be read
never fails the call.
"""
import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

from zenthros import errors, machines, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.remote import call as remote_call

STORE = "s3://bucket/calls"


def double(x):
    return 2 * x


def boom(x):
    raise ValueError("no")


class ReceiptTest(unittest.TestCase):
    def setUp(self):
        from test_remote import ExecutingClient

        self.store = MemoryBlobStore()
        self.client = ExecutingClient(self.store)
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo-ml")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)

    def result(self, call, **kwargs):
        out = io.StringIO()
        with redirect_stdout(out):
            value = call.result(**kwargs)
        return value, [line for line in out.getvalue().splitlines() if line.startswith("✓")]

    def test_it_says_how_long_and_where_by_the_default(self):
        call = remote.cloud(code_dir=None)(double).submit(4)
        value, lines = self.result(call)
        self.assertEqual(value, 8)
        self.assertEqual(len(lines), 1, lines)
        self.assertRegex(lines[0], r"^✓ done in \d+s on demo-ml$")

    def test_once_and_only_after_the_result(self):
        call = remote.cloud(code_dir=None)(double).submit(4)
        self.result(call)
        _, again = self.result(call)
        self.assertEqual(again, [])

    def test_not_when_the_logs_are_not_streamed(self):
        call = remote.cloud(code_dir=None)(double).submit(4)
        _, lines = self.result(call, stream_logs=False)
        self.assertEqual(lines, [])

    def test_never_for_a_failure(self):
        call = remote.cloud(code_dir=None)(boom).submit(1)
        out = io.StringIO()
        with redirect_stdout(out), self.assertRaises(remote.RemoteError):
            call.result()
        self.assertNotIn("✓", out.getvalue())

    def test_the_name_on_gave_and_the_price_when_there_is_one(self):
        self.client.job_cost = lambda name: {"job": name, "cost": 0.0123, "currency": "USD", "seconds": 90}
        with mock.patch.object(machines, "place", return_value=machines.Placement({"pool": "aws-spot"},
                                                                                  name="aws-spot")):
            call = remote.cloud(code_dir=None)(double).submit(4, on="aws-spot")
        _, lines = self.result(call)
        self.assertRegex(lines[0], r"^✓ done in \d+s on aws-spot · \$0\.0123$")

    def test_a_price_that_cannot_be_read_never_fails_the_call(self):
        def unreadable(name):
            raise errors.TransientError("zenthros-api is restarting")

        self.client.job_cost = unreadable
        call = remote.cloud(code_dir=None)(double).submit(4)
        value, lines = self.result(call)
        self.assertEqual(value, 8)
        self.assertRegex(lines[0], r"^✓ done in \d+s on demo-ml$")

    def test_how_long_reads_as_a_person_says_it(self):
        self.assertEqual([remote_call._took(s) for s in (9, 86, 3600 + 125)], ["9s", "1m26s", "1h02m"])


if __name__ == "__main__":
    unittest.main()
