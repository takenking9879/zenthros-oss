#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""What the code publishes with zenthros.report, in the notebook: a line
each time a call's last report changes while result() waits; a column in a
map's table with each config's last report; and calls.reports(), one row
per config to compare them. Zenthros's own reports (the result digests) are
never shown.
"""
import io
import time
import unittest
from unittest import mock

from zenthros.jobs.handle import ReportView
from zenthros.remote.call import RESULT_PRODUCER, RemoteCall, _ReportFollower, report_text
from zenthros.remote.spread import MapCalls


def view(producer, payload, sequence):
    return ReportView(job_id="j", producer_id=producer, payload=payload, sequence=sequence)


class Answers:
    """reports() answers in turn, then the last one forever."""

    def __init__(self, *answers):
        self.answers = list(answers)

    def __call__(self):
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]


class TextTest(unittest.TestCase):
    def test_a_payload_in_one_short_line(self):
        self.assertEqual(report_text({"epoch": 3, "loss": 0.213412345}), "epoch=3 · loss=0.2134")
        self.assertEqual(report_text({"m": {"auc": 0.9}}), 'm={"auc": 0.9}')
        self.assertLessEqual(len(report_text({"x": "y" * 1000})), 160)


class FollowerTest(unittest.TestCase):
    def follow(self, answers, name="zr-entrenar-1"):
        call = mock.Mock(spec=["reports", "name"])
        call.name, call.reports = name, Answers(*answers)
        out = io.StringIO()
        follower = _ReportFollower(call, poll_seconds=0.01, out=out)
        follower.start()
        time.sleep(0.3)
        follower.stop()
        return out.getvalue().splitlines()

    def test_a_line_when_the_last_report_changes_and_never_zenthros_own(self):
        internal = view(f"{RESULT_PRODUCER}-g1", {"sha256": "abc"}, 1)
        lines = self.follow([[], [view("host-a", {"epoch": 1}, 1)], [view("host-a", {"epoch": 1}, 1), internal],
                             [view("host-a", {"epoch": 2}, 2), internal]])
        self.assertEqual(lines, ["zenthros: zr-entrenar-1 · epoch=1", "zenthros: zr-entrenar-1 · epoch=2"])

    def test_a_map_says_which_task(self):
        lines = self.follow([[view("task-0", {"loss": 0.5}, 1), view("task-1", {"loss": 0.7}, 1)]])
        self.assertEqual(sorted(lines), ["zenthros: zr-entrenar-1 task 0 · loss=0.5",
                                         "zenthros: zr-entrenar-1 task 1 · loss=0.7"])

    def test_a_failing_read_is_skipped_and_the_next_one_shown(self):
        answers = [OSError("api down"), [view("host-a", {"epoch": 4}, 1)]]

        def reports():
            answer = answers.pop(0) if len(answers) > 1 else answers[0]
            if isinstance(answer, Exception):
                raise answer
            return answer

        call = mock.Mock(spec=["reports", "name"])
        call.name, call.reports = "zr-x", reports
        out = io.StringIO()
        follower = _ReportFollower(call, poll_seconds=0.01, out=out)
        follower.start()
        time.sleep(0.2)
        follower.stop()
        self.assertFalse(follower.is_alive())
        self.assertEqual(out.getvalue().splitlines(), ["zenthros: zr-x · epoch=4"])


class MapReportsTest(unittest.TestCase):
    def calls(self):
        first = RemoteCall("zr-v-a", "s3://b/a", handle=None, client=object(), blob_store=None)
        second = RemoteCall("zr-v-b", "s3://b/b", handle=None, client=object(), blob_store=None)
        # History may come in any order: the last is the highest sequence.
        first.reports = lambda: [view("task-0", {"loss": 0.5}, 2), view("task-1", {"loss": 0.4}, 1),
                                 view("task-0", {"loss": 0.99}, 1),
                                 view(f"{RESULT_PRODUCER}-g1-0", {"sha256": "x"}, 1)]
        second.reports = lambda: [view("task-0", {"loss": 0.9}, 1)]
        for c in (first, second):
            c.status = lambda: "Running"
        configs = [{"lr": 0.1}, {"lr": 0.2}, {"lr": 0.3}]
        return MapCalls(configs, [("aws-spot", [0, 2], first), ("mi-pc", [1], second)])

    def test_the_table_has_each_configs_last_report(self):
        table = repr(self.calls()).splitlines()
        self.assertIn("report", table[1])
        self.assertTrue(table[2].endswith("loss=0.5"), table)
        self.assertTrue(table[3].endswith("loss=0.9"), table)
        self.assertTrue(table[4].endswith("loss=0.4"), table)

    def test_reports_one_row_per_config(self):
        rows = self.calls().reports()
        rows = rows.to_dict("records") if hasattr(rows, "to_dict") else rows
        self.assertEqual(rows, [{"config": 0, "on": "aws-spot", "lr": 0.1, "loss": 0.5},
                                {"config": 1, "on": "mi-pc", "lr": 0.2, "loss": 0.9},
                                {"config": 2, "on": "aws-spot", "lr": 0.3, "loss": 0.4}])


class WaitTest(unittest.TestCase):
    def test_result_follows_the_reports_while_it_follows_the_log(self):
        # job_cost as a client without zenthros-api answers: no price.
        call = RemoteCall("zr-x", "s3://b/p", handle=mock.Mock(), client=mock.Mock(**{"job_cost.return_value": None}),
                          blob_store=None)
        call._collect = lambda failure: (True, 1)
        with mock.patch("zenthros.remote.call._ReportFollower") as follower, \
                mock.patch("zenthros.remote.call._LogFollower"):
            self.assertEqual(call.result(), 1)
        follower.assert_called_once()
        follower.return_value.stop.assert_called_once()
        with mock.patch("zenthros.remote.call._ReportFollower") as quiet:
            again = RemoteCall("zr-y", "s3://b/p", handle=mock.Mock(), client=mock.Mock(), blob_store=None)
            again._collect = lambda failure: (True, 1)
            again.result(stream_logs=False)
        quiet.assert_not_called()


if __name__ == "__main__":
    unittest.main()
