# SPDX-License-Identifier: Apache-2.0

"""A job's log as zenthros-api and zctl --records send it (zenthros.jobs.logrecords)."""
import unittest

from zenthros.jobs import logrecords
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI, packed_record, record


class RecordsTest(unittest.TestCase):
    def test_records_cut_anywhere_read_the_same(self):
        # The network cuts the stream where it likes: inside a header, inside
        # a record's bytes, inside a character.
        raw = record("ü line 1\n".encode(), cursor="10", handle="h", attempt=1) + record("ñ 2\n".encode(), cursor="15")
        for size in (1, 2, 3, 7, len(raw)):
            pieces = [raw[i:i + size] for i in range(0, len(raw), size)]
            with self.subTest(size=size):
                self.assertEqual(logrecords.read_text(pieces), "ü line 1\nñ 2\n")

    def test_a_read_that_broke_says_so_and_is_not_its_end(self):
        with self.assertRaises(logrecords.Broken):
            logrecords.read_text([record(b"a\n", cursor="2"), record(b"", error="the gateway restarted")])
        with self.assertRaises(logrecords.Broken):
            logrecords.read_text([record(b"a\n", cursor="2")[:-1]])

    def test_a_followed_read_keeps_the_line_it_did_not_end_for_the_next(self):
        text = logrecords.Text()
        lines = list(logrecords.follow_lines([record(b"a\nhal", cursor="5", handle="h", attempt=1), record(b"", error="cut")], text))
        self.assertEqual(lines, ["a\n"])
        after = text.position
        self.assertEqual((after.handle, after.cursor, after.partial), ("h", "5", "hal"))
        text = logrecords.Text(after)
        self.assertEqual(list(logrecords.follow_lines([record(b"f\nend", cursor="10")], text)), ["half\n", "end"])

    def test_what_never_comes_is_a_line_of_its_own_after_the_line_it_cut(self):
        text = logrecords.Text()
        lines = text.feed({"n": 4, "omitted": 512}, b"a\nha")
        lines += text.feed({"n": 3, "gap": 10, "gap_reason": "budget"}, b"lf\n")
        self.assertEqual("".join(lines), "[zenthros: the first 512 B of this log are not shown]\na\nha\n"
                                         "[zenthros: 10 B of this log were skipped: the machine's hourly log budget ran out]\nlf\n")

    def test_what_no_one_can_count_is_said_without_a_size(self):
        # The Kubernetes API serves only a log's latest file; the kubelet
        # removes old ones.
        text = logrecords.Text()
        lines = text.feed({"n": 2, "omitted": -1}, b"a\n") + text.feed({"n": 2, "gap": -1}, b"b\n")
        self.assertEqual("".join(lines), "[zenthros: an earlier part of this log may not be shown]\na\n"
                                         "[zenthros: part of this log is no longer on the machine]\nb\n")

    def test_a_log_kept_here_is_shown_as_the_api_shows_one(self):
        raw = b"".join(b"line %06d\n" % i for i in range(100_000))  # 1.2 MB
        shown = logrecords.clip(raw)
        first, rest = shown.split("\n", 1)
        self.assertRegex(first, r"^\[zenthros: the first [0-9.]+ KB of this log are not shown\]$")
        self.assertTrue(rest.startswith("line "), rest[:20])
        self.assertTrue(rest.endswith("line 099999\n"))
        self.assertLessEqual(len(rest), logrecords.HISTORY_BYTES)
        self.assertEqual(logrecords.clip(raw, tail=2), "line 099998\nline 099999\n")
        self.assertEqual(logrecords.clip(b"short\n"), "short\n")


class PackedRecordsTest(unittest.TestCase):
    def test_a_packed_record_is_read_as_its_text_wherever_it_is_cut(self):
        text = "epoch 1 loss=0.5\n" * 200
        raw = packed_record(text.encode(), cursor="1", handle="h") + record(b"raw line\n", cursor="2")
        for size in (1, 5, 333, len(raw)):
            pieces = [raw[i:i + size] for i in range(0, len(raw), size)]
            with self.subTest(size=size):
                self.assertEqual(logrecords.read_text(pieces), text + "raw line\n")

    def test_a_packed_record_is_unpacked_within_its_bounds(self):
        # What a machine sends is not trusted: as zctl, the SDK never unpacks
        # past what a record says, past a record, or through a window past
        # 1 MiB.
        zstd = logrecords.zstd_module()
        big = b"a" * (4 << 20)
        line = b"a line of log\n" * 100
        frame = zstd.compress(line)
        streamed = zstd.ZstdCompressor(options={zstd.CompressionParameter.window_log: 23})
        wide = streamed.compress(b"hello\n" * 10) + streamed.flush()
        cases = {
            "a bomb that says it is small": (zstd.compress(big), 100, "zstd"),
            "a bomb that says its size": (zstd.compress(big), len(big), "zstd"),
            "a window past 1 MiB": (wide, 60, "zstd"),
            "more than it says": (frame, len(line) - 1, "zstd"),
            "less than it says": (frame, len(line) + 1, "zstd"),
            "no size": (frame, 0, "zstd"),
            "cut short": (frame[:-6], len(line), "zstd"),
            "two frames": (frame + frame, len(line), "zstd"),
            "a packing this SDK does not know": (frame, len(line), "lz4"),
        }
        for name, (data, n, encoding) in cases.items():
            with self.subTest(name), self.assertRaises(logrecords.Corrupt):
                logrecords.unpack({"n": len(data), "encoding": encoding, "raw_len": n}, data)
        self.assertEqual(logrecords.unpack({"n": len(frame), "encoding": "zstd", "raw_len": len(line)}, frame), line)
        self.assertEqual(logrecords.unpack({"n": 4}, b"raw\n"), b"raw\n")

    def test_a_record_packed_wrong_ends_a_followed_read_for_good(self):
        # Opened again from its Position the same record would come again:
        # the read ends with the error, not as one that broke off.
        bad = record(b"not a frame", encoding="zstd", raw_len=20, cursor="3")
        text = logrecords.Text()
        with self.assertRaises(logrecords.Corrupt):
            list(logrecords.follow_lines([record(b"a\n", cursor="2"), bad], text))
        self.assertEqual(text.position.cursor, "2")


class FollowQueryTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (200, record(b"x\n", cursor="9"))
        self.client = HTTPJobClient(self.api.url, "risk", "t")

    def asked(self):
        return sorted(self.api.requests[-1]["path"].split("?", 1)[1].split("&"))

    def test_a_stream_goes_on_after_the_last_and_only_the_first_asks_a_tail(self):
        list(self.client.follow_logs("j", tail=5).stdout)
        self.assertEqual(self.asked(), ["follow=1", "tail=5"])
        list(self.client.follow_logs("j", after=logrecords.Position(handle="j-h", cursor="7"), tail=5).stdout)
        self.assertEqual(self.asked(), ["after=7", "follow=1", "handle=j-h"])
        self.assertEqual(self.api.requests[-1]["headers"].get("Accept"), logrecords.CONTENT_TYPE)


if __name__ == "__main__":
    unittest.main()
