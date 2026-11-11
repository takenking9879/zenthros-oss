#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""ws.clean(older_than=): the Workspace's store keeps every call and its
result until someone deletes them. clean deletes old calls with their results
and checkpoints, through DELETEs the API signs, and nothing else: not a call
whose job is still in the Workspace, not what a job wrote beside its call
unless outputs=True, not the code (code/, one copy per content), not what
ws.upload put there or a job wrote elsewhere.
"""
import datetime
import json
import unittest

from zenthros import errors, view
from zenthros.core import checkpoint
from zenthros.jobs.http import HTTPJobClient

from support import FakeAPI

ROOT = "s3://calls/zenthros/risk"
NOW = datetime.datetime(2026, 10, 15, 12, 0, tzinfo=datetime.timezone.utc)


def listing(objects):
    """A ListObjectsV2 page: (key, size, days before NOW, or None: a store
    that says not when)."""
    def modified(days):
        if days is None:
            return ""
        return f"<LastModified>{(NOW - datetime.timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S.000Z')}</LastModified>"

    contents = "".join(f"<Contents><Key>{key}</Key>{modified(days)}<Size>{size}</Size></Contents>"
                       for key, size, days in objects)
    return ('<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>calls</Name>{contents}<IsTruncated>false</IsTruncated></ListBucketResult>").encode()


class CleanTest(unittest.TestCase):
    def setUp(self):
        self.api, self.s3 = FakeAPI(), FakeAPI()
        self.addCleanup(self.api.close)
        self.addCleanup(self.s3.close)
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": ROOT})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"items": [{"metadata": {"name": "zr-live"}}]})
        sha = "0123456789abcdef"
        self.objects = [
            # An old call and its result: it goes.
            ("zenthros/risk/zr-old/call-" + sha + ".pkl", 100, 40),
            ("zenthros/risk/zr-old/result.pkl", 50, 40),
            # An old map: its results/ go with it.
            ("zenthros/risk/barrido-1-0/call-" + sha + ".pkl", 10, 35),
            ("zenthros/risk/barrido-1-0/results/0.pkl", 5, 35),
            ("zenthros/risk/barrido-1-0/results/1.pkl", 5, 35),
            # Its call is old but its result is recent: it stays.
            ("zenthros/risk/zr-recent/call-" + sha + ".pkl", 10, 40),
            ("zenthros/risk/zr-recent/result.pkl", 10, 2),
            # Its job is still in the Workspace: it stays, however old.
            ("zenthros/risk/zr-live/call-" + sha + ".pkl", 10, 90),
            # When one of its objects was written is unknown: it stays.
            ("zenthros/risk/zr-unknown/call-" + sha + ".pkl", 10, 90),
            ("zenthros/risk/zr-unknown/result.pkl", 10, None),
            # The code, and what the user uploaded: never.
            ("zenthros/risk/code/" + "a" * 64 + ".tar.gz", 1000, 90),
            ("zenthros/risk/datos/ventas.csv", 9999, 90),
            ("zenthros/risk/datos/call-" + sha + ".pkl.bak", 1, 90),
            ("zenthros/risk/datos/call-notas.pkl", 1, 90),
            ("zenthros/risk/salidas/zr-old/model.pkl", 7, 90),
        ]
        self.asked = []

        def sign(body):
            asked = json.loads(body)
            self.asked.append(asked)
            if asked["method"] == "LIST":
                prefix = asked["uri"][len("s3://calls/"):]
                self.s3.routes[("GET", "/list")] = (200, listing([o for o in self.objects if o[0].startswith(prefix)]))
                return 200, {"url": self.s3.url + "/list", "method": "GET", "headers": {}}
            key = asked["uri"][len("s3://calls/"):]
            # An object another clean already took: 404, and no error.
            gone = key.endswith("results/1.pkl")
            self.s3.routes[(asked["method"], "/" + key)] = (404, b"<Error/>") if gone else (204, b"")
            return 200, {"url": f"{self.s3.url}/{key}", "method": asked["method"], "headers": {}}

        self.api.routes[("POST", "/v1/workspaces/risk/objects/presign")] = sign
        self.ws = view.Workspace(HTTPJobClient(self.api.url, "risk", "bob-token"))
        self.ws._now = lambda: NOW

    def deleted(self):
        return [r["path"] for r in self.s3.requests if r["method"] == "DELETE"]

    def test_old_calls_go_and_nothing_else(self):
        cleaned = self.ws.clean(older_than="30d")
        self.assertEqual(cleaned.calls, ["barrido-1-0", "zr-old"])
        self.assertEqual((cleaned.objects, cleaned.bytes), (5, 170))
        self.assertEqual(cleaned.kept_live, ["zr-live"])
        self.assertEqual(sorted(self.deleted()), sorted("/" + key for key, _, _ in self.objects[:5]))
        self.assertIn("2 calls", repr(cleaned))
        self.assertIn("zr-live", repr(cleaned))
        self.assertTrue(all(r["auth"] is None for r in self.s3.requests), "the token reached the store")

    def test_code_goes_only_when_asked_and_never_what_a_job_pins(self):
        old, pinned, recent = "a" * 64, "b" * 64, "c" * 64
        self.objects += [(f"zenthros/risk/code/{pinned}.tar.gz", 1000, 90), (f"zenthros/risk/code/{recent}.tar.gz", 1000, 2)]
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"items": [
            {"metadata": {"name": "zr-live", "annotations": {"zenthros.ai/code-sha256": pinned}}}]})
        self.assertEqual(self.ws.clean(older_than="30d", dry_run=True).code, [], "code/ only when asked")
        cleaned = self.ws.clean(older_than="30d", code=True)
        self.assertEqual((cleaned.code, cleaned.kept_code), ([old], [pinned]))
        gone = self.deleted()
        self.assertIn(f"/zenthros/risk/code/{old}.tar.gz", gone)
        self.assertNotIn(f"/zenthros/risk/code/{pinned}.tar.gz", gone)
        self.assertNotIn(f"/zenthros/risk/code/{recent}.tar.gz", gone)
        self.assertIn("from code/", repr(cleaned))

    def test_the_call_goes_last_so_an_interrupted_clean_finishes_next_time(self):
        self.ws.clean(older_than="30d")
        for folder in ("zr-old", "barrido-1-0"):
            mine = [path for path in self.deleted() if f"/{folder}/" in path]
            self.assertIn("/call-", mine[-1], mine)

    def test_dry_run_says_what_would_go_and_deletes_nothing(self):
        cleaned = self.ws.clean(older_than="30d", dry_run=True)
        self.assertEqual(cleaned.calls, ["barrido-1-0", "zr-old"])
        self.assertTrue(cleaned.dry_run)
        self.assertIn("would delete", repr(cleaned))
        self.assertEqual(self.deleted(), [])
        self.assertEqual({a["method"] for a in self.asked}, {"LIST"})

    def test_older_than_is_when(self):
        self.assertEqual(self.ws.clean(older_than="36d", dry_run=True).calls, ["zr-old"])
        self.assertEqual(self.ws.clean(older_than="1d", dry_run=True).calls, ["barrido-1-0", "zr-old", "zr-recent"])
        for bad in (None, "", "soon", 0, -1):
            with self.assertRaises(errors.ValidationError, msg=bad):
                self.ws.clean(older_than=bad)
        self.assertEqual(self.deleted(), [])

    def test_calls_limits_it_to_those_calls(self):
        cleaned = self.ws.clean(older_than="30d", calls=["zr-old", "zr-live", "datos", "zr-nada"])
        self.assertEqual((cleaned.calls, cleaned.kept_live), (["zr-old"], ["zr-live"]))
        self.assertEqual(sorted(self.deleted()), sorted("/" + key for key, _, _ in self.objects[:2]))
        # Each call's own folder is listed, not the whole store.
        self.assertEqual(sorted(a["uri"] for a in self.asked if a["method"] == "LIST"),
                         [f"{ROOT}/{name}/" for name in ("cache", "datos", "zr-live", "zr-nada", "zr-old")])
        for bad in ("zr-old", ["../x"], ["a/b"], [""], [".."], ["."]):
            with self.assertRaises(errors.ValidationError, msg=bad):
                self.ws.clean(older_than="30d", calls=bad)

    def add_folder(self, name, *keys, days=40):
        self.objects += [(f"zenthros/risk/{name}/{key}", size, days) for key, size in keys]

    def test_a_calls_outputs_stay_with_its_call_and_its_result_goes(self):
        call = "call-0123456789abcdef.pkl"
        self.add_folder("zr-out", (call, 10), ("result.pkl", 5), ("model.onnx", 1000))
        cleaned = self.ws.clean(older_than="30d", calls=["zr-out"])
        self.assertEqual(self.deleted(), ["/zenthros/risk/zr-out/result.pkl"])
        self.assertEqual((cleaned.calls, cleaned.objects, cleaned.bytes), (["zr-out"], 1, 5))
        self.assertEqual((cleaned.kept_outputs, cleaned.kept_output_bytes), (1, 1000))
        self.assertIn("kept 1 outputs", repr(cleaned))

    def test_outputs_kept_once_can_still_be_deleted(self):
        """The call stays with them: without it the folder is no call's, and
        outputs=True could no longer find what an earlier clean kept."""
        call = "call-0123456789abcdef.pkl"
        self.add_folder("zr-out", (call, 10), ("model.onnx", 1000))
        again = self.ws.clean(older_than="30d", calls=["zr-out"])
        self.assertEqual((again.calls, again.objects, again.kept_outputs, self.deleted()), ([], 0, 1, []))
        cleaned = self.ws.clean(older_than="30d", calls=["zr-out"], outputs=True)
        self.assertEqual(self.deleted(), ["/zenthros/risk/zr-out/model.onnx", f"/zenthros/risk/zr-out/{call}"])
        self.assertEqual((cleaned.calls, cleaned.objects), (["zr-out"], 2))

    def test_outputs_true_deletes_them_too_with_the_call_still_last(self):
        call = "call-0123456789abcdef.pkl"
        self.add_folder("zr-out", (call, 10), ("result.pkl", 5), ("model.onnx", 1000))
        cleaned = self.ws.clean(older_than="30d", calls=["zr-out"], outputs=True)
        gone = self.deleted()
        self.assertEqual(sorted(gone), sorted(f"/zenthros/risk/zr-out/{key}" for key in (call, "result.pkl", "model.onnx")))
        self.assertIn("/call-", gone[-1])
        self.assertEqual((cleaned.objects, cleaned.bytes, cleaned.kept_outputs, cleaned.kept_output_bytes), (3, 1015, 0, 0))

    def test_a_gone_jobs_checkpoints_go_and_the_outputs_beside_them_stay(self):
        call = "call-0123456789abcdef.pkl"
        checkpoints = [checkpoint.object_key("uid-1", 1, "train", 3),
                       checkpoint.object_key("uid-1", 2, "fases/train", 12) + ".files/booster.bin"]
        # Named like the folder's own objects, but not them: the job wrote these.
        outputs = ["model.onnx", "jobs/notas.txt", "jobs/uid-1/gen/1/train/final.bin", "sub/result.pkl",
                   "results/resumen.csv", "results/3.pkl.bak", "call-notas.pkl"]
        self.add_folder("zr-ck", (call, 10), ("results/0.pkl", 5), *[(key, 100) for key in checkpoints + outputs])
        cleaned = self.ws.clean(older_than="30d", calls=["zr-ck"])
        self.assertEqual(sorted(self.deleted()),
                         sorted(f"/zenthros/risk/zr-ck/{key}" for key in ["results/0.pkl", *checkpoints]))
        self.assertEqual((cleaned.objects, cleaned.kept_outputs, cleaned.kept_output_bytes), (3, len(outputs), 700))

    def test_a_childs_checkpoints_go_and_what_it_left_stays(self):
        """A fan-out's child has its output under its parent's, and its
        checkpoints with it."""
        call = "call-0123456789abcdef.pkl"
        child = "children/grupo/3/"
        checkpoints = [child + checkpoint.object_key("uid-2", 1, "train", 3),
                       child + checkpoint.object_key("uid-2", 1, "train", 3) + ".files/pesos.bin"]
        self.add_folder("zr-fan", (call, 10), *[(key, 100) for key in checkpoints], (child + "result.json", 7))
        cleaned = self.ws.clean(older_than="30d", calls=["zr-fan"])
        self.assertEqual(sorted(self.deleted()), sorted(f"/zenthros/risk/zr-fan/{key}" for key in checkpoints))
        self.assertEqual((cleaned.kept_outputs, cleaned.kept_output_bytes), (1, 7))

    def test_dry_run_says_what_it_would_keep(self):
        self.add_folder("zr-out", ("call-0123456789abcdef.pkl", 10), ("model.onnx", 1 << 20))
        cleaned = self.ws.clean(older_than="30d", calls=["zr-out"], dry_run=True)
        self.assertEqual((cleaned.objects, cleaned.kept_outputs, cleaned.kept_output_bytes), (0, 1, 1 << 20))
        self.assertIn("kept 1 outputs (1.0 MiB)", repr(cleaned))
        self.assertEqual(self.deleted(), [])

    def test_a_recent_output_keeps_the_whole_call(self):
        self.add_folder("zr-out", ("call-0123456789abcdef.pkl", 10), ("result.pkl", 5))
        self.add_folder("zr-out", ("model.onnx", 1000), days=2)
        cleaned = self.ws.clean(older_than="30d", calls=["zr-out"], outputs=True)
        self.assertEqual((cleaned.calls, cleaned.kept_outputs), ([], 0))
        self.assertEqual(self.deleted(), [])

    def test_a_workspace_without_a_store_says_so(self):
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk"})
        with self.assertRaises(errors.ValidationError):
            self.ws.clean(older_than="30d")


if __name__ == "__main__":
    unittest.main()
