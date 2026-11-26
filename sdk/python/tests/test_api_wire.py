# SPDX-License-Identifier: Apache-2.0

"""The SDK sends and reads what zenthros-api takes and answers.

operator/pkg/api/testdata/wire/routes.json is one sample of each route of
the API. TestTheWireSamplesFitTheRoutes (Go) holds it to the types each
route decodes and encodes, and sets every key of an answer whose type the
API owns; this holds it to the body the SDK sends and the keys the SDK and
what reads its answers (Backend.from_api, the targets table, Cost...)
look up. A key the SDK reads that the sample lacks is one the API never
sends: a field renamed on one side fails here or there, without a cluster.
"""
import json
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

from wireread import paths as _paths
from wireread import wrap as _wrap

from zenthros import errors, machines, signin, spending, view
from zenthros.attaching import AttachCommand
from zenthros.jobs import http, logrecords
from zenthros.jobs import spec as S
from zenthros.jobs.http import HTTPJobClient, PresignedBlobStore
from zenthros.remote.call import Cost
from zenthros.targetlist import Target

from support import FakeAPI

FIXTURE = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "api" / "testdata" / "wire" / "routes.json"
WIRE = json.loads(FIXTURE.read_text())
REFUSALS = FIXTURE.parent / "refusals.json"
_loads = json.loads


class _Answer:
    def __init__(self, raw):
        self._raw = raw

    def read(self, *_):
        return self._raw

    def read1(self, *_):
        raw, self._raw = self._raw, b""
        return raw

    def getheader(self, name, default=None):
        return default

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class WireClient(HTTPJobClient):
    """An HTTPJobClient whose zenthros-api answers each route's sample."""

    def __init__(self, answering):
        super().__init__("http://api.test", "risk", "bob")
        self.sent = []
        self._answering = answering

    def _open(self, method, url, body=None, timeout=None, idempotent=None, lane="calls", gzip=False, accept=None,
              patience=None):
        path = urllib.parse.urlsplit(url).path
        self.sent.append((method, path, body))
        route = _route_of(method, path)
        self._answering[0] = route
        answer = WIRE["routes"][route].get("answer")
        if route.endswith("/logs"):
            return _Answer(_log_records())
        return _Answer(json.dumps(answer).encode())


def _log_records():
    """The logs route's answer: a record with every key of the sample's
    header, packed as it says, and the one that says why a read broke."""
    head = {k: v for k, v in WIRE["log_record"].items() if k != "error"}
    data = logrecords.zstd_module().compress(b"x" * head["raw_len"])
    head["n"] = len(data)
    return json.dumps(head).encode() + b"\n" + data + json.dumps({"n": 0, "error": WIRE["log_record"]["error"]}).encode() + b"\n"


def _answer_of(route):
    """The sample of what route answers, whose keys the SDK may read."""
    return WIRE["log_record"] if route.endswith("/logs") else WIRE["routes"][route].get("answer")


def _route_of(method, path):
    for route in WIRE["routes"]:
        verb, pattern = route.split(" ", 1)
        if verb != method:
            continue
        want, got = pattern.strip("/").split("/"), path.strip("/").split("/")
        if len(want) == len(got) and all(w.startswith("{") or w == g for w, g in zip(want, got)):
            return route
    raise AssertionError(f"the SDK called {method} {path}, which no sample names")


def _job():
    return S.Job(name="j", spec=S.Spec(workload=S.Workload(kind="Job", template=S.Template(
        entrypoint="echo hi", image="python:3.13"))))


def _hub(sent, answering):
    def hub(method, url, _ctx, body=None, timeout=60):
        path = urllib.parse.urlsplit(url).path
        sent.append((method, path, body))
        answering[0] = f"{method} {path}"
        return 200, json.dumps(WIRE["routes"][answering[0]]["answer"]).encode()
    return hub


# What the SDK does with each route it calls: the client's method, then what
# reads its answer.
DRIVERS = {
    "GET /v1/whoami": lambda c: c.whoami(),
    "GET /v1/workspaces/{ws}": lambda c: (c.store, c.store_access("")),
    "POST /v1/workspaces/{ws}/jobs": lambda c: c.submit(_job()),
    "GET /v1/workspaces/{ws}/jobs": lambda c: [view.JobInfo.from_api(item) for item in c.list_jobs()],
    "GET /v1/workspaces/{ws}/jobs/{name}": lambda c: (c.job_status("j"), view.JobInfo.from_api(c.job_object("j"))),
    "POST /v1/workspaces/{ws}/jobs/{name}/cancel": lambda c: c.cancel("j"),
    "DELETE /v1/workspaces/{ws}/jobs/{name}": lambda c: c.delete("j"),
    "GET /v1/workspaces/{ws}/jobs/{name}/wait": lambda c: c._status_since("j", "", 5),
    "GET /v1/workspaces/{ws}/jobs/{name}/logs": lambda c: (_broken(lambda: c.logs("j")), list(c.follow_logs("j").stdout)),
    "GET /v1/workspaces/{ws}/jobs/{name}/reports": lambda c: c.reports("j"),
    "GET /v1/workspaces/{ws}/jobs/{name}/cost": lambda c: Cost.from_api(c.job_cost("j")),
    "GET /v1/workspaces/{ws}/costs": lambda c: _day(c.costs("2026-11-23T00:00:00Z")),
    "GET /v1/workspaces/{ws}/jobs/{name}/source": lambda c: c.source_of("j"),
    "POST /v1/workspaces/{ws}/objects/presign": lambda c: _presigned(c),
    "POST /v1/workspaces/{ws}/store/credentials": lambda c: c.store_access("vm-a"),
    "POST /v1/workspaces/{ws}/seal": lambda c: c.seal("vm-a", {"HF_TOKEN": "x"}),
    "GET /v1/workspaces/{ws}/secrets": lambda c: [(s.get("name"), s.get("updatedAt")) for s in c.secrets()],
    "PUT /v1/workspaces/{ws}/secrets/{name}": lambda c: c.set_secret("hf-token", "v"),
    "DELETE /v1/workspaces/{ws}/secrets/{name}": lambda c: c.delete_secret("hf-token"),
    "GET /v1/workspaces/{ws}/pools": lambda c: [view.Pool.from_api(info) for info in c.pools()],
    "GET /v1/workspaces/{ws}/spend": lambda c: spending.Spend.from_api(c.spend()),
    "GET /v1/workspaces/{ws}/targets": lambda c: [Target.from_api(row) for row in c.targets()["targets"]],
    "GET /v1/workspaces/{ws}/machines": lambda c: machines._made(c),
    "POST /v1/workspaces/{ws}/machines": lambda c: machines._from_template(c.create_machine("gpu", "m1", "30m", boot=True, max_lifetime="24h", image="me/x@sha256:abc",
                                                                                       python="3.12", packages=["polars==1.0"])),
    "DELETE /v1/workspaces/{ws}/machines/{name}": lambda c: c.delete_machine("m1"),
    "GET /v1/workspaces/{ws}/backends": lambda c: [view.Backend.from_api(info) for info in c.backends()],
    "POST /v1/workspaces/{ws}/backends": lambda c: AttachCommand(c.join_backend("lab", kind="vm", runtime="containerd", python="3.12")),
    "DELETE /v1/workspaces/{ws}/backends/{name}": lambda c: c.retire_backend("vm-a"),
}

HUB_DRIVERS = {
    "GET /v1/signup": lambda server: signin.how_to_sign_in(server),
    "POST /v1/signup": lambda server: signin.exchange(server, "t", "v"),
    "GET /v1/version": lambda server: signin.hub_version(server),
}


def _day(answer):
    """What zenthros jobs reads of /costs: each job's cost, and the day's
    total beside its unpriced jobs (--today)."""
    from zenthros import tools

    return tools._job_costs(answer), answer["since"], answer["total"], answer["unpriced"]


def _broken(read):
    """read(), which raises: the sample's read broke."""
    try:
        read()
    except errors.TransientError:
        return
    raise AssertionError("a read that broke was taken for the whole log")


def _presigned(c):
    """What PresignedBlobStore reads of a signed URL (url, method,
    headers), without sending the request it signs."""
    with mock.patch("urllib.request.urlopen", side_effect=OSError("not sent")):
        try:
            PresignedBlobStore(c).put("s3://calls/zenthros/risk/c/call.pkl", b"x")
        except Exception:  # noqa: BLE001 - the store's own request is not the API's
            pass


def _substituted(route):
    verb, pattern = route.split(" ", 1)
    return verb, pattern.replace("{ws}", "risk").replace("{name}", "x")


class APIWireTest(unittest.TestCase):
    def test_every_route_the_sdk_calls_has_a_driver_and_a_sample(self):
        called = {route for route, entry in WIRE["routes"].items() if entry["sdk"]}
        self.assertEqual(set(DRIVERS) | set(HUB_DRIVERS), called,
                         "routes.json says which routes the SDK calls; each has a driver here")

    def _run(self, route, drive):
        """What the driver sent, and the keys it read of each route's
        answer."""
        seen, sent, answering = {}, [], [None]

        def loads(raw, *a, **kw):
            return _wrap(_loads(raw, *a, **kw), "", seen.setdefault(answering[0], set()))

        with mock.patch("json.loads", loads):
            if route in HUB_DRIVERS:
                with mock.patch.object(signin, "_hub", _hub(sent, answering)):
                    drive("http://api.test")
            else:
                client = WireClient(answering)
                drive(client)
                sent = client.sent
        return seen, sent

    def test_the_sdk_sends_each_routes_sample_and_reads_only_its_keys(self):
        for route, drive in {**DRIVERS, **HUB_DRIVERS}.items():
            with self.subTest(route=route):
                entry = WIRE["routes"][route]
                seen, sent = self._run(route, drive)
                verb, _ = route.split(" ", 1)
                mine = [(m, p, b) for m, p, b in sent if _route_of(m, p) == route]
                self.assertTrue(mine, f"{route}: the driver never called it; it called {sent}")
                method, _path, body = mine[0]
                self.assertEqual(method, verb)
                if "request" in entry:
                    self.assertEqual(sorted(_paths(body)), sorted(_paths(entry["request"])),
                                     f"{route}: the SDK sends other keys than the sample")
                else:
                    self.assertIsNone(body, f"{route}: the SDK sends a body the API takes none of")
                for answered, read in seen.items():
                    missing = sorted(p for p in read if p not in _paths(_answer_of(answered)))
                    self.assertFalse(missing, f"{answered} (driving {route}): the SDK reads keys the API never answers: {missing}")

    def test_a_calls_code_goes_as_job_source_says(self):
        """routes.json's job_source: a call's core by its digest beside its
        own files, then, the hub said it does not hold it, the core whole
        (base) beside them. Go's TestTheJobSourceSamplesAreWhatTheAPITakes
        holds zenthros-api to the same samples."""
        api = FakeAPI()
        self.addCleanup(api.close)
        core = b"print('zenthros')\n" * 1024
        absent = next(r for r in json.loads(REFUSALS.read_text())["refusals"] if r["means"] == "code-absent")
        said = json.loads(absent["body"])["details"]["name"]
        answers = [(404, absent["body"].replace(said, http.bundle_name(core)).encode()), (201, {"metadata": {"name": "j"}})]
        api.routes[("POST", "/v1/workspaces/risk/jobs")] = lambda _body: answers.pop(0)
        sent = S.Job(name="j", spec=S.Spec(workload=S.Workload(kind="Job", template=S.Template(
            entrypoint="echo hi", image="python:3.13", source=S.Source(inline="eA==")))))
        sent.code = S.Code(base=core, overlay=b"own")
        HTTPJobClient(api.url, "risk", "t", poll_seconds=0.01).submit(sent)
        sources = [json.loads(r["body"])["spec"]["workload"]["template"]["source"] for r in api.requests if r["method"] == "POST"]
        self.assertEqual([sorted(s) for s in sources], [sorted(WIRE["job_source"]["by_digest"]), sorted(WIRE["job_source"]["with_base"])])


if __name__ == "__main__":
    unittest.main()
