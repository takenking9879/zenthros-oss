#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""HTTPJobClient and zenthros.config against a fake zenthros-api.

The client carries a token and nothing else: every request has it, a
refusal is the server's words mapped to the SDK's errors, and the job's
namespace is the Workspace's, never the client's.
"""
import io
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.parse
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from zenthros import config, errors
from zenthros.jobs import http
from zenthros.jobs import spec as S
from zenthros.jobs.client import ZctlJobClient
from zenthros.jobs.handle import SUCCEEDED
from zenthros.jobs.http import HTTPJobClient, PresignedBlobStore
from zenthros.remote import options
from zenthros.remote.call import _LogFollower

from support import FakeAPI, status


def read(path):
    with open(path) as fh:
        return fh.read()


def job(name="j"):
    return S.Job(name=name, spec=S.Spec(workload=S.Workload(kind="Job", template=S.Template(
        entrypoint="echo hi", image="python:3.13"))))


class HTTPJobClientTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token", poll_seconds=0.01)

    def test_every_request_carries_the_token_and_nothing_decides_the_namespace_but_the_workspace(self):
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = (201, {"metadata": {"name": "j"}})
        self.api.routes[("POST", "/v1/workspaces/risk/jobs/j/cancel")] = (200, {"metadata": {"name": "j"}})
        self.api.routes[("DELETE", "/v1/workspaces/risk/jobs/j")] = (200, {"kind": "Status"})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (200, b"hello\n")
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/reports")] = (200, [])
        with mock.patch.dict(os.environ, {"ZENTHROS_NAMESPACE": "tenant-other"}):
            handle = self.client.submit(job())
        self.assertEqual(handle.job_id, "j")
        self.client.cancel("j")
        self.client.delete("j")
        self.assertEqual(self.client.logs("j"), "hello\n")
        self.assertEqual(self.client.reports("j"), [])
        self.assertTrue(all(r["auth"] == "Bearer bob-token" for r in self.api.requests), self.api.requests)
        sent = json.loads(self.api.requests[0]["body"])
        self.assertEqual(sent["kind"], "ZenthrosJob")
        self.assertNotIn("namespace", sent["metadata"], "the Workspace in the path decides the namespace")

    def test_job_lists_are_read_a_page_at_a_time(self):
        # One list of a busy Workspace held the whole namespace in
        # zenthros-api's memory at once (audit 2026-10-16 AUD-018).
        def pages(_body):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.api.requests[-1]["path"]).query)
            self.assertEqual(query.get("limit"), ["200"])
            if query.get("continue") == ["p2"]:
                return 200, {"kind": "ZenthrosJobList", "metadata": {}, "items": [{"metadata": {"name": "b"}}]}
            return 200, {"kind": "ZenthrosJobList", "metadata": {"continue": "p2"}, "items": [{"metadata": {"name": "a"}}]}
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = pages
        names = [item["metadata"]["name"] for item in self.client.list_jobs(selector="team=risk")]
        self.assertEqual(names, ["a", "b"])
        lists = [r["path"] for r in self.api.requests]
        self.assertEqual(len(lists), 2, lists)
        self.assertTrue(all("labelSelector=team%3Drisk" in path for path in lists), lists)

    def test_a_server_that_does_not_page_answers_in_one(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs")] = (200, {"items": [{"metadata": {"name": "a"}}]})
        self.assertEqual(len(self.client.list_jobs()), 1)
        self.assertEqual(len(self.api.requests), 1)

    def test_refusals_are_the_servers_words(self):
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = status(403, 'User "bob" cannot create resource "zenthrosjobs"')
        with self.assertRaises(errors.WorkspaceForbidden) as caught:
            self.client.submit(job())
        self.assertIn('User "bob" cannot create', str(caught.exception))
        # A key the server no longer takes is a login to renew, not a
        # Workspace the caller may not use; a name taken is not worth a
        # retry (audit 2026-10-16 AUD-040).
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = status(401, "Unauthorized", "Unauthorized")
        with self.assertRaises(errors.LoginRequired) as caught:
            self.client.logs("j")
        self.assertIn("zctl user token", str(caught.exception))
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = status(409, 'zenthrosjobs "j" already exists', "AlreadyExists")
        with self.assertRaises(errors.Conflict) as caught:
            self.client.submit(job())
        self.assertNotIsInstance(caught.exception, errors.TransientError)
        self.assertIn("already exists", str(caught.exception))
        self.api.routes[("POST", "/v1/workspaces/risk/jobs/j/cancel")] = status(400, "bad", "BadRequest")
        with self.assertRaises(errors.ValidationError):
            self.client.cancel("j")
        self.api.routes[("DELETE", "/v1/workspaces/risk/jobs/j")] = status(503, "no Gateway", "ServiceUnavailable")
        with self.assertRaises(errors.TransientError):
            self.client.delete("j")

    def test_a_refusal_is_transient_only_by_the_one_table(self):
        """Which answers are asked again is errors.transient_status's, the
        operator's table too (fault.TransientHTTP): any other 4xx is an
        answer that turns the request down. A 410 or a 405 read as a moment
        of the API was asked again for 300 s, and a 412 on a submit said the
        job may have been created."""
        for code in (405, 410, 412, 415):
            self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = status(code, "no", "Refused")
            with self.subTest(code=code), self.assertRaises(errors.Refused) as caught:
                self.client.job_object("j")
            self.assertNotIsInstance(caught.exception, errors.TransientError)
            self.assertEqual(caught.exception.status, code)
        for code in (408, 429, 500, 501, 503):
            self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = status(code, "later", "Unavailable")
            with self.subTest(code=code), mock.patch.object(http, "_sleep"), self.assertRaises(errors.TransientError):
                self.client.job_object("j")

    def test_a_rate_limit_is_waited_out_for_as_long_as_it_says(self):
        """A 429 of the API's limit comes before anything is done, so even a
        create is safe to send again; it waits what Retry-After says."""
        answers = [(*status(429, "too many", "TooManyRequests"), {"Retry-After": "2"}), (200, {"metadata": {"name": "j"}})]
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = lambda _body: answers.pop(0)
        with mock.patch.object(http, "_sleep") as slept:
            self.client.job_object("j")
        slept.assert_called_once_with(2)

    def test_a_rate_limit_that_does_not_end_is_transient(self):
        limited = (*status(429, "too many", "TooManyRequests"), {"Retry-After": "1"})
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = limited
        with mock.patch.object(http, "_sleep") as slept, self.assertRaises(errors.TransientError):
            self.client.job_object("j")
        self.assertEqual(slept.call_count, http.RATE_LIMIT_ATTEMPTS - 1)

    def test_a_rate_limit_never_waits_longer_than_its_ceiling(self):
        """The header is the server's word: a huge one does not hang a call."""
        answers = [(*status(429, "too many", "TooManyRequests"), {"Retry-After": "86400"}), (200, {"metadata": {"name": "j"}})]
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = lambda _body: answers.pop(0)
        with mock.patch.object(http, "_sleep") as slept:
            self.client.job_object("j")
        slept.assert_called_once_with(http.RATE_LIMIT_MAX_WAIT)

    def test_a_submit_whose_answer_is_lost_is_sent_again_by_its_name(self):
        # The API created the job and its answer was lost: the error named no
        # job, and a rerun named another one and ran the work twice (audit
        # 2026-10-16 AUD-075). Sent again, the name is taken by this very
        # submission, which is the job.
        created = {}

        def post(body):
            if created:
                return status(409, 'zenthrosjobs "j" already exists', "AlreadyExists")
            created.update(json.loads(body))
            return status(504, "upstream timed out", "Timeout")
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = post
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = lambda _body: (200, created)
        with mock.patch.object(http, "_sleep") as slept:
            handle = self.client.submit(job())
        self.assertEqual(handle.job_id, "j")
        posts = [json.loads(r["body"]) for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0], posts[1], "sent again as it was: the same name and the same submission")
        self.assertTrue(posts[0]["metadata"]["annotations"][http.SUBMISSION])
        slept.assert_called_once_with(http.SUBMIT_BACKOFF)

    def test_a_name_another_submission_took_is_refused_after_a_lost_answer(self):
        answers = [status(503, "busy", "ServiceUnavailable"), status(409, 'zenthrosjobs "j" already exists', "AlreadyExists")]
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = lambda _body: answers.pop(0)
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = (200, {"metadata": {"name": "j", "annotations": {http.SUBMISSION: "another"}}})
        with mock.patch.object(http, "_sleep"), self.assertRaises(errors.Conflict):
            self.client.submit(job())

    def test_a_name_that_could_not_be_read_back_is_no_conflict(self):
        # The read back of a name a lost answer may have taken did not
        # answer either: whose it is is unknown, not someone else's. A
        # Conflict told the user to rename, and the rerun ran the work twice
        # (R4 ronda 4, M4).
        answers = [status(503, "busy", "ServiceUnavailable"), status(409, 'zenthrosjobs "j" already exists', "AlreadyExists")]
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = lambda _body: answers.pop(0)
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = status(503, "busy", "ServiceUnavailable")
        with mock.patch.object(http, "_sleep"), self.assertRaises(errors.TransientError) as caught:
            self.client.submit(job())
        self.assertEqual(caught.exception.job, "j")
        self.assertIn("zenthros.get('j')", str(caught.exception))

    def test_a_submit_that_never_answers_names_its_job(self):
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = status(504, "upstream timed out", "Timeout")
        with mock.patch.object(http, "_sleep"), self.assertRaises(errors.TransientError) as caught:
            self.client.submit(job())
        self.assertEqual(caught.exception.job, "j")
        self.assertIn("zenthros.get('j')", str(caught.exception))
        self.assertEqual(len([r for r in self.api.requests if r["method"] == "POST"]), http.SUBMIT_ATTEMPTS)

    def test_unreachable_server_is_transient(self):
        client = HTTPJobClient("http://127.0.0.1:1", "risk", "t")
        with self.assertRaises(errors.TransientError):
            client.status("j")

    def test_wait_is_the_shared_one(self):
        self.api.phases = ["Pending", "Running", "Succeeded"]
        self.assertEqual(self.client.wait("j", timeout=10), SUCCEEDED)
        waits = [r for r in self.api.requests if r["path"].startswith("/v1/workspaces/risk/jobs/j/wait")]
        self.assertEqual([r["path"].split("seen=")[1].split("&")[0] for r in waits], ["", "Pending", "Running"],
                         "each wait sends the token of the state before")
        self.api.phases = ["Running", "Failed"]
        with self.assertRaises(errors.JobFailed) as caught:
            self.client.wait("j", timeout=10)
        self.assertEqual(str(caught.exception), "job is Failed")

    def test_wait_names_the_api_when_it_times_out_refused(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j")] = status(503, "Workspace has no namespace yet", "ServiceUnavailable")
        with self.assertRaises(errors.WaitTimeout) as caught:
            self.client.wait("j", timeout=0.05)
        self.assertIn("last zenthros-api error", str(caught.exception))

    def test_token_is_asked_for_on_every_request(self):
        tokens = iter(["first", "second"])
        client = HTTPJobClient(self.api.url, "risk", lambda: next(tokens))
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (200, b"")
        client.logs("j")
        client.logs("j")
        self.assertEqual([r["auth"] for r in self.api.requests], ["Bearer first", "Bearer second"])

    def test_followed_logs_drive_the_log_follower(self):
        self.api.routes[("GET", "/v1/workspaces/risk/jobs/j/logs")] = (200, b"line 1\nline 2\n")
        out = io.StringIO()
        follower = _LogFollower(self.client, "j", poll_seconds=0.01, out=out)
        follower.start()
        follower.stop(grace=0.2)
        self.assertEqual(out.getvalue(), "line 1\nline 2\n")
        self.assertTrue(any("follow=1" in r["path"] for r in self.api.requests))

    def test_whoami_is_the_servers_answer(self):
        self.api.routes[("GET", "/v1/whoami")] = (200, {"username": "system:serviceaccount:tenant-risk:bob"})
        self.assertEqual(self.client.whoami()["username"], "system:serviceaccount:tenant-risk:bob")


class PresignedStoreTest(unittest.TestCase):
    """The call store through signed URLs: the API signs, the store serves,
    and the bearer token never leaves for the store."""

    def setUp(self):
        self.api, self.s3 = FakeAPI(), FakeAPI()
        self.addCleanup(self.api.close)
        self.addCleanup(self.s3.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token")

    def sign(self, method, path, status=200):
        self.api.routes[("POST", "/v1/workspaces/risk/objects/presign")] = (status, {
            "url": f"{self.s3.url}{path}?X-Amz-Signature=abc", "method": method,
            "headers": {"X-Amz-Checksum": ["1"]}} if status == 200 else
            {"kind": "Status", "code": status, "message": "outside the Workspace's store"})

    def test_a_put_says_its_size_and_a_file_is_measured_not_read(self):
        store = PresignedBlobStore(self.client)
        self.sign("PUT", "/calls/zenthros/risk/c/f")
        self.s3.routes[("PUT", "/calls/zenthros/risk/c/f")] = (200, b"")
        with tempfile.TemporaryDirectory() as scratch:
            path = os.path.join(scratch, "f")
            with open(path, "wb") as fh:
                fh.write(b"12345")
            store.put_file("s3://calls/zenthros/risk/c/f", path)
        store.put("s3://calls/zenthros/risk/c/f", b"")
        asked = [json.loads(r["body"])["size"] for r in self.api.requests]
        self.assertEqual(asked, [5, 0])

    def test_over_the_limit_nothing_is_sent_and_it_says_where_to_put_it(self):
        store = PresignedBlobStore(self.client)
        self.api.routes[("POST", "/v1/workspaces/risk/objects/presign")] = (413, {
            "kind": "Status", "code": 413,
            "message": "an object of 9 bytes is over this store's limit of 8 bytes for one upload: "
                       "use your own bucket (zenthros.set_store) for data this large"})
        with self.assertRaises(Exception) as caught:
            store.put("s3://calls/zenthros/risk/c/f", b"123456789")
        self.assertIn("set_store", str(caught.exception))
        self.assertEqual(len(self.api.requests), 1, "a refusal is not worth asking again")
        self.assertFalse(self.s3.requests)

    def test_put_and_get_go_to_the_signed_url_without_the_token(self):
        store = PresignedBlobStore(self.client)
        self.sign("PUT", "/calls/zenthros/risk/c/call.pkl")
        self.s3.routes[("PUT", "/calls/zenthros/risk/c/call.pkl")] = (200, b"")
        store.put("s3://calls/zenthros/risk/c/call.pkl", b"payload")
        self.sign("GET", "/calls/zenthros/risk/c/result.pkl")
        self.s3.routes[("GET", "/calls/zenthros/risk/c/result.pkl")] = (200, b"result")
        self.assertEqual(store.get("s3://calls/zenthros/risk/c/result.pkl"), b"result")

        asked = [json.loads(r["body"]) for r in self.api.requests]
        self.assertEqual(asked, [{"uri": "s3://calls/zenthros/risk/c/call.pkl", "method": "PUT", "size": 7},
                                 {"uri": "s3://calls/zenthros/risk/c/result.pkl", "method": "GET"}])
        self.assertEqual(self.s3.requests[0]["body"], b"payload")
        self.assertTrue(all(r["auth"] is None for r in self.s3.requests), "the token reached the store")

    def test_a_missing_object_is_file_not_found_and_a_refusal_is_the_apis(self):
        store = PresignedBlobStore(self.client)
        self.sign("GET", "/calls/zenthros/risk/c/result.pkl")
        with self.assertRaises(FileNotFoundError):
            store.get("s3://calls/zenthros/risk/c/result.pkl")
        self.sign("PUT", "", status=403)
        with self.assertRaises(errors.WorkspaceForbidden):
            store.put("s3://calls/zenthros/ml/c/call.pkl", b"x")

    def test_run_through_the_api_uses_the_workspace_store(self):
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://calls/zenthros/risk"})
        self.assertEqual(self.client.store, "s3://calls/zenthros/risk")
        with options.configure(client=self.client):
            self.assertIsInstance(options.blob_store("s3://calls/zenthros/risk/x"), PresignedBlobStore)
            from zenthros.remote import call as remote_call

            self.assertEqual(remote_call._store(options.settings(), options.job_client()), "s3://calls/zenthros/risk")


SEALED = {"secrets": [
    {"type": "encrypted-payload", "encryptedPayload": {"ciphertext": f"-----BEGIN AGE ENCRYPTED FILE-----{name}",
                                                       "recipients": ["age1abc"], "keyId": "k1"},
     "materialization": {"mode": "env", "envName": name}}
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")],
    "env": {"AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL": "http://172.25.17.20:4566"},
    "expiresAt": "2026-10-15T12:00:00Z"}


def double_it(x):
    return 2 * x


class StoreAccessTest(unittest.TestCase):
    """A job reaches the Workspace's store as zenthros-api says: its region
    and the endpoint jobs reach it at for every job, and for a backend an
    hour of credentials sealed to that backend, asked before the job exists."""

    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token")
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {
            "name": "risk", "store": "s3://calls/zenthros/risk", "storeRegion": "us-east-1",
            "storeJobEndpoint": "http://172.25.17.20:4566"})
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = (201, {})
        # on="ct-1" finds the machine by its name.
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [
            {"backendId": "ct-1", "name": "ct-1", "backendType": "container"}]})

    def test_a_backend_gets_sealed_credentials_and_where_the_store_is(self):
        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = (200, SEALED)
        # The answer's env is where this backend reaches the store.
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://calls/zenthros/risk"})
        env, secrets = self.client.store_access("ct-1")
        self.assertEqual(env, {"AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL": "http://172.25.17.20:4566"})
        self.assertEqual([s.materialization.env_name for s in secrets],
                         ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"])
        sealed = secrets[0]
        self.assertEqual((sealed.type, sealed.encrypted_payload.recipients, sealed.encrypted_payload.key_id),
                         ("encrypted-payload", ["age1abc"], "k1"))
        asked = [r for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(json.loads(asked[0]["body"]), {"backend": "ct-1"})

    def test_a_pool_gets_where_the_store_is_and_its_machines_their_role(self):
        env, secrets = self.client.store_access("")
        self.assertEqual((env, secrets), ({"AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL": "http://172.25.17.20:4566"}, []))
        self.assertFalse([r for r in self.api.requests if r["method"] == "POST"])

    def test_an_api_without_a_role_leaves_the_backend_its_own(self):
        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = status(
            501, "zenthros-api gives no store credentials", "MethodNotAllowed")
        err = io.StringIO()
        with mock.patch.object(sys, "stderr", err):
            env, secrets = self.client.store_access("vm-1")
        self.assertEqual(secrets, [])
        self.assertEqual(env["AWS_REGION"], "us-east-1")
        self.assertIn("vm-1", err.getvalue())

    def test_an_outage_is_no_answer_that_there_is_no_role(self):
        # A job sent without its credentials on a 503 could not read its
        # call: the outage is raised, not read as "no role".
        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = status(
            503, "Workspace risk is not ready", "ServiceUnavailable")
        with self.assertRaises(errors.TransientError):
            self.client.store_access("vm-1")

    def test_run_on_a_backend_seals_first_then_creates_the_job(self):
        from zenthros.core.storage import MemoryBlobStore
        from zenthros.remote import call as remote_call

        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = (200, SEALED)
        with options.configure(client=self.client, blob_store=MemoryBlobStore()):
            remote_call.run(double_it, 21, on="ct-1", code_dir=None, env={"AWS_REGION": "eu-west-1"})
        posts = [r["path"] for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(posts, ["/v1/workspaces/risk/store/credentials", "/v1/workspaces/risk/jobs"])
        template = json.loads(self.api.requests[-1]["body"])["spec"]["workload"]["template"]
        env = {e["name"]: e["value"] for e in template["env"]}
        # What the caller says beats what the API says.
        self.assertEqual((env["AWS_REGION"], env["AWS_ENDPOINT_URL"]), ("eu-west-1", "http://172.25.17.20:4566"))
        self.assertEqual([s["materialization"]["envName"] for s in template["secretSources"]],
                         ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"])
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)

    def test_one_run_asks_for_the_workspace_once(self):
        # Each step made its own client from the login, and each asked the
        # hub for the Workspace again.
        from zenthros.core.storage import MemoryBlobStore
        from zenthros.remote import call as remote_call

        self.api.routes[("POST", "/v1/workspaces/risk/store/credentials")] = (200, SEALED)
        with mock.patch.dict(os.environ, {config.CONFIG_ENV: os.devnull + ".none"}), \
                options.configure(server=self.api.url, token="bob-token", workspace="risk",
                                  blob_store=MemoryBlobStore()):
            remote_call.run(double_it, 21, on="ct-1", code_dir=None)
        asked = [r for r in self.api.requests if r["method"] == "GET" and r["path"] == "/v1/workspaces/risk"]
        self.assertEqual(len(asked), 1)

    def test_credentials_of_the_callers_own_are_not_replaced(self):
        from zenthros.core.storage import MemoryBlobStore
        from zenthros.remote import call as remote_call

        mine = options.Secret("encrypted-payload", payload=S.EncryptedPayload(ciphertext="x" * 64, recipients=["age1me"]))
        with options.configure(client=self.client, blob_store=MemoryBlobStore()):
            remote_call.run(double_it, 21, on="ct-1", code_dir=None, secrets={"AWS_ACCESS_KEY_ID": mine})
        self.assertNotIn("/v1/workspaces/risk/store/credentials", [r["path"] for r in self.api.requests])

    def test_a_store_of_the_callers_own_is_left_to_the_caller(self):
        # With credentials of its own for it, the hub is not asked and its
        # endpoint not added (without them: test_userstore, minted here).
        from zenthros.core.storage import MemoryBlobStore
        from zenthros.remote import call as remote_call

        mine = options.Secret("encrypted-payload", payload=S.EncryptedPayload(ciphertext="x" * 64, recipients=["age1me"]))
        with options.configure(client=self.client, blob_store=MemoryBlobStore(), store="s3://mine/calls"):
            remote_call.run(double_it, 21, on="ct-1", code_dir=None, secrets={"AWS_ACCESS_KEY_ID": mine})
        self.assertEqual([r["path"] for r in self.api.requests if r["method"] == "POST"], ["/v1/workspaces/risk/jobs"])
        template = json.loads(self.api.requests[-1]["body"])["spec"]["workload"]["template"]
        self.assertNotIn("AWS_ENDPOINT_URL", {e["name"] for e in template["env"]})


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, "config.yaml")
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": self.path}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def write(self, doc):
        with open(self.path, "w") as fh:
            fh.write(doc if isinstance(doc, str) else json.dumps(doc))

    def zenconfig(self, **auth):
        return {"current-context": "risk", "contexts": {
            "risk": {"server": "https://api.test", "workspace": "risk", "auth": {"type": "token", **auth}},
            "ml": {"server": "https://ml.test", "workspace": "ml", "auth": {"type": "token", "token": "ml-token"}}}}

    def test_configure_beats_the_environment_beats_the_context(self):
        self.write(self.zenconfig(token="file-token"))
        conn = config.resolve()
        self.assertEqual((conn.server, conn.workspace, conn.token()), ("https://api.test", "risk", "file-token"))
        with mock.patch.dict(os.environ, {"ZENTHROS_SERVER": "https://env.test", "ZENTHROS_TOKEN": "env-token",
                                          "ZENTHROS_WORKSPACE": "env-ws"}):
            conn = config.resolve()
            self.assertEqual((conn.server, conn.workspace, conn.token()), ("https://env.test", "env-ws", "env-token"))
            conn = config.resolve(server="https://arg.test", token="arg-token", workspace="arg-ws")
            self.assertEqual((conn.server, conn.workspace, conn.token()), ("https://arg.test", "arg-ws", "arg-token"))
        conn = config.resolve(context="ml")
        self.assertEqual((conn.server, conn.token()), ("https://ml.test", "ml-token"))
        with mock.patch.dict(os.environ, {"ZENTHROS_CONTEXT": "ml"}):
            self.assertEqual(config.resolve().workspace, "ml")

    def test_no_server_means_zctl_as_before(self):
        self.assertIsNone(config.resolve())
        # A context that names the gateway, not zenthros-api: no server.
        self.write({"current-context": "prod", "contexts": {"prod": {"controller": "gw:9443", "cert": "/etc/z/admin.crt"},
                                                            "risk": {"server": "https://api.test", "workspace": "risk"}}})
        self.assertIsNone(config.resolve())
        # Whether this machine has a zctl is not what this asks.
        with options.configure(client=None), mock.patch.object(options, "_zctl_configured", return_value=True):
            self.assertIsInstance(options.job_client(), ZctlJobClient)

    def test_a_broken_zenconfig_is_an_error_not_no_connection(self):
        # A trailing comma read as "no connection" sent the caller to zctl
        # ("zctl not found") or asked for a server the file has.
        self.write('{"current-context": "risk", "contexts": {"risk": {"server": "https://api.test",}}}')
        with self.assertRaises(errors.ValidationError) as caught:
            config.resolve()
        self.assertIn(self.path, str(caught.exception))
        with self.assertRaises(errors.ValidationError):
            options.job_client()

    def test_a_server_means_the_http_client(self):
        self.write(self.zenconfig(token="t"))
        client = options.job_client()
        self.assertIsInstance(client, HTTPJobClient)
        self.assertEqual((client.server, client.workspace), ("https://api.test", "risk"))
        with options.configure(server="https://other.test", token="x", workspace="w"):
            client = options.job_client()
        self.assertEqual((client.server, client.workspace), ("https://other.test", "w"))

    def test_token_file_is_read_on_every_request(self):
        token_file = os.path.join(self.dir.name, "token")
        self.write(self.zenconfig(**{"token-file": token_file}))
        source = config.resolve().token
        for value in ("one", "two"):
            with open(token_file, "w") as fh:
                fh.write(value + "\n")
            self.assertEqual(source(), value)

    def test_token_command_speaks_exec_credential(self):
        script = os.path.join(self.dir.name, "cred.py")
        counter = os.path.join(self.dir.name, "count")
        with open(script, "w") as fh:
            fh.write(
                "import json, os, sys\n"
                "assert json.loads(os.environ['KUBERNETES_EXEC_INFO'])['spec']['interactive'] is False\n"
                f"open({counter!r}, 'a').write('x')\n"
                "print(json.dumps({'apiVersion': 'client.authentication.k8s.io/v1', 'kind': 'ExecCredential',"
                " 'status': {'token': 'oidc-token', 'expirationTimestamp': sys.argv[1]}}))\n")
        self.write(self.zenconfig(**{"token-command": {"command": sys.executable, "args": [script, "2999-01-01T00:00:00Z"]}}))
        source = config.resolve().token
        self.assertEqual(source(), "oidc-token")
        self.assertEqual(source(), "oidc-token")
        self.assertEqual(read(counter), "x", "a token that has not expired is reused")
        self.write(self.zenconfig(**{"token-command": {"command": sys.executable, "args": [script, "2000-01-01T00:00:00Z"]}}))
        source = config.resolve().token
        source()
        source()
        self.assertEqual(read(counter), "xxx", "an expired token is fetched again")

    def test_a_server_without_a_workspace_or_a_token_is_refused(self):
        self.write({"current-context": "x", "contexts": {"x": {"server": "https://api.test", "auth": {"token": "t"}}}})
        with self.assertRaises(errors.ValidationError):
            config.resolve()
        self.write({"current-context": "x", "contexts": {"x": {"server": "https://api.test", "workspace": "w", "auth": {}}}})
        with self.assertRaises(errors.ValidationError):
            config.resolve()
        with self.assertRaises(errors.ValidationError):
            config.resolve(context="missing")


if __name__ == "__main__":
    unittest.main()


class LogStreamCloseTest(unittest.TestCase):
    """Found on Floci: wait() closed the followed log while the follower thread
    was reading it, and http.client raised AttributeError in that thread."""

    class Response:
        """Yields a line, then reads as http.client does once closed."""

        def __init__(self):
            self.closed = False

        def __iter__(self):
            yield b"first\n"
            if self.closed:
                raise AttributeError("'NoneType' object has no attribute 'readline'")
            yield b"second\n"

        def close(self):
            self.closed = True

        def getheader(self, name, default=None):
            return default

    def test_closing_mid_read_ends_the_stream_quietly(self):
        from zenthros.jobs.http import _LogStream

        stream = _LogStream(self.Response())
        self.assertEqual(next(stream.stdout), "first\n")
        stream.wait()
        self.assertEqual(list(stream.stdout), [])
        self.assertEqual(stream.poll(), 0)

    def test_the_same_error_on_an_open_stream_is_a_bug_and_propagates(self):
        from zenthros.jobs.http import _LogStream

        response = self.Response()
        stream = _LogStream(response)
        next(stream.stdout)
        response.closed = True  # closed by nobody we know of
        with self.assertRaises(AttributeError):
            list(stream.stdout)


class LogStreamBreaksOffTest(unittest.TestCase):
    """A followed log cut mid-chunk, or with a piece that does not
    decompress, ends: the follower thread died of it (code review
    2026-11-08)."""

    class Cut:
        def __iter__(self):
            yield b"first\n"
            raise IncompleteRead(b"")

        def close(self):
            pass

        def getheader(self, name, default=None):
            return default

    class Corrupt(Cut):
        def read1(self, n):
            return b"not gzip at all"

        def getheader(self, name, default=None):
            return "gzip" if name == "Content-Encoding" else default

    def test_a_stream_that_breaks_off_ends_with_what_came(self):
        from zenthros.jobs.http import _LogStream

        self.assertEqual(list(_LogStream(self.Cut()).stdout), ["first\n"])
        stream = _LogStream(self.Corrupt())
        self.assertEqual(list(stream.stdout), [])
        self.assertEqual(stream.poll(), 0)


class SharedClientTest(unittest.TestCase):
    """The process keeps one client per server, Workspace and CA: another
    credential replaces it, and login() and logout() drop them, as the
    session behind a context's name may be another person's (code review
    2026-11-08)."""

    def setUp(self):
        http.forget_shared()
        self.addCleanup(http.forget_shared)

    def conn(self, token, workspace="risk"):
        return config.Connection("https://api.test", workspace, config.TokenSource(token=token))

    def test_one_client_per_credential_in_force(self):
        first = HTTPJobClient.for_connection(self.conn("a"))
        self.assertIs(HTTPJobClient.for_connection(self.conn("a")), first)
        second = HTTPJobClient.for_connection(self.conn("b"))
        self.assertIsNot(second, first)
        self.assertEqual(len(http._shared), 1, "a credential refreshed replaces the client, never adds one")
        HTTPJobClient.for_connection(self.conn("b", workspace="ops"))
        self.assertEqual(len(http._shared), 2)

    def test_login_and_logout_drop_the_processs_clients(self):
        from zenthros import auth

        HTTPJobClient.for_connection(self.conn("a"))
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": os.path.join(tmp, "config")}):
            for name in (config.TOKEN_ENV, config.TOKEN_FILE_ENV, config.API_KEY_ENV):
                os.environ.pop(name, None)
            ws = auth.login(server="https://api.test", workspace="risk", token="t")
            self.assertEqual(list(http._shared.values()), [(("token", "t"), ws._client)])
            auth.logout()
            self.assertEqual(http._shared, {})


class HangingLog(BaseHTTPRequestHandler):
    """A followed log that sends a line and then nothing, and never ends:
    the Gateway's stream of an attempt whose machine is gone."""

    release = threading.Event()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"first\n")
        self.wfile.flush()
        self.release.wait(30)

    def log_message(self, *args):
        pass


class LogStreamStopTest(unittest.TestCase):
    """result() stops its log follower when the job ends. The follower's
    thread is inside a read on a stream that may never end; stopping must
    not wait for that read (it held the buffer's lock, and the call hung
    after its job had succeeded)."""

    def setUp(self):
        HangingLog.release.clear()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), HangingLog)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.addCleanup(HangingLog.release.set)
        self.client = HTTPJobClient(server=f"http://127.0.0.1:{self.server.server_port}", workspace="w", token="t")

    def test_stopping_a_follower_mid_read_returns(self):
        out = io.StringIO()
        follower = _LogFollower(self.client, "job", poll_seconds=0.05, out=out)
        follower.start()
        for _ in range(100):
            if out.getvalue():
                break
            threading.Event().wait(0.05)
        self.assertEqual(out.getvalue(), "first\n")
        stopper = threading.Thread(target=follower.stop, kwargs={"grace": 0.3}, daemon=True)
        stopper.start()
        stopper.join(5)
        self.assertFalse(stopper.is_alive(), "stop() hung on a log that never ends")
        follower.join(5)
        self.assertFalse(follower.is_alive(), "the follower kept reading after stop()")

    def test_wait_lets_a_log_end_on_its_own_first(self):
        stream = self.client.follow_logs("job")
        lines = []
        reader = threading.Thread(target=lambda: lines.extend(stream.stdout), daemon=True)
        reader.start()
        threading.Timer(0.2, HangingLog.release.set).start()
        self.assertEqual(stream.wait(timeout=5), 0)
        reader.join(5)
        self.assertEqual(lines, ["first\n"])
        self.assertFalse(stream._closed, "stopped a log that was about to end")
