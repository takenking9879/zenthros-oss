# SPDX-License-Identifier: Apache-2.0

"""zenthros.serve and RemoteCall.url, without a cluster: the spec a Service
is submitted with, and how its URL is read from the status."""

import io
import json
import os
import shutil
import stat
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import errors, remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.jobs.fake import FakeJobClient
from zenthros.jobs.handle import CANCELLED, FAILED, status_from_object
from zenthros.view import Backend
from zenthros.zctl import Zctl

from support import script_fake

STORE = "s3://zr-test/calls"
URL = "https://abc123-8000.proxy.runpod.net"


def lagging_server(not_found):
    """A local HTTP server that answers 404 to its first not_found requests,
    then 200: a provider front that lags the app. Returns (server, paths)."""
    import http.server
    import threading

    hits = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            code = 404 if len(hits) <= not_found else 200
            self.send_response(code)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, hits


def serve(target, *args, client=None, **kwargs):
    client = client or FakeJobClient()
    kwargs.setdefault("on", "demo-llm")
    kwargs.setdefault("code_dir", None)
    with remote.configure(store=STORE, client=client, blob_store=MemoryBlobStore()), redirect_stderr(io.StringIO()):
        return remote.serve(target, *args, **kwargs), client


def app(port):  # pragma: no cover - runs on the machine
    import http.server

    http.server.HTTPServer(("", port), http.server.SimpleHTTPRequestHandler).serve_forever()


class ServeSpecTest(unittest.TestCase):
    def test_a_command_is_a_job_with_its_ports(self):
        call, client = serve("vllm serve m --port 8000", port=8000, name="llm")
        spec = client.submitted("llm").to_manifest()["spec"]
        template = spec["workload"]["template"]
        self.assertEqual(spec["workload"]["kind"], "Job")
        self.assertEqual(template["service"], {"ports": [{"name": "http", "port": 8000, "protocol": "HTTP"}]})
        self.assertEqual(template["command"], ["sh", "-c", "vllm serve m --port 8000"])
        # A process that exits is run again; a lost machine is replaced.
        self.assertEqual(template["restartPolicy"], {"backoffLimit": 2})
        self.assertEqual(spec["resilience"]["onWorkerLost"], "Reschedule")
        self.assertEqual(spec["poolRef"], "demo-llm")
        # A Service has no deadline and no result for another job to count.
        self.assertNotIn("activeDeadlineSeconds", spec)
        self.assertNotIn(remote.call.RESULT_COUNT, client.submitted("llm").annotations)
        self.assertEqual(call.service, ["http"])

    def test_ports_by_name_list_and_protocol(self):
        _, client = serve(["python", "-m", "app"], ports={"api": 8000, "metrics": (9090, "tcp")}, name="a")
        ports = client.submitted("a").to_manifest()["spec"]["workload"]["template"]["service"]["ports"]
        self.assertEqual(ports, [{"name": "api", "port": 8000, "protocol": "HTTP"},
                                 {"name": "metrics", "port": 9090, "protocol": "TCP"}])
        _, client = serve("x", ports=[8000, 8080], name="b")
        names = [p["name"] for p in client.submitted("b").to_manifest()["spec"]["workload"]["template"]["service"]["ports"]]
        self.assertEqual(names, ["port-8000", "port-8080"])

    def test_refusals(self):
        for kwargs in (dict(), dict(port=8000, ports=[8000]), dict(port=0), dict(port=70000),
                       dict(ports={"Bad_Name": 8000}), dict(ports=[8000, 8000]), dict(ports=[(8000, "UDP")]),
                       dict(port=8000, timeout="1h"), dict(port=8000, retries=-1), dict(port=8000, gpus=1)):
            with self.subTest(kwargs=kwargs), self.assertRaises(errors.ValidationError):
                serve("x", **kwargs)

    def test_stop_after_is_the_controllers_to_keep(self):
        # The notebook may be closed by then: the controller stops it.
        _, client = serve("x", port=8000, name="s", stop_after="4h")
        service = client.submitted("s").to_manifest()["spec"]["workload"]["template"]["service"]
        self.assertEqual(service["stopAfterSeconds"], 4 * 3600)
        _, client = serve("x", port=8000, name="t")
        self.assertNotIn("stopAfterSeconds", client.submitted("t").to_manifest()["spec"]["workload"]["template"]["service"])
        for bad in ("0", "soon", -1):
            with self.subTest(stop_after=bad), self.assertRaises(errors.ValidationError) as caught:
                serve("x", port=8000, stop_after=bad)
            self.assertIn("stop_after", str(caught.exception))

    def test_warns_without_secrets(self):
        out = io.StringIO()
        with remote.configure(store=STORE, client=FakeJobClient(), blob_store=MemoryBlobStore()), redirect_stderr(out):
            remote.serve("x", port=8000, on="p", code_dir=None)
        self.assertIn("no secrets=", out.getvalue())

    def test_a_function_serves_with_the_same_spec(self):
        call, client = serve(app, 8000, port=8000, name="fn")
        template = client.submitted("fn").to_manifest()["spec"]["workload"]["template"]
        self.assertEqual(template["service"]["ports"][0]["port"], 8000)
        self.assertEqual(template["command"][1:4], ["-m", "zenthros.remote", "exec"])
        self.assertEqual(template["restartPolicy"], {"backoffLimit": 2})

    def test_cloud_function_serve(self):
        fn = zenthros.cloud(code_dir=None)(app)
        client = FakeJobClient()
        with remote.configure(store=STORE, client=client, blob_store=MemoryBlobStore(), on="demo-llm"), \
                redirect_stderr(io.StringIO()):
            call = fn.serve(8000, port=8000, name="cf")
        self.assertEqual(call.service, ["http"])
        self.assertEqual(client.submitted("cf").spec.pool_ref, "demo-llm")


class ServiceCallTest(unittest.TestCase):
    def test_url_waits_until_every_port_is_published(self):
        call, client = serve("x", ports={"api": 8000, "metrics": 9090}, name="svc")
        polls = []

        def publish_later(job_id):
            polls.append(job_id)
            if len(polls) == 2:
                client.publish("svc", [{"name": "api", "url": URL}], ready=False, reason="WaitingForPort",
                               message="port 9090 (metrics) does not accept connections yet")
            if len(polls) == 3:
                client.publish("svc", [{"name": "api", "url": URL}, {"name": "metrics", "url": "http://1.2.3.4:40000"}])
            return FakeJobClient.job_status(client, job_id)

        with mock.patch.object(client, "job_status", side_effect=publish_later):
            self.assertEqual(call.url(timeout=5, poll_seconds=0), URL)
        self.assertEqual(len(polls), 3)
        self.assertEqual(call.url("metrics", timeout=0), "http://1.2.3.4:40000")
        self.assertIn("ready", call.timings)
        self.assertEqual([e.name for e in call.endpoints()], ["api", "metrics"])

    def test_url_is_read_again_each_time(self):
        call, client = serve("x", port=8000, name="svc")
        client.publish("svc", [{"name": "http", "url": URL, "attempt": 0}])
        self.assertEqual(call.url(timeout=0), URL)
        # A re-run or a replacement machine publishes another URL.
        client.publish("svc", [{"name": "http", "url": "https://def456-8000.proxy.runpod.net", "attempt": 1}])
        self.assertEqual(call.url(timeout=0), "https://def456-8000.proxy.runpod.net")

    def test_url_timeout_says_why(self):
        call, client = serve("x", port=8000, name="svc")
        client.publish("svc", [], ready=False, reason="WaitingForPort",
                       message="port 8000 (http) does not accept connections yet")
        with self.assertRaises(errors.WaitTimeout) as caught:
            call.url(timeout=0)
        self.assertIn("WaitingForPort", str(caught.exception))

    def test_url_of_a_service_that_ended(self):
        call, client = serve("x", port=8000, name="svc")
        client._jobs["svc"]["phase"] = FAILED
        client._jobs["svc"]["message"] = "process exited with code 1"
        with self.assertRaises(errors.JobFailed) as caught:
            call.url(timeout=10)
        self.assertIn("exited", str(caught.exception))
        call.cancel()
        with self.assertRaises(errors.JobCancelled):
            call.url(timeout=10)

    def test_url_health_waits_for_the_app_through_the_front(self):
        # RunPod's proxy answers 404 for seconds after the port opens
        # (measured): published is not yet reachable from here.
        server, hits = lagging_server(not_found=2)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_address[1]}"
        call, client = serve("x", port=8000, name="svc")
        client.publish("svc", [{"name": "http", "url": base}])
        self.assertEqual(call.url(timeout=5, poll_seconds=0, health="/health"), base)
        self.assertEqual(hits, ["/health"] * 3)
        self.assertLessEqual(call.timings["ready"], call.timings["reachable"])
        # Without health, published is enough: no request is made.
        self.assertEqual(call.url(timeout=0), base)
        self.assertEqual(len(hits), 3)

    def test_url_health_timeout_says_what_answered(self):
        server, _ = lagging_server(not_found=10**6)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_address[1]}"
        call, client = serve("x", port=8000, name="svc")
        client.publish("svc", [{"name": "http", "url": base}])
        with self.assertRaises(errors.WaitTimeout) as caught:
            call.url(timeout=0, health="/health")
        self.assertIn(f"{base}/health answered HTTP 404", str(caught.exception))
        with self.assertRaises(errors.ValidationError):
            call.url(health="health")

    def test_url_health_follows_a_new_url(self):
        # The first machine is gone (nothing listens); the replacement's URL,
        # published since, is the one probed and returned.
        server, hits = lagging_server(not_found=0)
        self.addCleanup(server.shutdown)
        live = f"http://127.0.0.1:{server.server_address[1]}"
        call, client = serve("x", port=8000, name="svc")
        client.publish("svc", [{"name": "http", "url": "http://127.0.0.1:9", "attempt": 0}])
        polls = []

        def replaced(job_id):
            polls.append(job_id)
            if len(polls) == 2:
                client.publish("svc", [{"name": "http", "url": live, "attempt": 1}])
            return FakeJobClient.job_status(client, job_id)

        with mock.patch.object(client, "job_status", side_effect=replaced):
            self.assertEqual(call.url(timeout=5, poll_seconds=0, health="/"), live)
        self.assertEqual(hits, ["/"])

    def test_url_of_an_unknown_port(self):
        call, _ = serve("x", port=8000, name="svc")
        with self.assertRaises(errors.ValidationError):
            call.url("metrics")

    def test_a_service_has_no_result_and_nothing_waits_for_it(self):
        call, client = serve("x", port=8000, name="svc")
        with self.assertRaises(TypeError):
            call.result()
        with remote.configure(store=STORE, client=client, blob_store=MemoryBlobStore()), \
                self.assertRaises(errors.ValidationError):
            remote.run(["echo"], on="p", after=[call], code_dir=None)
        with self.assertRaises(TypeError):
            call.__reduce__()

    def test_with_block_cancels(self):
        call, client = serve("x", port=8000, name="svc")
        with call as svc:
            self.assertIs(svc, call)
        self.assertEqual(client.job_status("svc").phase, CANCELLED)

    def test_replace_cancels_the_old_only_once_the_new_answers(self):
        # Updating is replacing: the new one serves first, then the old one
        # stops. Never a moment with neither.
        client = FakeJobClient()
        old, _ = serve("x", port=8000, name="v1", client=client)
        new, _ = serve("x", port=8001, name="v2", client=client)
        client.publish("v1", [{"name": "http", "url": URL}])
        polls = []

        def ready_later(job_id):
            polls.append(job_id)
            if job_id == "v2":
                self.assertNotEqual(FakeJobClient.job_status(client, "v1").phase, CANCELLED,
                                    "the old one stopped before the new one was ready")
                if polls.count("v2") == 2:
                    client.publish("v2", [{"name": "http", "url": "https://def456-8001.proxy.runpod.net"}])
            return FakeJobClient.job_status(client, job_id)

        with mock.patch.object(client, "job_status", side_effect=ready_later):
            self.assertIs(old.replace(new, timeout=5, poll_seconds=0), new)
        self.assertEqual(client.job_status("v1").phase, CANCELLED)
        self.assertNotEqual(client.job_status("v2").phase, CANCELLED)

    def test_replace_keeps_the_old_when_the_new_never_answers(self):
        client = FakeJobClient()
        old, _ = serve("x", port=8000, name="v1", client=client)
        new, _ = serve("x", port=8001, name="v2", client=client)
        client.publish("v2", [], ready=False, reason="WaitingForPort", message="port 8001 does not accept connections yet")
        with self.assertRaises(errors.WaitTimeout):
            old.replace(new, timeout=0)
        self.assertNotEqual(client.job_status("v1").phase, CANCELLED)
        # The new one is cancelled, unless it is to be kept to look at.
        self.assertEqual(client.job_status("v2").phase, CANCELLED)
        third, _ = serve("x", port=8002, name="v3", client=client)
        client._jobs["v3"]["phase"] = FAILED
        with self.assertRaises(errors.JobFailed):
            old.replace(third, timeout=5, keep_failed=True)
        self.assertNotEqual(client.job_status("v1").phase, CANCELLED)
        self.assertEqual(client.job_status("v3").phase, FAILED)

    def test_replace_refuses_a_new_one_on_the_old_ones_machine(self):
        # A Service holds its machine alone (the controller queues another
        # behind it: EXCLUSIVE_QUEUED): on the same machine the new one would
        # wait for the old one forever. Refused at once, the old one serving.
        client = FakeJobClient()
        old, _ = serve("x", port=8000, name="v1", client=client, on=Backend(id="ct-1"))
        new, _ = serve("x", port=8001, name="v2", client=client, on=Backend(id="ct-1"))
        with self.assertRaises(errors.ValidationError) as caught:
            old.replace(new, timeout=600)
        self.assertIn("ct-1", str(caught.exception))
        self.assertIn("another machine", str(caught.exception))
        self.assertNotEqual(client.job_status("v1").phase, CANCELLED)
        self.assertEqual(client.job_status("v2").phase, CANCELLED)
        elsewhere, _ = serve("x", port=8000, name="v3", client=client, on=Backend(id="ct-2"))
        client.publish("v3", [{"name": "http", "url": URL}])
        self.assertIs(old.replace(elsewhere, timeout=0), elsewhere)

    def test_replace_takes_a_service(self):
        client = FakeJobClient()
        old, _ = serve("x", port=8000, name="v1", client=client)
        with remote.configure(store=STORE, client=client, blob_store=MemoryBlobStore()):
            plain = remote.run(["echo"], on="p", code_dir=None)
        for bad in (plain, old, "v2"):
            with self.assertRaises(errors.ValidationError):
                old.replace(bad, timeout=0)
        with self.assertRaises(TypeError):
            plain.replace(old, timeout=0)
        # Refused, nothing was stopped: not the service, not what was passed.
        self.assertNotEqual(client.job_status("v1").phase, CANCELLED)
        self.assertNotEqual(client.job_status(plain.name).phase, CANCELLED)

    def test_a_call_is_not_a_service(self):
        client = FakeJobClient()
        with remote.configure(store=STORE, client=client, blob_store=MemoryBlobStore()):
            call = remote.run(["echo", "hi"], on="p", code_dir=None)
        with self.assertRaises(TypeError):
            call.url()


class StatusTest(unittest.TestCase):
    def test_a_cancelled_job_says_while_its_workload_may_still_run(self):
        # "Cancelled" is written before the backend is told, and stays while
        # it cannot be reached: the status says so until the controller marks
        # the workload gone, as zctl get does (api/v1 CleanupPending).
        cancelled = {"status": {"phase": CANCELLED}}
        self.assertTrue(status_from_object(cancelled).cleanup_pending)
        for mark in ("zenthros.ai/gc-workload-cleaned", "zenthros.ai/gc-orphan-reaped"):
            self.assertFalse(status_from_object({**cancelled, "metadata": {"annotations": {mark: "x"}}}).cleanup_pending)
        self.assertFalse(status_from_object({"status": {"phase": FAILED}}).cleanup_pending)

    def test_endpoints_and_service_ready_from_the_stored_job(self):
        status = status_from_object({"status": {
            "phase": "Running", "lifecycle": "Service",
            "endpoints": [{"name": "http", "port": 8000, "url": URL, "scope": "Public", "backend": "runpod-abc",
                           "attempt": 1, "readyAt": "2026-10-04T00:00:00Z"}],
            "conditions": [{"type": "ServiceReady", "status": "True", "reason": "EndpointsPublished"}],
        }})
        self.assertEqual(status.lifecycle, "Service")
        self.assertEqual(status.service_ready()["status"], "True")
        e = status.endpoints[0]
        self.assertEqual((e.name, e.url, e.port, e.scope, e.backend, e.attempt), ("http", URL, 8000, "Public",
                                                                                   "runpod-abc", 1))


class EncryptedSecretTest(unittest.TestCase):
    def setUp(self):
        # Not the developer's zenconfig: with a hub in it, on= is that hub's
        # machine, which it does not have (a Google login on the lab,
        # 2026-10-18).
        scratch = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, scratch, True)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.path.join(scratch, "config.yaml")}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def fake_zctl(self, record):
        """A zctl that records its argv and env and prints a sealed secret."""
        directory = tempfile.mkdtemp()
        path = Path(directory) / "zctl"
        path.write_text(f"""#!/usr/bin/env python3
import json, os, sys
json.dump({{"argv": sys.argv[1:], "plain": os.environ.get("ZENTHROS_SECRET_PLAINTEXT", "")}}, open({str(record)!r}, "w"))
print(json.dumps({{"type": "encrypted-payload", "encryptedPayload": {{
    "ciphertext": "-----BEGIN AGE ENCRYPTED FILE-----\\nabc\\n-----END AGE ENCRYPTED FILE-----",
    "recipients": ["age1xyz"], "keyId": "k1"}}, "materialization": {{"mode": "env", "envName": "ZENTHROS_SECRET"}}}}))
""")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
        return Zctl(str(path))

    @script_fake("zctl")
    def test_value_goes_through_the_env_never_argv(self):
        record = Path(tempfile.mkdtemp()) / "record.json"
        secret = remote.Secret.encrypted("s3cr3t-value", on="demo-llm", zctl=self.fake_zctl(record))
        seen = json.loads(record.read_text())
        self.assertNotIn("s3cr3t-value", " ".join(seen["argv"]))
        self.assertEqual(seen["plain"], "s3cr3t-value")
        self.assertIn("--pool", seen["argv"])
        self.assertEqual(seen["argv"][:4], ["secrets", "encrypt", "-o", "json"])
        source = secret.source("VLLM_API_KEY")
        self.assertEqual(source.type, "encrypted-payload")
        self.assertEqual(source.encrypted_payload.recipients, ["age1xyz"])
        self.assertEqual((source.materialization.mode, source.materialization.env_name), ("env", "VLLM_API_KEY"))
        self.assertNotIn("s3cr3t-value", json.dumps(remote.call.S.to_plain(source)))

    def test_one_source_and_one_destination(self):
        for kwargs in (dict(), dict(value="a", from_env="B", on="p"), dict(value="a"),
                       dict(value="a", on="p", recipients=["age1x"])):
            with self.subTest(kwargs=kwargs), self.assertRaises(errors.ValidationError):
                remote.Secret.encrypted(**kwargs)


if __name__ == "__main__":
    unittest.main()
