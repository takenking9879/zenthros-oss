# SPDX-License-Identifier: Apache-2.0

"""The SDK reuses its connection to zenthros-api (zenthros.jobs.hubconn).

Against a local TLS hub that counts the TCP connections it accepts: calls in
a row share one; a connection the server closed is seen closed before it is
used, so even a join goes on a new one; a request the server took and never
answered is sent again for a GET and a submit (its nonce), never for a join
(a second token, of the Workspace's 10), which says it may have been done; a
request that timed out, or an answer that broke off while read, leaves its
connection behind; each thread has its own; one idle IDLE_SECONDS is not
used again; a followed log has its own; and an https hub is reached through
the environment's proxy, by a CONNECT tunnel.
"""

import datetime
import ipaddress
import json
import os
import select
import socket
import ssl
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
except ImportError:  # the SDK without extras: the hub's certificate cannot be made
    x509 = None

from zenthros import errors
from zenthros.jobs import hubconn
from zenthros.jobs import spec as S
from zenthros.jobs.http import HTTPJobClient

JOB = {"metadata": {"name": "j", "generation": 1}, "status": {"phase": "Running"}}


def _certificate(folder):
    """A self-signed certificate for 127.0.0.1: (cert file, key file, PEM)."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
            .sign(key, hashes.SHA256()))
    pem = cert.public_bytes(serialization.Encoding.PEM)
    cert_file, key_file = os.path.join(folder, "hub.crt"), os.path.join(folder, "hub.key")
    with open(cert_file, "wb") as fh:
        fh.write(pem)
    with open(key_file, "wb") as fh:
        fh.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                   serialization.NoEncryption()))
    return cert_file, key_file, pem


class TLSHub:
    """A zenthros-api over TLS that keeps connections alive, counts those it
    accepts and the requests it served, and can close every one it holds."""

    def __init__(self, cert_file, key_file):
        self.accepted, self.requests, self.open = 0, [], []
        # The next request is read and its connection closed, unanswered.
        self.drop_next = False
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def setup(self):
                super().setup()
                outer.open.append(self.connection)

            def _answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                outer.requests.append((self.command, self.path.split("?")[0]))
                if outer.drop_next:
                    outer.drop_next = False
                    self.close_connection = True
                    return
                if self.path.endswith("/jobs/slow"):
                    # Answers after the client gave up.
                    time.sleep(1.5)
                if self.path.endswith("/jobs/short"):
                    # The rest of the body comes after the client gave up,
                    # on a connection kept alive.
                    self.send_response(200)
                    self.send_header("Content-Length", "1000")
                    self.end_headers()
                    self.wfile.write(b'{"metadata"')
                    self.wfile.flush()
                    time.sleep(1.5)
                    self.wfile.write(b" " * 989)
                    return
                if self.path.endswith("/logs?follow=1"):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", "6")
                    self.end_headers()
                    self.wfile.write(b"line\n\n")
                    return
                code = 201 if self.command == "POST" else 200
                raw = json.dumps(JOB if "/jobs" in self.path else {"backendId": "b"}).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            do_GET = do_POST = _answer

        class Server(ThreadingHTTPServer):
            def handle_error(self, request, client_address):
                pass  # a connection the test closed

            def get_request(self):
                pair = super().get_request()
                outer.accepted += 1
                return pair

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(cert_file, key_file)
        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"https://127.0.0.1:{self.server.server_address[1]}"

    def close_kept(self):
        """Close every connection it holds, as an idle timeout does."""
        for conn in self.open:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.open.clear()
        time.sleep(0.2)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class ConnectProxy:
    """An HTTP proxy that takes CONNECT only, counts each, and pipes bytes."""

    def __init__(self):
        self.tunnels = []
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._tunnel, args=(client,), daemon=True).start()

    def _tunnel(self, client):
        head = b""
        while b"\r\n\r\n" not in head:
            head += client.recv(4096)
        line = head.split(b"\r\n", 1)[0].decode()
        method, where, _ = line.split(" ")
        if method != "CONNECT":
            client.sendall(b"HTTP/1.1 405 CONNECT only\r\n\r\n")
            client.close()
            return
        self.tunnels.append(where)
        host, port = where.rsplit(":", 1)
        upstream = socket.create_connection((host, int(port)))
        client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        pair = {client: upstream, upstream: client}
        while True:
            ready, _, _ = select.select(list(pair), [], [], 5)
            if not ready:
                break
            for sock in ready:
                data = sock.recv(65536)
                if not data:
                    client.close()
                    upstream.close()
                    return
                pair[sock].sendall(data)

    def close(self):
        self.sock.close()


def _job():
    return S.Job(name="j", spec=S.Spec(workload=S.Workload(kind="Job", template=S.Template(
        entrypoint="echo hi", image="python:3.13"))))


@unittest.skipIf(x509 is None, "cryptography makes the test hub's certificate: pip install 'zenthros[seal]'")
class SessionTest(unittest.TestCase):
    def setUp(self):
        folder = tempfile.mkdtemp()
        cert_file, key_file, self.pem = _certificate(folder)
        self.hub = TLSHub(cert_file, key_file)
        self.addCleanup(self.hub.close)
        # No proxy of this computer's between the test and its hub.
        environ = mock.patch.dict(os.environ, {"NO_PROXY": "", "no_proxy": ""})
        environ.start()
        self.addCleanup(environ.stop)
        for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
            os.environ.pop(name, None)

    def client(self):
        client = HTTPJobClient(self.hub.url, "risk", "t", ca_data=self.pem)
        self.addCleanup(client.close)
        return client

    def test_calls_in_a_row_share_one_connection(self):
        client = self.client()
        for _ in range(5):
            client.job_status("j")
        self.assertEqual(self.hub.accepted, 1)
        self.assertEqual(len(self.hub.requests), 5)

    def test_a_get_takes_up_a_connection_the_server_closed(self):
        client = self.client()
        client.job_status("j")
        self.hub.close_kept()
        self.assertEqual(client.job_status("j").phase, "Running")
        self.assertEqual(self.hub.accepted, 2)

    def test_a_connection_the_server_closed_is_seen_closed_before_a_join(self):
        # A hub's restart closes every idle connection: a join after it said
        # "it may have been done" of a request the hub never got (code
        # review 2026-11-08).
        client = self.client()
        client.job_status("j")
        self.hub.close_kept()
        self.assertEqual(client.join_backend("lab", kind="vm"), {"backendId": "b"})
        self.assertEqual(self.hub.requests.count(("POST", "/v1/workspaces/risk/backends")), 1)
        self.assertEqual(self.hub.accepted, 2)

    def test_a_request_taken_and_never_answered_is_sent_again_only_if_it_may_be(self):
        client = self.client()
        client.job_status("j")
        self.hub.drop_next = True
        client.submit(_job())
        # Taken and dropped, then sent again: its nonce makes it one job.
        self.assertEqual(self.hub.requests.count(("POST", "/v1/workspaces/risk/jobs")), 2)

        self.hub.drop_next = True
        with self.assertRaises(errors.TransientError) as caught:
            client.join_backend("lab", kind="vm")
        self.assertIn("may have been done", str(caught.exception))
        self.assertEqual(self.hub.requests.count(("POST", "/v1/workspaces/risk/backends")), 1)
        # The next call has a connection again.
        self.assertEqual(client.job_status("j").phase, "Running")

    def test_a_request_that_timed_out_leaves_its_connection_behind(self):
        # http.client leaves a request with no answer half done: every call
        # after it on that connection was refused (CannotSendRequest) until
        # it had sat IDLE_SECONDS (code review 2026-11-08).
        client = self.client()
        client.job_status("j")
        with self.assertRaises(errors.TransientError):
            client._json("GET", "/jobs/slow", timeout=0.3)
        self.assertEqual(client.job_status("j").phase, "Running")
        self.assertEqual(self.hub.accepted, 2)

    def test_an_answer_that_broke_off_while_read_leaves_its_connection_behind(self):
        # Closed after a read that failed, the answer looked read to the end,
        # and the next request was sent where the rest of it was coming: a
        # join read it as its answer and said it may have been done.
        client = self.client()
        with self.assertRaises(errors.TransientError):
            client._json("GET", "/jobs/short", timeout=0.3)
        self.assertEqual(client.join_backend("lab", kind="vm"), {"backendId": "b"})
        self.assertEqual(self.hub.accepted, 2)

    def test_each_thread_has_its_own_connection(self):
        client = self.client()
        barrier = threading.Barrier(2)
        failed = []

        def calls():
            try:
                barrier.wait()
                for _ in range(3):
                    client.job_status("j")
            except Exception as exc:  # noqa: BLE001 - the test reports it
                failed.append(exc)

        threads = [threading.Thread(target=calls) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failed, [])
        self.assertEqual(self.hub.accepted, 2)
        self.assertEqual(len(self.hub.requests), 6)

    def test_a_connection_idle_a_minute_is_not_used_again(self):
        now = [1000.0]
        with mock.patch.object(hubconn, "_clock", lambda: now[0]):
            client = self.client()
            client.job_status("j")
            now[0] += hubconn.IDLE_SECONDS - 1
            client.job_status("j")
            self.assertEqual(self.hub.accepted, 1)
            now[0] += hubconn.IDLE_SECONDS + 1
            client.job_status("j")
        self.assertEqual(self.hub.accepted, 2)

    def test_a_followed_log_has_a_connection_of_its_own(self):
        client = self.client()
        client.job_status("j")
        stream = client.follow_logs("j")
        self.assertEqual(list(stream.stdout), ["line\n", "\n"])
        stream.wait(5)
        client.job_status("j")
        # The kept one served both calls; the log came on another.
        self.assertEqual(self.hub.accepted, 2)

    def test_an_https_hub_is_reached_through_the_environments_proxy(self):
        proxy = ConnectProxy()
        self.addCleanup(proxy.close)
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": proxy.url}):
            client = self.client()
        client.job_status("j")
        client.job_status("j")
        port = self.hub.server.server_address[1]
        # One tunnel, to the hub, kept for both; TLS is to the hub by its name.
        self.assertEqual(proxy.tunnels, [f"127.0.0.1:{port}"])
        self.assertEqual(self.hub.accepted, 1)

    def test_the_process_has_one_session_with_its_hub(self):
        # zenthros.session() names it: the Workspace on the process's one
        # client of the connection in effect, the one zenthros.workspace()
        # and login() give too, every call of it on one kept connection.
        import zenthros
        from zenthros.jobs import http

        folder = tempfile.mkdtemp()
        ca_file, config_file = os.path.join(folder, "ca.pem"), os.path.join(folder, "config")
        with open(ca_file, "wb") as fh:
            fh.write(self.pem)
        with open(config_file, "w") as fh:
            fh.write("{}")
        for name in ("ZENTHROS_API_KEY", "ZENTHROS_CONTEXT", "ZENTHROS_TOKEN_FILE"):
            os.environ.pop(name, None)
        os.environ.update({"ZENTHROS_CONFIG": config_file, "ZENTHROS_SERVER": self.hub.url, "ZENTHROS_TOKEN": "t",
                           "ZENTHROS_WORKSPACE": "risk", "ZENTHROS_CA_FILE": ca_file})
        http.forget_shared()
        self.addCleanup(http.forget_shared)
        first, second = zenthros.session(), zenthros.session()
        self.assertIs(first._client, second._client)
        self.assertIs(zenthros.workspace()._client, first._client)
        self.assertEqual(first.name, "risk")
        first._client.job_status("j")
        second._client.job_status("j")
        self.assertEqual(self.hub.accepted, 1)


if __name__ == "__main__":
    unittest.main()
