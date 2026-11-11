#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.login(): how the SDK gets the token it carries.

Two ways. A token someone issued (zctl user add) is written into the
zenconfig. An OIDC issuer is asked with the device grant (RFC 8628): the
person opens a URL and types a code, the SDK waits, and what it is given is
kept for the owner only and renewed without asking again.

Nothing here shows a token: the prompt has the URL and the code a person
types, and an error has the issuer's words.
"""
import base64
import contextlib
import io
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import zenthros
from zenthros import auth, config, errors

from support import assert_mode


def b64(data):
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


def id_token(expires_in, serial=""):
    return ".".join([b64({"alg": "RS256"}), b64({"sub": "bob", "exp": int(time.time() + expires_in), "jti": serial}), "sig"])


def read(path):
    with open(path) as fh:
        return fh.read()


# A certificate made for this test; its key was never kept.
TEST_CA = """-----BEGIN CERTIFICATE-----
MIIBjjCCATWgAwIBAgIUCZgBrSntMvFGJCrfLzYp58CyvxgwCgYIKoZIzj0EAwIw
HDEaMBgGA1UEAwwRemVudGhyb3Mtc2RrLXRlc3QwIBcNMjYxMDE0MDE0MTEwWhgP
MjEyNjA5MjAwMTQxMTBaMBwxGjAYBgNVBAMMEXplbnRocm9zLXNkay10ZXN0MFkw
EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEPnQnD4jK+sufmJ4WAXV/Wzau0QMO/t14
SN9uI1QcGJ2Xz4j0m4Rugex4c8Akka4cnAtLx+7VR28T2hCupHa9+KNTMFEwHQYD
VR0OBBYEFEot2Xv0pfcG6akCfDp6KsmQUIQ/MB8GA1UdIwQYMBaAFEot2Xv0pfcG
6akCfDp6KsmQUIQ/MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIg
TSVpQfZ1DkArjCXrRf9RTOe9ldVaJR3IqLbu1NXlN5cCIBM0ooBAGbLwN8/BV8BF
5QdYk2nY6bUQzs5ncuCEbAjl
-----END CERTIFICATE-----
"""


class FakeIssuer:
    """An OIDC issuer with the device grant: discovery, the device endpoint
    and a token endpoint that answers from a script, in order."""

    def __init__(self):
        self.requests = []
        self.answers = []
        self.discovery = {}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, code, payload):
                raw = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                if self.path != "/.well-known/openid-configuration":
                    return self._send(404, {"error": "not_found"})
                self._send(200, {"issuer": outer.url, "device_authorization_endpoint": outer.url + "/device",
                                 "token_endpoint": outer.url + "/token", **outer.discovery})

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                form = {k: v[0] for k, v in urllib.parse.parse_qs(self.rfile.read(length).decode()).items()}
                outer.requests.append((self.path, form))
                if self.path == "/device":
                    return self._send(200, {"device_code": "device-secret-1", "user_code": "WDJB-MJHT",
                                            "verification_uri": outer.url + "/activate",
                                            "verification_uri_complete": outer.url + "/activate?user_code=WDJB-MJHT",
                                            "expires_in": outer.code_lifetime, "interval": 5})
                if not outer.answers:
                    return self._send(500, {"error": "server_error", "error_description": "the script ran out"})
                self._send(*outer.answers.pop(0))

        self.code_lifetime = 600
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def token_requests(self):
        return [form for path, form in self.requests if path == "/token"]


def pending():
    return 400, {"error": "authorization_pending"}


def granted(token, refresh="refresh-secret-1"):
    body = {"access_token": "access-secret", "id_token": token, "token_type": "Bearer", "expires_in": 300}
    if refresh:
        body["refresh_token"] = refresh
    return 200, body


class LoginCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, "zenthros", "config.yaml")
        self.session_file = os.path.join(self.dir.name, "zenthros", "tokens", "risk.json")
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        for patcher in (mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": self.path}, clear=True),
                        # Never the developer's own keyring.
                        mock.patch.object(auth, "_keyring", return_value=None)):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.slept = []
        patcher = mock.patch.object(auth, "_sleep", self.slept.append)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.issuer = FakeIssuer()
        self.addCleanup(self.issuer.close)
        self.shown = []

    def doc(self):
        return json.loads(read(self.path))

    def session(self):
        return json.loads(read(self.session_file))

    def write(self, doc):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as fh:
            fh.write(doc if isinstance(doc, str) else json.dumps(doc))

    def oidc_context(self, **oidc):
        self.write({"current-context": "risk", "contexts": {"risk": {
            "server": "https://api.test", "workspace": "risk",
            "auth": {"type": "oidc", "oidc": {"issuer": self.issuer.url, "client-id": "zenthros-sdk", **oidc}}}}})

    def login(self, **kwargs):
        return zenthros.login(prompt=lambda url, code, complete: self.shown.append((url, code, complete)), **kwargs)


class TokenLoginTest(LoginCase):
    def test_an_issued_token_is_written_for_its_owner_only(self):
        self.write({"current-context": "ml", "contexts": {"ml": {"server": "https://ml.test", "workspace": "ml",
                                                                 "auth": {"type": "token", "token": "ml-token"}}}})
        os.chmod(self.path, 0o644)
        ws = zenthros.login(server="https://api.test/", workspace="risk", token="bob-token", context="risk")
        self.assertEqual((ws.name, ws.server), ("risk", "https://api.test"))
        doc = self.doc()
        self.assertEqual(doc["current-context"], "risk")
        self.assertEqual(doc["contexts"]["risk"], {"server": "https://api.test", "workspace": "risk",
                                                   "auth": {"type": "token", "token": "bob-token"}})
        self.assertEqual(doc["contexts"]["ml"]["auth"]["token"], "ml-token", "the other contexts stay")
        assert_mode(self, self.path, 0o600)
        assert_mode(self, os.path.dirname(self.path), 0o700)
        self.assertEqual(config.resolve().token(), "bob-token")

    def test_the_context_is_named_after_the_workspace_and_keeps_its_ca(self):
        ca = os.path.join(self.dir.name, "ca.pem")
        with open(ca, "w", newline="\n") as fh:
            fh.write(TEST_CA)
        zenthros.login(server="https://api.test", workspace="risk", token="t", ca_file=ca)
        context = self.doc()["contexts"]["risk"]
        self.assertEqual(base64.b64decode(context["ca-data"]).decode(), TEST_CA)
        # A new token for the same context keeps where it is and its CA.
        zenthros.login(token="t2", context="risk")
        context = self.doc()["contexts"]["risk"]
        self.assertEqual((context["server"], context["auth"]["token"], "ca-data" in context), ("https://api.test", "t2", True))

    def test_a_ca_file_without_a_certificate_is_refused_before_anything_is_written(self):
        ca = os.path.join(self.dir.name, "ca.pem")
        with open(ca, "w") as fh:
            fh.write("not a certificate\n")
        with self.assertRaises(errors.ValidationError) as caught:
            zenthros.login(server="https://api.test", workspace="risk", token="t", ca_file=ca)
        self.assertIn(ca, str(caught.exception))
        self.assertFalse(os.path.exists(self.path))

    def test_a_config_it_cannot_read_is_not_rewritten(self):
        old = "current-context: prod\ncontexts:\n  prod:\n    controller: gw:9443\n"
        self.write(old)
        with self.assertRaises(errors.ValidationError):
            zenthros.login(server="https://api.test", workspace="risk", token="t")
        self.assertEqual(read(self.path), old)

    def test_it_says_what_is_missing(self):
        with self.assertRaises(errors.ValidationError) as caught:
            zenthros.login(token="t")
        self.assertIn("server", str(caught.exception))
        self.write({"current-context": "risk", "contexts": {"risk": {"server": "https://api.test", "workspace": "risk"}}})
        with self.assertRaises(errors.ValidationError) as caught:
            zenthros.login()
        self.assertIn("issuer", str(caught.exception), "neither a token nor an issuer to ask")


# What `zctl user add` prints without --write-config, for this context
# (operator/pkg/zctl/user_test.go pins the same string on the Go side).
PINNED_KEY = ("zen_eyJzZXJ2ZXIiOiJodHRwczovL2FwaS5leGFtcGxlLnRlc3QiLCJ3b3Jrc3BhY2UiOiJyaXNrIiwiY2EtZGF0YSI6IlEwRWdV"
              "RVZOIiwidG9rZW4iOiJmYWtlLXRva2VuIn0")


def api_key(ca=TEST_CA, token="fake-token"):
    """A key as zctl prints it, with a CA that is a certificate."""
    raw = {"server": "https://api.example.test", "workspace": "risk", "ca-data": base64.b64encode(ca.encode()).decode(),
           "token": token}
    return "zen_" + b64(raw)


class APIKeyTest(LoginCase):
    def test_the_key_zctl_prints_is_read_as_its_context(self):
        self.assertEqual(config.parse_api_key(PINNED_KEY), {
            "server": "https://api.example.test", "workspace": "risk", "ca-data": base64.b64encode(b"CA PEM").decode(),
            "auth": {"type": "token", "token": "fake-token"}})

    def test_login_with_a_key_keeps_it_as_the_current_context(self):
        ws = zenthros.login(key=api_key())
        self.assertEqual((ws.name, ws.server), ("risk", "https://api.example.test"))
        doc = self.doc()
        self.assertEqual(doc["current-context"], "risk")
        self.assertEqual(doc["contexts"]["risk"]["auth"], {"type": "token", "token": "fake-token"})
        assert_mode(self, self.path, 0o600)
        # From now on, login() with nothing finds it.
        self.assertEqual(zenthros.login().name, "risk")

    def test_logout_removes_the_key_and_keeps_the_context(self):
        # It did nothing for a key and said nothing: the token stayed in
        # the file and the process stayed logged in (audit 2026-10-16 AUD-079).
        zenthros.login(key=api_key())
        self.write({**self.doc(), "contexts": {**self.doc()["contexts"],
                                               "admin": {"server": "https://b.test", "workspace": "ops", "auth": {"type": "token", "token": "other"}}}})
        zenthros.logout()
        doc = self.doc()
        self.assertNotIn("auth", doc["contexts"]["risk"])
        self.assertEqual((doc["contexts"]["risk"]["server"], doc["contexts"]["risk"]["workspace"], doc["current-context"]),
                         ("https://api.example.test", "risk", "risk"))
        self.assertEqual(doc["contexts"]["admin"]["auth"]["token"], "other", "another context keeps its login")
        self.assertNotIn("fake-token", read(self.path))
        assert_mode(self, self.path, 0o600)
        with self.assertRaises(errors.ValidationError):
            config.resolve().token()

    def test_logout_refuses_a_credential_it_cannot_drop(self):
        for env in ("ZENTHROS_API_KEY", "ZENTHROS_TOKEN", "ZENTHROS_TOKEN_FILE"):
            with self.subTest(env=env), mock.patch.dict(os.environ, {env: api_key() if env == "ZENTHROS_API_KEY" else "x"}):
                self.write({"current-context": "risk", "contexts": {"risk": {"server": "https://a.test", "workspace": "risk",
                                                                             "auth": {"type": "token", "token": "kept"}}}})
                with self.assertRaises(errors.ValidationError) as caught:
                    zenthros.logout()
                self.assertIn(env, str(caught.exception))
                self.assertEqual(self.doc()["contexts"]["risk"]["auth"]["token"], "kept", "refused before anything changed")

    def test_the_key_in_the_environment_is_the_connection(self):
        os.environ["ZENTHROS_API_KEY"] = api_key()
        connection = config.resolve()
        self.assertEqual((connection.server, connection.workspace, connection.token(), connection.ca_data),
                         ("https://api.example.test", "risk", "fake-token", TEST_CA.encode()))
        self.assertFalse(os.path.exists(self.path), "the environment's key writes nothing")
        self.assertEqual(zenthros.login().name, "risk", "login() with nothing takes the environment's key")

    def test_what_is_not_a_key_is_refused_without_showing_it(self):
        secret = "zen_" + b64({"server": "https://a.test", "token": "very-secret"})
        no_token = "zen_" + b64({"server": "https://a.test", "workspace": "risk"})
        for bad in ("", "abc", "zen_", "zen_%%%", secret, no_token):
            with self.assertRaises(errors.ValidationError) as caught:
                zenthros.login(key=bad)
            self.assertNotIn("very-secret", str(caught.exception))
        self.assertFalse(os.path.exists(self.path))


class DeviceLoginTest(LoginCase):
    def test_it_shows_the_code_and_waits_as_the_issuer_paces_it(self):
        self.oidc_context()
        token = id_token(3600)
        self.issuer.answers = [pending(), (400, {"error": "slow_down"}), granted(token)]
        ws = self.login()
        self.assertEqual(ws.name, "risk")
        self.assertEqual(self.shown, [(self.issuer.url + "/activate", "WDJB-MJHT",
                                       self.issuer.url + "/activate?user_code=WDJB-MJHT")])
        self.assertEqual(self.slept, [5, 5, 10], "it waits before every ask, and 5 s longer once told to slow down")
        self.assertEqual(self.issuer.requests[0], ("/device", {"client_id": "zenthros-sdk", "scope": "openid offline_access"}))
        self.assertEqual(self.issuer.token_requests()[0], {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": "device-secret-1",
            "client_id": "zenthros-sdk"})
        # Kubernetes checks the ID token: that is the bearer token.
        asked = len(self.issuer.requests)
        self.assertEqual(config.resolve().token(), token)
        self.assertEqual(len(self.issuer.requests), asked, "a token still good is not asked for again")

    def test_the_issuer_and_its_scopes_can_be_given_to_login(self):
        self.issuer.answers = [granted(id_token(3600))]
        self.login(server="https://api.test", workspace="risk", issuer=self.issuer.url, client_id="cli",
                   scopes=["openid", "groups"])
        self.assertEqual(self.doc()["contexts"]["risk"]["auth"], {"type": "oidc", "oidc": {
            "issuer": self.issuer.url, "client-id": "cli", "scopes": ["openid", "groups"]}})
        self.assertEqual(self.issuer.requests[0], ("/device", {"client_id": "cli", "scope": "openid groups"}))

    def test_a_refusal_or_a_code_nobody_typed_ends_the_wait(self):
        self.oidc_context()
        self.issuer.answers = [pending(), (400, {"error": "access_denied", "error_description": "bob said no"})]
        with self.assertRaises(errors.LoginRequired) as caught:
            self.login()
        self.assertIn("bob said no", str(caught.exception))
        self.issuer.answers = [(400, {"error": "expired_token"})]
        with self.assertRaises(errors.LoginRequired):
            self.login()
        # The issuer said the code lasts 12 s: asked at 5 and 10, not at 15.
        self.issuer.code_lifetime, self.issuer.answers = 12, [pending()] * 5
        before = len(self.issuer.token_requests())
        with self.assertRaises(errors.LoginRequired) as caught:
            self.login()
        self.assertEqual(len(self.issuer.token_requests()) - before, 2)
        self.assertIn("WDJB-MJHT", str(caught.exception))
        self.assertFalse(os.path.exists(self.session_file))

    def test_the_caller_can_wait_less_than_the_code_lasts(self):
        self.oidc_context()
        self.issuer.answers = [pending()] * 5
        with self.assertRaises(errors.LoginRequired):
            self.login(timeout=7)
        self.assertEqual(len(self.issuer.token_requests()), 1)

    def test_a_config_that_cannot_be_written_fails_before_anyone_is_sent_to_a_browser(self):
        self.write("current-context: prod\ncontexts:\n  prod:\n    controller: gw:9443\n")
        with self.assertRaises(errors.ValidationError):
            self.login(server="https://api.test", workspace="risk", issuer=self.issuer.url, client_id="cli")
        self.assertEqual((self.shown, self.issuer.requests), ([], []))

    def test_an_issuer_that_is_down_is_not_a_refusal(self):
        # Any status the SDK asks again by (errors.transient_status): a 429
        # ended a login the person was typing (R4 ronda 5, M3).
        for status in (408, 429, 503):
            self.oidc_context()
            self.issuer.requests.clear()
            self.issuer.answers = [(status, {"error": "temporarily_unavailable"}), granted(id_token(3600))]
            self.login()
            self.assertEqual(len(self.issuer.token_requests()), 2, f"{status}: it asked again")

    def test_a_grant_without_an_id_token_is_refused(self):
        self.oidc_context()
        self.issuer.answers = [(200, {"access_token": "access-secret", "token_type": "Bearer"})]
        with self.assertRaises(errors.ValidationError) as caught:
            self.login()
        self.assertIn("openid", str(caught.exception))

    def test_the_session_is_kept_apart_from_the_config_for_its_owner_only(self):
        self.oidc_context()
        token = id_token(3600)
        self.issuer.answers = [granted(token)]
        self.login()
        assert_mode(self, self.session_file, 0o600)
        assert_mode(self, os.path.dirname(self.session_file), 0o700)
        kept = self.session()
        self.assertEqual((kept["id_token"], kept["refresh_token"]), (token, "refresh-secret-1"))
        config_text = read(self.path)
        for secret in (token, "refresh-secret-1", "access-secret", "device-secret-1"):
            self.assertNotIn(secret, config_text)
        self.assertNotIn("access-secret", read(self.session_file), "the access token is not what Kubernetes checks")

    def test_what_a_person_sees_has_no_secret(self):
        self.oidc_context()
        token = id_token(3600)
        self.issuer.answers = [granted(token)]
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            zenthros.login()
        text = out.getvalue()
        self.assertIn(f"open {self.issuer.url}/activate and enter the code WDJB-MJHT", text)
        for secret in (token, "refresh-secret-1", "access-secret", "device-secret-1"):
            self.assertNotIn(secret, text)

    def test_discovery_must_be_the_issuers_own(self):
        self.oidc_context()
        self.issuer.discovery = {"issuer": "https://evil.test"}
        with self.assertRaises(errors.ValidationError) as caught:
            self.login()
        self.assertIn("https://evil.test", str(caught.exception))
        self.issuer.discovery = {"token_endpoint": "http://tokens.evil.test/token"}
        with self.assertRaises(errors.ValidationError):
            self.login()
        self.issuer.discovery = {"device_authorization_endpoint": ""}
        with self.assertRaises(errors.ValidationError) as caught:
            self.login()
        self.assertIn("device", str(caught.exception))
        self.assertEqual(self.issuer.requests, [], "nothing was sent to an endpoint it did not trust")

    def test_an_issuer_is_https_unless_it_is_this_machine(self):
        self.oidc_context(issuer="http://idp.acme.com")
        with self.assertRaises(errors.ValidationError) as caught:
            self.login()
        self.assertIn("https://", str(caught.exception))


class SessionTest(LoginCase):
    def logged_in(self, expires_in, refresh="refresh-secret-1"):
        self.oidc_context()
        token = id_token(expires_in, "first")
        self.issuer.answers = [granted(token, refresh)]
        self.login()
        return token

    def test_a_token_about_to_expire_is_renewed_and_the_new_refresh_token_kept(self):
        self.logged_in(expires_in=10)
        renewed = id_token(3600, "second")
        self.issuer.answers = [granted(renewed, "refresh-secret-2")]
        source = config.resolve().token
        self.assertEqual(source(), renewed)
        self.assertEqual(self.issuer.token_requests()[-1], {"grant_type": "refresh_token", "refresh_token": "refresh-secret-1",
                                                           "client_id": "zenthros-sdk"})
        asked = len(self.issuer.requests)
        self.assertEqual(source(), renewed)
        # Another process finds the renewed session and asks nobody.
        self.assertEqual(config.resolve().token(), renewed)
        self.assertEqual(len(self.issuer.requests), asked)
        self.assertEqual(self.session()["refresh_token"], "refresh-secret-2")
        assert_mode(self, self.session_file, 0o600)

    def test_an_issuer_that_does_not_rotate_keeps_the_refresh_token(self):
        self.logged_in(expires_in=10)
        self.issuer.answers = [granted(id_token(3600, "second"), refresh="")]
        config.resolve().token()
        self.assertEqual(self.session()["refresh_token"], "refresh-secret-1")

    def test_a_refused_renewal_asks_for_a_login_and_shows_no_token(self):
        self.logged_in(expires_in=10)
        self.issuer.answers = [(400, {"error": "invalid_grant", "error_description": "the session was revoked"})]
        with self.assertRaises(errors.LoginRequired) as caught:
            config.resolve().token()
        self.assertIn("zenthros.login()", str(caught.exception))
        self.assertIn("the session was revoked", str(caught.exception))
        self.assertNotIn("refresh-secret-1", str(caught.exception))
        self.assertFalse(os.path.exists(self.session_file), "a refresh token the issuer refused is dropped")

    def test_an_issuer_that_is_down_keeps_the_session(self):
        # A 408 forgot the refresh token for good (R4 ronda 5, M3).
        for status in (408, 429, 503):
            self.logged_in(expires_in=10)
            self.issuer.answers = [(status, {"error": "temporarily_unavailable"})]
            with self.assertRaises(errors.TransientError, msg=status):
                config.resolve().token()
            self.assertTrue(os.path.exists(self.session_file), status)
        self.issuer.answers = [granted(id_token(3600, "second"))]
        config.resolve().token()

    def test_without_a_session_or_a_refresh_token_it_asks_for_a_login(self):
        self.oidc_context()
        with self.assertRaises(errors.LoginRequired) as caught:
            config.resolve().token()
        self.assertIn("zenthros.login()", str(caught.exception))
        self.logged_in(expires_in=10, refresh="")
        with self.assertRaises(errors.LoginRequired):
            config.resolve().token()
        self.assertEqual(len(self.issuer.token_requests()), 1, "there was nothing to renew with")

    def test_a_session_of_another_issuer_is_not_this_contexts(self):
        self.logged_in(expires_in=3600)
        other = FakeIssuer()
        self.addCleanup(other.close)
        self.oidc_context(issuer=other.url)
        with self.assertRaises(errors.LoginRequired):
            config.resolve().token()
        self.assertEqual(other.requests, [], "the first issuer's refresh token went nowhere else")

    def test_logout_forgets_the_session(self):
        self.logged_in(expires_in=3600)
        zenthros.logout()
        self.assertFalse(os.path.exists(self.session_file))
        with self.assertRaises(errors.LoginRequired):
            config.resolve().token()
        zenthros.logout()

    def test_a_context_name_cannot_leave_the_sessions_directory(self):
        self.write({"current-context": "../x", "contexts": {"../x": {
            "server": "https://api.test", "workspace": "risk",
            "auth": {"type": "oidc", "oidc": {"issuer": self.issuer.url, "client-id": "c"}}}}})
        with self.assertRaises(errors.ValidationError):
            config.resolve().token()

    def test_an_explicit_token_beats_the_session(self):
        self.oidc_context()
        with mock.patch.dict(os.environ, {"ZENTHROS_TOKEN": "env-token"}):
            self.assertEqual(config.resolve().token(), "env-token")


class ConfigPathTest(unittest.TestCase):
    def test_zctl_and_the_sdk_find_one_zenconfig_on_every_os(self):
        # operator/pkg/zctl TestConfigPathIsTheSDKsOnEveryOS reads the same
        # cases: zctl's own directory on a Mac was not the SDK's (audit
        # 2026-10-16 AUD-038).
        from pathlib import Path
        fixture = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "zctl" / "testdata" / "config-path.json"
        cases = json.loads(fixture.read_text())["cases"]
        self.assertTrue(cases)
        for case in cases:
            env = {k: v for k, v in os.environ.items() if k not in ("ZENTHROS_CONFIG", "XDG_CONFIG_HOME")}
            env.update(case["env"], HOME=case["home"], USERPROFILE=case["home"])
            with self.subTest(case=case), mock.patch.dict(os.environ, env, clear=True):
                self.assertEqual(os.path.normpath(config.config_path()), os.path.normpath(case["want"]))


class FakeKeyring:
    def __init__(self, broken=False):
        self.items, self.broken = {}, broken

    def _check(self):
        if self.broken:
            raise RuntimeError("no keyring backend")

    def get_password(self, service, name):
        self._check()
        return self.items.get((service, name))

    def set_password(self, service, name, value):
        self._check()
        self.items[(service, name)] = value

    def delete_password(self, service, name):
        self._check()
        del self.items[(service, name)]


class KeyringTest(LoginCase):
    def test_the_keyring_holds_the_session_when_there_is_one(self):
        ring = FakeKeyring()
        auth._keyring.return_value = ring
        self.oidc_context()
        token = id_token(3600)
        self.issuer.answers = [granted(token)]
        self.login()
        self.assertFalse(os.path.exists(self.session_file), "nothing on disk")
        self.assertEqual(list(ring.items), [("zenthros", "risk")])
        self.assertEqual(config.resolve().token(), token)
        zenthros.logout()
        self.assertEqual(ring.items, {})

    def test_a_keyring_that_does_not_work_falls_back_to_the_file(self):
        auth._keyring.return_value = FakeKeyring(broken=True)
        self.oidc_context()
        token = id_token(3600)
        self.issuer.answers = [granted(token)]
        self.login()
        assert_mode(self, self.session_file, 0o600)
        self.assertEqual(config.resolve().token(), token)


if __name__ == "__main__":
    unittest.main()
