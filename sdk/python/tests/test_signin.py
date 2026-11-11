# SPDX-License-Identifier: Apache-2.0

"""zenthros login with Google (zenthros.signin): the hub says what to sign
in with, a page on localhost signs in and hands the ID token back, and the
key the hub answers is kept as login(key=) keeps one."""

import base64
import contextlib
import io
import json
import os
import socket
import subprocess
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import zenthros
from zenthros import cli, errors, signin

from test_auth import LoginCase

ID_TOKEN = "google-id-token-of-ana"
OWNER_TOKEN = "owner-token-of-ana"
TERMS = {"version": "2026-11-08", "termsURL": "https://zenthros.example/terms",
         "privacyURL": "https://zenthros.example/privacy"}


class FakeHub:
    """zenthros-api with sign-ups: GET and POST /v1/signup."""

    def __init__(self, signup=True, refuse=None, auth_domain="", terms=None):
        self.signed, self.accepted = [], []
        self.terms = terms or TERMS
        hub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _json(self, code, body):
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                if self.path != "/v1/signup" or not signup:
                    self._json(404, {"kind": "Status", "message": "the server could not find the requested resource"})
                    return
                self._json(200, {"provider": "google", "firebase": {
                    "apiKey": "public-web-key", "authDomain": auth_domain, "projectId": "zenthros"}, "terms": hub.terms})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                hub.signed.append(body.get("idToken"))
                hub.accepted.append(body.get("acceptedTerms"))
                if refuse:
                    self._json(refuse[0], {"kind": "Status", "message": refuse[1]})
                    return
                key = "zen_" + base64.urlsafe_b64encode(json.dumps({
                    "server": hub.url, "workspace": "ana-0123ab", "token": OWNER_TOKEN}).encode()).decode().rstrip("=")
                self._json(200, {"key": key, "workspace": "ana-0123ab", "email": "ana@example.com", "created": True})

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def _open(url, data=None, headers=None):
    """A request to the sign-in page as this computer's browser makes it:
    to localhost, here reached at 127.0.0.1."""
    request = urllib.request.Request(url.replace("//localhost:", "//127.0.0.1:"), data=data, headers=headers or {},
                                     method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


class Browser:
    """The person's browser: opens the page and, signed in, posts the ID
    token from it as the page's script does."""

    def __init__(self, before=None):
        self.before = before
        self.page = ""
        self.answer = None
        self.url = ""
        self.done = threading.Event()

    def __call__(self, url):
        self.url = url
        threading.Thread(target=self._run, daemon=True).start()

    def post(self, path, body, kind="application/json", origin=None):
        origin = origin or self.url.split("/signin/")[0]
        return _open(path, json.dumps(body).encode(), {"Content-Type": kind, "Origin": origin})

    def _run(self):
        try:
            _, self.page = _open(self.url)
            if self.before:
                self.before(self)
            code, raw = self.post(self.url + "/token", {"idToken": ID_TOKEN})
            self.answer = (code, json.loads(raw))
        finally:
            self.done.set()


class SignInCase(LoginCase):
    def hub(self, **kwargs):
        hub = FakeHub(**kwargs)
        self.addCleanup(hub.close)
        return hub


class GoogleSignInTest(SignInCase):
    def test_a_person_with_no_key_signs_in_and_their_key_is_the_current_context(self):
        hub = self.hub()
        browser = Browser()
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            ws = zenthros.login(server=hub.url, prompt=browser)
        self.assertEqual(ws.name, "ana-0123ab")
        doc = self.doc()
        self.assertEqual(doc["current-context"], "ana-0123ab")
        self.assertEqual(doc["contexts"]["ana-0123ab"], {
            "server": hub.url, "workspace": "ana-0123ab", "auth": {"type": "token", "token": OWNER_TOKEN}})
        self.assertEqual(hub.signed, [ID_TOKEN])
        # The page signs in with the hub's Firebase project, on its own
        # authDomain when the hub names none.
        for want in ('"apiKey": "public-web-key"', '"authDomain": "zenthros.firebaseapp.com"',
                     '"projectId": "zenthros"', signin.FIREBASE_JS + "/firebase-auth-compat.js", "signInWithPopup",
                     # A web API key restricted to the hub's sites refuses this
                     # page (lab, 2026-10-18): the page says what to allow.
                     "requests-from-referer", "http://localhost/* on its web API key"):
            self.assertIn(want, browser.page)
        self.assertTrue(browser.done.wait(5))
        self.assertEqual(browser.answer[0], 200)
        self.assertTrue(browser.answer[1]["ok"])
        self.assertIn("Signed in as ana@example.com: Workspace ana-0123ab", browser.answer[1]["message"])
        # On localhost (the Firebase project allows it by default), at a path
        # with a secret in it.
        self.assertRegex(browser.url, r"^http://localhost:\d+/signin/[A-Za-z0-9_-]{32}$")
        for secret in (ID_TOKEN, OWNER_TOKEN, "zen_"):
            self.assertNotIn(secret, printed.getvalue() + browser.answer[1]["message"])

    def test_the_page_takes_a_token_from_no_one_else(self):
        hub = self.hub()
        refused = {}

        def strangers(browser):
            base = browser.url.split("/signin/")[0]
            refused["another path"] = _open(base + "/")[0]
            refused["another secret"] = browser.post(base + "/signin/guess/token", {"idToken": "stolen"})[0]
            refused["a form post"] = browser.post(browser.url + "/token", {"idToken": "stolen"}, kind="text/plain")[0]
            refused["another origin"] = browser.post(browser.url + "/token", {"idToken": "stolen"},
                                                     origin="https://evil.example")[0]

        browser = Browser(before=strangers)
        zenthros.login(server=hub.url, prompt=browser)
        self.assertEqual(refused, {"another path": 404, "another secret": 404, "a form post": 404, "another origin": 404})
        self.assertEqual(hub.signed, [ID_TOKEN])

    def test_the_page_takes_the_port_a_web_key_allows(self):
        # A key that allows localhost:5173 only refused a page on a free port
        # (lab, 2026-10-18).
        hub = self.hub()
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        browser = Browser()
        with mock.patch.dict(os.environ, {signin.PORT_ENV: str(port)}):
            zenthros.login(server=hub.url, prompt=browser)
        self.assertTrue(browser.url.startswith(f"http://localhost:{port}/signin/"))
        self.assertEqual(hub.signed, [ID_TOKEN])

    def test_a_port_in_use_or_not_a_port_is_said_before_any_page(self):
        hub = self.hub()
        with socket.socket() as busy:
            busy.bind(("127.0.0.1", 0))
            busy.listen()
            port = busy.getsockname()[1]
            for value, want in ((str(port), f"cannot use port {port}.*unset {signin.PORT_ENV}"),
                                ("vite", f"{signin.PORT_ENV}='vite': a port number")):
                shown = []
                with self.subTest(value=value), mock.patch.dict(os.environ, {signin.PORT_ENV: value}), \
                        self.assertRaisesRegex(errors.ValidationError, want):
                    zenthros.login(server=hub.url, prompt=shown.append)
                self.assertEqual(shown, [])

    def test_the_terms_are_said_before_the_page_opens_and_their_version_is_sent(self):
        hub = self.hub()
        printed = io.StringIO()
        said = []
        browser = Browser()

        def prompt(url):
            said.append(printed.getvalue())
            browser(url)

        with contextlib.redirect_stdout(printed):
            zenthros.login(server=hub.url, prompt=prompt)
        self.assertIn("By continuing you accept the Terms (https://zenthros.example/terms) and Privacy "
                      "(https://zenthros.example/privacy)", said[0])
        self.assertEqual(hub.accepted, ["2026-11-08"])
        self.assertIn('<a href="https://zenthros.example/terms">Terms</a>', browser.page)
        self.assertIn('<a href="https://zenthros.example/privacy">Privacy</a>', browser.page)

    def test_terms_that_changed_meanwhile_are_a_refusal_that_says_so(self):
        changed = ("signing in accepts this hub's Terms (https://zenthros.example/terms) and Privacy "
                   "(https://zenthros.example/privacy), version 2026-12-01; this sign-in accepted version 2026-11-08: "
                   "read them, then run zenthros login again")
        hub = self.hub(refuse=(428, changed))
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(errors.ValidationError) as caught:
            zenthros.login(server=hub.url, prompt=Browser())
        self.assertEqual(str(caught.exception), changed)
        with self.assertRaises(FileNotFoundError):
            self.doc()

    def test_terms_at_no_web_address_open_no_page(self):
        hub = self.hub(terms={**TERMS, "termsURL": "javascript:alert(1)"})
        shown = []
        with self.assertRaisesRegex(errors.TransientError, "not a web address"):
            zenthros.login(server=hub.url, prompt=shown.append)
        self.assertEqual((shown, hub.signed), ([], []))

    def test_a_hub_without_sign_ups_says_to_ask_for_a_key_and_opens_nothing(self):
        hub = self.hub(signup=False)
        shown = []
        with self.assertRaisesRegex(errors.ValidationError, "takes no sign-ups.*zenthros login --key"):
            zenthros.login(server=hub.url, prompt=shown.append)
        self.assertEqual(shown, [])
        self.assertEqual(hub.signed, [])

    def test_a_refused_sign_in_keeps_nothing_and_both_sides_say_why(self):
        full = "this hub takes no new sign-ups: ask its administrator for a key"
        hub = self.hub(refuse=(403, full))
        browser = Browser()
        with self.assertRaisesRegex(errors.ValidationError, full):
            zenthros.login(server=hub.url, prompt=browser)
        self.assertTrue(browser.done.wait(5))
        self.assertEqual((browser.answer[1]["ok"], browser.answer[1]["message"]), (False, full))
        with self.assertRaises(FileNotFoundError):
            self.doc()

    def test_a_sign_in_never_finished_ends_and_closes_the_page(self):
        hub = self.hub()
        shown = []
        with self.assertRaisesRegex(errors.ValidationError, "did not finish within 0.3s"):
            zenthros.login(server=hub.url, prompt=shown.append, timeout=0.3)
        port = int(shown[0].split(":")[2].split("/")[0])
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=2).close()

    def test_signing_in_again_goes_to_the_hub_of_the_context(self):
        hub = self.hub()
        self.write({"current-context": "ana-0123ab", "contexts": {"ana-0123ab": {
            "server": hub.url, "workspace": "ana-0123ab", "auth": {"type": "token", "token": "ended"}}}})
        zenthros.login(google=True, prompt=Browser())
        self.assertEqual(self.doc()["contexts"]["ana-0123ab"]["auth"]["token"], OWNER_TOKEN)
        self.assertEqual(hub.signed, [ID_TOKEN])

    def test_a_token_or_a_workspace_is_not_a_google_sign_in(self):
        hub = self.hub()
        zenthros.login(server=hub.url, workspace="risk", token="given")
        self.assertEqual(hub.signed, [])
        self.assertEqual(self.doc()["contexts"]["risk"]["auth"], {"type": "token", "token": "given"})


class CLISignInTest(SignInCase):
    def test_zenthros_login_with_a_server_signs_in_with_google(self):
        hub = self.hub()
        out = io.StringIO()
        browser = Browser()
        signin_show = signin._show
        signin._show = browser
        self.addCleanup(setattr, signin, "_show", signin_show)
        self.assertEqual(cli.main(["login", "--server", hub.url], out=out), 0)
        self.assertEqual(out.getvalue(), "✓ Logged in (workspace: ana-0123ab)\n")
        self.assertEqual(self.doc()["current-context"], "ana-0123ab")

    def test_zenthros_login_google_again(self):
        hub = self.hub()
        self.write({"current-context": "ana-0123ab", "contexts": {"ana-0123ab": {
            "server": hub.url, "workspace": "ana-0123ab", "auth": {"type": "token", "token": "ended"}}}})
        signin_show = signin._show
        signin._show = Browser()
        self.addCleanup(setattr, signin, "_show", signin_show)
        self.assertEqual(cli.main(["login", "--google"], out=io.StringIO()), 0)
        self.assertEqual(self.doc()["contexts"]["ana-0123ab"]["auth"]["token"], OWNER_TOKEN)


class ShowTest(SignInCase):
    """Where the page is opened. In WSL there is no Linux browser and
    xdg-open listed every one it did not find, and the person stopped a
    sign-in that was waiting for them (lab, 2026-10-18)."""

    URL = "http://localhost:40367/signin/secret"

    def show(self, wsl, env=None, which=None):
        printed = io.StringIO()
        with mock.patch.object(signin, "_is_wsl", return_value=wsl), \
                mock.patch.object(signin.sys, "platform", "linux"), \
                mock.patch.dict(os.environ, env or {}, clear=False), \
                mock.patch.object(signin.shutil, "which", side_effect=lambda name: (which or {}).get(name)), \
                mock.patch.object(signin.subprocess, "Popen") as popen, \
                mock.patch.object(signin.webbrowser, "open", return_value=True) as browser, \
                contextlib.redirect_stdout(printed):
            for name in ("DISPLAY", "WAYLAND_DISPLAY"):
                if name not in (env or {}):
                    os.environ.pop(name, None)
            signin._show(self.URL)
        return printed.getvalue().splitlines(), popen, browser

    def test_in_wsl_windows_opens_it_quietly(self):
        lines, popen, browser = self.show(True, which={"explorer.exe": "/mnt/c/windows/explorer.exe"})
        popen.assert_called_once_with(["/mnt/c/windows/explorer.exe", self.URL], stdin=subprocess.DEVNULL,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        browser.assert_not_called()
        self.assertEqual(lines, [f"Sign in with Google: {self.URL}",
                                 "Opened in your browser. Waiting for the sign-in (Ctrl+C stops)…"])

    def test_in_wsl_wslview_first(self):
        _, popen, _ = self.show(True, which={"wslview": "/usr/bin/wslview", "explorer.exe": "/mnt/c/windows/explorer.exe"})
        self.assertEqual(popen.call_args.args[0][0], "/usr/bin/wslview")

    def test_with_no_display_nothing_is_tried_and_it_says_it_waits(self):
        lines, popen, browser = self.show(False)
        popen.assert_not_called()
        browser.assert_not_called()
        self.assertEqual(lines, [f"Sign in with Google: {self.URL}",
                                 "Open it in your browser. Waiting for the sign-in (Ctrl+C stops)…"])

    def test_with_a_display_the_desktop_browser(self):
        lines, popen, browser = self.show(False, env={"DISPLAY": ":0"})
        browser.assert_called_once_with(self.URL)
        popen.assert_not_called()
        self.assertTrue(lines[1].startswith("Opened in your browser"))
