# SPDX-License-Identifier: Apache-2.0

"""Sign in with Google for a Workspace of your own, on a hub that allows it.

    zenthros.login(server="https://api.zenthros.app")      # no key yet
    $ zenthros login --server https://api.zenthros.app
    $ zenthros login --google                              # again, when the key ends

The hub says what to sign in with (GET /v1/signup: a Firebase web config,
public) and what signing in accepts (its terms and privacy, and their
version), which is said before the browser opens. A page this process serves
on http://localhost:<port> signs in with Google in the browser and hands the
Firebase ID token back here, and this process trades it with the hub (POST
/v1/signup, with the version accepted) for an API key to the person's
Workspace, made on their first sign-in. login() keeps the key as it
keeps one from `zctl user add`. Neither the ID token nor the key is printed.

The page is on localhost because a Firebase project allows sign-ins from
localhost by default. It answers only at a path with a secret in it, and
takes the token only as JSON from its own origin, so no other page can hand
this process a token. A notebook whose kernel runs on another computer
cannot be reached at localhost: there, use a key (login(key=)).

Standard library only.
"""

import base64
import html
import json
import os
import queue
import secrets
import shutil
import ssl
import subprocess
import sys
import threading
import urllib.error
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from zenthros import errors
from zenthros.jobs.wire import HEADERS

# Firebase's JS SDK, as the page loads it.
FIREBASE_JS = "https://www.gstatic.com/firebasejs/10.14.1"
TIMEOUT = 300
# The page's port; unset, any free one. For a web API key that allows
# localhost on one port only.
PORT_ENV = "ZENTHROS_SIGNIN_PORT"


def _port():
    raw = os.environ.get(PORT_ENV, "").strip()
    if not raw:
        return 0
    if not raw.isdigit() or not 0 < int(raw) < 65536:
        raise errors.ValidationError(f"{PORT_ENV}={raw!r}: a port number")
    return int(raw)


def _ssl_context(server, ca_data):
    if not server.startswith("https://"):
        return None
    if ca_data:
        return ssl.create_default_context(cadata=base64.b64decode(ca_data).decode())
    return ssl.create_default_context()


def _message(raw, fallback):
    try:
        body = json.loads(raw)
        if isinstance(body, dict) and body.get("message"):
            return str(body["message"])
    except (ValueError, UnicodeDecodeError, TypeError):
        pass
    return fallback


def _hub(method, url, context, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=HEADERS)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, context=context, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        raw = err.read()
        if err.code == 426:
            # The refusal's class is the job client's table.
            from zenthros.jobs.http import _refusal

            raise _refusal(426, _message(raw, "this hub speaks another format of the API"), {}) from None
        return err.code, raw
    except (urllib.error.URLError, OSError) as err:
        raise errors.TransientError(f"zenthros-api at {url.split('/v1/')[0]}: {getattr(err, 'reason', err)}") from err


def how_to_sign_in(server, ca_data=""):
    """The hub's Firebase web config and the terms signing in accepts
    ({"version", "termsURL", "privacyURL"}); ValidationError when it takes no
    sign-ups."""
    code, raw = _hub("GET", server + "/v1/signup", _ssl_context(server, ca_data))
    if code == 404:
        raise errors.ValidationError(f"{server} takes no sign-ups: ask its administrator for a key "
                                     "(zenthros login --key)")
    if code != 200:
        raise errors.TransientError(f"zenthros-api {code}: {_message(raw, 'cannot ask how to sign in')}")
    answer = json.loads(raw)
    firebase = (answer.get("firebase") or {})
    if not firebase.get("apiKey") or not firebase.get("projectId"):
        raise errors.TransientError(f"{server} names no Firebase project to sign in with")
    firebase.setdefault("authDomain", "")
    if not firebase["authDomain"]:
        firebase["authDomain"] = firebase["projectId"] + ".firebaseapp.com"
    terms = answer["terms"]
    # Links on the page: a web address, never a script.
    for key in ("termsURL", "privacyURL"):
        if not str(terms[key]).startswith(("https://", "http://")):
            raise errors.TransientError(f"{server} names its terms at {terms[key]!r}, not a web address")
    return ({k: firebase[k] for k in ("apiKey", "authDomain", "projectId")},
            {k: str(terms[k]) for k in ("version", "termsURL", "privacyURL")})


def terms_notice(terms):
    """What signing in accepts, said before the browser opens."""
    return f"By continuing you accept the Terms ({terms['termsURL']}) and Privacy ({terms['privacyURL']}) of this hub."


def exchange(server, id_token, accepted_terms, ca_data=""):
    """The hub's answer to a Google sign-in that accepts its terms of that
    version: {"key", "workspace", "email", "created"}."""
    code, raw = _hub("POST", server + "/v1/signup", _ssl_context(server, ca_data),
                     {"idToken": id_token, "acceptedTerms": accepted_terms})
    if code == 200:
        return json.loads(raw)
    message = _message(raw, f"the sign-in was refused ({code})")
    # 428: the terms changed since they were shown.
    if code in (401, 403, 400, 428):
        raise errors.ValidationError(message)
    raise errors.TransientError(f"zenthros-api {code}: {message}")


_PAGE = """<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zenthros sign-in</title>
<style>
:root{color-scheme:light dark}
body{font:16px/1.5 system-ui,sans-serif;max-width:30rem;margin:4rem auto;padding:0 16px}
button{font:inherit;padding:.6rem 1.1rem;border-radius:.4rem;border:1px solid #888;cursor:pointer}
</style>
<h1>Sign in to Zenthros</h1>
<p id="m">Sign in with your Google account for a Workspace of your own on <b>HUB</b>.</p>
<p>Signing in accepts its <a href="TERMS">Terms</a> and <a href="PRIVACY">Privacy</a>.</p>
<button id="b">Sign in with Google</button>
<script src="JS/firebase-app-compat.js"></script>
<script src="JS/firebase-auth-compat.js"></script>
<script>
firebase.initializeApp(CONFIG);
const m = document.getElementById("m"), b = document.getElementById("b");
b.onclick = async () => {
  b.disabled = true;
  m.textContent = "Signing in…";
  let done = false;
  try {
    const signed = await firebase.auth().signInWithPopup(new firebase.auth.GoogleAuthProvider());
    const idToken = await signed.user.getIdToken();
    await firebase.auth().signOut();
    const res = await fetch(location.pathname + "/token", {
      method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({idToken})});
    const answer = await res.json();
    // Answered, either way: this page's part is over.
    m.textContent = answer.message;
    done = true;
  } catch (e) {
    const why = String(e.code || e.message || e);
    m.textContent = why.includes("requests-from-referer") || why.includes("unauthorized-domain")
      ? "This hub's Firebase project does not take sign-ins from this page: ask its administrator to allow " +
        "http://localhost/* on its web API key and localhost in its authorized domains (" + why + ")."
      : "The sign-in did not finish (" + why + "): try again.";
  }
  b.disabled = false;
  b.hidden = done;
};
</script>
"""


def _page(server, firebase, terms):
    config = json.dumps(firebase).replace("<", "\\u003c")
    return (_PAGE.replace("HUB", html.escape(server)).replace("JS/", FIREBASE_JS + "/")
            .replace("TERMS", html.escape(terms["termsURL"])).replace("PRIVACY", html.escape(terms["privacyURL"]))
            .replace("CONFIG", config)).encode()


def _is_wsl():
    if os.environ.get("WSL_DISTRO_NAME"):
        return True
    try:
        with open("/proc/sys/kernel/osrelease", encoding="utf-8") as fh:
            return "microsoft" in fh.read().lower()
    except OSError:
        return False


def _open_browser(url):
    """Open url in this person's browser, quietly; False when there is none
    to open. In WSL that is Windows' browser (its localhost reaches this
    page); on Linux with no display, none: xdg-open there only lists the
    browsers it did not find."""
    if sys.platform.startswith("linux"):
        if _is_wsl():
            opener = shutil.which("wslview") or shutil.which("explorer.exe")
            if not opener:
                return False
            try:
                subprocess.Popen([opener, url], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
            except OSError:
                return False
            return True
        if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
            return False
    try:
        return bool(webbrowser.open(url))
    except Exception:  # noqa: BLE001 - no browser: the URL is printed
        return False


def _show(url):
    opened = _open_browser(url)
    print(f"Sign in with Google: {url}")
    print(("Opened in your browser" if opened else "Open it in your browser") +
          ". Waiting for the sign-in (Ctrl+C stops)…", flush=True)


def sign_in(server, ca_data="", prompt=None, timeout=None):
    """The hub's answer to a Google sign-in made in this person's browser.
    prompt(url) shows the page (by default, print and open a browser); it
    waits at most timeout seconds."""
    server = server.rstrip("/")
    firebase, terms = how_to_sign_in(server, ca_data)
    state = secrets.token_urlsafe(24)
    tokens, answers = queue.Queue(), queue.Queue()
    page = _page(server, firebase, terms)

    class Page(BaseHTTPRequestHandler):
        def log_message(self, *args):  # nothing of a request is logged
            pass

        def _send(self, code, body, kind):
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path.split("?")[0] != "/signin/" + state:
                self._send(404, b"not here", "text/plain")
                return
            self._send(200, page, "text/html; charset=utf-8")

        def do_POST(self):
            origin = self.headers.get("Origin")
            if (self.path != f"/signin/{state}/token" or
                    self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json" or
                    (origin is not None and origin != f"http://localhost:{port}")):
                self._send(404, b"not here", "text/plain")
                return
            try:
                length = min(int(self.headers.get("Content-Length") or 0), 64 << 10)
                id_token = json.loads(self.rfile.read(length)).get("idToken") or ""
            except (ValueError, AttributeError):
                id_token = ""
            if not isinstance(id_token, str) or not id_token:
                self._send(400, json.dumps({"ok": False, "message": "no sign-in in that request"}).encode(), "application/json")
                return
            tokens.put(id_token)
            try:
                ok, message = answers.get(timeout=90)
            except queue.Empty:
                ok, message = False, "Zenthros did not answer: try again."
            self._send(200, json.dumps({"ok": ok, "message": message}).encode(), "application/json")

    port = _port()
    try:
        httpd = ThreadingHTTPServer(("127.0.0.1", port), Page)
    except OSError as err:
        raise errors.ValidationError(f"the sign-in page cannot use port {port} ({err.strerror}): "
                                     f"stop what uses it, or unset {PORT_ENV}") from None
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        # Said before any page opens, whoever shows it.
        print(terms_notice(terms), flush=True)
        (prompt or _show)(f"http://localhost:{port}/signin/{state}")
        wait = timeout or TIMEOUT
        try:
            id_token = tokens.get(timeout=wait)
        except queue.Empty:
            raise errors.ValidationError(f"the Google sign-in did not finish within {wait:g}s: run login again") from None
        try:
            answer = exchange(server, id_token, terms["version"], ca_data)
        except Exception as err:
            answers.put((False, f"{err}"))
            raise
        answers.put((True, f"Signed in as {answer.get('email', '')}: Workspace {answer.get('workspace', '')}. "
                           "You can close this tab."))
        return answer
    finally:
        # The page's answer is on its own connection: it outlives the server.
        httpd.shutdown()
        httpd.server_close()
