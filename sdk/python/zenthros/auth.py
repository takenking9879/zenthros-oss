# SPDX-License-Identifier: Apache-2.0

"""How this process gets the token it carries to zenthros-api.

    zenthros.login(server="https://api.zenthros.acme.com", workspace="risk", token="...")
    zenthros.login()            # the current context names an OIDC issuer
    zenthros.logout()

A token someone issued (zctl user add) is written into the zenconfig. An
OIDC issuer is asked with the device grant (RFC 8628): login() shows a URL
and a code, the person approves in a browser, and the SDK keeps what the
issuer gives it. The bearer token is the ID token, which is what the
kube-apiserver checks; the refresh token renews it without asking again.

The session is kept apart from the zenconfig, for its owner only: in the
system keyring when the keyring package is installed and works, else in
<config dir>/tokens/<context>.json (0600). Nothing here prints a token.
Standard library only.
"""

import base64
import contextlib
import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

from zenthros import config, errors

DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
DEFAULT_SCOPES = ("openid", "offline_access")
KEYRING_SERVICE = "zenthros"
# A token this close to its end is renewed. More than TokenSource's own
# margin, so a token it is handed is one it keeps.
RENEW_BEFORE = 60

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1"})

_sleep = time.sleep


def _keyring():
    try:
        import keyring
    except ImportError:
        return None
    return keyring


def _checked(url, what):
    """url, if it is one a token may be sent to: https, or this machine."""
    parts = urllib.parse.urlsplit(str(url or ""))
    if parts.scheme == "https" and parts.hostname:
        return str(url)
    if parts.scheme == "http" and parts.hostname in _LOOPBACK:
        return str(url)
    raise errors.ValidationError(f"{what} {url!r}: an https:// URL (http:// only on this machine)")


def _words(body, code):
    """What the issuer said about a refusal."""
    return str(body.get("error_description") or body.get("error") or f"HTTP {code}")[:300]


def _expiry(token, body):
    """When the ID token ends: its own exp claim, else what the issuer said
    of the grant."""
    try:
        payload = token.split(".")[1]
        return float(json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["exp"])
    except (IndexError, ValueError, KeyError, TypeError):
        return time.time() + float(body.get("expires_in") or 0)


def _show(url, code, complete):
    print(f"To log in, open {url} and enter the code {code}")
    if complete:
        print(f"(or open {complete})")
    print("Waiting for the login to be approved...", flush=True)


class Session:
    """One context's login at its OIDC issuer: started by login(), renewed
    by token(), dropped by forget()."""

    def __init__(self, name, oidc):
        if not _NAME.match(name or ""):
            raise errors.ValidationError(f"context {name!r}: a login session needs a name of letters, digits, '.', '_' or '-'")
        if not isinstance(oidc, dict):
            raise errors.ValidationError(f"context {name!r}: auth.oidc is an object with issuer and client-id")
        self.name = name
        self.issuer = str(oidc.get("issuer") or "").rstrip("/")
        self.client_id = str(oidc.get("client-id") or "")
        if not self.issuer or not self.client_id:
            raise errors.ValidationError(f"context {name!r}: auth.oidc needs an issuer and a client-id")
        _checked(self.issuer, "issuer")
        self.scopes = [str(scope) for scope in (oidc.get("scopes") or DEFAULT_SCOPES)]
        self._ssl = None
        if self.issuer.startswith("https://"):
            ca = oidc.get("ca-data")
            self._ssl = ssl.create_default_context(cadata=base64.b64decode(ca).decode()) if ca \
                else ssl.create_default_context()
        self._endpoints = None

    # --- the issuer -------------------------------------------------------

    def _request(self, url, form=None):
        """One call to the issuer: its status and its JSON body."""
        data = None if form is None else urllib.parse.urlencode(form).encode()
        request = urllib.request.Request(url, data=data, method="GET" if data is None else "POST")
        request.add_header("Accept", "application/json")
        try:
            with urllib.request.urlopen(request, context=self._ssl, timeout=30) as resp:
                code, raw = resp.status, resp.read()
        except urllib.error.HTTPError as err:
            code, raw = err.code, err.read()
        except (urllib.error.URLError, OSError) as err:
            raise errors.TransientError(f"issuer {self.issuer}: {getattr(err, 'reason', err)}") from None
        try:
            body = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = {}
        return code, body if isinstance(body, dict) else {}

    def _discover(self):
        """The device and token endpoints, from the issuer's own discovery
        document and nobody else's."""
        if self._endpoints is None:
            code, doc = self._request(self.issuer + "/.well-known/openid-configuration")
            if code != 200:
                raise errors.TransientError(f"issuer {self.issuer}: discovery answered {code}")
            declared = str(doc.get("issuer") or "").rstrip("/")
            if declared != self.issuer:
                raise errors.ValidationError(f"{self.issuer} describes itself as {declared!r}: not this context's issuer")
            if not doc.get("device_authorization_endpoint"):
                raise errors.ValidationError(f"issuer {self.issuer} has no device login (device_authorization_endpoint)")
            self._endpoints = (_checked(doc["device_authorization_endpoint"], "the issuer's device endpoint"),
                               _checked(doc.get("token_endpoint"), "the issuer's token endpoint"))
        return self._endpoints

    def login(self, prompt=None, timeout=None):
        """Ask for a device code, show it, and wait as the issuer paces it
        until the person approves, refuses, or the code ends."""
        device_url, token_url = self._discover()
        code, grant = self._request(device_url, {"client_id": self.client_id, "scope": " ".join(self.scopes)})
        if code != 200 or not grant.get("device_code"):
            if errors.transient_status(code):
                raise errors.TransientError(f"issuer {self.issuer}: {_words(grant, code)}")
            raise errors.LoginRequired(f"issuer {self.issuer} refused a device login for client "
                                       f"{self.client_id}: {_words(grant, code)}")
        user_code = str(grant.get("user_code") or "")
        url = str(grant.get("verification_uri") or grant.get("verification_url") or "")
        (prompt or _show)(url, user_code, str(grant.get("verification_uri_complete") or ""))
        interval = max(int(grant.get("interval") or 5), 1)
        lifetime = int(grant.get("expires_in") or 600)
        if timeout is not None:
            lifetime = min(lifetime, int(timeout))
        waited = 0
        while waited + interval <= lifetime:
            _sleep(interval)
            waited += interval
            try:
                code, body = self._request(token_url, {"grant_type": DEVICE_GRANT, "device_code": grant["device_code"],
                                                       "client_id": self.client_id})
            except errors.TransientError:
                continue
            if code == 200:
                return self._keep(body, {})
            error = body.get("error")
            if error == "authorization_pending" or errors.transient_status(code):
                continue
            if error == "slow_down":
                interval += 5
                continue
            raise errors.LoginRequired(f"the login at {self.issuer} was not completed: {_words(body, code)}")
        raise errors.LoginRequired(f"nobody entered the code {user_code} at {url} in time")

    def token(self):
        """The ID token and when it expires, renewed when it is about to."""
        session = self._read()
        if session.get("id_token") and time.time() < float(session.get("expires") or 0) - RENEW_BEFORE:
            return session["id_token"], float(session["expires"])
        refresh = session.get("refresh_token")
        if not refresh:
            raise errors.LoginRequired(f"context {self.name}: no session at {self.issuer} that can be renewed; "
                                       "zenthros.login() starts one")
        _, token_url = self._discover()
        code, body = self._request(token_url, {"grant_type": "refresh_token", "refresh_token": refresh,
                                               "client_id": self.client_id})
        if code == 200:
            kept = self._keep(body, session)
            return kept["id_token"], kept["expires"]
        if errors.transient_status(code):
            raise errors.TransientError(f"issuer {self.issuer}: {_words(body, code)}")
        # A refresh token the issuer refused is of no more use.
        self.forget()
        raise errors.LoginRequired(f"context {self.name}: {self.issuer} did not renew the session "
                                   f"({_words(body, code)}); zenthros.login() starts a new one")

    # --- where the session is kept -----------------------------------------

    def _keep(self, body, previous):
        token = body.get("id_token")
        if not token:
            raise errors.ValidationError(f"issuer {self.issuer} returned no id_token: the client needs the openid scope")
        # An issuer that does not rotate refresh tokens sends none back.
        session = {"issuer": self.issuer, "id_token": token, "expires": _expiry(token, body),
                   "refresh_token": body.get("refresh_token") or previous.get("refresh_token") or ""}
        self._save(session)
        return session

    def _path(self):
        return os.path.join(os.path.dirname(config.config_path()), "tokens", self.name + ".json")

    def _read(self):
        """The kept session of this issuer, {} when there is none."""
        raw = None
        ring = _keyring()
        if ring is not None:
            # A keyring without a backend (a server, a container) raises
            # its own errors: the file is then where the session is.
            with contextlib.suppress(Exception):
                raw = ring.get_password(KEYRING_SERVICE, self.name)
        if not raw:
            try:
                with open(self._path(), encoding="utf-8") as fh:
                    raw = fh.read()
            except OSError:
                return {}
        try:
            session = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if not isinstance(session, dict) or session.get("issuer") != self.issuer:
            return {}
        return session

    def _save(self, session):
        text = json.dumps(session)
        ring = _keyring()
        if ring is not None:
            try:
                ring.set_password(KEYRING_SERVICE, self.name, text)
            except Exception:  # as in _read
                pass
            else:
                with contextlib.suppress(OSError):
                    os.unlink(self._path())
                return
        config.write_private(self._path(), text)

    def forget(self):
        ring = _keyring()
        if ring is not None:
            with contextlib.suppress(Exception):
                ring.delete_password(KEYRING_SERVICE, self.name)
        with contextlib.suppress(OSError):
            os.unlink(self._path())


def _read_ca(ca_file):
    with open(ca_file, "rb") as fh:
        pem = fh.read()
    try:
        ssl.create_default_context(cadata=pem.decode())
    except (ssl.SSLError, UnicodeDecodeError):
        raise errors.ValidationError(f"ca_file {ca_file}: no PEM certificate in it") from None
    return base64.b64encode(pem).decode()


def _google(context, server, ca_file, prompt, timeout):
    """Sign in with Google on a hub that takes sign-ups, and keep the key it
    answers (zenthros.signin)."""
    from zenthros import signin

    ca_data = ""
    if not server:
        # Again, on the hub of the context: its key ended.
        try:
            found = config._load(context)[1]
        except errors.ValidationError:
            found = {}
        server, ca_data = found.get("server", ""), found.get("ca-data", "")
    if ca_file:
        ca_data = _read_ca(ca_file)
    if not server:
        raise errors.ValidationError("login: give server=, the hub's https:// URL, to sign in with Google")
    if not server.startswith(("https://", "http://")):
        raise errors.ValidationError(f"server {server!r}: an https:// URL")
    answer = signin.sign_in(server, ca_data, prompt, timeout)
    return login(context, key=answer["key"])


def login(context="", *, key="", server="", workspace="", token="", ca_file="", issuer="", client_id="", scopes=None,
          google=False, prompt=None, timeout=None):
    """Log this user in to a context and make it the current one. Returns
    its Workspace (zenthros.workspace()).

    With token, the context carries that token. Without, its OIDC issuer
    (the context's auth.oidc, or issuer and client_id) is asked for a device
    login: prompt(url, code, complete_url) shows where to approve it (by
    default, print) and login() waits, for at most timeout seconds.

    With google (or a server and no Workspace, token or issuer: a person
    with no key yet), the person signs in with Google in their browser, on a
    hub that takes sign-ups, for a Workspace of their own (zenthros.signin):
    prompt(url) shows the page to open.
    """
    if google:
        return _google(context, server, ca_file, prompt, timeout)
    if key:
        # A key (zctl user add) is a whole context: kept as the current one.
        found = config.parse_api_key(key)
        name = context or found["workspace"]
        config.save_context(name, found)
        return _workspace_of(name)
    try:
        name, found = config._load(context)
    except errors.ValidationError:
        # A context that is not there yet is the one to write.
        if not context:
            raise
        name, found = context, {}
    merged = dict(found)
    if server:
        merged["server"] = server.rstrip("/")
    if workspace:
        merged["workspace"] = workspace
    if ca_file:
        merged["ca-data"] = _read_ca(ca_file)
    missing = [key for key in ("server", "workspace") if not merged.get(key)]
    name = name or context or merged.get("workspace", "")
    if missing == ["workspace"] and server and not token and not issuer and not client_id:
        # Only the hub: a person with no key yet.
        return _google(context, server, ca_file, prompt, timeout)
    if missing:
        raise errors.ValidationError(f"login: context {name!r} has no {' and no '.join(missing)}: "
                                     f"give {' and '.join(key + '=' for key in missing)}")
    if not merged["server"].startswith(("https://", "http://")):
        raise errors.ValidationError(f"server {merged['server']!r}: an https:// URL")
    session = None
    kept = found.get("auth") or {}
    if token:
        merged["auth"] = {"type": "token", "token": token}
    elif kept.get("type") == "token" and kept.get("token") and not issuer:
        # Its token is already there (a key, an earlier login): nothing to ask.
        merged["auth"] = {"type": "token", "token": kept["token"]}
    else:
        oidc = dict((found.get("auth") or {}).get("oidc") or {})
        if issuer:
            oidc["issuer"] = issuer.rstrip("/")
        if client_id:
            oidc["client-id"] = client_id
        if scopes:
            oidc["scopes"] = list(scopes)
        if not oidc.get("issuer") or not oidc.get("client-id"):
            raise errors.ValidationError(f"login: context {name!r} has neither a token nor an issuer to ask: "
                                         "give token=, or issuer= and client_id=")
        session = Session(name, oidc)
        merged["auth"] = {"type": "oidc", "oidc": oidc}
    # Written first: a zenconfig that cannot be written fails before anyone
    # is sent to a browser.
    config.save_context(name, merged)
    if session is not None:
        session.login(prompt, timeout)
    return _workspace_of(name)


def _workspace_of(name):
    """The Workspace of context name as the zenconfig has it now: the
    process's clients made before are dropped, as the session behind a
    context's name may be another person's (logout, then login)."""
    from zenthros import view
    from zenthros.jobs.http import HTTPJobClient, forget_shared

    forget_shared()
    return view.Workspace(HTTPJobClient.for_connection(config.resolve(context=name)))


def logout(context=""):
    """Log out of a context (the current one): its kept OIDC session is
    dropped, and a token or key login() wrote into the zenconfig is removed
    from it. The context stays, with its server and Workspace: login() logs
    in again. A key removed here still works until `zctl user revoke`.

    A credential in the environment is the process's, which logout() cannot
    drop: it is refused, naming the variable, before anything changes. It
    did nothing for a key and said nothing (audit 2026-10-16 AUD-079)."""
    held = [env for env in (config.TOKEN_ENV, config.TOKEN_FILE_ENV) if os.environ.get(env)]
    if not context and os.environ.get(config.API_KEY_ENV):
        held.insert(0, config.API_KEY_ENV)
    if held:
        raise errors.ValidationError(f"the credential in use is in {held[0]}, which logout() cannot drop: unset it")
    name, found = config._load(context)
    auth = found.get("auth") or {}
    if name and auth.get("oidc"):
        Session(name, auth["oidc"]).forget()
    if name and auth.get("token"):
        config.forget_token(name)
    from zenthros.jobs.http import forget_shared

    # A client kept in this process holds the session's token until it
    # expires: the next call reads the zenconfig, logged out.
    forget_shared()
