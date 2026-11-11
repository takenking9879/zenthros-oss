# SPDX-License-Identifier: Apache-2.0

"""The connections of one HTTPJobClient to zenthros-api. Standard library only.

A connection is kept per thread and per lane and reused, so a call costs no
TCP and TLS handshake but the first: "calls" for questions answered at once,
"waits" for a long poll that holds its connection while it waits. map runs
its calls on threads, and http.client's connection serves one request at a
time. A followed log takes a connection of its own, closed with it.

A kept connection that sat IDLE_SECONDS is closed before it is used again:
under zenthros-api's 120 s and Traefik's 180 s, so the server seldom closed
it first; one the server did close (a restart closes them all) is seen
closed before it is used. When the server closed it in between, the request
fails before any answer, and is sent once more on a new connection only if
sending it twice does what sending it once does (the caller says so): a GET,
a PUT, a DELETE, a submit (its nonce), a cancel. Another, a join, says it may
have been done. A connection whose request or answer failed any other way is
never used again: what is left on it is not known.

A proxy is the environment's, as urllib takes it (HTTPS_PROXY, NO_PROXY, the
system's): a CONNECT tunnel to an https hub, the absolute URL to an http one.
"""

import base64
import http.client
import select
import threading
import time
import urllib.parse
import urllib.request
import weakref

# Below the API's IdleTimeout (cmd/api) and Traefik's.
IDLE_SECONDS = 60.0

# What a kept connection the server had closed raises before any answer.
# RemoteDisconnected is a ConnectionResetError.
_STALE = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, http.client.BadStatusLine)

_clock = time.monotonic


class MaybeDone(ConnectionError):
    """A request that may not be sent twice lost its connection before any
    answer: the server may have done it."""


class Hub:
    """One zenthros-api's connections: server is its https:// (or http://)
    URL, context the TLS context of an https one."""

    def __init__(self, server, context):
        parts = urllib.parse.urlsplit(server)
        self.scheme, self.host = parts.scheme, parts.hostname
        self.port = parts.port or (443 if self.scheme == "https" else 80)
        self._context = context
        self._proxy = _proxy_for(self.scheme, self.host)
        self._local = threading.local()
        # Every thread's, for close().
        self._every = weakref.WeakSet()

    def request(self, method, url, body, headers, timeout, idempotent, lane="calls"):
        """The answer to one request, its status and headers read and its
        body to read. lane None: on a connection of its own, which the
        answer closes."""
        target = self._target(url)
        if lane is None:
            conn = self._connect(timeout)
            return self._send(conn, method, target, body, {**headers, "Connection": "close"}, timeout)
        conn, reused = self._kept(lane, timeout)
        try:
            return self._kept_answer(lane, conn, method, target, body, headers, timeout)
        except Exception as err:
            self._forget(lane, conn)
            if not (reused and isinstance(err, _STALE)):
                raise
            if not idempotent:
                raise MaybeDone(f"the connection broke before an answer ({err}); it may have been done") from err
        conn, _ = self._kept(lane, timeout)
        try:
            return self._kept_answer(lane, conn, method, target, body, headers, timeout)
        except Exception:
            self._forget(lane, conn)
            raise

    def discard(self, lane):
        """This thread's connection of lane, whose answer broke off while it
        was read: closed, never used again (what is left of the answer is on
        it). lane None: none is kept."""
        if lane is not None:
            self._drop(lane)

    def close(self):
        """Every thread's kept connections; the next call opens one again."""
        for lane in list(vars(self._local)):
            self._drop(lane)
        for conn in list(self._every):
            conn.close()

    def _kept_answer(self, lane, conn, method, target, body, headers, timeout):
        answer = self._send(conn, method, target, body, headers, timeout)
        # Idle from when the answer came: what is read of it after comes
        # at once, and a little sooner is only a connection closed early.
        setattr(self._local, lane, (conn, answer, _clock()))
        return answer

    def _kept(self, lane, timeout):
        """This thread's connection of lane and whether it served before: a
        new one when there is none, its last answer was not closed, it sat
        IDLE_SECONDS, or the server closed it (an idle connection with
        something to read has only its end to read)."""
        kept = getattr(self._local, lane, None)
        if kept is not None:
            conn, answer, at = kept
            if conn.sock is not None and answer.isclosed() and _clock() - at < IDLE_SECONDS and not _readable(conn.sock):
                return conn, True
            self._drop(lane)
        conn = self._connect(timeout)
        self._every.add(conn)
        return conn, False

    def _drop(self, lane):
        kept = getattr(self._local, lane, None)
        if kept is not None:
            delattr(self._local, lane)
            kept[0].close()

    def _forget(self, lane, conn):
        """lane's connection and conn, which a failure left in a state not
        known: conn may be a new one not kept yet."""
        self._drop(lane)
        conn.close()

    def _connect(self, timeout):
        if self._proxy is None:
            if self.scheme == "https":
                return http.client.HTTPSConnection(self.host, self.port, timeout=timeout, context=self._context)
            return http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        host, port, auth = self._proxy
        if self.scheme == "https":
            # The tunnel's TLS is to the hub, by its name.
            conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=self._context)
            conn.set_tunnel(self.host, self.port, headers={"Proxy-Authorization": auth} if auth else None)
            return conn
        return http.client.HTTPConnection(host, port, timeout=timeout)

    def _target(self, url):
        parts = urllib.parse.urlsplit(url)
        path = parts.path + ("?" + parts.query if parts.query else "")
        if self._proxy is not None and self.scheme == "http":
            return f"http://{self.host}:{self.port}{path}"
        return path

    def _send(self, conn, method, target, body, headers, timeout):
        if self._proxy is not None and self.scheme == "http" and self._proxy[2]:
            headers = {**headers, "Proxy-Authorization": self._proxy[2]}
        conn.timeout = timeout
        if conn.sock is not None:
            conn.sock.settimeout(timeout)
        conn.request(method, target, body=body, headers=headers)
        return conn.getresponse()


def _readable(sock):
    """Whether an idle socket has something to read: the server's end (a
    FIN, TLS's close_notify), as an idle connection has nothing else."""
    try:
        return bool(select.select([sock], [], [], 0)[0])
    except (OSError, ValueError):
        return True


def _proxy_for(scheme, host):
    """(host, port, Proxy-Authorization or None) of the environment's proxy
    for scheme://host, or None for none."""
    url = urllib.request.getproxies().get(scheme)
    if not url or urllib.request.proxy_bypass(host):
        return None
    parts = urllib.parse.urlsplit(url if "://" in url else "http://" + url)
    auth = None
    if parts.username:
        user = urllib.parse.unquote(parts.username) + ":" + urllib.parse.unquote(parts.password or "")
        auth = "Basic " + base64.b64encode(user.encode()).decode()
    return parts.hostname, parts.port or (443 if parts.scheme == "https" else 80), auth
