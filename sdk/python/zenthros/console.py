# SPDX-License-Identifier: Apache-2.0

"""What the SDK writes for a person to read, on any console.

A Windows program whose output goes to a file or a pipe writes it in the
system's code page (cp1252), which has no ✓ and no █: writing one raised
UnicodeEncodeError, which ended a call after its job had succeeded (its
receipt) and the thread that followed a job's log (a progress bar). A sign
the stream cannot encode shows as "?". The zenthros command makes the same
choice for its whole process (cli.entry).

One writer at a time (LOCK): a call's log, followed on a thread of its own,
and this process's own lines never cut into each other, on the console and
in a transcript.
"""

import contextlib
import sys
import threading

LOCK = threading.RLock()


def write(text, out=None):
    out = out or sys.stdout
    encoding = getattr(out, "encoding", None)
    if encoding:
        text = text.encode(encoding, "replace").decode(encoding)
    with LOCK:
        out.write(text)
        out.flush()


def show(line, out=None):
    write(line + "\n", out)


class _Tee:
    """A stream that also writes to a copy, under LOCK."""

    def __init__(self, stream, copy):
        self._stream, self._copy = stream, copy

    def write(self, text):
        with LOCK:
            written = self._stream.write(text)
            self._copy.write(text)
            return written

    def flush(self):
        with LOCK:
            self._stream.flush()
            self._copy.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


@contextlib.contextmanager
def transcript(path):
    """Everything this process shows while the block runs, also appended to
    path: its own output and its calls' logs as they arrive, in the order
    they come, each line whole.

        with zenthros.transcript("session.log"):
            model = train.remote(cfg)
    """
    with open(path, "a", encoding="utf-8", errors="replace") as copy:
        out, err = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = _Tee(out, copy), _Tee(err, copy)
        try:
            yield path
        finally:
            with LOCK:
                sys.stdout, sys.stderr = out, err
            copy.flush()
