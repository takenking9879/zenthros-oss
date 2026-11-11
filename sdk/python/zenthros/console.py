# SPDX-License-Identifier: Apache-2.0

"""What the SDK writes for a person to read, on any console.

A Windows program whose output goes to a file or a pipe writes it in the
system's code page (cp1252), which has no ✓ and no █: writing one raised
UnicodeEncodeError, which ended a call after its job had succeeded (its
receipt) and the thread that followed a job's log (a progress bar). A sign
the stream cannot encode shows as "?". The zenthros command makes the same
choice for its whole process (cli.entry).
"""

import sys


def write(text, out=None):
    out = out or sys.stdout
    encoding = getattr(out, "encoding", None)
    if encoding:
        text = text.encode(encoding, "replace").decode(encoding)
    out.write(text)
    out.flush()


def show(line, out=None):
    write(line + "\n", out)
