# SPDX-License-Identifier: Apache-2.0

"""A job's log as zenthros-api and zctl --records send it: records
(CONTENT_TYPE), each a header on one JSON line and the chunk's bytes after it,
packed as the machine's agent packed them: one zstd frame (unpack), or the
bytes as they are (operator/pkg/logstream). Read as text, what was left out or
never comes is a line of its own, in the words zctl prints.

A followed log goes on after its Position: the cursor of the last record
read, of the attempt the read was of. Opened again from it, each byte comes
once; a log of another attempt starts from its history, with a line that says
so.
"""

import codecs
import json
from dataclasses import dataclass

from zenthros.errors import ZenthrosError

CONTENT_TYPE = "application/vnd.zenthros.log-records"

# One header line is short (a cursor and a handle); a record is one chunk, at
# most 256 KiB packed and unpacked (operator/pkg/backend MaxLogChunk). A frame
# may ask for a window of 1 MiB at most: the agent's are one chunk each.
MAX_HEADER = 4 << 10
MAX_RECORD = 256 << 10
MAX_WINDOW_LOG = 20

# The history a read starts with when it names none: the end of the log, from
# the start of a line (operator/pkg/backend DefaultHistoryBytes).
HISTORY_BYTES = 512 << 10


class Corrupt(ZenthrosError):
    """A record its machine packed wrong. It is not unpacked, and the read
    ends with this error, not as Broken: opened again from its Position, the
    same record would come again."""


class Broken(Exception):
    """The read broke before its end: a record said why, or the stream was
    cut inside one. A follower opens it again from its Position."""


@dataclass
class Position:
    """Where a followed log goes on: right after cursor, of attempt handle.
    What it has not shown yet of a line goes on with it."""

    handle: str = ""
    cursor: str = ""
    partial: str = ""


def records(pieces):
    """(header, data) for each record in pieces, an iterable of bytes as they
    come. Raises Broken when the read broke before its end."""
    buf = bytearray()
    pieces = iter(pieces)
    while True:
        end = buf.find(b"\n")
        while end < 0:
            if len(buf) > MAX_HEADER:
                raise ValueError(f"a log record's header past {MAX_HEADER} bytes")
            piece = next(pieces, None)
            if piece is None:
                if buf:
                    raise Broken("the log stream was cut inside a record")
                return
            buf += piece
            end = buf.find(b"\n")
        head = json.loads(bytes(buf[:end]))
        del buf[:end + 1]
        if head.get("error"):
            raise Broken(head["error"])
        n = head.get("n") or 0
        if not 0 <= n <= MAX_RECORD:
            raise ValueError(f"a log record of {n} bytes")
        while len(buf) < n:
            piece = next(pieces, None)
            if piece is None:
                raise Broken("the log stream was cut inside a record")
            buf += piece
        data = bytes(buf[:n])
        del buf[:n]
        yield head, unpack(head, data)


def unpack(head, data):
    """A record's bytes of log. What a machine sends is not trusted, so a
    packed one is unpacked bounded, as zctl does (operator/pkg/logstream
    Unpack): to exactly the raw_len it says, never past a record, through a
    window of 1 MiB at most; one cut short, of a bigger window, that unpacks
    to more, or packed in a way this SDK does not know raises Corrupt."""
    encoding = head.get("encoding")
    if not encoding:
        return data
    if encoding != "zstd":
        raise Corrupt(f"a log record packed as {encoding!r}, which this SDK does not know")
    n = head.get("raw_len") or 0
    if not 0 < n <= MAX_RECORD:
        raise Corrupt(f"a packed log record of {n} bytes: a record holds 1 to {MAX_RECORD}")
    zstd = zstd_module()
    frame = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: MAX_WINDOW_LOG})
    try:
        out = frame.decompress(data, max_length=n)
        if len(out) == n and not frame.eof and frame.decompress(b"", max_length=1):
            raise Corrupt(f"a packed log record of {n} bytes unpacks to more")
    except zstd.ZstdError as err:
        raise Corrupt(f"a packed log record: {err}") from None
    if len(out) != n or not frame.eof or frame.unused_data:
        raise Corrupt(f"a packed log record of {n} bytes unpacked to {len(out)}, or did not end where it should")
    return out


def zstd_module():
    """Python's zstd from 3.14, its backport before (the same API). Imported
    on the first packed record: `import zenthros` loads the standard library
    alone (tests/test_layers.py)."""
    try:
        from compression import zstd
    except ImportError:
        from backports import zstd
    return zstd


def size(n):
    """n bytes as a person reads them, as zctl says them: 512 B, 2.0 KB."""
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if n >= scale:
            return f"{n / scale:.1f} {unit}"
    return f"{n} B"


def omitted_note(n):
    if n < 0:
        # The source cannot say: the Kubernetes API serves only the latest
        # file of a log.
        return "[zenthros: an earlier part of this log may not be shown]\n"
    return f"[zenthros: the first {size(n)} of this log are not shown]\n"


class Text:
    """A log's records as lines of text, each a str ending in a newline, and a
    line of its own for what was left out or never comes. It goes on from a
    Position: the attempt it was showing, and the end of a line it had not."""

    def __init__(self, after=None):
        after = after or Position()
        self.position = Position(after.handle, after.cursor, "")
        self._partial = after.partial
        self._decode = codecs.getincrementaldecoder("utf-8")("replace")

    def feed(self, head, data):
        """The lines one record completes."""
        lines = []
        # Another attempt has another handle; its number cannot say it, as
        # the first attempt is 0.
        handle = head.get("handle")
        if handle and self.position.handle and handle != self.position.handle:
            lines += self._note(f"[zenthros: attempt {head.get('attempt') or 0}]\n")
        if handle:
            self.position.handle = handle
        if head.get("cursor"):
            self.position.cursor = head["cursor"]
        if head.get("omitted"):
            lines += self._note(omitted_note(head["omitted"]))
        gap = head.get("gap")
        if gap and gap < 0:
            # A part no one can count: a file the cluster removed.
            lines += self._note("[zenthros: part of this log is no longer on the machine]\n")
        elif gap:
            if head.get("gap_reason") == "budget":
                why = "were skipped: the machine's hourly log budget ran out"
            else:
                why = "are no longer on the machine"
            lines += self._note(f"[zenthros: {size(gap)} of this log {why}]\n")
        text = self._partial + self._decode.decode(data)
        *done, self._partial = text.split("\n")
        return lines + [line + "\n" for line in done]

    def end(self):
        """The log ended: the line it had not ended, if any."""
        rest, self._partial = self._partial + self._decode.decode(b"", final=True), ""
        return [rest] if rest else []

    def broke(self):
        """The read broke: where to go on, with what is not shown yet."""
        self.position.partial = self._partial + self._decode.decode(b"", final=True)
        self._partial = ""
        return self.position

    def _note(self, note):
        if not self._partial:
            return [note]
        line, self._partial = self._partial + "\n", ""
        return [line, note]


def read_text(pieces):
    """A whole read's text, what was left out said on lines of its own.
    Raises Broken when it broke before its end."""
    text = Text()
    out = []
    for head, data in records(pieces):
        out += text.feed(head, data)
    return "".join(out + text.end())


def follow_lines(pieces, text):
    """The lines of a followed read as they come, through text. A read that
    broke leaves its Position in text.position for the next one."""
    try:
        for head, data in records(pieces):
            yield from text.feed(head, data)
    except Broken:
        text.broke()
        return
    yield from text.end()


def clip(raw, tail=None):
    """A log read whole from where the SDK keeps it (raw bytes) as a read of
    zenthros-api answers it: its last tail lines, or its last HISTORY_BYTES
    from the start of a line, with a line that says what was left out."""
    if tail is not None:
        text = raw[-HISTORY_BYTES:].decode("utf-8", "replace")
        return "".join(text.splitlines(keepends=True)[-int(tail):]) if int(tail) > 0 else ""
    if len(raw) <= HISTORY_BYTES:
        return raw.decode("utf-8", "replace")
    start = len(raw) - HISTORY_BYTES
    if raw[start - 1:start] != b"\n":
        newline = raw.find(b"\n", start)
        start = len(raw) if newline < 0 else newline + 1
    return omitted_note(start) + raw[start:].decode("utf-8", "replace")
