# SPDX-License-Identifier: Apache-2.0

"""A record a job and its caller exchange through a store (a call, a
result): pickled, and packed with zstd when that makes it smaller. The one
place that says how such bytes are made and read: a reader of its own reads a
packed record as a pickle and fails.

A packed record starts with zstd's frame magic; a pickle (protocol 2 and up)
starts with 0x80. A small record, or one that does not pack (random bytes, an
image already compressed), stays a pickle: packing it costs time and saves
nothing.

The bytes are opened only once their SHA-256 matched the one their writer
reported or the spec pinned: what is unpacked was already checked."""

import pickle

from zenthros.jobs.logrecords import zstd_module

PROTOCOL = 4
# Under this a record is not worth a frame.
PACK_FROM = 4 << 10
# Packed, a record must come to this share of itself or less.
PACK_SHARE = 0.9
LEVEL = 3
FRAME_MAGIC = b"\x28\xb5\x2f\xfd"


def dump(record):
    """record's bytes for a store: a pickle, packed when that pays."""
    raw = pickle.dumps(record, protocol=PROTOCOL)
    if len(raw) < PACK_FROM:
        return raw
    packed = zstd_module().compress(raw, level=LEVEL)
    return packed if len(packed) <= len(raw) * PACK_SHARE else raw


def load(raw):
    """The record dump() made raw from."""
    if raw[:4] == FRAME_MAGIC:
        raw = zstd_module().decompress(raw)
    return pickle.loads(raw)
