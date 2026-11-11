# SPDX-License-Identifier: Apache-2.0

"""Files of the Workspace's store by URI, the same here and in a job.

    from zenthros import files

    df = pd.read_parquet(files.open(data_uri))       # s3://... or a local path
    files.write_bytes(model_uri, pickle.dumps(model))
    with files.open(f"{out}/report.txt", "w") as fh:  # sent when closed
        fh.write(text)
    files.ls(out)                                     # [StoredObject(uri, size, modified)]

Where it reads and writes:
  - in a job (ZENTHROS_JOB_ID): the store as the machine reaches it, with the
    access and endpoint the hub gave the job; never zenthros-api;
  - here, with a zenthros-api connection: through URLs the API signs, so no
    cloud credentials on this side;
  - a path that is no s3:// or gs:// URI: a local file, so a function that
    uses files runs here on local data as well.

This module travels with every call (zenthros.remote.bundle): it imports
nothing the machine does not have.
"""

import builtins
import io
import os

from zenthros.core.env import JobEnv

_REMOTE = ("s3://", "gs://")


def _store(uri):
    from zenthros.core.storage import store_for

    if JobEnv.current().job_id or not str(uri).startswith(_REMOTE):
        return store_for(uri)
    from zenthros.remote import options

    return options.blob_store(uri)


def read_bytes(uri):
    """The object's bytes; FileNotFoundError if there is none."""
    uri = os.fspath(uri)
    if not uri.startswith(_REMOTE):
        with builtins.open(uri, "rb") as fh:
            return fh.read()
    return _store(uri).get(uri)


def write_bytes(uri, data):
    """Write data (bytes) to uri, a folder's parents made as needed; returns uri."""
    uri = os.fspath(uri)
    _store(uri).put(uri, bytes(data))
    return uri


def ls(prefix):
    """Every object whose URI starts with prefix (a folder: its whole tree),
    as [StoredObject(uri, size, modified)]."""
    prefix = os.fspath(prefix)
    return _store(prefix).ls(prefix)


class _Upload(io.BytesIO):
    """What is written goes to the store once, when closed without an error."""

    def __init__(self, uri):
        super().__init__()
        self._uri, self._failed = uri, False

    def __exit__(self, kind, value, traceback):
        self._failed = kind is not None
        return super().__exit__(kind, value, traceback)

    def close(self):
        # Closed even when the send fails: the garbage collector would close
        # it again later and send it a second time.
        try:
            if not self.closed and not self._failed:
                write_bytes(self._uri, self.getvalue())
        finally:
            super().close()


def open(uri, mode="rb", encoding="utf-8"):  # noqa: A001 - files.open, as io.open
    """A file object for uri: "rb"/"r" read it whole, "wb"/"w" send it when
    closed (not when the with block raised). A local path is opened as is."""
    if mode not in ("rb", "r", "wb", "w"):
        raise ValueError(f"files.open mode {mode!r}: 'rb', 'r', 'wb' or 'w'")
    uri = os.fspath(uri)
    if not uri.startswith(_REMOTE):
        if "w" in mode:
            os.makedirs(os.path.dirname(os.path.abspath(uri)), exist_ok=True)
        return builtins.open(uri, mode, encoding=None if "b" in mode else encoding)
    if "r" in mode:
        raw = io.BytesIO(read_bytes(uri))
        return raw if "b" in mode else io.TextIOWrapper(raw, encoding=encoding)
    upload = _Upload(uri)
    return upload if "b" in mode else _TextUpload(upload, encoding)


class _TextUpload(io.TextIOWrapper):
    def __exit__(self, kind, value, traceback):
        self.buffer._failed = kind is not None
        return super().__exit__(kind, value, traceback)
