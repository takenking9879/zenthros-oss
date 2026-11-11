# SPDX-License-Identifier: Apache-2.0

"""A call with no bucket. What a machine needs of it rides in the job's own
files, up to what a job carries; its result comes back in the job's final
report, with its SHA-256, up to what a report carries. Beyond those, it says
what to do. The control plane carries no data past that: a small call is all
this is for.

The URIs have the shape of any store's, so nothing else tells them apart: the
machine reads them from its unpacked files, and this computer from the reports."""

import base64
import gzip
import io
import tarfile

from zenthros import errors
from zenthros.core.report import MAX_PAYLOAD_BYTES
from zenthros.core.storage import CARRIED_FOLDER as FOLDER
from zenthros.core.storage import CARRIED_ROOT as ROOT
from zenthros.core.storage import BlobStore, carried_on_this_machine, is_carried

# What a result may weigh: base64 of it and the report's own fields fit one report.
MAX_RESULT_BYTES = (MAX_PAYLOAD_BYTES - (4 << 10)) // 4 * 3
BYTES_KEY = "bytes"

NEEDS_A_BUCKET = "zenthros store gs://your-bucket/zenthros (or s3://)"


carries = is_carried
root = carried_on_this_machine


def too_large(size):
    """What a result over the limit ends its call with."""
    return (f"the result weighs {size >> 10} KiB, and with no bucket a call brings back {MAX_RESULT_BYTES >> 10} KiB at most. "
            f"Give it a bucket ({NEEDS_A_BUCKET}), return less, or write it where it runs "
            "and return its path")


class CarriedStore(BlobStore):
    """This computer's side: what goes with the job, held until it is packed; and
    what came back, read from the job's reports. The caller verifies it against
    the digests the job reported, as with any store."""

    def __init__(self, client):
        self.client = client
        self.files = {}

    def put(self, uri, payload):
        self.files[uri] = payload
        return uri

    def get(self, uri):
        if uri in self.files:
            return self.files[uri]
        from zenthros.remote.call import RESULT_REPORT

        name = uri[len(ROOT) + 1:].partition("/")[0]
        for view in reversed(self.client.reports(name)):
            entry = view.payload.get(RESULT_REPORT)
            if isinstance(entry, dict) and entry.get("uri") == uri and entry.get(BYTES_KEY):
                return base64.b64decode(entry[BYTES_KEY])
        return self._from_its_job(name, uri)

    def _from_its_job(self, name, uri):
        """What went in the job: its call and its project, in the job's own files."""
        inline = self.client.source_of(name)
        if not inline:
            raise FileNotFoundError(uri)
        with tarfile.open(fileobj=io.BytesIO(base64.b64decode(inline))) as tar:
            try:
                return tar.extractfile(f"{FOLDER}/job/{uri[len(ROOT) + 1:]}").read()
            except KeyError:
                raise FileNotFoundError(uri) from None

    def ls(self, prefix):
        return []

    def delete(self, uri):
        self.files.pop(uri, None)


def pack(code, files, limit):
    """code, the job's gzipped tar, with files in it. The whole has to fit a job."""
    raw = io.BytesIO(gzip.decompress(code) if code else b"")
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w", format=tarfile.PAX_FORMAT) as tar:
        if code:
            with tarfile.open(fileobj=raw) as old:
                for member in old.getmembers():
                    tar.addfile(member, old.extractfile(member))
        for uri in sorted(files):
            info = tarfile.TarInfo(f"{FOLDER}/job/{uri[len(ROOT) + 1:]}")
            info.size, info.mode, info.mtime = len(files[uri]), 0o644, 0
            tar.addfile(info, io.BytesIO(files[uri]))
    packed = gzip.compress(out.getvalue(), mtime=0)
    if len(packed) > limit:
        raise errors.ValidationError(f"this call and its project pack to {len(packed) >> 10} KiB, and with no bucket a job "
                                     f"carries {limit >> 10} KiB at most. Give it a bucket ({NEEDS_A_BUCKET}), or pass "
                                     "code_dir= a smaller directory and large data by URI")
    return packed
