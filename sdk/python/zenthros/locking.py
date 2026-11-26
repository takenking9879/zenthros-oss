# SPDX-License-Identifier: Apache-2.0

"""A file held by one at a time: by one thread or process, whichever asks."""

import contextlib
import os

if os.name == "nt":
    import errno
    import msvcrt

    def _lock(handle):
        # Its first byte, waited for as long as another holds it: LK_LOCK
        # alone gives up after 10 s, with EDEADLOCK.
        handle.seek(0)
        while True:
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                return
            except OSError as err:
                if err.errno != errno.EDEADLOCK:
                    raise
else:
    import fcntl

    def _lock(handle):
        fcntl.flock(handle, fcntl.LOCK_EX)


@contextlib.contextmanager
def held(path):
    """path's lock, until the block ends. Each open of the file is its own
    holder: two threads of one process exclude each other as two processes
    do. Not reentrant: a holder that asks again waits for itself."""
    with open(path, "a", encoding="utf-8") as handle:
        _lock(handle)
        yield
