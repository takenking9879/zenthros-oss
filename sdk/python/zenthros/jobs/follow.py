# SPDX-License-Identifier: Apache-2.0

"""A job's log as it is written, for as long as its job runs."""

import subprocess
import threading

from zenthros import errors


class FollowedLog:
    """A job's log as it is written: iterate it for its lines, from the end
    of the log (its last 512 KiB, or its last tail lines) on. A stream that
    breaks, or ends with an attempt of the job, is opened again right after
    what it gave, so each line comes once; one of another attempt starts
    with a line that says so. It ends when ended() says the job did, after
    the stream open then; close() (or a with block) stops it at once.

    client.follow_logs(job_id, after=, tail=) opens one stream: .stdout its
    lines, .position where the next one goes on, poll(), wait() and
    terminate() as a process's. after: a position a log followed before
    left, to go on from."""

    def __init__(self, client, job_id, ended, poll=2.0, tail=None, after=None):
        self._client, self._job_id, self._ended = client, job_id, ended
        self._poll, self._tail = poll, tail
        self._stopping = threading.Event()
        self._proc = None
        # Whether any stream opened: a job that ended before one did is read
        # whole instead.
        self.opened = False
        # Whether it gave the log to the job's end.
        self.done = False
        self.position = after
        self._lines = self._follow()

    def __iter__(self):
        return self._lines

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def _follow(self):
        # A job that ended before any stream opened (refused while it had
        # no machine yet) is opened once more: its log is whole by then.
        last_look = False
        while True:
            try:
                proc = self._client.follow_logs(self._job_id, after=self.position, tail=self._tail)
            except errors.LogGone:
                # Its machine no longer keeps it: no stream comes back.
                return
            except (errors.ZenthrosError, OSError):  # opened again below
                proc = None
            if proc is not None:
                self._proc, self.opened = proc, True
                for line in proc.stdout:
                    self.position = getattr(proc, "position", None) or self.position
                    yield line
                proc.wait()
                self.position = getattr(proc, "position", None) or self.position
            if self._stopping.is_set():
                return
            if self._ended():
                if not self.opened and not last_look:
                    last_look = True
                    continue
                self.done = self.opened
                return
            self._stopping.wait(self._poll)
            if self._stopping.is_set():
                return

    def finish(self, grace):
        """End after the stream open now: it is waited for, at most grace
        (the last lines reach it a moment after the job ends), then stopped."""
        self._stopping.set()
        proc = self._proc
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                proc.terminate()

    def close(self):
        """Stop now."""
        self._stopping.set()
        proc = self._proc
        if proc is not None and proc.poll() is None:
            proc.terminate()


class EndedLog:
    """The followed log of what never ran here: nothing."""

    def __iter__(self):
        return iter(())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass


def job_ended(client, job_id):
    """Whether the job is done, for FollowedLog: one gone counts as done."""
    from zenthros.jobs.handle import TERMINAL_PHASES

    try:
        return client.status(job_id) in TERMINAL_PHASES
    except errors.JobNotFound:
        return True
    except errors.ZenthrosError:
        return False
