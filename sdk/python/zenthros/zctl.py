# SPDX-License-Identifier: Apache-2.0

"""The one place the SDK runs zctl, and the one reading of what it said.

zenthros.jobs and zenthros.infra both go through Zctl, so a refusal means
the same thing whichever object it was about. zctl says what a refusal
means by its exit code (operator/pkg/zctl/exit.go), never by its words:

    67 the credential is missing or refused -> errors.LoginRequired
    77 forbidden                            -> errors.WorkspaceForbidden
    66 the zenthrosjob is not there         -> errors.JobNotFound
    64 the caller's to fix                  -> errors.ValidationError
    75 a later attempt may get past it      -> errors.ZctlError (made nothing)
    any other (1: the outcome is not known) -> errors.ZctlError
    no zctl at all                          -> errors.ZctlMissing (a ValidationError)

operator/pkg/zctl/testdata/sdk-wire.json holds both sides to this table.
The message is zctl's stderr, whole.
"""

import os
import subprocess

from zenthros import errors

MISSING = 127
# Every command the SDK runs is one request (no --wait): a zctl still running
# after this hangs on a controller that does not answer.
TIMEOUT = 300
# zctl's exit codes (operator/pkg/zctl/exit.go).
EXIT_INVALID = 64
EXIT_JOB_ABSENT = 66
EXIT_LOGIN = 67
EXIT_TRANSIENT = 75
EXIT_FORBIDDEN = 77

# A checkout's own zctl, next to sdk/ (`make -C operator sdk-zctl`). An
# installed package has no such file and runs zctl from PATH.
CHECKOUT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "bin", "zctl"))


class Zctl:
    def __init__(self, binary=None):
        self.binary = (binary or os.environ.get("ZENTHROS_ZCTL")
                       or (CHECKOUT if os.access(CHECKOUT, os.X_OK) else "zctl"))

    def __call__(self, *args, env=None):
        """zctl's stdout, or the error its words map to. env adds to the
        process's environment (a secret's value, kept off the command line)."""
        try:
            proc = subprocess.run([self.binary, *args], capture_output=True, text=True, check=False,
                                  env={**os.environ, **env} if env else None, timeout=TIMEOUT)
        except FileNotFoundError as err:
            raise errors.ZctlMissing(errors.zctl_missing(self.binary), MISSING) from err
        except subprocess.TimeoutExpired:
            raise errors.ZctlError(f"zctl {args[0] if args else ''} did not answer in {TIMEOUT}s") from None
        if proc.returncode != 0:
            raise refusal((proc.stderr or proc.stdout or "").strip(), proc.returncode)
        return proc.stdout or ""

    def popen(self, *args):
        """zctl as a running process (logs --watch): iterate .stdout."""
        try:
            proc = subprocess.Popen([self.binary, *args], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        except FileNotFoundError as err:
            raise errors.ZctlMissing(errors.zctl_missing(self.binary), MISSING) from err
        proc.stdout = _read_closing(proc.stdout)
        return proc


def _read_closing(pipe):
    """The pipe's lines, closed by the thread that reads it once it ends
    (as jobs/http.py's followed log is): nothing else closes a Popen's pipe."""
    with pipe:
        yield from pipe


def refusal(text, returncode=1):
    if returncode == EXIT_LOGIN:
        return errors.login_needed(text)
    if returncode == EXIT_FORBIDDEN:
        return errors.WorkspaceForbidden(text or "workspace forbidden")
    if returncode == EXIT_JOB_ABSENT:
        return errors.JobNotFound(text)
    if returncode == EXIT_INVALID:
        return errors.ValidationError(text or "invalid zctl invocation")
    return errors.ZctlError(text or f"zctl exited {returncode}", returncode)
