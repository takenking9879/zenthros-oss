# SPDX-License-Identifier: Apache-2.0

"""Every error the SDK raises is a ZenthrosError, and also the built-in
exception it is (a ValidationError is a ValueError, JobNotFound a
LookupError): `except zenthros.ZenthrosError` catches what Zenthros said,
and code that catches the built-in ones keeps working."""

import contextlib
import sys


class ZenthrosError(Exception):
    """What the SDK raises."""


class ReportError(ZenthrosError):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class PayloadTooLarge(ReportError):
    pass


class Unauthorized(ReportError):
    pass


class InvalidSnapshot(ReportError):
    pass


class CheckpointError(ReportError):
    pass


class CheckpointFrozen(CheckpointError):
    """The job's cursor is frozen: its lease ended (a replacement was
    submitted, or the worker was declared lost) and no further commit from
    this run is accepted. Stop writing; the replacement resumes from the last
    acked checkpoint."""


class StoppedOnNotice(ZenthrosError):
    """A Task with stop_on_notice saved its step at the interruption notice and
    stopped: the replacement resumes from that save and returns the result."""


class SpawnRefused(ReportError):
    """The controller refused a spawn; nothing was created. code and
    message are its words (SPAWN_QUOTA_EXCEEDED, LEASE_NOT_LIVE, ...)."""


def transient_status(status):
    """An HTTP error answer asked again: a timeout, a rate limit, any 5xx.
    The SDK's one table, the operator's too (fault.TransientHTTP;
    operator/pkg/jobcli/testdata/wire.json)."""
    return status in (408, 429) or status >= 500


def from_code(code, message):
    mapping = {
        "PAYLOAD_TOO_LARGE": PayloadTooLarge,
        "UNAUTHORIZED": Unauthorized,
        "INVALID_SNAPSHOT": InvalidSnapshot,
        "INVALID_JSON": InvalidSnapshot,
        "INVALID_ARTIFACT_URI": InvalidSnapshot,
        "CHECKPOINT_REJECTED": CheckpointError,
        "CHECKPOINT_FROZEN": CheckpointFrozen,
    }
    cls = mapping.get(code, ReportError)
    return cls(code, message)


def zctl_missing(path):
    """The message for a zctl binary that is not there."""
    return (f"not logged in: run `zenthros login` (an administrator's kubeconfig needs zctl instead, "
            f"and there is none at {path}: set ZENTHROS_ZCTL)")


# Orchestration / train / data errors named in agent-sdk.md.
class ValidationError(ZenthrosError, ValueError):
    """Bad spec. Callers must not retry the same payload."""


class JobFailed(ZenthrosError, RuntimeError):
    """The job failed. str() is the job's status.message, word for word:
    what the backend or the operator said, never a summary of it."""

    def __init__(self, message, reasons=None, status=None):
        super().__init__(message)
        self.message = message
        self.reasons = list(reasons or [])
        self.status = status

    @property
    def reason(self):
        """The first reason code (IMAGE_PULL_FAILED, WORKER_LOST, ...), or ""."""
        first = self.reasons[0] if self.reasons else {}
        return str(first.get("code") or "") if isinstance(first, dict) else str(first)


class JobCancelled(ZenthrosError, RuntimeError):
    """Job was cancelled or wait() timed out into cancel."""


class WaitTimeout(ZenthrosError, TimeoutError):
    """wait() ran out of time; the job still runs unless cancel_on_timeout."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class NotFound(ZenthrosError, LookupError):
    """Something named is not there: a secret, a Workspace the hub does not
    route, a route this hub does not serve. A job is JobNotFound."""


class JobNotFound(NotFound):
    """The job does not exist: never created, deleted, or collected after
    its TTL (1 h by default). Not a TransientError: waiting does not bring
    it back."""


class WorkspaceForbidden(ZenthrosError, PermissionError):
    """Caller is not authorized for this Workspace (T9A)."""


class Conflict(ZenthrosError, RuntimeError):
    """The name is taken, or a limit is reached (zenthros-api 409). Not a
    TransientError: the same call fails the same way until something else
    changes (another name, a machine deleted)."""

    def __init__(self, message):
        super().__init__(message)
        self.status = 409


class ZctlMissing(ValidationError):
    """There is no zctl to run. Not a TransientError: waiting does not
    install it."""

    def __init__(self, message, returncode=127):
        super().__init__(message)
        self.message = message
        self.returncode = returncode


class EngineMissing(ValidationError):
    """No container engine here to run a local job in: waiting does not
    install one. `zenthros run` exits 125 on it."""


class BackendJoinError(ZenthrosError, RuntimeError):
    """A container could not be joined as a backend: docker or the agent's
    install or join said why (the token never in the message)."""


class Refused(ZenthrosError, RuntimeError):
    """zenthros-api answered and turned the request down: asked again, it
    gives the same answer. status is its HTTP code."""


class WireMismatch(Refused):
    """The hub speaks another format of zenthros-api than this SDK (426):
    its message says which side is older and what to install."""

    status = 426


class TransientError(ZenthrosError, RuntimeError):
    """Store/network/CLI failure. Retry inside the SDK, not the LLM."""


class AgentUnanswered(ReportError, TransientError):
    """The agent gave only transient answers for a call's 45 s, or was never
    reached (restarting, its controller or Hub away): what was sent may have
    been kept, so it is no refusal of the route. Sent again it is the same
    call (a report and a commit by their seq, a spawn by its name)."""


class ZctlError(TransientError):
    """zctl refused or is missing: its stderr, whole, as the message."""

    def __init__(self, message, returncode=1):
        super().__init__(message)
        self.message = message
        self.returncode = returncode


class LoginRequired(ZenthrosError, PermissionError):
    """There is no session to take a token from: none was started, the
    issuer refused it, or it ended and cannot be renewed. zenthros.login()
    starts one."""


def login_expired(message):
    """LoginRequired for a credential the server no longer takes (401): a
    key from `zctl user add` lasts 720 h by default."""
    return LoginRequired(f"{message}: the token expired or was revoked. "
                         "zenthros.login() starts a new session; an API key is renewed by an admin "
                         "with `zctl user token`")


def login_needed(message):
    """LoginRequired for zctl's exit 67: no credential, or one refused.
    zctl's message says which; the SDK adds only how to get one."""
    return LoginRequired(f"{message}: zenthros.login() starts a session; an API key comes from an admin "
                         "(`zctl user add`, renewed with `zctl user token`)")


@contextlib.contextmanager
def undone_on_failure(*undo):
    """The block; if it raises, each (what, step) of undo runs in order
    before the error goes on, so a failure leaves no machine behind. A step
    that fails is said, not raised: the block's error is the one to see."""
    try:
        yield
    except BaseException:
        for what, step in undo:
            try:
                step()
            except Exception as exc:  # noqa: BLE001 - the block's error is raised
                print(f"zenthros: could not {what}: {exc}", file=sys.stderr)
        raise
