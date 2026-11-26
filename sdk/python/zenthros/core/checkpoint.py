# SPDX-License-Identifier: Apache-2.0

"""Checkpoints: write state to object storage, have the controller ack it,
resume from the acked URI after the machine is replaced.

Bytes never go through the controller, and resume never lists a bucket: the
replacement is handed the acked cursor in its env (JobEnv.cursors).

Three pieces, shared by everything that checkpoints:

- CheckpointPolicy says when: every N steps, and at once on the machine's
  interruption notice.
- Checkpointer holds one phase's cursor: its resumed state, save(), complete().
- @phase is the function form; zenthros.Task is the class form.

A checkpoint is one JSON object, the state, plus the files any library
saves (a booster, torch weights, a pickle): each its own object next to it,
written before it, with its SHA-256 in it. The ack names the JSON only.
"""

import hashlib
import json
import os
import re
import tempfile
import time
from functools import wraps

from zenthros import errors
from zenthros.core import preemption
from zenthros.core.agent import AgentClient
from zenthros.core.env import Cursor, JobEnv
from zenthros.core.storage import store_for
from zenthros.logging import create_logger

logger = create_logger("zenthros.core.checkpoint")

KIND = "Checkpoint"
_ACK_ATTEMPTS = 8


def object_key(job_id, generation, phase, seq):
    generation = int(generation) if generation else 1
    return f"jobs/{job_id}/gen/{generation}/{phase}/{seq}"


def checkpoint_uri(prefix, job_id, generation, phase, seq):
    return f"{str(prefix).rstrip('/')}/{object_key(job_id, generation, phase, seq)}"


# object_key, and the files write_files puts next to it. A phase is any name.
_OBJECT = re.compile(r"(children/[^/]+/\d+/)*jobs/[^/]+/gen/\d+/.+/\d+(\.files/[^/]+)?")


def is_object(key):
    """Whether key, a path under the checkpoint prefix, is a checkpoint or one
    of its files, and not something else a job put there. A fan-out's child
    has its prefix under its parent's."""
    return _OBJECT.fullmatch(key) is not None


def sha256_hex(payload):
    return hashlib.sha256(payload).hexdigest()


def write_blob(uri, state, store=None, files=None):
    """The checkpoint's JSON at uri. files ({name: {uri, sha256}}) are
    already stored; state must be JSON (a model goes in files)."""
    body = {"kind": KIND, "state": state}
    if files:
        body["files"] = files
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    (store or store_for(uri)).put(uri, payload)
    return payload, sha256_hex(payload)


def load_state(uri, store=None, expected_sha256=""):
    return _load(uri, store, expected_sha256).get("state")


def _load(uri, store, expected_sha256):
    payload = (store or store_for(uri)).get(uri)
    if expected_sha256 and sha256_hex(payload) != expected_sha256:
        raise errors.CheckpointError("CHECKPOINT_REJECTED", "sha256 mismatch for resume URI")
    return json.loads(payload.decode("utf-8"))


def write_files(uri, files, store):
    """files ({name: bytes or a local path}) next to the checkpoint at uri,
    as <uri>.files/<name>: {name: {uri, sha256}}."""
    written = {}
    for name, source in files.items():
        if not name or "/" in name or name in (".", ".."):
            raise errors.ValidationError(f"checkpoint file name {name!r}: one plain file name, no '/'")
        target = f"{uri}.files/{name}"
        if isinstance(source, (bytes, bytearray, memoryview)):
            store.put(target, bytes(source))
            digest = sha256_hex(bytes(source))
        else:
            store.put_file(target, source)
            digest = _file_sha256(source)
        written[name] = {"uri": target, "sha256": digest}
    return written


def _file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class CheckpointPolicy:
    """When to save: every `every` steps (None: never on a cadence), and once
    when the interruption notice appears (on_notice). due() asks for the
    notice on every step, so it is taken on the step it appears."""

    def __init__(self, every=None, on_notice=True):
        self.every = max(1, int(every)) if every else None
        self.on_notice = on_notice

    def due(self, ckpt, step, total=None):
        """Why step (0-based) should be saved: "notice", "last", "every", or None."""
        notice = self.on_notice and ckpt.notice()
        if notice:
            return "notice"
        finished = int(step) + 1
        if total and finished >= int(total):
            return "last"
        if self.every and finished % self.every == 0:
            return "every"
        return None


class Checkpointer:
    """One phase's checkpoint cursor.

    Outside a job (no job id and no prefix) save() writes nothing and only
    keeps the state, so the same code runs in a notebook.
    """

    def __init__(self, name, store=None, agent=None, env=None):
        env = env or JobEnv.current()
        self.phase = name
        self.env = env
        self.job_id = env.job_id
        self.generation = env.generation or 1
        self.prefix = env.prefix
        self.store = store
        self._agent = agent
        self.seq = 0
        self.state = None
        self.files = {}
        self.completed = False
        self._notice_seen = False
        cursor = env.cursor(name)
        mine = _ACKED.get(self._key())
        if mine is not None and (cursor is None or mine.seq > cursor.seq):
            cursor = mine
        if cursor is not None:
            self.seq = cursor.seq
            self.completed = cursor.complete
            body = _load(cursor.uri, self._store(cursor.uri), cursor.sha256)
            self.state, self.files = body.get("state"), body.get("files") or {}
            logger.info(f"resumed phase={name} seq={self.seq} uri={cursor.uri}")

    @property
    def active(self):
        """False outside a job: save() writes nothing there."""
        return bool(self.job_id or self.prefix)

    @property
    def configured(self):
        return bool(self.job_id and self.prefix)

    def notice(self):
        """True once, when this machine's interruption notice first appears.

        Save then: the replacement is already being placed and resumes from
        the last acked checkpoint. Loops ask on every step.
        """
        if self._notice_seen:
            return False
        if preemption.requested(self.env):
            self._notice_seen = True
            logger.info(f"interruption notice: checkpointing phase={self.phase} now")
            return True
        return False

    def file(self, name):
        """The bytes of a file the resumed checkpoint holds, checked against
        the SHA-256 it was saved with."""
        entry = self.files[name]
        raw = self._store(entry["uri"]).get(entry["uri"])
        if sha256_hex(raw) != entry["sha256"]:
            raise errors.CheckpointError("CHECKPOINT_REJECTED", f"sha256 mismatch for checkpoint file {name}")
        return raw

    def file_path(self, name, path=None):
        """A file the resumed checkpoint holds, downloaded to path (default: a
        new temporary directory), checked against its SHA-256."""
        entry = self.files[name]
        path = path or os.path.join(tempfile.mkdtemp(prefix="zenthros-checkpoint-"), name)
        self._store(entry["uri"]).get_file(entry["uri"], path)
        if _file_sha256(path) != entry["sha256"]:
            os.remove(path)
            raise errors.CheckpointError("CHECKPOINT_REJECTED", f"sha256 mismatch for checkpoint file {name}")
        return path

    def save(self, state, complete=False, files=None):
        """Write files ({name: bytes or a local path}), then state, under
        jobs/<uid>/gen/<generation>/, and wait for the controller's ack: a
        cursor never names a file that is not there yet. A file saved before
        stays in later checkpoints until one saves that name again."""
        if not self.active:
            self.seq += 1
            self.state = state
            self.completed = complete
            logger.info(f"skipping checkpoint write phase={self.phase} seq={self.seq} (job env unset)")
            return {"uri": "", "seq": self.seq, "sha256": "", "receipt": None}
        if not self.job_id:
            raise errors.CheckpointError("REJECTED", "ZENTHROS_JOB_ID is not set")
        if not self.prefix:
            raise errors.CheckpointError("REJECTED", "ZENTHROS_CHECKPOINT_PREFIX is not set")
        self.seq += 1
        self.state = state
        uri = checkpoint_uri(self.prefix, self.job_id, self.generation, self.phase, self.seq)
        store = self._store(uri)
        self.files = {**self.files, **write_files(uri, files or {}, store)}
        _, digest = write_blob(uri, state, store=store, files=self.files)
        logger.info(f"wrote checkpoint phase={self.phase} seq={self.seq} uri={uri}")
        commit = {
            "job_id": self.job_id,
            "generation": self.generation,
            "phase": self.phase,
            "seq": self.seq,
            "uri": uri,
            "sha256": digest,
            "complete": complete,
        }
        receipt = self._ack(commit)
        self.completed = complete
        _ACKED[self._key()] = Cursor(self.seq, uri, digest, complete)
        return {"uri": uri, "seq": self.seq, "sha256": digest, "receipt": receipt}

    def complete(self, state=None, files=None):
        if state is None:
            state = self.state if self.state is not None else {"complete": True}
        return self.save(state, complete=True, files=files)

    def _store(self, uri):
        return self.store or store_for(uri, self.env)

    def _key(self):
        return (self.job_id, self.generation, self.prefix, self.phase, id(self.store) if self.store else None)

    def _ack(self, commit):
        agent = self._agent or AgentClient(self.env)
        last_err = None
        for attempt in range(_ACK_ATTEMPTS):
            try:
                return agent.ack_checkpoint(commit)
            except errors.CheckpointFrozen as err:
                # The replacement already took its cursor: no retry lands.
                raise errors.CheckpointFrozen(
                    "CHECKPOINT_FROZEN", f"phase {self.phase} seq {self.seq} was not acked: {err}") from err
            except errors.CheckpointError as err:
                # By the controller's code, never its words: a lease being
                # handed over may still take the commit.
                if err.code != "LEASE_NOT_LIVE":
                    raise
                last_err = err
                time.sleep(0.2 * (attempt + 1))
        # The controller kept refusing: this run's lease is over and the blob
        # it just wrote will never be resumed from. Say so; a caller that goes
        # on working would lose the work silently.
        raise errors.CheckpointFrozen(
            "CHECKPOINT_FROZEN", f"phase {self.phase} seq {self.seq} was not acked: {last_err}") from last_err


# The last cursor this process had acked, per phase: a second Checkpointer
# for a phase in the same process (a retry in the runner, a Task run again,
# code that peeks then trains) continues from it, not from the cursor the
# job started with, which the controller has since moved past.
_ACKED = {}


def forget_acked():
    """A new job in this process (tests): drop the cursors acked so far."""
    _ACKED.clear()


def phase(name, resume_done=None):
    """Mark a resumable phase: fn(ckpt, *args, **kwargs) gets its Checkpointer.

    A completed phase returns its saved state without running.
    resume_done(state) narrows that: a checkpoint that only saved rounds is
    not done, and the replacement still has to score.
    """

    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            ckpt = Checkpointer(name)
            if ckpt.completed and (resume_done is None or resume_done(ckpt.state)):
                logger.info(f"skipping completed phase {name} at seq={ckpt.seq}")
                return ckpt.state
            if ckpt.completed:
                logger.info(f"phase {name} checkpoint is not done; running again")
                ckpt.completed = False
            result = fn(ckpt, *args, **kwargs)
            if not ckpt.completed and ckpt.configured:
                ckpt.complete(ckpt.state if ckpt.state is not None else result)
            return result

        wrapped.phase_name = name
        return wrapped

    return decorator
