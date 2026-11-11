# SPDX-License-Identifier: Apache-2.0

"""What the agent tells a job about itself, read in one place.

The agent sets these variables on every job it runs. Outside a job (a
notebook, a test, a zenthros.run call run locally) they are unset:
checkpoints and reports are then skipped, so the same code runs everywhere.
"""

import json
import os
from dataclasses import dataclass, field

# Every ZENTHROS_* variable a job reads about itself. test_layers checks no
# other module reads one of these directly.
JOB_VARS = (
    "ZENTHROS_JOB_ID",
    "ZENTHROS_GENERATION",
    "ZENTHROS_CHECKPOINT_PREFIX",
    "ZENTHROS_DATA_OUTPUT",
    "ZENTHROS_DATA_INPUT",
    "ZENTHROS_REPORT_ENDPOINT",
    "ZENTHROS_REPORT_TOKEN",
    "ZENTHROS_PRODUCER_ID",
    "ZENTHROS_PREEMPT_FILE",
    "ZENTHROS_CHECKPOINT_CURSORS",
    "ZENTHROS_CHECKPOINT_PHASE",
    "ZENTHROS_CHECKPOINT_URI",
    "ZENTHROS_CHECKPOINT_SEQ",
    "ZENTHROS_CHECKPOINT_SHA256",
    "ZENTHROS_CHECKPOINT_COMPLETE",
    "ZENTHROS_CHECKPOINT_LOCAL_ROOT",
    "ZENTHROS_PARENT_JOB_ID",
    "ZENTHROS_MAP_ITEMS",
    "TASK_INDEX",
    "TASK_COUNT",
)


def _get(environ, name, default=""):
    value = environ.get(name)
    return default if value is None or value == "" else value


def _int(raw, default=0):
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Cursor:
    """The controller-acked checkpoint of one phase."""

    seq: int
    uri: str
    sha256: str = ""
    complete: bool = False


@dataclass(frozen=True)
class JobEnv:
    job_id: str = ""
    generation: int = 0
    checkpoint_prefix: str = ""
    data_output: str = ""
    data_input: str = ""
    endpoint: str = ""
    token: str = ""
    producer_id: str = ""
    preempt_file: str = ""
    local_root: str = ""
    parent_job_id: str = ""
    map_items: str = ""
    task_index: int = -1
    task_count: int = 0
    cursors: dict = field(default_factory=dict)

    @classmethod
    def current(cls, environ=None):
        """Read the process env now. Not cached: tests and the runner change it."""
        e = os.environ if environ is None else environ
        return cls(
            job_id=_get(e, "ZENTHROS_JOB_ID"),
            generation=max(_int(_get(e, "ZENTHROS_GENERATION", "0")), 0),
            checkpoint_prefix=_get(e, "ZENTHROS_CHECKPOINT_PREFIX"),
            data_output=_get(e, "ZENTHROS_DATA_OUTPUT"),
            data_input=_get(e, "ZENTHROS_DATA_INPUT"),
            endpoint=_get(e, "ZENTHROS_REPORT_ENDPOINT"),
            token=_get(e, "ZENTHROS_REPORT_TOKEN"),
            producer_id=_get(e, "ZENTHROS_PRODUCER_ID"),
            preempt_file=_get(e, "ZENTHROS_PREEMPT_FILE"),
            local_root=_get(e, "ZENTHROS_CHECKPOINT_LOCAL_ROOT"),
            parent_job_id=_get(e, "ZENTHROS_PARENT_JOB_ID"),
            map_items=_get(e, "ZENTHROS_MAP_ITEMS"),
            task_index=_int(_get(e, "TASK_INDEX", "-1"), -1),
            task_count=_int(_get(e, "TASK_COUNT", "0")),
            cursors=_cursors(e),
        )

    @property
    def prefix(self):
        """Where checkpoints go: the checkpoint prefix, else the job's output."""
        return self.checkpoint_prefix or self.data_output

    @property
    def in_job(self):
        """True inside a Zenthros job: checkpoints are written and acked."""
        return bool(self.job_id and self.prefix)

    def cursor(self, phase):
        """The acked cursor of phase, or None."""
        return self.cursors.get(phase)


def _cursors(e):
    """ZENTHROS_CHECKPOINT_CURSORS carries every phase the job checkpointed
    ({"<phase>": {"seq", "uri", "sha256", "complete"}}), so a resumed job
    skips all phases it completed; the single-phase ZENTHROS_CHECKPOINT_*
    variables are its latest cursor."""
    out = {}
    raw = _get(e, "ZENTHROS_CHECKPOINT_CURSORS")
    if raw:
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = None
            from zenthros.logging import create_logger

            create_logger("zenthros.core.env").warning(
                "ZENTHROS_CHECKPOINT_CURSORS is not JSON; using the latest cursor only")
        if isinstance(parsed, dict):
            for name, cursor in parsed.items():
                if isinstance(cursor, dict) and cursor.get("uri"):
                    out[name] = Cursor(
                        seq=_int(cursor.get("seq") or 0),
                        uri=cursor["uri"],
                        sha256=cursor.get("sha256", "") or "",
                        complete=bool(cursor.get("complete")),
                    )
    latest = _get(e, "ZENTHROS_CHECKPOINT_PHASE")
    uri = _get(e, "ZENTHROS_CHECKPOINT_URI")
    if latest and uri and latest not in out:
        out[latest] = Cursor(
            seq=_int(_get(e, "ZENTHROS_CHECKPOINT_SEQ", "0")),
            uri=uri,
            sha256=_get(e, "ZENTHROS_CHECKPOINT_SHA256"),
            complete=_get(e, "ZENTHROS_CHECKPOINT_COMPLETE") == "true",
        )
    return out
