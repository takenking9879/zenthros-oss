# SPDX-License-Identifier: Apache-2.0

"""ExecutionReport snapshots: the SDK keeps the whole payload and always
publishes all of it. There is no merge or patch on the control plane.

A strict Reporter raises outside a job (no report endpoint): the caller
published on purpose. A lenient one logs and drops the snapshot there, so a
trainer or a Task runs the same in a notebook as in a job.
"""

import copy
import json
import platform
import uuid
from datetime import datetime, timezone

from zenthros import errors
from zenthros.core.agent import AgentClient
from zenthros.core.env import JobEnv
from zenthros.logging import create_logger

MAX_PAYLOAD_BYTES = 256 * 1024
API_VERSION = "zenthros.ai/v1"
KIND = "ExecutionReport"

logger = create_logger("zenthros.core.report")


def new_snapshot(payload, artifacts=None, final=False, producer_id=None, job_id=None, report_id=None, env=None):
    env = env or JobEnv.current()
    return {
        "apiVersion": API_VERSION,
        "kind": KIND,
        "job_id": job_id or env.job_id,
        "producer_id": producer_id or env.producer_id or platform.node(),
        "report_id": report_id or str(uuid.uuid4()),
        "produced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payload": payload,
        "artifacts": artifacts or [],
        "final": final,
    }


class Reporter:
    def __init__(self, producer_id=None, strict=True, agent=None, env=None):
        self._payload = {}
        self._artifacts = []
        self._producer_id = producer_id
        self.strict = strict
        self._env = env
        self._agent = agent

    def set(self, key, value):
        self._payload[key] = value
        return self

    def update(self, payload):
        if not isinstance(payload, dict):
            raise errors.InvalidSnapshot("INVALID_JSON", "payload must be a JSON object")
        self._payload.update(payload)
        return self

    def add_artifact(self, uri, name=""):
        self._artifacts.append({"uri": uri, "name": name})
        return self

    def publish(self, final=False):
        """The agent's answer, or None when a lenient reporter is outside a job."""
        payload = copy.deepcopy(self._payload)
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_PAYLOAD_BYTES:
            raise errors.PayloadTooLarge("PAYLOAD_TOO_LARGE", f"payload exceeds {MAX_PAYLOAD_BYTES} bytes")
        env = self._env or JobEnv.current()
        agent = self._agent or AgentClient(env)
        if not agent.endpoint and not self.strict:
            logger.debug(f"no report endpoint (not in a job); report kept local final={final}")
            return None
        snapshot = new_snapshot(payload, artifacts=list(self._artifacts), final=final,
                                producer_id=self._producer_id, env=env)
        return agent.report(snapshot)


def report(payload, final=False, producer_id=None, strict=True):
    """One-shot publish of a complete payload dictionary."""
    return Reporter(producer_id=producer_id, strict=strict).update(payload).publish(final=final)


def progress(payload, final=False, producer_id=None):
    """Publish payload in a job; log and drop it outside one."""
    return report(payload, final=final, producer_id=producer_id, strict=False)
