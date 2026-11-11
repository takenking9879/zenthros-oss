# SPDX-License-Identifier: Apache-2.0

"""Has this machine been told it is being taken back?

The agent writes the provider's interruption notice to $ZENTHROS_PREEMPT_FILE
(JSON: source, action, deadline, notedAt) before anything else happens, so any
language can poll the file; this module only reads it. After a notice, a
checkpoint committed before the replacement starts is the one it resumes from.
"""

import json

from zenthros.core.env import JobEnv


def notice(env=None):
    """The notice as a dict, or None while there is none."""
    path = (env or JobEnv.current()).preempt_file
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as f:
            raw = f.read()
    except OSError:
        return None
    try:
        body = json.loads(raw)
    except ValueError:
        body = {}
    return body if isinstance(body, dict) else {}


def requested(env=None):
    """True once the machine has an interruption notice."""
    return notice(env) is not None
