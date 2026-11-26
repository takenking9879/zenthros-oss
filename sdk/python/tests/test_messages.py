# SPDX-License-Identifier: Apache-2.0

"""The rule of the messages (R9, usability report 2026-10-17): what a
person at a notebook reads is English, one line, ends with what to do next,
and never names the inside of the hub (VMPool, CapacityRequest, backend,
pool, zctl...). The notebook side logs only what asks something of them."""

import ast
import io
import logging
import os
import pathlib
import re
import subprocess
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "zenthros"

INSIDE = re.compile(r"VMPool|CapacityRequest|BackendAccess|\bbackends?\b(?!=)|\bzctl\b|\bpool\b(?!=)|exposedPorts|"
                    r"make -C|spec\.runtime", re.I)
# Reached only through an administrator's kubeconfig (zctl): they speak to
# the person who runs the hub.
ADMIN = {"zctl.py", "infra.py", "view.py", "jobs/client.py"}
ADMIN_MESSAGES = {("remote/options.py", "zctl secrets encrypt -o json: unexpected output")}


def raised_messages():
    """(module, line, text) of every exception the package raises with words."""
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel in ADMIN:
            continue
        source = path.read_text(encoding="utf-8")
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Raise) and node.exc is not None:
                text = " ".join(c.value for c in ast.walk(node.exc) if isinstance(c, ast.Constant) and isinstance(c.value, str))
                if text:
                    yield rel, node.lineno, text


class MessagesTest(unittest.TestCase):
    def test_no_error_a_user_reads_names_the_inside_of_the_hub(self):
        found = [f"{rel}:{line}: {text[:120]!r}" for rel, line, text in raised_messages()
                 if INSIDE.search(text) and not any(rel == r and text.startswith(t) for r, t in ADMIN_MESSAGES)]
        self.assertEqual(found, [])

    def test_the_scan_sees_what_it_must_refuse(self):
        # The pattern is the test: a message from before R9 must match it.
        for old in ("on= says where it runs: a pool's name", "its pool's exposedPorts decide that", "build it with make -C operator sdk-zctl",
                    "Workspace x cannot access VMPool y", "backend vm-1 not ready after 600 s"):
            self.assertRegex(old, INSIDE)

    def test_not_logged_in_says_zenthros_login(self):
        from zenthros import errors

        message = errors.zctl_missing("zctl")
        self.assertTrue(message.startswith("not logged in: run `zenthros login`"), message)
        self.assertNotIn("\n", message)


def _log_lines(env, level, user=True):
    """What a fresh process's zenthros.jobs logger prints at level."""
    code = ("import logging; from zenthros.logging import create_logger; "
            f"log = create_logger('zenthros.jobs', user={user}); log.log({level}, 'Submitted job j-1')")
    done = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env={**os.environ, **env, "PYTHONPATH": str(ROOT)},
                          capture_output=True, text=True, check=True)
    return done.stderr.splitlines()


class NotebookLogTest(unittest.TestCase):
    def setUp(self):
        self.env = {k: v for k, v in os.environ.items() if k != "ZENTHROS_LOG_LEVEL"}

    def test_an_info_line_does_not_reach_the_user(self):
        # "2026-10-17 ... - zenthros.jobs - INFO - Submitted job ..." under
        # every call was the first thing a notebook showed.
        env = {"ZENTHROS_LOG_LEVEL": ""}
        self.assertEqual(_log_lines(env, logging.INFO), [])

    def test_a_warning_is_one_short_line(self):
        self.assertEqual(_log_lines({"ZENTHROS_LOG_LEVEL": ""}, logging.WARNING), ["zenthros: Submitted job j-1"])

    def test_the_level_brings_the_detail_back(self):
        lines = _log_lines({"ZENTHROS_LOG_LEVEL": "info"}, logging.INFO)
        self.assertEqual(len(lines), 1)
        self.assertRegex(lines[0], r" - zenthros\.jobs - INFO - Submitted job j-1$")

    def test_a_job_side_debug_line_shows_only_when_asked(self):
        # "zenthros.core.report - DEBUG - no report endpoint (not in a job)"
        # came out of a notebook that ran a checkpointed function locally.
        self.assertEqual(_log_lines({"ZENTHROS_LOG_LEVEL": ""}, logging.DEBUG, user=False), [])
        self.assertEqual(len(_log_lines({"ZENTHROS_LOG_LEVEL": "debug"}, logging.DEBUG, user=False)), 1)

    def test_a_job_side_logger_keeps_every_line(self):
        # Inside the job the log is the record: checkpoints, rounds.
        from zenthros.logging import create_logger

        logger = create_logger("zenthros.test.jobside")
        stream = io.StringIO()
        logger.handlers[0].setStream(stream)
        logger.info("wrote checkpoint phase=train seq=3")
        self.assertIn(" - INFO - wrote checkpoint", stream.getvalue())


if __name__ == "__main__":
    unittest.main()
