#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Secret.encrypted through zenthros-api where this Python cannot seal (no
cryptography): zctl seals, the API says the keys. Where it can, it seals in
the process: tests/test_seal.py.

What is pinned here: with an API connection, the machine's age keys come from
the API's list of the Workspace's backends and reach zctl as --recipient
(sealing to keys given needs no cluster: TestSecretEncryptToGivenKeysNeedsNoConnection);
the value reaches zctl through its environment, never an argument; what
cannot be sealed is refused before zctl runs; without an API connection
zctl looks the backend up itself, as before.
"""
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from zenthros import errors, remote, seal
from zenthros.view import Backend

from support import FakeAPI, script_fake

VALUE = "aws-session-s3cr3t-n0t-in-any-argument"
KEY_A = "age1" + "a" * 58
KEY_B = "age1" + "b" * 58


def fake_zctl(directory):
    """A zctl that appends what it got (arguments, the plaintext's variable)
    to calls.jsonl and answers as secrets encrypt -o json does, sealed to
    the --recipient keys (or age1fromdestination for --destination)."""
    path = Path(directory) / "zctl"
    record = Path(directory) / "calls.jsonl"
    path.write_text(f"""#!/usr/bin/env python3
import json, os, sys
argv = sys.argv[1:]
plain = os.environ.get(argv[argv.index("--from-env") + 1], "") if "--from-env" in argv else ""
with open({str(record)!r}, "a") as fh:
    fh.write(json.dumps({{"argv": argv, "plain": plain}}) + "\\n")
keys = [argv[i + 1] for i, a in enumerate(argv) if a == "--recipient"] or ["age1fromdestination"]
print(json.dumps({{"type": "encrypted-payload", "encryptedPayload": {{
    "ciphertext": "-----BEGIN AGE ENCRYPTED FILE-----\\nYWdl\\n-----END AGE ENCRYPTED FILE-----", "recipients": keys,
    "keyId": keys[0][4:16]}}, "materialization": {{"mode": "env", "envName": "ZENTHROS_SECRET"}}}}))
""")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return str(path), record


def calls(record):
    return [json.loads(line) for line in record.read_text().splitlines()] if record.exists() else []


class SealingTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.dir = tempfile.mkdtemp()
        self.zctl, self.record = fake_zctl(self.dir)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        self.env = {**clean, "ZENTHROS_CONFIG": os.devnull + ".none", "ZENTHROS_ZCTL": self.zctl}
        self.through_api = {"ZENTHROS_SERVER": self.api.url, "ZENTHROS_TOKEN": "bob-token", "ZENTHROS_WORKSPACE": "risk"}
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [
            {"backendId": "other", "ageRecipients": ["age1" + "o" * 58]},
            {"backendId": "ct-1", "ageRecipients": [KEY_A, KEY_B]},
            {"backendId": "ct-new", "ageRecipients": []}]})

    def environ(self, api=True):
        return mock.patch.dict(os.environ, {**self.env, **(self.through_api if api else {})}, clear=True)

    def run(self, result=None):
        # zctl's path: this Python is taken to have no cryptography.
        with mock.patch.object(seal, "available", return_value=False):
            return super().run(result)

    @script_fake("zctl")
    def test_a_backends_keys_come_from_the_api_and_go_to_zctl_as_recipients(self):
        with self.environ():
            secret = remote.Secret.encrypted(VALUE, on="ct-1")
        (call,) = calls(self.record)
        argv = call["argv"]
        self.assertEqual(argv[:2], ["secrets", "encrypt"])
        self.assertEqual([argv[i + 1] for i, a in enumerate(argv) if a == "--recipient"], [KEY_A, KEY_B])
        self.assertNotIn("--destination", argv)
        self.assertNotIn(VALUE, " ".join(argv))
        self.assertEqual(call["plain"], VALUE, "the value goes through zctl's environment")
        self.assertEqual(secret.payload.recipients, [KEY_A, KEY_B])
        self.assertEqual([r["path"] for r in self.api.requests], ["/v1/workspaces/risk/backends"])

    @script_fake("zctl")
    def test_keys_given_need_no_lookup(self):
        with self.environ():
            remote.Secret.encrypted(VALUE, recipients=[KEY_A])
        self.assertEqual(self.api.requests, [])
        (call,) = calls(self.record)
        self.assertEqual(call["argv"][call["argv"].index("--recipient") + 1], KEY_A)

    def test_what_cannot_be_sealed_is_refused_before_zctl_runs(self):
        for kwargs, said in ((dict(on="gone"), "no machine called that"),
                             (dict(on="ct-new"), "has no key yet")):
            with self.subTest(kwargs), self.environ(), self.assertRaises(errors.ValidationError) as caught:
                remote.Secret.encrypted(VALUE, **kwargs)
            self.assertIn(said, str(caught.exception))
            self.assertNotIn(VALUE, str(caught.exception))
        self.assertEqual(calls(self.record), [])

    @script_fake("zctl")
    def test_without_the_api_zctl_looks_the_backend_up(self):
        # Without the API a name is a pool's: one machine is named by its object.
        with self.environ(api=False):
            remote.Secret.encrypted(VALUE, on=Backend(id="ct-1"))
        (call,) = calls(self.record)
        self.assertEqual(call["argv"][call["argv"].index("--destination") + 1], "ct-1")
        self.assertNotIn("--recipient", call["argv"])
        self.assertEqual(self.api.requests, [])


if __name__ == "__main__":
    unittest.main()
