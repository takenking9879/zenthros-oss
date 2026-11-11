#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Secret.encrypted sealed here, in Python: the value never leaves this
process, and what it writes is what the agent opens (filippo.io/age).

The two sides are pinned on one fixture: sealed with fixed randomness, this
SDK writes operator/pkg/agepayload/testdata/sdk-sealed.age byte for byte,
and operator/pkg/agepayload's TestTheSDKsSealOpensWithAge opens it with
age, as the agent does. Regenerate both with
`PYTHONPATH=. python3 tests/test_seal.py --write-fixture` after a deliberate change.
"""
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from zenthros import errors, seal
from zenthros.remote import options

from support import FakeAPI

FIXTURES = Path(__file__).resolve().parents[3] / "operator" / "pkg" / "agepayload" / "testdata"
PLAINTEXT = "sealed by the zenthros SDK"


def fixed_random(label):
    counter = [0]

    def take(n):
        out = b""
        while len(out) < n:
            out += hashlib.sha256(f"{label}-{counter[0]}".encode()).digest()
            counter[0] += 1
        return out[:n]

    return take


def keypair(label):
    """(identity, recipient) of a test-only X25519 key."""
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

    secret = hashlib.sha256(label.encode()).digest()
    public = X25519PrivateKey.from_private_bytes(secret).public_key().public_bytes_raw()
    return seal.bech32_encode("age-secret-key-", secret).upper(), seal.bech32_encode("age", public)


def fixture():
    """The fixture's text: sealed to another key first, then the identity's,
    so opening it walks past a stanza that is not its own."""
    _, other = keypair("zenthros-seal-fixture-other")
    _, mine = keypair("zenthros-seal-fixture-identity")
    return seal.seal(PLAINTEXT, [other, mine], random=fixed_random("zenthros-seal-fixture"))


@unittest.skipUnless(seal.available(), "pip install 'zenthros[seal]'")
class SealTest(unittest.TestCase):
    def test_writes_the_fixture_age_opens(self):
        identity, recipient = keypair("zenthros-seal-fixture-identity")
        self.assertEqual((FIXTURES / "sdk-identity.txt").read_text().splitlines()[-1], identity)
        self.assertEqual(fixture(), (FIXTURES / "sdk-sealed.age").read_text(),
                         "the SDK's seal changed: TestTheSDKsSealOpensWithAge pins what the agent opens")

    def test_the_armor_and_header_age_reads(self):
        _, a = keypair("a")
        _, b = keypair("b")
        text = seal.seal(b"x" * 100, [a, b, a])
        lines = text.splitlines()
        self.assertEqual((lines[0], lines[-1]), (seal.ARMOR_BEGIN, seal.ARMOR_END))
        self.assertTrue(text.endswith(seal.ARMOR_END + "\n"))
        self.assertTrue(all(len(line) == 64 for line in lines[1:-2]) and 0 < len(lines[-2]) <= 64)
        import base64

        raw = base64.b64decode("".join(lines[1:-1]))
        self.assertTrue(raw.startswith(b"age-encryption.org/v1\n-> X25519 "))
        # One stanza per recipient, a repeated one once.
        self.assertEqual(raw.count(b"\n-> X25519 "), 2)
        self.assertNotIn(b"x" * 10, raw)
        # Fresh randomness each time: the same value never seals the same.
        self.assertNotEqual(seal.seal(b"same", [a]), seal.seal(b"same", [a]))

    def test_what_it_refuses(self):
        _, a = keypair("a")
        bad_checksum = a[:-1] + ("q" if a[-1] != "q" else "p")
        for plaintext, recipients in ((b"", [a]), (b"x" * (seal.MAX_PLAINTEXT + 1), [a]), (b"x", []),
                                      (b"x", [keypair(str(i))[1] for i in range(seal.MAX_RECIPIENTS + 1)]),
                                      (b"x", [bad_checksum]), (b"x", ["age1notakey"]), (b"x", [a.upper()[:5] + a[5:]]),
                                      (b"x", [seal.bech32_encode("age", b"\x01" * 31)]),
                                      (b"x", [seal.bech32_encode("age", bytes(32))])):
            with self.assertRaises(errors.ValidationError, msg=(len(plaintext), recipients[:1])):
                seal.seal(plaintext, recipients)


class SealingAPI(unittest.TestCase):
    """zenthros-api listing backend ct-1 with self.recipient as its key."""

    recipient = "age1zctlsealsthisonenothingopensit"

    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [
            {"backendId": "ct-1", "name": "mi-pc", "healthy": True, "ageRecipients": [self.recipient]}]})
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        patcher = mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.devnull + ".none",
                                               "ZENTHROS_SERVER": self.api.url, "ZENTHROS_TOKEN": "bob-token",
                                               "ZENTHROS_WORKSPACE": "risk"}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)


@unittest.skipUnless(seal.available(), "pip install 'zenthros[seal]'")
class EncryptedSecretTest(SealingAPI):
    """Through zenthros-api, Secret.encrypted(on=) seals here to the
    keys the API lists for that backend: no zctl, and the value is in no
    request."""

    def setUp(self):
        _, self.recipient = keypair("zenthros-seal-fixture-identity")
        super().setUp()

    def test_sealed_here_without_zctl(self):
        zctl = mock.Mock(side_effect=AssertionError("zctl was called"))
        secret = options.Secret.encrypted("s3cr3t-value", on="ct-1", zctl=zctl)
        payload = secret.payload
        self.assertEqual(payload.recipients, [self.recipient])
        self.assertEqual(payload.key_id, seal.key_id(self.recipient))
        self.assertTrue(payload.ciphertext.startswith(seal.ARMOR_BEGIN))
        for request in self.api.requests:
            self.assertNotIn(b"s3cr3t-value", request["body"])
        self.assertEqual(secret.source("API_KEY").encrypted_payload.ciphertext, payload.ciphertext)

    def test_from_env_and_from_file(self):
        with mock.patch.dict(os.environ, {"MY_TOKEN": "t0k"}):
            self.assertTrue(options.Secret.encrypted(from_env="MY_TOKEN", on="ct-1").payload.ciphertext)
        with tempfile.TemporaryDirectory() as scratch:
            path = os.path.join(scratch, "token.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("from a file")
            self.assertTrue(options.Secret.encrypted(from_file=path, on="ct-1").payload.ciphertext)
        with self.assertRaises(errors.ValidationError) as caught:
            options.Secret.encrypted(from_env="NOT_SET_ANYWHERE_42", on="ct-1")
        self.assertIn("NOT_SET_ANYWHERE_42", str(caught.exception))


class ZctlSealTest(SealingAPI):
    """Without cryptography (a minimal install), zctl seals."""

    def test_without_cryptography_zctl_seals(self):
        zctl = mock.Mock(return_value=json.dumps({"encryptedPayload": {"ciphertext": "c", "keyId": "k",
                                                                       "recipients": [self.recipient]}}))
        with mock.patch.object(seal, "available", return_value=False):
            secret = options.Secret.encrypted("v", on="ct-1", zctl=zctl)
        self.assertEqual(secret.payload.ciphertext, "c")
        args = zctl.call_args.args
        self.assertIn("--recipient", args)
        self.assertNotIn("v", args, "the value went in zctl's arguments")


if __name__ == "__main__":
    if sys.argv[1:] == ["--write-fixture"]:
        identity, _ = keypair("zenthros-seal-fixture-identity")
        (FIXTURES / "sdk-identity.txt").write_text(
            "# A test-only age identity: it opens sdk-sealed.age and nothing else.\n" + identity + "\n")
        (FIXTURES / "sdk-sealed.age").write_text(fixture())
        print(f"wrote {FIXTURES}")
    else:
        unittest.main()
