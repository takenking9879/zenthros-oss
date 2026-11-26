#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""zenthros.aws: an hour of S3 credentials for the prefixes a job uses,
sealed to the machine that reads them.

What is pinned here: the session policy names only the places given, lists
each bucket only under its own prefixes, and writes only when asked; STS is asked for no longer than allowed; the
three values become sealed secrets, never plain env, and none of them is
in what the job's spec carries.
"""
import json
import os
import tempfile
import unittest
from unittest import mock

from zenthros import aws, errors, seal
from zenthros.jobs import spec as S

from support import script_fake
from test_sealing import KEY_A, calls, fake_zctl

CREDS = {"AccessKeyId": "ASIAFAKEKEYID000", "SecretAccessKey": "s3cr3t-access-key-n0t-in-spec",
         "SessionToken": "FwoGZXIvYXdzEFAKEsessiontoken", "Expiration": "2026-10-13T13:00:00Z"}


class FakeSTS:
    def __init__(self):
        self.calls = []

    def get_federation_token(self, **kwargs):
        self.calls.append(("get_federation_token", kwargs))
        return {"Credentials": dict(CREDS)}

    def assume_role(self, **kwargs):
        self.calls.append(("assume_role", kwargs))
        return {"Credentials": dict(CREDS)}


class ReachTest(unittest.TestCase):
    def test_after_import_zenthros(self):
        import zenthros

        self.assertIs(zenthros.aws.s3_secrets, aws.s3_secrets)


class PolicyTest(unittest.TestCase):
    def objects(self, policy):
        return [s for s in policy["Statement"] if "s3:GetObject" in s["Action"]]

    def listings(self, policy):
        return {s["Resource"]: s for s in policy["Statement"] if s["Action"] == ["s3:ListBucket"]}

    def test_reads_one_prefix_of_one_bucket(self):
        policy = aws.s3_policy("s3://acme-data/pipeline/run-7")
        (objects,) = self.objects(policy)
        self.assertEqual(objects["Action"], ["s3:GetObject"])
        self.assertEqual(objects["Resource"], ["arn:aws:s3:::acme-data/pipeline/run-7/*"])
        listing = self.listings(policy)["arn:aws:s3:::acme-data"]
        self.assertEqual(listing["Condition"], {"StringLike": {"s3:prefix": ["pipeline/run-7/*", "pipeline/run-7/"]}})
        self.assertEqual(len(policy["Statement"]), 2)
        self.assertTrue(all(s["Effect"] == "Allow" for s in policy["Statement"]))

    def test_writes_only_when_asked(self):
        (objects,) = self.objects(aws.s3_policy("s3://acme-data/out/", write=True))
        self.assertEqual(sorted(objects["Action"]),
                         ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:PutObject"])

    def test_several_places_each_listed_only_under_its_own_prefixes(self):
        # The pipeline's data, and the Workspace's call store the job reads
        # its call from and writes its result to.
        policy = aws.s3_policy("s3://acme-data/pipeline/", "s3://zenthros-remote/api/risk", "s3://acme-data/ref/")
        (objects,) = self.objects(policy)
        self.assertEqual(objects["Resource"], ["arn:aws:s3:::acme-data/pipeline/*", "arn:aws:s3:::zenthros-remote/api/risk/*",
                                               "arn:aws:s3:::acme-data/ref/*"])
        listings = self.listings(policy)
        self.assertEqual(set(listings), {"arn:aws:s3:::acme-data", "arn:aws:s3:::zenthros-remote"})
        self.assertEqual(listings["arn:aws:s3:::acme-data"]["Condition"]["StringLike"]["s3:prefix"],
                         ["pipeline/*", "pipeline/", "ref/*", "ref/"])
        self.assertEqual(listings["arn:aws:s3:::zenthros-remote"]["Condition"]["StringLike"]["s3:prefix"],
                         ["api/risk/*", "api/risk/"])

    def test_the_whole_bucket_only_by_saying_so(self):
        policy = aws.s3_policy("s3://acme-data/", whole_bucket=True)
        self.assertEqual(self.objects(policy)[0]["Resource"], ["arn:aws:s3:::acme-data/*"])
        self.assertNotIn("Condition", self.listings(policy)["arn:aws:s3:::acme-data"])
        for uri in ("s3://acme-data", "s3://acme-data/"):
            with self.subTest(uri), self.assertRaises(errors.ValidationError):
                aws.s3_policy(uri)

    def test_places_that_would_widen_it_are_refused(self):
        for uri in ("s3://acme*/x", "s3:///x", "s3://acme-data/x*", "s3://acme-data/a?b", "s3://acme-data//abs",
                    "acme-data/x", "gs://acme-data/x", "s3://Acme/x"):
            with self.subTest(uri), self.assertRaises(errors.ValidationError):
                aws.s3_policy(uri)
        with self.assertRaises(errors.ValidationError):
            aws.s3_policy()


class CredentialsTest(unittest.TestCase):
    def test_federation_token_with_the_policy_for_an_hour(self):
        sts = FakeSTS()
        env = aws.s3_credentials("s3://acme-data/pipeline/", sts=sts)
        self.assertEqual(env, {"AWS_ACCESS_KEY_ID": CREDS["AccessKeyId"],
                               "AWS_SECRET_ACCESS_KEY": CREDS["SecretAccessKey"],
                               "AWS_SESSION_TOKEN": CREDS["SessionToken"]})
        (op, kwargs), = sts.calls
        self.assertEqual(op, "get_federation_token")
        self.assertEqual(kwargs["DurationSeconds"], 3600)
        self.assertEqual(json.loads(kwargs["Policy"]), aws.s3_policy("s3://acme-data/pipeline/"))
        self.assertRegex(kwargs["Name"], r"^[\w+=,.@-]{2,32}$")

    def test_a_role_is_assumed_with_the_same_policy(self):
        sts = FakeSTS()
        aws.s3_credentials("s3://acme-data/pipeline/", write=True, seconds=900,
                           role_arn="arn:aws:iam::123456789012:role/notebook-s3", sts=sts)
        (op, kwargs), = sts.calls
        self.assertEqual(op, "assume_role")
        self.assertEqual(kwargs["RoleArn"], "arn:aws:iam::123456789012:role/notebook-s3")
        self.assertEqual(kwargs["DurationSeconds"], 900)
        self.assertEqual(json.loads(kwargs["Policy"]), aws.s3_policy("s3://acme-data/pipeline/", write=True))
        self.assertRegex(kwargs["RoleSessionName"], r"^[\w+=,.@-]{2,64}$")

    def test_lifetimes_sts_would_refuse_are_refused_before_asking(self):
        for seconds in (899, 43201, 0):
            sts = FakeSTS()
            with self.subTest(seconds), self.assertRaises(errors.ValidationError):
                aws.s3_credentials("s3://acme-data/p/", seconds=seconds, sts=sts)
            self.assertEqual(sts.calls, [])


class SealedTest(unittest.TestCase):
    def run(self, result=None):
        # zctl's path: this Python is taken to have no cryptography
        # (tests/test_seal.py seals in the process).
        with mock.patch.object(seal, "available", return_value=False):
            return super().run(result)

    @script_fake("zctl")
    def test_three_values_each_sealed_by_zctl_and_none_in_an_argument(self):
        directory = tempfile.mkdtemp()
        zctl, record = fake_zctl(directory)
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        with mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.devnull + ".none", "ZENTHROS_ZCTL": zctl},
                             clear=True):
            secrets = aws.s3_secrets("s3://acme-data/pipeline/", recipients=[KEY_A], sts=FakeSTS())
        self.assertEqual(set(secrets), {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"})
        seen = calls(record)
        self.assertEqual(sorted(call["plain"] for call in seen),
                         sorted([CREDS["AccessKeyId"], CREDS["SecretAccessKey"], CREDS["SessionToken"]]))
        spec = json.dumps([S.to_plain(secret.source(name)) for name, secret in secrets.items()])
        for value in (CREDS["SecretAccessKey"], CREDS["SessionToken"], CREDS["AccessKeyId"]):
            self.assertNotIn(value, spec)
            self.assertNotIn(value, json.dumps([call["argv"] for call in seen]))
        for secret in secrets.values():
            self.assertEqual((secret.type, secret.payload.recipients), ("encrypted-payload", [KEY_A]))

    def test_one_destination(self):
        for kwargs in (dict(), dict(on="ct-1", recipients=["age1x"])):
            with self.subTest(kwargs), self.assertRaises(errors.ValidationError):
                aws.s3_secrets("s3://acme-data/p/", sts=FakeSTS(), **kwargs)


if __name__ == "__main__":
    unittest.main()
