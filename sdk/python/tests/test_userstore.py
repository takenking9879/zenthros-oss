#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""Your own bucket (zenthros store s3://...): a hub that keeps none of your
data.

Pinned: the bucket is kept with your login only once it answers (a probe
written, read and removed), with its region; a call to one of your machines
goes to your bucket with your credentials, never the hub's signed URLs; the
machine gets credentials for the bucket's prefix only (federated from
long-term keys, or the temporary ones you hold), sealed to its own keys,
never in clear in the spec; the hub is not asked for store credentials;
credentials you pass yourself are left alone.
"""
import base64
import io
import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

from zenthros import config, errors, seal, userstore
from zenthros.core.storage import GCSBlobStore
from zenthros.jobs.http import HTTPJobClient, PresignedBlobStore
from zenthros.remote import options

from support import FakeAPI, assert_mode
from test_http_client import double_it
from test_seal import keypair


class FakeS3:
    def __init__(self, cloud):
        self.cloud = cloud

    def get_bucket_location(self, Bucket):
        self.cloud.calls.append(("location", Bucket))
        if Bucket in self.cloud.denied:
            raise _ClientError("AccessDenied", "Access Denied")
        return {"LocationConstraint": "us-east-2"}

    def put_object(self, Bucket, Key, Body):
        self.cloud.calls.append(("put", Bucket, Key))
        self.cloud.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        self.cloud.calls.append(("get", Bucket, Key))
        return {"Body": io.BytesIO(self.cloud.objects[(Bucket, Key)])}

    def delete_object(self, Bucket, Key):
        self.cloud.calls.append(("delete", Bucket, Key))
        self.cloud.objects.pop((Bucket, Key), None)


class FakeSTS:
    def __init__(self, cloud):
        self.cloud = cloud

    def get_federation_token(self, Name, Policy, DurationSeconds):
        self.cloud.calls.append(("federate", Name, json.loads(Policy), DurationSeconds))
        return {"Credentials": {"AccessKeyId": "ASIAFED", "SecretAccessKey": "fed-secret", "SessionToken": "fed-token"}}


class _ClientError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.response = {"Error": {"Code": code, "Message": message}}


class FakeBoto3(types.ModuleType):
    """boto3 as far as the bucket needs it: the caller's credentials (long
    term unless a token is given), S3 and STS."""

    def __init__(self, token=""):
        super().__init__("boto3")
        self.calls, self.objects, self.denied = [], {}, set()
        frozen = types.SimpleNamespace(access_key="AKIAMINE", secret_key="my-secret", token=token)
        credentials = types.SimpleNamespace(get_frozen_credentials=lambda: frozen)
        self.session = types.SimpleNamespace(Session=lambda: types.SimpleNamespace(get_credentials=lambda: credentials))

    def client(self, name, region_name=None):
        self.calls.append(("client", name, region_name))
        return {"s3": FakeS3, "sts": FakeSTS}[name](self)


class Logged(unittest.TestCase):
    """A zenconfig with one context, logged in to a hub."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "config.yaml")
        with open(self.path, "w") as fh:
            json.dump({"current-context": "risk", "contexts": {"risk": {
                "server": "https://hub.example", "workspace": "risk", "auth": {"type": "token", "token": "t"}}}}, fh)
        clean = {k: v for k, v in os.environ.items() if not k.startswith(("ZENTHROS_", "AWS_"))}
        patcher = mock.patch.dict(os.environ, {**clean, config.CONFIG_ENV: self.path}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        userstore._minted.clear()
        self.addCleanup(userstore._minted.clear)
        self.boto3 = FakeBoto3()
        modules = mock.patch.dict(sys.modules, {"boto3": self.boto3})
        modules.start()
        self.addCleanup(modules.stop)

    def context(self):
        with open(self.path) as fh:
            return json.load(fh)["contexts"]["risk"]


class SetStoreTest(Logged):
    def test_a_bucket_that_answers_is_kept_with_its_region(self):
        out = io.StringIO()
        self.assertEqual(userstore.set_store("s3://my-bucket/zenthros/", out=out), "s3://my-bucket/zenthros")
        kinds = [c[0] for c in self.boto3.calls if c[0] in ("put", "get", "delete")]
        self.assertEqual(kinds, ["put", "get", "delete"], "the probe is written, read back, then removed")
        self.assertTrue(all(c[2].startswith("zenthros/.zenthros-probe-") for c in self.boto3.calls if c[0] == "put"))
        self.assertEqual(self.boto3.objects, {}, "the probe was left behind")
        self.assertEqual((self.context()["store"], self.context()["store-region"]), ("s3://my-bucket/zenthros", "us-east-2"))
        self.assertIn("the hub keeps none of them", out.getvalue())
        self.assertEqual(options.settings().store, "s3://my-bucket/zenthros")
        # configure(store=) still says otherwise for this process.
        with options.configure(store="s3://other/x"):
            self.assertEqual(options.settings().store, "s3://other/x")

    def test_temporary_credentials_are_said_to_reach_the_machine_whole(self):
        # GetFederationToken refuses them: the machine gets them as they are,
        # not narrowed to the prefix, and the person should know.
        self.assertNotIn("temporary", self._said())
        self.boto3 = FakeBoto3(token="sso-session")
        with mock.patch.dict(sys.modules, {"boto3": self.boto3}):
            said = self._said()
        self.assertIn("temporary (SSO or a role)", said)
        self.assertIn("not only s3://my-bucket/zenthros", said)

    def _said(self):
        out = io.StringIO()
        userstore.set_store("s3://my-bucket/zenthros", out=out)
        return out.getvalue()

    def test_a_bucket_that_does_not_answer_is_not_kept(self):
        self.boto3.denied.add("not-mine")
        with self.assertRaises(errors.ValidationError) as raised:
            userstore.set_store("s3://not-mine/z", out=io.StringIO())
        self.assertIn("s3://not-mine/z does not answer to your AWS credentials here: AccessDenied", str(raised.exception))
        self.assertNotIn("store", self.context())

    def test_what_is_not_an_s3_bucket_is_refused(self):
        for bad in ("http://b/x", "my-bucket", "s3://", "s3://B/x", "gs://", "gs://B/x"):
            with self.assertRaises(errors.ValidationError, msg=bad):
                userstore.parse(bad)

    def test_the_cli_is_the_twin(self):
        from zenthros import cli

        out = io.StringIO()
        self.assertEqual(cli.main(["store", "s3://my-bucket/zenthros"], out=out), 0)
        out = io.StringIO()
        cli.main(["store"], out=out)
        self.assertEqual(out.getvalue(), "s3://my-bucket/zenthros (us-east-2)\n")
        cli.main(["store", "--forget"], out=io.StringIO())
        self.assertNotIn("store", self.context())


class FakeGCSBlob:
    def __init__(self, cloud, bucket, name):
        self.cloud, self.bucket, self.name = cloud, bucket, name

    def _denied(self):
        if self.bucket in self.cloud.denied:
            raise type("Forbidden", (Exception,), {})("403 storage.objects.create denied")

    def upload_from_string(self, payload):
        self._denied()
        self.cloud.calls.append("put")
        self.cloud.objects[(self.bucket, self.name)] = payload

    def download_as_bytes(self):
        self.cloud.calls.append("get")
        return self.cloud.objects[(self.bucket, self.name)]

    def delete(self):
        self.cloud.calls.append("delete")
        if (self.bucket, self.name) not in self.cloud.objects:
            raise type("NotFound", (Exception,), {})("404")
        del self.cloud.objects[(self.bucket, self.name)]


class FakeGCS:
    """google.cloud.storage's client, as far as your bucket needs it."""

    def __init__(self):
        self.calls, self.objects, self.denied = [], {}, set()

    def bucket(self, name):
        return types.SimpleNamespace(blob=lambda key: FakeGCSBlob(self, name, key))


class GoogleBucketTest(Logged):
    def setUp(self):
        super().setUp()
        self.gcs = FakeGCS()
        patcher = mock.patch.object(userstore, "_gcs", lambda: self.gcs)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_it_is_kept_once_it_answers(self):
        out = io.StringIO()
        self.assertEqual(userstore.set_store("gs://my-bucket/zenthros/", out=out), "gs://my-bucket/zenthros")
        self.assertEqual(self.gcs.calls, ["put", "get", "delete"], "the probe is written, read back, then removed")
        self.assertEqual(self.gcs.objects, {})
        self.assertEqual(self.context()["store"], "gs://my-bucket/zenthros")
        self.assertNotIn("store-region", self.context())
        self.assertEqual(options.settings().store, "gs://my-bucket/zenthros")
        self.assertEqual([c for c in self.boto3.calls], [], "no AWS for a Google bucket")

    def test_one_that_does_not_answer_says_how_to_log_in(self):
        self.gcs.denied.add("not-mine")
        with self.assertRaises(errors.ValidationError) as raised:
            userstore.set_store("gs://not-mine/z", out=io.StringIO())
        self.assertIn("gs://not-mine/z does not answer to your Google credentials here", str(raised.exception))
        self.assertIn("gcloud auth application-default login", str(raised.exception))
        self.assertNotIn("store", self.context())

    def test_this_process_reads_it_with_the_google_store(self):
        from zenthros.core.storage import GCSBlobStore

        userstore.set_store("gs://my-bucket/z", out=io.StringIO())
        found = userstore.blobs()
        self.assertIsInstance(found, GCSBlobStore)
        found.put("gs://my-bucket/z/a", b"x")
        self.assertEqual(found.get("gs://my-bucket/z/a"), b"x")

    def test_a_deleted_object_stays_deleted_without_an_error(self):
        from zenthros.core.storage import GCSBlobStore

        store = GCSBlobStore(self.gcs)
        store.put("gs://my-bucket/z/a", b"x")
        store.delete("gs://my-bucket/z/a")
        store.delete("gs://my-bucket/z/a")
        self.assertEqual(self.gcs.objects, {})

    def test_a_machine_reads_it_with_its_own_role_nothing_is_sealed(self):
        """A VM of the cloud that holds the bucket: no key leaves this computer."""
        self.assertEqual(userstore.machine_access("gs://my-bucket/z", "gpu-1", "risk"), ({}, []))
        self.assertEqual(self.boto3.calls, [])

    def test_the_cli_is_the_twin(self):
        from zenthros import cli

        cli.main(["store", "gs://my-bucket/zenthros"], out=io.StringIO())
        out = io.StringIO()
        cli.main(["store"], out=out)
        self.assertEqual(out.getvalue(), "gs://my-bucket/zenthros\n")


class NoGoogleLibraryTest(Logged):
    def test_it_says_what_to_install(self):
        with mock.patch.dict(sys.modules, {"google.cloud": None, "google.cloud.storage": None}), \
                self.assertRaises(errors.ValidationError) as raised:
            userstore.set_store("gs://my-bucket/z", out=io.StringIO())
        self.assertIn('pip install "zenthros[gcs]"', str(raised.exception))


class CredentialsTest(Logged):
    def test_long_term_keys_are_federated_to_the_prefix(self):
        values = userstore.credentials("s3://my-bucket/zenthros", "risk")
        self.assertEqual(values, {"AWS_ACCESS_KEY_ID": "ASIAFED", "AWS_SECRET_ACCESS_KEY": "fed-secret",
                                  "AWS_SESSION_TOKEN": "fed-token"})
        (_, name, policy, seconds), = [c for c in self.boto3.calls if c[0] == "federate"]
        self.assertEqual((name, seconds), ("zenthros-risk", 12 * 3600))
        objects, listing = policy["Statement"]
        self.assertEqual(objects["Resource"], "arn:aws:s3:::my-bucket/zenthros/*")
        self.assertNotIn("*", objects["Action"])
        self.assertEqual(listing["Condition"], {"StringLike": {"s3:prefix": ["zenthros", "zenthros/*"]}})
        # Kept: a second call mints nothing.
        userstore.credentials("s3://my-bucket/zenthros", "risk")
        self.assertEqual(len([c for c in self.boto3.calls if c[0] == "federate"]), 1)

    def test_temporary_credentials_go_as_they_are(self):
        sys.modules["boto3"] = FakeBoto3(token="my-session")
        values = userstore.credentials("s3://my-bucket/zenthros", "risk")
        self.assertEqual(values["AWS_SESSION_TOKEN"], "my-session")
        self.assertFalse([c for c in sys.modules["boto3"].calls if c[0] == "federate"])


@unittest.skipUnless(seal.available(), "pip install 'zenthros[seal]'")
class RunTest(Logged):
    """A call to one of your machines, with your bucket kept."""

    def setUp(self):
        super().setUp()
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.client = HTTPJobClient(self.api.url, "risk", "bob-token")
        # A hub with no store of its own.
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk"})
        self.api.routes[("POST", "/v1/workspaces/risk/jobs")] = (201, {})
        _, self.key = keypair("my-vm")
        self.api.routes[("GET", "/v1/workspaces/risk/backends")] = (200, {"backends": [
            {"backendId": "vm-1", "name": "my-vm", "backendType": "vm", "ageRecipients": [self.key]}]})
        userstore.set_store("s3://my-bucket/zenthros", out=io.StringIO())
        self.sealed = []
        real = seal.seal

        def recording(plaintext, recipients, **kwargs):
            self.sealed.append((plaintext, list(recipients)))
            return real(plaintext, recipients, **kwargs)

        patcher = mock.patch.object(seal, "seal", recording)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_it(self, **kwargs):
        from zenthros.remote import call as remote_call

        with options.configure(client=self.client):
            return remote_call.run(double_it, 21, on="my-vm", code_dir=None, **kwargs)

    def test_the_call_goes_to_your_bucket_and_the_machine_gets_sealed_credentials_for_it(self):
        self.run_it()
        paths = [r["path"] for r in self.api.requests if r["method"] == "POST"]
        self.assertEqual(paths, ["/v1/workspaces/risk/jobs"], "the hub was asked to sign or seal")
        self.assertFalse([r for r in self.api.requests if "presign" in r["path"]])
        put = [c for c in self.boto3.calls if c[0] == "put" and "/call-" in c[2]]
        self.assertEqual(len(put), 1, "the call did not go to your bucket")
        self.assertEqual(put[0][1], "my-bucket")
        self.assertTrue(put[0][2].startswith("zenthros/"))
        template = json.loads(self.api.requests[-1]["body"])["spec"]["workload"]["template"]
        env = {e["name"]: e["value"] for e in template["env"]}
        self.assertEqual(env["AWS_REGION"], "us-east-2")
        sources = template["secretSources"]
        self.assertEqual([s["materialization"]["envName"] for s in sources],
                         ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"])
        self.assertTrue(all(s["encryptedPayload"]["recipients"] == [self.key] for s in sources))
        self.assertEqual(self.sealed, [(b"ASIAFED", [self.key]), (b"fed-secret", [self.key]), (b"fed-token", [self.key])])
        body = self.api.requests[-1]["body"]
        for value in ("fed-secret", "fed-token", "my-secret"):
            self.assertNotIn(value, body if isinstance(body, str) else body.decode())

    def test_credentials_you_pass_are_left_alone(self):
        mine = options.Secret.encrypted("AKIAELSE", recipients=[self.key])
        self.sealed.clear()
        self.run_it(secrets={"AWS_ACCESS_KEY_ID": mine})
        self.assertFalse([c for c in self.boto3.calls if c[0] == "federate"])
        self.assertEqual(self.sealed, [], "credentials were minted over the caller's own")

    def test_reading_and_writing_your_bucket_is_yours_and_the_hubs_store_is_signed(self):
        with options.configure(client=self.client):
            self.assertNotIsInstance(options.blob_store("s3://my-bucket/zenthros/x"), PresignedBlobStore)
        self.api.routes[("GET", "/v1/workspaces/risk")] = (200, {"name": "risk", "store": "s3://hub/zenthros/risk"})
        fresh = HTTPJobClient(self.api.url, "risk", "bob-token")
        with options.configure(client=fresh, store="s3://hub/zenthros/risk"):
            self.assertIsInstance(options.blob_store("s3://hub/zenthros/risk/x"), PresignedBlobStore)

    def test_no_bucket_and_no_hub_store_says_what_to_do(self):
        from zenthros.remote import call as remote_call

        with options.configure(client=self.client, store=""):
            with self.assertRaises(errors.ValidationError) as raised:
                remote_call._store(options.settings(), options.job_client())
        self.assertIn("zenthros store s3://your-bucket/zenthros", str(raised.exception))



class _FakeBucket:
    def __init__(self, uniform=True):
        self.iam_configuration = mock.Mock(uniform_bucket_level_access_enabled=uniform)
        self.policy = mock.Mock(bindings=[{"role": "roles/owner", "members": {"user:me"}}], version=1)
        self.written = 0

    def reload(self):
        pass

    def get_iam_policy(self, requested_policy_version=None):
        return self.policy

    def set_iam_policy(self, policy):
        self.written += 1


def _google_api_core():
    try:
        import google.api_core.exceptions  # noqa: F401
    except ImportError:
        return False
    return True


@unittest.skipUnless(_google_api_core() and seal.available(), "pip install 'zenthros[gcs,seal]'")
class GoogleJobKeyTest(unittest.TestCase):
    """A machine with no role reads a gs:// store with a key that reaches only its
    prefix, made once, kept here, and sealed: it does not go in the clear."""

    STORE = "gs://my-bucket/zenthros"

    def setUp(self):
        self.home = tempfile.TemporaryDirectory()
        self.addCleanup(self.home.cleanup)
        patches = [mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": f"{self.home.name}/config"}),
                   mock.patch("zenthros.userstore._iam", self.iam),
                   mock.patch("zenthros.userstore._gcs", lambda: self.client),
                   mock.patch("zenthros.userstore._wait_for_permission", lambda key, bucket, prefix: self.waited.append(key))]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.bucket = _FakeBucket()
        self.client = mock.Mock(project="p")
        self.client.bucket.return_value = self.bucket
        self.calls, self.waited = [], []

    def iam(self, method, path, body=None):
        self.calls.append((method, path))
        if path.endswith("/keys"):
            return {"privateKeyData": base64.b64encode(b'{"type": "service_account"}').decode()}
        return {}

    def test_it_is_made_once_and_kept_only_for_you(self):
        key = userstore.job_key(self.STORE)
        self.assertEqual(key, '{"type": "service_account"}')
        kept = userstore._job_account(self.STORE)[1]
        assert_mode(self, kept, 0o600)
        made = list(self.calls)
        self.assertEqual(userstore.job_key(self.STORE), key)
        self.assertEqual(self.calls, made, "a second call made another key")
        self.assertEqual(self.waited, [key], "a job must not start before the permission reaches the bucket")

    def test_it_reaches_only_the_prefix(self):
        userstore.job_key(self.STORE)
        binding = self.bucket.policy.bindings[-1]
        self.assertEqual(self.bucket.policy.version, 3)
        self.assertEqual(binding["role"], "roles/storage.objectAdmin")
        self.assertIn('resource.name.startsWith("projects/_/buckets/my-bucket/objects/zenthros/")', binding["condition"]["expression"])
        self.assertIn(userstore._job_account(self.STORE)[0], next(iter(binding["members"])))

    def test_a_new_account_takes_a_moment_to_exist(self):
        # Found on real GCP: the account just made was not there yet for the
        # bucket's permissions (400, "does not exist"), nor for its keys (404).
        from google.api_core.exceptions import BadRequest

        refusals = [BadRequest("Service account zenthros-job-x@p.iam.gserviceaccount.com does not exist.")] * 2
        written = self.bucket.set_iam_policy

        def set_iam_policy(policy):
            if refusals:
                raise refusals.pop()
            written(policy)

        self.bucket.set_iam_policy = set_iam_policy
        no_key_yet = [None]
        iam = self.iam

        def keys_later(method, path, body=None):
            if path.endswith("/keys") and no_key_yet:
                return no_key_yet.pop()
            return iam(method, path, body)

        with mock.patch("zenthros.userstore._iam", keys_later), mock.patch("zenthros.userstore.time.sleep") as slept:
            key = userstore.job_key(self.STORE)
        self.assertEqual(key, '{"type": "service_account"}')
        self.assertEqual(self.bucket.written, 1)
        self.assertEqual(slept.call_count, 3)

    def test_a_refusal_that_is_not_about_a_new_account_is_said_at_once(self):
        from google.api_core.exceptions import BadRequest

        self.bucket.set_iam_policy = mock.Mock(side_effect=BadRequest("the condition is not valid"))
        with mock.patch("zenthros.userstore.time.sleep") as slept, self.assertRaises(BadRequest):
            userstore.job_key(self.STORE)
        self.assertEqual(slept.call_count, 0)
        self.assertFalse(userstore._job_account(self.STORE)[1].exists())

    def test_an_account_that_never_comes_says_so(self):
        from google.api_core.exceptions import BadRequest

        self.bucket.set_iam_policy = mock.Mock(side_effect=BadRequest("Service account x does not exist."))
        with mock.patch("zenthros.userstore.time.sleep"), mock.patch("zenthros.userstore._PERMISSION_WAIT", -1), \
                self.assertRaisesRegex(errors.ValidationError, "(?i)still.*account"):
            userstore.job_key(self.STORE)

    def test_a_bucket_without_uniform_access_says_the_command(self):
        self.bucket = _FakeBucket(uniform=False)
        self.client.bucket.return_value = self.bucket
        with self.assertRaisesRegex(errors.ValidationError, "--uniform-bucket-level-access"):
            userstore.job_key(self.STORE)
        self.assertFalse(userstore._job_account(self.STORE)[1].exists())
        self.assertFalse([c for c in self.calls if c[1].endswith("/keys")], "no key for a limit that cannot hold")

    def test_forgetting_ends_the_account_its_permission_and_the_file(self):
        userstore.job_key(self.STORE)
        self.bucket.written = 0
        userstore.forget_job_key(self.STORE)
        self.assertEqual(self.bucket.written, 1)
        self.assertEqual([item["role"] for item in self.bucket.policy.bindings], ["roles/owner"])
        self.assertTrue(any(method == "DELETE" for method, _ in self.calls))
        self.assertFalse(userstore._job_account(self.STORE)[1].exists())

    def test_a_machine_with_a_role_gets_nothing(self):
        self.assertEqual(userstore.machine_access(self.STORE, "gpu-1", "risk"), ({}, []))
        self.assertEqual(self.calls, [])

    def test_a_machine_with_no_role_gets_it_sealed(self):
        with mock.patch("zenthros.seal.available", return_value=True), \
                mock.patch("zenthros.seal.seal", return_value="sealed") as sealed, \
                mock.patch("zenthros.seal.key_id", return_value="k"), \
                mock.patch("zenthros.remote.options._api_recipients", return_value=["age1x"]):
            env, sources = userstore.machine_access(self.STORE, "gpu-1", "risk", no_role=True)
        self.assertEqual(env, {})
        self.assertEqual([s.materialization.env_name for s in sources], ["GOOGLE_STORE_KEY"])
        self.assertEqual(sources[0].encrypted_payload.ciphertext, "sealed")
        self.assertEqual(sealed.call_args.args[1], ["age1x"])

    def test_a_job_reads_the_store_with_it(self):
        store = GCSBlobStore()
        with mock.patch.dict(os.environ, {"GOOGLE_STORE_KEY": '{"a": 1}'}), \
                mock.patch("google.cloud.storage.Client.from_service_account_info") as made:
            self.assertIs(store._gcs(), made.return_value)
        made.assert_called_once_with({"a": 1})

if __name__ == "__main__":
    unittest.main()
