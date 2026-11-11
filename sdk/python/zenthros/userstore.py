# SPDX-License-Identifier: Apache-2.0

"""Your own bucket: where your calls and results live on a hub that keeps
none of them.

    $ zenthros store s3://my-bucket/zenthros      (or gs://my-bucket/zenthros)
    zenthros.set_store("s3://my-bucket/zenthros")

Set once, it is kept with your login (the zenconfig's current context: store,
store-region). This process writes each call there and reads each result
with your own credentials; the hub never sees them, nor the data.

gs://: your gcloud login here (gcloud auth application-default login), and
the machine's own role there: a VM of GCP whose service account reads the
bucket. No key leaves this computer. pip install "zenthros[gcs,seal]".

s3://: a job on one of your machines gets credentials for that bucket's prefix only:
minted here from yours (STS GetFederationToken with a policy for the prefix,
12 hours) or, when yours are already temporary (SSO, an assumed role), yours
as they are; sealed here to that machine's key, so only its agent opens
them. A machine with a role of its own that reads the bucket needs none:
pass secrets={"AWS_ACCESS_KEY_ID": ...} yourself and nothing is minted.

Needs boto3 and cryptography: pip install "zenthros[s3,seal]".
"""

import base64
import hashlib
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

from zenthros import console, errors
from zenthros.core.storage import GCS_KEY_ENV

STORE_KEY = "store"
REGION_KEY = "store-region"
HOURS_ENV = "ZENTHROS_STORE_CREDENTIAL_HOURS"
HOURS = 12
CREDENTIALS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
_URI = re.compile(r"(?:s3|gs)://([a-z0-9][a-z0-9._-]{1,61}[a-z0-9])(/[A-Za-z0-9!_.*'()/-]*)?")
_minted = {}


def parse(uri):
    """(bucket, prefix) of s3://bucket[/prefix] or gs://..., the prefix without
    slashes at either end."""
    match = _URI.fullmatch(str(uri or "").rstrip("/"))
    if not match:
        raise errors.ValidationError(f"{uri!r}: your bucket as s3://bucket/prefix or gs://bucket/prefix")
    return match.group(1), (match.group(2) or "").strip("/")


def _boto3():
    try:
        import boto3
    except ImportError:
        raise errors.ValidationError('your bucket needs boto3 here: pip install "zenthros[s3,seal]"') from None
    return boto3


def _gcs():
    try:
        from google.cloud import storage
    except ImportError:
        raise errors.ValidationError('a gs:// bucket needs google-cloud-storage here: pip install "zenthros[gcs]"') from None
    return storage.Client()


def _google(store):
    return str(store).startswith("gs://")


def saved():
    """(store, region) kept with your login; ("", "") when none."""
    from zenthros import config

    try:
        context = config.load_context()
    except errors.ValidationError:
        return "", ""
    return str(context.get(STORE_KEY) or "").rstrip("/"), str(context.get(REGION_KEY) or "")


def blobs():
    """The store this process reads and writes your bucket with: your
    credentials, in the bucket's region when it is kept."""
    from zenthros.core.storage import GCSBlobStore, S3BlobStore

    kept, region = saved()
    if _google(kept):
        return GCSBlobStore(_gcs())
    return S3BlobStore(_boto3().client("s3", region_name=region) if region else None)


def set_store(uri, out=None):
    """Keep uri (s3:// or gs://bucket/prefix) as where your calls and results
    go, once it answers: a small object is written, read back and removed
    with your credentials. Returns the store. uri=None forgets it."""
    from zenthros import config

    if uri is None:
        kept = saved()[0]
        if _google(kept) and _job_account(kept)[1].exists():
            forget_job_key(kept)
        config.update_context({STORE_KEY: None, REGION_KEY: None})
        console.show("✓ no bucket kept: calls use the hub's store, if it has one", out)
        return ""
    bucket, prefix = parse(uri)
    if _google(uri):
        store = f"gs://{bucket}" + (f"/{prefix}" if prefix else "")
        _probe_google(bucket, prefix, store)
        config.update_context({STORE_KEY: store, REGION_KEY: None})
        console.show(f"✓ your bucket: {store}. Calls and results go there; the hub keeps none of them. A VM of GCP "
                     "reaches it with its service account; a host with no role gets a key that reaches only this prefix", out)
        return store
    store = f"s3://{bucket}" + (f"/{prefix}" if prefix else "")
    s3 = _boto3().client("s3")
    try:
        region = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint") or "us-east-1"
        probe = (f"{prefix}/" if prefix else "") + f".zenthros-probe-{uuid.uuid4().hex[:12]}"
        regional = _boto3().client("s3", region_name=region)
        regional.put_object(Bucket=bucket, Key=probe, Body=b"zenthros")
        regional.get_object(Bucket=bucket, Key=probe)["Body"].read()
        regional.delete_object(Bucket=bucket, Key=probe)
    except Exception as exc:  # noqa: BLE001 - boto3's errors: said in one line
        raise errors.ValidationError(f"{store} does not answer to your AWS credentials here: {_said(exc)}") from None
    config.update_context({STORE_KEY: store, REGION_KEY: region})
    console.show(f"✓ your bucket: {store} ({region}). Calls and results go there; the hub keeps none of them", out)
    if _boto3().session.Session().get_credentials().get_frozen_credentials().token:
        # credentials() cannot narrow these (GetFederationToken refuses them).
        print(f"  your AWS credentials here are temporary (SSO or a role): a machine gets them as they are, "
              f"for all they reach, not only {store}, until they end", file=out or sys.stdout)
    return store


def _probe_google(bucket, prefix, store):
    client = _gcs()
    try:
        blob = client.bucket(bucket).blob((f"{prefix}/" if prefix else "") + f".zenthros-probe-{uuid.uuid4().hex[:12]}")
        blob.upload_from_string(b"zenthros")
        blob.download_as_bytes()
        blob.delete()
    except Exception as exc:  # noqa: BLE001 - google's errors: said in one line
        raise errors.ValidationError(f"{store} does not answer to your Google credentials here (gcloud auth "
                                     f"application-default login): {_said(exc)}") from None


def _said(exc):
    error = getattr(exc, "response", {}).get("Error", {}) if hasattr(exc, "response") else {}
    if error.get("Code"):
        return f"{error.get('Code')}: {error.get('Message', '')}".strip(": ")
    return str(exc).splitlines()[0] if str(exc) else type(exc).__name__


def policy(bucket, prefix):
    """The most a machine's credentials may do: objects under the prefix,
    and listing it."""
    objects = f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"
    listing = {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{bucket}"}
    if prefix:
        listing["Condition"] = {"StringLike": {"s3:prefix": [prefix, f"{prefix}/*"]}}
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
         "Resource": objects},
        listing,
    ]}


def _hours():
    raw = os.environ.get(HOURS_ENV, "").strip()
    if not raw:
        return HOURS
    if not raw.isdigit() or not 1 <= int(raw) <= 36:
        raise errors.ValidationError(f"{HOURS_ENV}={raw!r}: whole hours, 1 to 36")
    return int(raw)


def credentials(store, workspace=""):
    """{AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN} for
    store's prefix: federated from long-term keys, else the temporary ones
    this process holds. Kept in this process until an hour before they end."""
    kept = _minted.get(store)
    if kept and kept[1] - time.time() > 3600:
        return kept[0]
    boto3 = _boto3()
    found = boto3.session.Session().get_credentials()
    if found is None:
        raise errors.ValidationError(f"no AWS credentials here to reach {store}: "
                                     "aws configure, aws sso login, or AWS_PROFILE")
    frozen = found.get_frozen_credentials()
    if frozen.token:
        # Already temporary: GetFederationToken refuses them. They end when
        # this session does, and so does the job's access.
        values = dict(zip(CREDENTIALS, (frozen.access_key, frozen.secret_key, frozen.token)))
        _minted[store] = (values, time.time() + 3600 + 60)
        return values
    bucket, prefix = parse(store)
    name = "zenthros-" + re.sub(r"[^\w+=,.@-]", "-", workspace or "calls")[:23]
    hours = _hours()
    try:
        token = boto3.client("sts").get_federation_token(Name=name, Policy=json.dumps(policy(bucket, prefix)),
                                                         DurationSeconds=hours * 3600)["Credentials"]
    except Exception as exc:  # noqa: BLE001
        raise errors.ValidationError(f"cannot mint credentials for {store} from yours (sts:GetFederationToken): "
                                     f"{_said(exc)}") from None
    values = dict(zip(CREDENTIALS, (token["AccessKeyId"], token["SecretAccessKey"], token["SessionToken"])))
    _minted[store] = (values, time.time() + hours * 3600)
    return values


def machine_access(store, backend, workspace="", no_role=False):
    """(env, secret sources) a job on backend needs for store: its region,
    and credentials for the prefix sealed to that machine's key. A gs://
    bucket: nothing where the machine's own role reaches it; with no_role, the
    key of an account that reaches only that prefix (job_key)."""
    if _google(store) and not no_role:
        return {}, []
    from zenthros import seal
    from zenthros.jobs import spec as S
    from zenthros.remote.options import _api_recipients

    if not seal.available():
        raise errors.ValidationError('your bucket needs cryptography here, to seal its credentials to the machine: '
                                     'pip install "zenthros[s3,seal]"')
    recipients = list(dict.fromkeys(_api_recipients("", backend) or []))
    env = {}
    kept, region = saved()
    if kept == store and region:
        env["AWS_REGION"] = region
    sources = []
    values = {GCS_KEY_ENV: job_key(store)} if _google(store) else credentials(store, workspace)
    for name, value in values.items():
        ciphertext = seal.seal(value.encode(), recipients)
        sources.append(S.SecretSource(
            type="encrypted-payload",
            encrypted_payload=S.EncryptedPayload(ciphertext=ciphertext, key_id=seal.key_id(recipients[0]),
                                                 recipients=recipients),
            materialization=S.Materialization(mode="env", env_name=name)))
    return env, sources


# How long a new account's permission takes to reach the bucket, at most.
_PERMISSION_WAIT = 180


def _job_account(store):
    """The account for store's machines: its id, and the file its key stays in."""
    from zenthros import config

    digest = hashlib.sha256(store.encode()).hexdigest()[:12]
    return f"zenthros-job-{digest}", Path(config.config_path()).parent / "gcs-job-keys" / f"{digest}.json"


def _iam(method, path, body=None):
    """An IAM API call with your Google credentials, or None for a 404. A 409 is
    what was asked already being there."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    session = AuthorizedSession(google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])[0])
    answer = session.request(method, f"https://iam.googleapis.com/v1/{path}", json=body)
    if answer.status_code in (404, 409):
        return None
    if not answer.ok:
        raise errors.ValidationError(f"Google refused {method} {path} with your credentials ({answer.status_code}): making an "
                                     "account for hosts with no role needs roles/iam.serviceAccountAdmin and "
                                     "roles/iam.serviceAccountKeyAdmin on your project")
    return answer.json()


def _limit_to_prefix(client, bucket, prefix, email):
    """email may read, write and list under prefix only. A permission with a
    condition needs uniform bucket-level access."""
    found = client.bucket(bucket)
    found.reload()
    if not found.iam_configuration.uniform_bucket_level_access_enabled:
        raise errors.ValidationError(f"gs://{bucket} needs uniform access to limit a key to its prefix: "
                                     f"gcloud storage buckets update gs://{bucket} --uniform-bucket-level-access")
    objects = f"projects/_/buckets/{bucket}/objects/{prefix + '/' if prefix else ''}"
    binding = {"role": "roles/storage.objectAdmin", "members": {f"serviceAccount:{email}"},
               "condition": {"title": "zenthros-prefix", "expression":
                             f'resource.type == "storage.googleapis.com/Bucket" || resource.name.startsWith("{objects}")'}}
    policy = found.get_iam_policy(requested_policy_version=3)
    policy.version = 3
    if not any(f"serviceAccount:{email}" in item.get("members", ()) for item in policy.bindings):
        policy.bindings.append(binding)
        _once_it_exists(lambda: _granted(found, policy), email)


def _granted(found, policy):
    """The policy written, False while its new account does not exist for the
    bucket yet: a 400 that says so, the only one that passes."""
    from google.api_core.exceptions import BadRequest

    try:
        found.set_iam_policy(policy)
        return True
    except BadRequest as exc:
        if "does not exist" not in str(exc):
            raise
        return False


def _once_it_exists(attempt, email):
    """What attempt returns once a new account has reached it: an account just
    made takes up to a minute or two to exist for the rest of Google."""
    started = time.monotonic()
    while True:
        done = attempt()
        if done:
            return done
        if time.monotonic() - started > _PERMISSION_WAIT:
            raise errors.ValidationError(f"Google still does not know the account just made, {email}: try again in a few minutes")
        time.sleep(5)


def _wait_for_permission(key, bucket, prefix):
    """Once the new key reads the bucket: a permission reaches it within minutes,
    and a job started before that would read as a broken bucket."""
    from google.cloud import storage

    client = storage.Client.from_service_account_info(json.loads(key))
    started = time.monotonic()
    while True:
        try:
            list(client.list_blobs(bucket, prefix=prefix, max_results=1))
            return
        except Exception as exc:  # noqa: BLE001 - google's errors: said in one line
            if time.monotonic() - started > _PERMISSION_WAIT:
                raise errors.ValidationError(f"the key made for gs://{bucket} still cannot read it: {_said(exc)}") from None
            time.sleep(5)


def job_key(store):
    """The key of an account that reaches only store's prefix, for machines with
    no role (Vast, RunPod). Made once with your Google login and kept here 0600;
    only sealed to a machine does it leave this computer, and
    forget_job_key(store) ends it. A token that narrow lasts an hour at most,
    less than a training run, so it is a key."""
    bucket, prefix = parse(store)
    account, path = _job_account(store)
    if path.exists():
        return path.read_text()
    client = _gcs()
    project = client.project
    email = f"{account}@{project}.iam.gserviceaccount.com"
    _iam("POST", f"projects/{project}/serviceAccounts",
         {"accountId": account, "serviceAccount": {"displayName": f"Zenthros jobs on {store}"}})
    _limit_to_prefix(client, bucket, prefix, email)
    made = _once_it_exists(lambda: _iam("POST", f"projects/{project}/serviceAccounts/{email}/keys", {}), email)
    key = base64.b64decode(made["privateKeyData"]).decode()
    _wait_for_permission(key, bucket, prefix)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.touch(mode=0o600)
    path.write_text(key)
    return key


def forget_job_key(store):
    """Ends what job_key made for store: the account, its keys and its permission.
    Whatever a machine holds stops working."""
    bucket, _ = parse(store)
    account, path = _job_account(store)
    client = _gcs()
    email = f"{account}@{client.project}.iam.gserviceaccount.com"
    found = client.bucket(bucket)
    policy = found.get_iam_policy(requested_policy_version=3)
    policy.version = 3
    kept = [item for item in policy.bindings if f"serviceAccount:{email}" not in item.get("members", ())]
    if len(kept) != len(policy.bindings):
        policy.bindings[:] = kept
        found.set_iam_policy(policy)
    _iam("DELETE", f"projects/{client.project}/serviceAccounts/{email}")
    path.unlink(missing_ok=True)
