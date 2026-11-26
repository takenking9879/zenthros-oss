# SPDX-License-Identifier: Apache-2.0

"""An hour of S3 credentials for the prefixes a job uses, sealed to the
machine that reads them.

    store = zenthros.workspace().store        # where the job reads its call
    secrets = zenthros.aws.s3_secrets("s3://acme-data/pipeline/", store, write=True, on=lb)
    zenthros.run(fn, on=lb, secrets=secrets, env={"AWS_REGION": "us-east-2"})

A job reads its call from the Workspace's call store and writes its result
beside it with the machine's own credentials: name that store too.

STS gives them (boto3, pip install 'zenthros[s3]'; zctl seals them) from the
credentials this process already has: GetFederationToken with an IAM user's
keys, or AssumeRole of role_arn. Either way a session policy narrows them to
the places named, so they can do no more than that even where the keys they
come from can do more. They end on their own (an hour by default): a sealed
copy that is reused stops working when they do.

A cloud machine needs none of this: its pool's instance role reads S3, with
no secret at all. This is for a machine that has no role, as a container
on your computer.
"""

import json
import re

from zenthros import errors

MIN_SECONDS = 900  # STS's floor
MAX_SECONDS = 12 * 3600  # AssumeRole's ceiling, when the role allows it

_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_SESSION_NAME = "zenthros-s3"


def _place(uri, whole_bucket):
    """(bucket, prefix ending in / or empty) of an s3:// URI."""
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise errors.ValidationError(f"s3_policy: {uri!r} is not an s3://bucket/prefix")
    bucket, _, prefix = uri[len("s3://"):].partition("/")
    if not _BUCKET.match(bucket):
        raise errors.ValidationError(f"s3_policy: {bucket!r} is not an S3 bucket name")
    if any(c in prefix for c in "*?") or prefix.startswith("/"):
        raise errors.ValidationError(f"s3_policy: {uri!r}: no wildcards, and no // after the bucket")
    if not prefix and not whole_bucket:
        raise errors.ValidationError(f"s3_policy: {uri!r} is the whole bucket: pass whole_bucket=True to mean it")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def s3_policy(*uris, write=False, whole_bucket=False):
    """The session policy: read (and, with write=True, write and delete)
    objects under each s3://bucket/prefix, and list each bucket only under
    those prefixes. A URI with no prefix is the whole bucket, only with
    whole_bucket=True."""
    if not uris:
        raise errors.ValidationError("s3_policy: at least one s3://bucket/prefix")
    places = [_place(uri, whole_bucket) for uri in uris]
    actions = ["s3:GetObject"]
    if write:
        actions += ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    statements = [{"Effect": "Allow", "Action": actions,
                   "Resource": [f"arn:aws:s3:::{bucket}/{prefix}*" for bucket, prefix in places]}]
    prefixes = {}
    for bucket, prefix in places:
        prefixes.setdefault(bucket, []).append(prefix)
    for bucket, under in prefixes.items():
        listing = {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{bucket}"}
        if "" not in under:
            listing["Condition"] = {"StringLike": {"s3:prefix": [p for prefix in under for p in (prefix + "*", prefix)]}}
        statements.append(listing)
    return {"Version": "2012-10-17", "Statement": statements}


def s3_credentials(*uris, write=False, whole_bucket=False, seconds=3600, role_arn="", region="", sts=None):
    """Temporary credentials narrowed by s3_policy, as the three environment
    variables boto3, pyarrow, Polars and Spark's S3A read. Keep them out of
    env= and logs: pass s3_secrets() to run() instead."""
    if not MIN_SECONDS <= int(seconds) <= MAX_SECONDS:
        raise errors.ValidationError(f"s3_credentials: seconds={seconds}; STS gives {MIN_SECONDS} to {MAX_SECONDS}")
    policy = json.dumps(s3_policy(*uris, write=write, whole_bucket=whole_bucket), separators=(",", ":"))
    if sts is None:
        try:
            import boto3
        except ImportError:
            raise errors.ValidationError("s3_credentials needs boto3: pip install 'zenthros[s3]'") from None
        sts = boto3.client("sts", region_name=region or None)
    if role_arn:
        answer = sts.assume_role(RoleArn=role_arn, RoleSessionName=_SESSION_NAME, Policy=policy,
                                 DurationSeconds=int(seconds))
    else:
        answer = sts.get_federation_token(Name=_SESSION_NAME, Policy=policy, DurationSeconds=int(seconds))
    creds = answer["Credentials"]
    return {"AWS_ACCESS_KEY_ID": creds["AccessKeyId"], "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
            "AWS_SESSION_TOKEN": creds["SessionToken"]}


def s3_secrets(*uris, on=None, recipients=(), write=False, whole_bucket=False, seconds=3600, role_arn="",
               region="", sts=None):
    """s3_credentials() sealed to the machine on= names (its age keys, from
    zenthros-api) or to the age1 recipients given: {env name: Secret} for
    run(secrets=)."""
    if (on is None) == (not recipients):
        raise errors.ValidationError("s3_secrets: one of on= or recipients=")
    from zenthros.remote.options import Secret

    env = s3_credentials(*uris, write=write, whole_bucket=whole_bucket, seconds=seconds, role_arn=role_arn,
                         region=region, sts=sts)
    if on is not None:
        return {name: Secret.encrypted(value, on=on) for name, value in env.items()}
    return {name: Secret.encrypted(value, recipients=list(recipients)) for name, value in env.items()}
