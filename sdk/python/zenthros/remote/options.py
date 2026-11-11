# SPDX-License-Identifier: Apache-2.0

"""Where Zenthros is for this process (configure) and the values run() takes
that are more than a string: secrets and durations.

    zenthros.configure(store="s3://acme/calls", workspace="risk")

What a call runs and what it needs (on, cpu, image, secrets...) goes to
run(); where calls are stored and how Zenthros is reached goes here, once,
or in the environment:

    store      ZENTHROS_REMOTE_STORE   the s3:// prefix calls and results go to
    on         ZENTHROS_ON             where a call with no on= runs (else this
                                       computer, "local")
    workspace  ZENTHROS_WORKSPACE
    namespace  ZENTHROS_NAMESPACE      (read by zctl)
    server     ZENTHROS_SERVER         zenthros-api; with it, no zctl or kubeconfig
    token      ZENTHROS_TOKEN          the bearer token for it (zenthros.config)
    context    ZENTHROS_CONTEXT        the zenconfig context
    python     ZENTHROS_REMOTE_PYTHON  the machine's interpreter (python3)
    env        ZENTHROS_REMOTE_ENV     a JSON object every job gets
    untrusted_host_secrets             send secrets= to a pool whose host a
                                       third party runs (Vast.ai); its owner
                                       can read them. Off: the controller
                                       refuses such a job.
    untrusted_host_spawn               let a job on such a host spawn children:
                                       its owner can act as the job and run
                                       what they want on its machines. Off:
                                       the controller refuses the spawn.
    client, blob_store, ship_sdk       for tests and embedding
"""

import json
import os
import re
from dataclasses import dataclass, field, replace
from typing import Optional

from zenthros import errors
from zenthros.jobs import spec as S

STORE_ENV = "ZENTHROS_REMOTE_STORE"
ON_ENV = "ZENTHROS_ON"
PYTHON_ENV = "ZENTHROS_REMOTE_PYTHON"
REMOTE_ENV = "ZENTHROS_REMOTE_ENV"
WORKSPACE_ENV = "ZENTHROS_WORKSPACE"


@dataclass
class Settings:
    store: str = ""
    on: object = None
    workspace: str = ""
    namespace: str = ""
    server: str = ""
    token: str = ""
    context: str = ""
    python: str = ""
    env: dict = field(default_factory=dict)
    client: object = None
    blob_store: object = None
    ship_sdk: bool = True
    untrusted_host_secrets: bool = False
    untrusted_host_spawn: bool = False
    # The local container's size (else all the engine's CPUs and 3/4 of its
    # memory) and where its jobs' outputs go (else under .zenthros/local).
    local_cpus: object = None
    local_memory: object = None
    local_output: str = ""
    # Where zenthros env push sends an environment that names no registry.
    registry: str = ""
    # The cluster of on="local-k8s"; empty is kubectl's current context.
    kube_context: str = ""
    # Where each call's local record goes (zenthros.remote.runlog): "" is
    # ZENTHROS_LOG_DIR, else ./.zenthros/runs; "off" writes none.
    log_dir: str = ""


_configured = {}


class _Configured:
    """What configure() returns: in a `with`, the settings go back on exit."""

    def __init__(self, before):
        self._before = before

    def __enter__(self):
        return settings()

    def __exit__(self, *exc):
        _configured.clear()
        _configured.update(self._before)
        return False


def configure(**changes):
    """Set where Zenthros is for this process. Returns a context manager that
    restores the previous settings, for a `with` block."""
    known = set(Settings.__dataclass_fields__)
    unknown = set(changes) - known
    if unknown:
        raise errors.ValidationError(f"configure: unknown settings {sorted(unknown)}; known: {sorted(known)}")
    before = dict(_configured)
    _configured.update(changes)
    return _Configured(before)


def settings():
    """The settings in effect: configure()'s, else the environment's."""
    env = {}
    raw = os.environ.get(REMOTE_ENV, "").strip()
    if raw:
        try:
            env = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise errors.ValidationError(f"{REMOTE_ENV} is not a JSON object: {exc}") from exc
    base = Settings(
        store=os.environ.get(STORE_ENV, ""),
        on=os.environ.get(ON_ENV) or None,
        workspace=os.environ.get(WORKSPACE_ENV, ""),
        namespace=os.environ.get("ZENTHROS_NAMESPACE", ""),
        python=os.environ.get(PYTHON_ENV, "") or "python3",
        env=env,
    )
    chosen = dict(_configured)
    if "env" in chosen:
        chosen["env"] = {**env, **chosen["env"]}
    resolved = replace(base, **chosen)
    if not resolved.store and "store" not in chosen:
        # Your own bucket, kept with your login (zenthros store s3://...).
        from zenthros import userstore

        resolved.store = userstore.saved()[0]
    resolved.store = (resolved.store or "").rstrip("/")
    return resolved


def job_client(current=None):
    current = current or settings()
    if current.client is not None:
        return current.client
    from zenthros import config

    # server, token and context are read by zenthros.config, which also
    # looks at the environment and the zenconfig.
    conn = config.resolve(server=current.server, token=current.token, workspace=current.workspace,
                          context=current.context)
    if conn is not None:
        from zenthros.jobs.http import HTTPJobClient

        return HTTPJobClient.for_connection(conn)
    from zenthros.jobs.client import ZctlJobClient

    zctl = ZctlJobClient(namespace=current.namespace or None)
    if not _zctl_configured(zctl):
        # No account, no cluster of an administrator's: this computer.
        return local_client()
    return zctl


def _zctl_configured(zctl):
    """Whether a zctl is there to reach a cluster: an administrator's way in.
    Anything else with no hub runs here."""
    import shutil

    return bool(os.environ.get("ZENTHROS_NAMESPACE") or os.environ.get("ZENTHROS_ZCTL") or shutil.which(zctl.zctl))


def local_client():
    """The one local client of this process, for its project."""
    global _local
    from zenthros.jobs.local import LocalJobClient, project_root

    if _local is None or _local.project != project_root():
        _local = LocalJobClient()
    return _local


def local_k8s_client():
    """The one client of on="local-k8s" of this process, for its project."""
    global _local_k8s
    from zenthros.jobs.local import project_root
    from zenthros.jobs.localk8s import LocalK8sJobClient

    if _local_k8s is None or _local_k8s.project != project_root():
        _local_k8s = LocalK8sJobClient()
    return _local_k8s


def local_clients():
    """Where a job with no hub may be: its record says which."""
    return [local_client(), local_k8s_client()]


def local_client_of(name):
    """The client that ran this job of the project, or None."""
    return next((client for client in local_clients() if client.knows(name)), None)


_local = None
_local_k8s = None


def blob_store(uri, current=None, client=None):
    from zenthros.jobs.http import HTTPJobClient, PresignedBlobStore
    from zenthros.remote import carried

    current = current or settings()
    if carried.carries(uri):
        return carried.CarriedStore(client or job_client(current))
    if current.blob_store is not None and not (client is not None and client.transport == "local"):
        return current.blob_store
    client = client or job_client(current)
    if client.transport == "local":
        from zenthros.core.storage import FileBlobStore

        return FileBlobStore()
    if isinstance(client, HTTPJobClient):
        if _own_bucket(uri, current, client):
            from zenthros import userstore

            return userstore.blobs()
        # Through zenthros-api the caller holds a token, not cloud keys.
        return PresignedBlobStore(client)
    from zenthros.core.storage import store_for

    return store_for(uri)


def _own_bucket(uri, current, client):
    """Whether uri is in a bucket of the caller's own (current.store), not
    the hub's: there this process reads and writes with its own
    credentials, as the hub cannot sign for it."""
    if not current.store:
        return False
    uri = str(uri or "")
    hub = client.store.rstrip("/")
    if hub and (uri == hub or uri.startswith(hub + "/")):
        return False
    return uri == current.store or uri.startswith(current.store + "/")


@dataclass
class Secret:
    """A secret the job reads, never its value: the controller resolves it
    where the job runs. Pass as run(secrets={NAME: Secret...}); NAME is the
    environment variable it becomes, or a path ("/run/secrets/gcp.json")
    for a file."""

    type: str
    name: str = ""
    key: str = ""
    namespace: str = ""
    secrets_manager: Optional[S.AWSSecretsManager] = None
    payload: Optional[S.EncryptedPayload] = None
    # on="local": the value, in this process's memory only, until the job's
    # container is up: its agent has a new key each time it starts.
    local_value: Optional[bytes] = field(default=None, repr=False)

    @classmethod
    def aws(cls, secret_id, key="", region="", version=""):
        """An AWS Secrets Manager secret; key picks one field of a JSON secret."""
        return cls("aws-secrets-manager", secrets_manager=S.AWSSecretsManager(
            secret_id=secret_id, json_key=key, region=region, version_id=version))

    @classmethod
    def k8s(cls, name, key, namespace=""):
        """A key of a Kubernetes Secret."""
        return cls("k8s-secret", name=name, key=key, namespace=namespace)

    @classmethod
    def workspace(cls, name):
        """A secret of the Workspace, set once with ws.secrets[name] = value:
        the controller seals it to the machine the job goes to as it sends
        it, so it reaches a template's machine or one that wakes later,
        which no secret sealed here can. The spec names it, nothing more."""
        return cls("workspace-secret", name=workspace_secret_name(name))

    @classmethod
    def encrypted(cls, value=None, *, from_env="", from_file="", on=None, recipients=(), zctl=None):
        """A value sealed here to the age keys of the machines that read it
        (encrypted-payload): the only secret a container backend, or a
        RunPod or Vast.ai machine, resolves. The spec carries ciphertext;
        only those machines' agents can open it.

        The keys are those of the machine on= names (or the age1 recipients
        given): a machine made later has its own and cannot open it. Seal
        once the machine is ready, and run on it. Without zenthros-api (an
        administrator's kubeconfig) a name is a pool's: its Ready machines'
        keys. on="local" is this computer's container: sealed as the job
        starts there, and nothing else can read it.

        It is sealed here, in this process (zenthros.seal, with
        cryptography: pip install 'zenthros[seal]'), when the keys are known:
        recipients=, or through zenthros-api the machine's, from the API's
        list of the Workspace's machines. Nothing but the ciphertext leaves
        this process. Otherwise zctl seals it, on this machine: the value is
        given to zctl through its environment, never its arguments, and
        never printed."""
        sources = [bool(value is not None), bool(from_env), bool(from_file)]
        if sum(sources) != 1:
            raise errors.ValidationError("Secret.encrypted: one of value, from_env or from_file")
        if (on is None) == (not recipients):
            raise errors.ValidationError("Secret.encrypted: one of on= or recipients=")
        if on in ("local", "local-k8s"):
            from zenthros import seal

            if not seal.available():
                raise errors.ValidationError("a secret here is sealed by this SDK: pip install 'zenthros[seal]'")
            return cls("encrypted-payload", local_value=_plaintext(value, from_env, from_file))
        pool, backend, machine = sealed_to(on)
        found = _api_recipients(pool, backend, machine)
        if found is not None:
            backend, recipients = "", found
        from zenthros import seal

        if recipients and seal.available():
            recipients = list(dict.fromkeys(recipients))
            ciphertext = seal.seal(_plaintext(value, from_env, from_file), recipients)
            return cls("encrypted-payload", payload=S.EncryptedPayload(
                ciphertext=ciphertext, key_id=seal.key_id(recipients[0]), recipients=recipients))
        from zenthros.zctl import Zctl

        args = ["secrets", "encrypt", "-o", "json", "--mode", "env", "--env-name", "ZENTHROS_SECRET"]
        env = {}
        if value is not None:
            env[_PLAINTEXT_ENV] = value if isinstance(value, str) else bytes(value).decode()
            args += ["--from-env", _PLAINTEXT_ENV]
        elif from_env:
            args += ["--from-env", from_env]
        else:
            args += ["--from-file", str(from_file)]
        if pool:
            args += ["--pool", pool]
        elif backend:
            args += ["--destination", backend]
        for recipient in recipients:
            args += ["--recipient", recipient]
        out = (zctl or Zctl())(*args, env=env)
        try:
            ref = json.loads(out)
            sealed = ref["encryptedPayload"]
            payload = S.EncryptedPayload(ciphertext=sealed["ciphertext"], key_id=sealed.get("keyId", ""),
                                         recipients=list(sealed["recipients"]))
        except (ValueError, KeyError, TypeError) as exc:
            raise errors.ZctlError(f"zctl secrets encrypt -o json: unexpected output ({exc})") from None
        return cls("encrypted-payload", payload=payload)

    def source(self, target):
        if target.startswith("/"):
            where = S.Materialization(mode="file", path=target)
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target):
            where = S.Materialization(mode="env", env_name=target)
        else:
            raise errors.ValidationError(f"secret {target!r}: an environment variable name, or an absolute path")
        return S.SecretSource(type=self.type, name=self.name, key=self.key, namespace=self.namespace,
                              aws_secrets_manager=self.secrets_manager, encrypted_payload=self.payload,
                              materialization=where)


# A Workspace secret's name: a DNS label of at most 47 characters (the hub's
# rule, operator api/v1 WorkspaceSecretName).
_WORKSPACE_SECRET_MAX = 47
_WORKSPACE_SECRET_NAME = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?")


def workspace_secret_name(name):
    """name, if the hub can keep a Workspace secret by it."""
    if not isinstance(name, str) or len(name) > _WORKSPACE_SECRET_MAX \
            or not _WORKSPACE_SECRET_NAME.fullmatch(name):
        raise errors.ValidationError(
            f"Workspace secret {name!r}: lowercase letters, digits and '-', at most "
            f"{_WORKSPACE_SECRET_MAX} characters, such as 'hf-token'")
    return name


def _plaintext(value, from_env, from_file):
    """The secret's bytes, read here and kept here."""
    from zenthros.seal import MAX_PLAINTEXT

    if value is not None:
        return value.encode() if isinstance(value, str) else bytes(value)
    if from_env:
        found = os.environ.get(from_env, "")
        if not found:
            raise errors.ValidationError(f"Secret.encrypted(from_env={from_env!r}): that variable is empty or unset")
        return found.encode()
    with open(from_file, "rb") as fh:
        data = fh.read(MAX_PLAINTEXT + 1)
    return data


def _through_api(client=None):
    from zenthros.jobs.http import HTTPJobClient

    return isinstance(client if client is not None else job_client(), HTTPJobClient)


def sealed_to(on):
    """(pool, backend, its Backend as the API listed it) of on= for a secret
    sealed to its machine: a machine, or its name through zenthros-api;
    without it a name is a pool's."""
    if on is None:
        return "", "", None
    if not isinstance(on, str):
        if getattr(on, "pool", ""):
            return on.pool, "", None
        return "", on.id, None
    if not _through_api():
        return on, "", None
    from zenthros.view import Backend

    for info in job_client().backends():
        found = Backend.from_api(info)
        if on in (found.name, found.id):
            return "", found.id, found
    raise errors.ValidationError(f"on={on!r}: the Workspace has no machine called that (zenthros.targets())")


def _api_recipients(pool, backend, machine=None):
    """The machine's age keys from zenthros-api's list of the Workspace's
    machines (machine: as sealed_to already read it); None without an API
    connection, where zctl looks them up."""
    if not (pool or backend):
        return None
    client = job_client()
    if not _through_api(client):
        return None
    if machine is not None:
        if not machine.age_recipients:
            raise errors.ValidationError(f"{machine.name or backend} has no key yet: wait until it is ready")
        return list(machine.age_recipients)
    if pool:
        raise errors.ValidationError(
            f"{pool} is a template's machine with no fixed keys: seal to one running machine (on=), or use "
            "Secret.workspace(name), which the hub seals to whichever machine the job lands on")
    from zenthros.view import Backend

    for info in client.backends():
        found = Backend.from_api(info)
        if found.id != backend:
            continue
        if not found.age_recipients:
            raise errors.ValidationError(f"{found.name or backend} has no key yet: wait until it is ready")
        # During a key rotation an agent advertises both: either opens it.
        return list(found.age_recipients)
    raise errors.ValidationError(f"{backend!r} is not a machine of the Workspace (zenthros.targets())")


def secret_sources(secrets):
    sources = []
    for target, secret in sorted((secrets or {}).items()):
        if not isinstance(secret, Secret):
            raise errors.ValidationError(
                f"secrets[{target!r}] is a {type(secret).__name__}; pass a zenthros.Secret "
                "(a value would sit in the job's spec in clear text: use env= for what is not secret)")
        if secret.local_value is not None:
            raise errors.ValidationError(f"secrets[{target!r}] was sealed with on='local', which works only on this "
                                         "computer: seal it to where this job runs")
        sources.append(secret.source(target))
    return sources


def split_local_secrets(secrets):
    """(the rest, those sealed with on="local"): a local job's sources and
    what its container's agent seals as it starts."""
    secrets = secrets or {}
    held = {target: secret for target, secret in secrets.items() if isinstance(secret, Secret) and secret.local_value is not None}
    return {target: secret for target, secret in secrets.items() if target not in held}, held


_PLAINTEXT_ENV = "ZENTHROS_SECRET_PLAINTEXT"

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def seconds(duration, what="timeout"):
    """90, "90s", "30m", "2h", "1d" -> whole seconds (None stays None);
    what names the option in the error."""
    if duration is None:
        return None
    if isinstance(duration, (int, float)):
        value = float(duration)
    else:
        match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", str(duration))
        if not match:
            raise errors.ValidationError(f"{what} {duration!r}: seconds, or a number with s, m, h or d")
        value = float(match.group(1)) * _UNITS[match.group(2) or "s"]
    if value <= 0:
        raise errors.ValidationError(f"{what} {duration!r} must be positive")
    return max(1, int(round(value)))
