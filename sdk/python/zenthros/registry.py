# SPDX-License-Identifier: Apache-2.0

"""Whether a registry shows an image to anyone: the question of who can read it,
asked with no login. Only HEADs, which no registry counts against its limits."""

import json
import re
import urllib.error
import urllib.parse
import urllib.request

from zenthros.imagebuild import _is_local, _repository_of, host_of

_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.docker.distribution.manifest.v2+json",
))
TIMEOUT = 10


def _where(ref):
    """(the registry's host, the repository in it, the tag or digest) of ref."""
    host = host_of(ref)
    repository = _repository_of(ref)
    if repository.startswith(host + "/"):
        repository = repository[len(host) + 1:]
    reference = ref.partition("@")[2] or (ref.rpartition(":")[2] if ":" in ref.rpartition("/")[2] else "latest")
    if host == "docker.io":
        host = "registry-1.docker.io"
        repository = repository if "/" in repository else f"library/{repository}"
    return host, repository, reference


def _head(url, token=""):
    headers = {"Accept": _ACCEPT, **({"Authorization": f"Bearer {token}"} if token else {})}
    request = urllib.request.Request(url, headers=headers, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as answer:
            return answer.status, ""
    except urllib.error.HTTPError as err:
        return err.code, err.headers.get("WWW-Authenticate", "")


def _get(url, token=""):
    headers = {"Accept": _ACCEPT, **({"Authorization": f"Bearer {token}"} if token else {})}
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=TIMEOUT) as answer:
        return json.load(answer)


def _anonymous_token(challenge):
    """The token a registry gives anyone for what its 401 asked, or ""."""
    fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
    if not challenge.lower().startswith("bearer") or "realm" not in fields:
        return ""
    query = urllib.parse.urlencode({key: fields[key] for key in ("service", "scope") if key in fields})
    try:
        with urllib.request.urlopen(f"{fields['realm']}?{query}", timeout=TIMEOUT) as answer:
            body = json.load(answer)
    except urllib.error.HTTPError:
        return ""
    return body.get("token") or body.get("access_token") or ""


def anyone_can_read(ref):
    """True if the registry shows ref with no login, False if it does not (a
    private image and a missing one read alike), None if it could not be asked."""
    if _is_local(ref):
        return False
    host, repository, reference = _where(ref)
    url = f"https://{host}/v2/{repository}/manifests/{reference}"
    try:
        status, challenge = _head(url)
        if status == 401:
            token = _anonymous_token(challenge)
            if not token:
                return False
            status, _ = _head(url, token)
    except (urllib.error.URLError, OSError, ValueError):
        return None
    return status == 200


def architectures(ref):
    """The architectures a registry publishes ref for, with no login; None if it
    cannot be told. A list of platforms is one GET; a single image, its config too."""
    if _is_local(ref):
        return None
    host, repository, reference = _where(ref)
    root = f"https://{host}/v2/{repository}"
    url = f"{root}/manifests/{reference}"
    try:
        status, challenge = _head(url)
        token = _anonymous_token(challenge) if status == 401 else ""
        if status not in (200, 401) or (status == 401 and not token):
            return None
        manifest = _get(url, token)
        if "manifests" in manifest:
            return sorted({entry["platform"]["architecture"] for entry in manifest["manifests"]
                           if entry.get("platform", {}).get("architecture") not in (None, "unknown")})
        return [_get(f"{root}/blobs/{manifest['config']['digest']}", token)["architecture"]]
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        return None
