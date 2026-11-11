# SPDX-License-Identifier: Apache-2.0

"""zenthros.install: packages every function on a machine sees (R5).

    zenthros.install("my-vm", requirements="requirements.txt")
    zenthros.install("my-vm", packages=["xgboost", "scikit-learn"])
    $ zenthros install my-vm -r requirements.txt

The machine keeps them as its base for this Python's minor (on the machine:
zenthros.remote.envs.install_base). The same list again does nothing ("up
to date"); another is built beside the current one and swapped in at once,
so a job running on the old one keeps it. A function's own packages
(@zenthros.cloud(packages=...)) come before the base and win where they
differ. A cluster's pods start fresh each time: there, give the function
its packages or an image.
"""

import os
import time

from zenthros import console, errors

TIMEOUT = 1800


def requirements_text(requirements=None, packages=None):
    """What to install: the requirements file's lines, then packages."""
    lines = []
    if requirements:
        path = os.path.expanduser(str(requirements))
        try:
            with open(path, encoding="utf-8") as fh:
                lines.extend(fh.read().splitlines())
        except OSError as exc:
            raise errors.ValidationError(f"cannot read {requirements}: {exc.strerror}") from None
    if isinstance(packages, str):
        packages = [packages]
    lines.extend(str(p) for p in packages or ())
    return "".join(line + "\n" for line in lines)


def said(name, answer, seconds):
    """The one line install prints: what changed on the machine."""
    head = f"{name} (py{answer.get('python', '?')}): "
    if answer.get("up_to_date"):
        return head + "up to date"
    parts = [f"+ {n} {v}" for n, v in answer.get("added") or []]
    parts += [f"~ {n} {old} → {new}" for n, old, new in answer.get("changed") or []]
    parts += [f"- {n} {v}" for n, v in answer.get("removed") or []]
    from zenthros.attaching import seconds_text

    return head + ("  ".join(parts) if parts else "no change") + f"   ✓ {seconds_text(seconds)}"


def install(name, requirements=None, packages=None, out=None, timeout=TIMEOUT):
    """Install packages on the machine name for every function it runs:
    requirements (a requirements.txt) and packages (names, pins). Returns
    what changed: {"python", "up_to_date", "added", "removed", "changed"}."""
    from zenthros import machines
    from zenthros.remote import envs, run

    text = envs.normal_requirements(requirements_text(requirements, packages))
    if not text:
        raise errors.ValidationError("nothing to install: give requirements= (a requirements.txt) or packages")
    client = machines._api()
    found = machines._find(client, name)
    if found is None:
        names = sorted(b.name or b.id for b in machines._backends(client))
        raise errors.ValidationError(f"no machine named {name}: it has {names}")
    if found.type == "k8s":
        raise errors.ValidationError(f"{name} is a cluster: its pods start fresh each time. "
                                     "Give the function its packages (@zenthros.cloud(packages=...)) or an image=")
    started = time.monotonic()
    answer = run(envs.install_base, text, on=name).result(timeout=timeout, stream_logs=False)
    console.show(said(name, answer, time.monotonic() - started), out)
    return answer
