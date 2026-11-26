# SPDX-License-Identifier: Apache-2.0

"""What never leaves this computer on its own: the one list, for what a call
carries, what a snapshot takes, what a local container mounts and what an
upload sends. Credentials stay home unless an ignore file names them with
"!": whatever carries a file makes it readable by whoever reads the store.

A call's code (code_dir) leaves notebooks and data out too (NOT_CODE): a
notebook's function travels pickled, and data goes by upload= or a bucket.
"!" in an ignore file sends one.
"""

import fnmatch
import os

from zenthros import errors

CREDENTIALS = (
    "*.env", ".env", ".env.*", "*.pem", "*.key", "*.p12", "*.pfx", "id_rsa*", "id_ed25519*", "kubeconfig", "*.kubeconfig",
    "*.tfvars", "*.tfstate", "*credentials*.json", "*service-account*.json", "client_secret*.json",
)

NOT_CODE = (
    "*.ipynb", "*.csv", "*.tsv", "*.parquet", "*.feather", "*.arrow", "*.pkl", "*.pickle", "*.joblib", "*.npy",
    "*.npz", "*.pt", "*.pth", "*.ckpt", "*.safetensors", "*.onnx", "*.h5", "*.hdf5", "*.png", "*.jpg", "*.jpeg",
    "*.webp", "*.gif", "*.xlsx", "*.xls", "*.zip", "*.tar", "*.gz",
)


def is_credential(path):
    """Whether path's name is one of the credentials' (*.env, a key, ...)."""
    name = os.path.basename(str(path).rstrip("/"))
    return any(fnmatch.fnmatch(name, pattern) for pattern in CREDENTIALS)


def refuse_path(path):
    """A path named to go somewhere, refused if it is a credential."""
    if is_credential(path):
        raise errors.ValidationError(
            f"{path} looks like a credential, and it does not leave this computer that way: pass it with secrets=")
