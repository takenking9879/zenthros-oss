# SPDX-License-Identifier: Apache-2.0

"""Run code on a Zenthros machine and get its result back.

    zenthros.configure(store="s3://acme/calls")                # once: where calls are stored

    call = zenthros.run(fn, *args, on="my-vm", gpu="A10G")      # a function or a zenthros.Task
    call = zenthros.run(["python", "prep.py"], on="my-vm")      # a command
    calls = zenthros.map(fn, items, on="aws-spot")               # a machine per item
    zenthros.gather(call, calls)                                 # results, in order
    zenthros.get("zr-train-1a2b3c").status()                     # from another process
    llm = zenthros.serve("vllm serve m --port 8000", on="gpu-box", port=8000)
    llm.url()                                                    # where it answers, once it does

    @zenthros.cloud(gpu="A10G")
    def train(uri): ...
    train(uri)          # runs here, as before
    train.remote(uri)   # runs on your machine (on=, or the only one) and returns its value

bundle is what travels (the pickled call, the project's .py files, the SDK's
core); call is the caller's side; runner the machine's; options the settings
and values run() takes.
"""

from zenthros.remote.bundle import (
    MAX_CODE_BYTES,
    is_project_module,
    loaded_packages,
    pack_code,
    python_version,
    sdk_files,
)
from zenthros.remote.call import (
    CloudFunction,
    RemoteCall,
    RemoteError,
    _LogFollower,
    cloud,
    gather,
    get,
    map,
    results_pin,
    run,
    serve,
)
from zenthros.remote.options import PYTHON_ENV, REMOTE_ENV, STORE_ENV, Secret, configure, settings
from zenthros.remote.runlog import last_failure
from zenthros.remote.runner import EXIT_ERROR, EXIT_MISMATCH, EXIT_SUPERSEDED, EXIT_TAMPERED, execute
from zenthros.remote.servicelist import services

__all__ = [
    "CloudFunction", "EXIT_ERROR", "EXIT_MISMATCH", "EXIT_SUPERSEDED", "EXIT_TAMPERED", "MAX_CODE_BYTES", "PYTHON_ENV",
    "REMOTE_ENV", "RemoteCall", "RemoteError", "STORE_ENV", "Secret", "cloud", "configure", "execute", "gather",
    "get", "is_project_module", "loaded_packages", "map", "pack_code", "python_version", "results_pin", "run", "sdk_files",
    "serve", "services", "settings",
]
