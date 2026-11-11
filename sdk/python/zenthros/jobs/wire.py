# SPDX-License-Identifier: Apache-2.0

"""Who the SDK is and which format of zenthros-api it speaks, on every
request to a hub (operator/pkg/api/version.go). A hub of another format
answers 426 with what to install: errors.WireMismatch.

WIRE moves with the hub's: operator/pkg/api/testdata/wire/version.json holds
the number, and tests/test_wire_version.py holds this one to it.
"""

import platform
import sys
from importlib import metadata

WIRE = 2


def sdk_version():
    """This SDK's version, as installed; "dev" from a tree that is not."""
    try:
        return metadata.version("zenthros")
    except metadata.PackageNotFoundError:
        return "dev"


USER_AGENT = (f"zenthros-sdk/{sdk_version()} python/{sys.version_info.major}.{sys.version_info.minor} "
              f"{platform.system().lower() or sys.platform}")

# What every request to a hub carries.
HEADERS = {"User-Agent": USER_AGENT, "Zenthros-Wire": str(WIRE)}
