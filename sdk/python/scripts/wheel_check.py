# SPDX-License-Identifier: Apache-2.0

"""make wheel-check: build the wheel, install it alone, and import all of it.

An editable install imports from the source tree, so a module the wheel
does not carry (pipeline.py, 2026-11-06) passed every test. Here the wheel
is installed in a throwaway venv outside the repository and checked with
`python -I`, which never puts the current directory on sys.path.
"""

import os
import pathlib
import subprocess
import sys
import tempfile
import zipfile

SDK = pathlib.Path(__file__).resolve().parents[1]
UV = os.environ.get("UV", "uv")

PROBE = """
import importlib, pkgutil, sys, zenthros
assert not zenthros.__file__.startswith(sys.argv[1]), zenthros.__file__
names = [m.name for m in pkgutil.walk_packages(zenthros.__path__, "zenthros.") if not m.name.endswith(".__main__")]
for name in names:
    importlib.import_module(name)
print(len(names), "modules of zenthros import from the wheel")
"""


def run(*args: str, **kw) -> None:
    subprocess.run(args, check=True, **kw)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="zenthros-wheel-") as tmp:
        out = pathlib.Path(tmp)
        run(sys.executable, "-m", "build", "--wheel", "--outdir", str(out / "dist"), str(SDK), stdout=subprocess.DEVNULL)
        (wheel,) = (out / "dist").glob("zenthros-*.whl")
        shipped = {n for n in zipfile.ZipFile(wheel).namelist() if n.endswith(".py")}
        source = {p.relative_to(SDK).as_posix() for p in (SDK / "zenthros").rglob("*.py") if "__pycache__" not in p.parts}
        missing = sorted(source - shipped)
        if missing:
            print("the wheel lacks:", *missing, sep="\n  ")
            return 1
        venv = out / "venv"
        run(UV, "venv", "--quiet", "--python", sys.executable, str(venv))
        python = str(venv / "bin" / "python")
        run(UV, "pip", "install", "--quiet", "--python", python, f"{wheel}[s3,gcs,seal]")
        run(python, "-I", "-c", PROBE, str(SDK), cwd=tmp)
        run(str(venv / "bin" / "zenthros"), "--help", cwd=tmp, stdout=subprocess.DEVNULL)
        print(f"{wheel.name}: {len(shipped)} files, every module imports, zenthros --help runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
