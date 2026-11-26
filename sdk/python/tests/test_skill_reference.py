# SPDX-License-Identifier: Apache-2.0

"""The skill an agent reads (zenthros/_data/skill): SKILL.md short, with the
rules and an index, and reference/*.md with every public name of the SDK.

Pinned, with no tokens spent and no AI (observaciones 19, 21):
- every name of zenthros.__all__ has its "## zenthros.<name>" section, in
  one reference file, and every parameter it takes is named there: a
  parameter added and not documented fails here;
- a class's public methods each have their "### <Class>.<method>" section,
  with their parameters;
- SKILL.md links every reference file, and only files that are there;
- every Python block of the skill parses, and every call it makes to a
  public name fits that name's real signature: an example with a parameter
  made up or renamed fails here;
- every reason code SKILL.md's table names is one api/v1 has.

Whether an AI understands it is measured apart (benchmarks/agentevals).
"""

import ast
import inspect
import re
import unittest
from pathlib import Path

import zenthros
from zenthros import attaching
from zenthros.remote import call as remote_call
from zenthros.remote.options import Settings

SKILL = Path(zenthros.__file__).parent / "_data" / "skill"
REFERENCE = SKILL / "reference"
REASONS = Path(__file__).resolve().parents[3] / "operator" / "api" / "v1" / "reasons.go"

# Classes a person gets and never builds: their constructor is the SDK's.
# Their methods are documented; their constructor's parameters are not.
BUILT_BY_THE_SDK = {
    "RemoteCall": "run(), submit(), map() and get() return it",
    "LocalBackend": "create('local') makes it",
    "Machine": "create() and attach() return it",
    "JobFailed": "raised by the SDK",
    "RemoteError": "raised by the SDK",
    "ZenthrosError": "raised by the SDK",
    "JobEnv": "JobEnv.current() reads it",
    "CloudFunction": "@zenthros.cloud makes it",
    "Secret": "built by its class methods (Secret.workspace, ...)",
}
# Methods that are the SDK's own and not for a person.
NOT_FOR_PEOPLE = {
    "Secret.source": "the SDK's: what the spec carries for a target",
    "Artifact.from_dict": "the SDK's: a stored record read back",
    "Artifact.to_dict": "the SDK's: a record to store",
    "Checkpointer.configured": "the SDK's: whether a store was given",
    "Task.phase_name": "the SDK's: its phase's name",
    "Task.run": "the SDK's: what run() calls on the machine",
}
# What the modules among the public names are for a person.
MODULE_FUNCTIONS = {
    "cache": ["find", "ls", "show", "rm", "clean", "sync", "export_key", "import_key"],
    "files": ["read_bytes", "write_bytes", "ls", "open"],
    "preemption": ["notice", "requested"],
}
# Where a name's **kwargs go: their parameters are documented as that one's.
FORWARDS = {
    "attach": attaching.attach,
}
# configure's settings a person sets; the others are the SDK's own.
SETTINGS_OF_THE_SDK = {"namespace", "client", "blob_store", "ship_sdk", "untrusted_host_secrets", "untrusted_host_spawn"}


def _sections():
    """{heading: (file, its text up to the next heading of its level or above)}."""
    found = {}
    for path in sorted(REFERENCE.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        # A name's heading is the name alone (zenthros.run, RemoteCall.result).
        heads = list(re.finditer(r"^(#{2,3}) ([A-Za-z_]\w*(?:\.\w+)+)\s*$", text, re.M))
        for i, head in enumerate(heads):
            level = len(head.group(1))
            end = next((h.start() for h in heads[i + 1:] if len(h.group(1)) <= level), len(text))
            if head.group(2) in found:
                raise AssertionError(f"{head.group(2)} has two sections: {found[head.group(2)][0].name} and {path.name}")
            found[head.group(2)] = (path, text[head.end():end])
    return found


def _parameters(fn):
    """The parameters of fn a person passes: not self, cls, *args, **kwargs
    or the SDK's own (a leading _)."""
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return []
    return [p.name for p in signature.parameters.values()
            if p.name not in ("self", "cls") and not p.name.startswith("_")
            and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)]


def _public_methods(cls):
    for name, value in vars(cls).items():
        if name.startswith("_") or f"{cls.__name__}.{name}" in NOT_FOR_PEOPLE:
            continue
        if isinstance(value, (staticmethod, classmethod)):
            yield name, value.__func__
        elif isinstance(value, property):
            yield name, None
        elif callable(value):
            yield name, value


def _named(text, name):
    return f"`{name}`" in text or f"`{name}=" in text


def _blocks():
    """(file, Python source) of every ```python block of the skill."""
    for path in sorted(SKILL.rglob("*.md")):
        for block in re.findall(r"^```python\n(.*?)^```", path.read_text(encoding="utf-8"), re.M | re.S):
            yield path, block


def _target(node):
    """The public object a call names (zenthros.X, zenthros.X.Y), or None."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name) or node.id != "zenthros" or not parts:
        return None
    obj = zenthros
    for part in reversed(parts):
        obj = getattr(obj, part, None)
        if obj is None:
            return None
    return obj


# What a CloudFunction's methods take besides their own: run()'s options.
RUN = set(_parameters(remote_call.run)) | {"name"}
OPTIONS_ARE_RUNS = {zenthros.cloud, zenthros.map, zenthros.serve}
CLOUD_METHODS = {"remote", "submit", "map", "serve", "with_options", "local"}


class ReferenceTest(unittest.TestCase):
    def test_every_public_name_has_its_section_with_every_parameter(self):
        sections = _sections()
        for name in zenthros.__all__:
            with self.subTest(name=name):
                head = f"zenthros.{name}"
                self.assertIn(head, sections, f"no '## {head}' in {REFERENCE}")
                text = sections[head][1]
                obj = getattr(zenthros, name)
                if inspect.ismodule(obj):
                    for fn in MODULE_FUNCTIONS[name]:
                        self.assertIn(f"{head}.{fn}", sections, f"no '### {head}.{fn}'")
                        for p in _parameters(getattr(obj, fn)):
                            self.assertTrue(_named(sections[f"{head}.{fn}"][1], p), f"{head}.{fn}: `{p}` is not named")
                    continue
                params = _parameters(FORWARDS.get(name, obj))
                if inspect.isclass(obj) and name in BUILT_BY_THE_SDK:
                    params = []
                for p in params:
                    self.assertTrue(_named(text, p), f"{head}: `{p}` is not named in its section")
                if name == "configure":
                    for field in set(Settings.__dataclass_fields__) - SETTINGS_OF_THE_SDK:
                        self.assertTrue(_named(text, field), f"{head}: the setting `{field}` is not named")
                if obj in OPTIONS_ARE_RUNS:
                    self.assertIn("zenthros.run", text, f"{head}: its options are run()'s: say so, with a link")
                if inspect.isclass(obj):
                    for method, fn in _public_methods(obj):
                        if fn is None:
                            self.assertTrue(_named(text, method) or f"{name}.{method}" in sections,
                                            f"{head}: the property `{method}` is not named")
                            continue
                        key = f"{name}.{method}"
                        self.assertIn(key, sections, f"no '### {key}'")
                        for p in _parameters(fn):
                            self.assertTrue(_named(sections[key][1], p), f"{key}: `{p}` is not named")

    def test_skill_links_every_reference_file_and_only_files_there(self):
        text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
        linked = set(re.findall(r"\]\((reference/[^)#]+\.md)", text))
        there = {f"reference/{p.name}" for p in REFERENCE.glob("*.md")}
        self.assertTrue(there, "no reference files")
        self.assertEqual(linked - there, set(), "SKILL.md links files that are not there")
        self.assertEqual(there - linked, set(), "SKILL.md does not link these")
        for path in REFERENCE.glob("*.md"):
            for target in re.findall(r"\]\(([^)#:]+\.md)", path.read_text(encoding="utf-8")):
                self.assertTrue((path.parent / target).exists(), f"{path.name} links {target}, which is not there")

    def test_every_python_block_parses_and_fits_the_real_signatures(self):
        for path, block in _blocks():
            with self.subTest(file=path.name, block=block[:60]):
                tree = ast.parse(block)
                cloud = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                         and any(_target(d.func if isinstance(d, ast.Call) else d) is zenthros.cloud for d in n.decorator_list)}
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    keywords = [k.arg for k in node.keywords if k.arg]
                    positional = sum(not isinstance(a, ast.Starred) for a in node.args)
                    spread = any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords)
                    func = node.func
                    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id in cloud:
                        self.assertIn(func.attr, CLOUD_METHODS, f"{func.value.id}.{func.attr}: a CloudFunction has no such method")
                        own = set(_parameters(getattr(zenthros.CloudFunction, func.attr)))
                        # f.serve(...) is zenthros.serve's: its ports, stop_after, params.
                        own |= set(_parameters(zenthros.serve)) if func.attr == "serve" else set()
                        unknown = set(keywords) - own - RUN
                        self.assertFalse(unknown, f"{func.value.id}.{func.attr}({', '.join(sorted(unknown))}=): not an option")
                        continue
                    obj = _target(func)
                    if obj is None or not callable(obj):
                        continue
                    where = ast.unparse(func)
                    if obj in OPTIONS_ARE_RUNS:
                        unknown = set(keywords) - set(_parameters(obj)) - RUN
                        self.assertFalse(unknown, f"{where}({', '.join(sorted(unknown))}=): not an option")
                        continue
                    if obj is zenthros.configure:
                        unknown = set(keywords) - set(Settings.__dataclass_fields__)
                        self.assertFalse(unknown, f"zenthros.configure({', '.join(sorted(unknown))}=): no such setting")
                        continue
                    signature = inspect.signature(attaching.attach if obj is zenthros.attach else obj)
                    if spread:
                        names = {p.name for p in signature.parameters.values()}
                        takes_any = any(p.kind is p.VAR_KEYWORD for p in signature.parameters.values())
                        self.assertTrue(takes_any or set(keywords) <= names, f"{where}: {keywords}")
                        continue
                    try:
                        signature.bind_partial(*range(positional), **dict.fromkeys(keywords))
                    except TypeError as err:
                        self.fail(f"{where}(...) in {path.name}: {err}")

    def test_every_reason_skill_names_is_one_there_is(self):
        rows = [line for line in (SKILL / "SKILL.md").read_text(encoding="utf-8").splitlines() if line.startswith("|")]
        named = set(re.findall(r"`([A-Z][A-Z_]{3,})`", "\n".join(rows)))
        there = set(re.findall(r'^\s*Reason\w+\s*=\s*"([A-Z_]+)"', REASONS.read_text(encoding="utf-8"), re.M))
        self.assertTrue(named, "the table names no reason")
        self.assertEqual(named - there, set(), f"SKILL.md names reasons api/v1 does not have ({REASONS})")


if __name__ == "__main__":
    unittest.main()
