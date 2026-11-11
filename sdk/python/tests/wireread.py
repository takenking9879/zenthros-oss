# SPDX-License-Identifier: Apache-2.0

"""Answers that note every key read, for the wire tests: a key the SDK reads
that a sample lacks is one the other side never sends."""


class Read(dict):
    """An answer's object that notes every key looked up, by its path."""

    def __init__(self, items, path, seen):
        super().__init__(items)
        self._path, self._seen = path, seen

    def _note(self, key):
        self._seen.add(join(self._path, key))

    def __getitem__(self, key):
        self._note(key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        self._note(key)
        return super().get(key, default)

    def __contains__(self, key):
        self._note(key)
        return super().__contains__(key)

    def setdefault(self, key, default=None):
        self._note(key)
        return super().setdefault(key, default)


def join(path, key):
    return f"{path}.{key}" if path else str(key)


def wrap(doc, path, seen):
    """doc with every object a Read noting into seen."""
    if isinstance(doc, dict):
        return Read({k: wrap(v, join(path, k), seen) for k, v in doc.items()}, path, seen)
    if isinstance(doc, list):
        return [wrap(v, path + "[]", seen) for v in doc]
    return doc


def paths(doc, path=""):
    """Every key path of doc: dots, and [] for a list's elements."""
    out = {path}
    if isinstance(doc, dict):
        for k, v in doc.items():
            out |= paths(v, join(path, k))
    elif isinstance(doc, list):
        for v in doc:
            out |= paths(v, path + "[]")
    return out
