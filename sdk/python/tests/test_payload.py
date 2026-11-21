# SPDX-License-Identifier: Apache-2.0

"""What a job and its caller exchange through a store (a call, a result) is
made and read in one place, zenthros.remote.payload: packed with zstd when
that makes it smaller. With no bucket a table of 744 KiB did not come back,
though it packs to a fraction of that (observaciones P20)."""

import ast
import os
import pathlib
import pickle
import unittest

from zenthros import remote
from zenthros.core.storage import MemoryBlobStore
from zenthros.remote import payload

from test_remote import STORE, ExecutingClient

ZSTD_FRAME = b"\x28\xb5\x2f\xfd"


def a_table(rows):
    """A table as a notebook makes one: labels that repeat, prices on a grid."""
    return {"zone": [f"zone-{i % 40}" for i in range(rows)],
            "price": [float(100_000 + (i * 37) % 5_000) for i in range(rows)],
            "rooms": [i % 6 for i in range(rows)]}


def rows_of(tables):
    return sum(len(table["zone"]) for table in tables)


class Payload(unittest.TestCase):
    def test_a_record_that_packs_goes_packed_and_reads_back(self):
        record = {"ok": True, "value": pickle.dumps(a_table(20_000))}
        raw = payload.dump(record)
        self.assertEqual(raw[:4], ZSTD_FRAME)
        self.assertLess(len(raw), len(pickle.dumps(record, protocol=4)) // 2)
        self.assertEqual(payload.load(raw), record)

    def test_random_bytes_stay_as_they_were(self):
        record = {"ok": True, "value": os.urandom(64 << 10)}
        raw = payload.dump(record)
        self.assertEqual(raw, pickle.dumps(record, protocol=4))
        self.assertEqual(payload.load(raw), record)

    def test_a_small_record_is_not_framed(self):
        record = {"ok": True, "value": b"x" * 100}
        self.assertEqual(payload.dump(record), pickle.dumps(record, protocol=4))

    def test_a_packed_upstream_reaches_the_downstream(self):
        store = MemoryBlobStore()
        client = ExecutingClient(store)
        with remote.configure(store=STORE, client=client, blob_store=store, on="demo-ml"):
            up = remote.map(a_table, [5_000, 6_000], code_dir=None)
            down = remote.run(rows_of, up, code_dir=None)
        self.assertEqual(down.result(stream_logs=False), 11_000)
        self.assertEqual(store.get(f"{up.prefix}/results/0.pkl")[:4], ZSTD_FRAME)
        self.assertEqual([len(t["zone"]) for t in up.result(stream_logs=False)], [5_000, 6_000])

    def test_bytes_from_a_store_are_unpickled_only_in_payload(self):
        """A reader of its own read a packed record as a pickle and failed.
        A field of a record (its value, its call) is pickled by the caller
        on its own and stays as it is."""
        package = pathlib.Path(payload.__file__).parents[1]
        found = []
        for path in sorted(package.rglob("*.py")):
            if path == pathlib.Path(payload.__file__):
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr in ("loads", "load") and isinstance(node.func.value, ast.Name)
                        and node.func.value.id in ("pickle", "pickler", "cloudpickle")
                        and not (node.args and isinstance(node.args[0], ast.Subscript))):
                    found.append(f"{path.relative_to(package)}:{node.lineno}")
        self.assertEqual(found, [], "a record from a store opens with zenthros.remote.payload.load")


if __name__ == "__main__":
    unittest.main()
