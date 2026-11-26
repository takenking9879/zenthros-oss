# SPDX-License-Identifier: Apache-2.0

"""What a function says it takes and leaves, when it says so. A contract travels
with the function, so it holds in or out of a Pipeline. Hints are not read: they
get taken for documentation.

The runner checks what comes out before reporting it: one that fails ends the
call with OUTPUT_CONTRACT, its value reaches neither the next step nor the
cache. A Parquet file's columns come from its footer alone, never its data, and
take pyarrow only if you declared columns."""

import json
import os
import struct
from dataclasses import dataclass, field

from zenthros import errors
from zenthros.artifact import Artifact

FORMATS = ("parquet", "json", "file", "dir")
# The report that says it: a job's status only knows it ended badly.
REPORT = "zenthros.contract"
OUTPUT_CONTRACT = "OUTPUT_CONTRACT"
INPUT_CONTRACT = "INPUT_CONTRACT"
_PARQUET_MAGIC = b"PAR1"
# A JSON file up to this is parsed whole; a larger one by its first character.
_JSON_PARSED_BYTES = 64 << 20
_FOOTER_GUESS = 1 << 20


class ContractError(errors.ValidationError):
    """A value is not what a contract says."""

    reason = OUTPUT_CONTRACT
    hint = "fix what the function leaves, or the contract in its outputs="


class InputContractError(ContractError):
    reason = INPUT_CONTRACT
    hint = "give it what its inputs= says, or change the contract"


@dataclass(frozen=True)
class Contract:
    """A value's format and, for Parquet, the columns it must have (name ->
    type as pyarrow says it). Other columns may be there."""

    format: str
    columns: dict = field(default=None)

    def __post_init__(self):
        if self.format not in FORMATS:
            raise errors.ValidationError(f"contract format {self.format!r}: one of {', '.join(FORMATS)}")
        if self.columns and self.format != "parquet":
            raise errors.ValidationError("columns= is a Parquet file's")

    def to_dict(self):
        return {"format": self.format, **({"columns": dict(self.columns)} if self.columns else {})}


def parquet(columns=None):
    """outputs=zenthros.parquet(columns={"user_id": "int64", "score": "float32"})."""
    return Contract("parquet", dict(columns) if columns else None)


def _one(raw):
    if isinstance(raw, Contract):
        return raw
    if isinstance(raw, str):
        return Contract(raw)
    if isinstance(raw, dict) and "format" in raw:
        return Contract(raw["format"], raw.get("columns"))
    raise errors.ValidationError(f"a contract is 'parquet', 'json', 'file', 'dir' or zenthros.parquet(...); got {raw!r}")


def declared(raw):
    """inputs= or outputs= as plain data: {"format": ...}, or {name: {"format": ...}}."""
    if raw is None:
        return None
    if isinstance(raw, dict) and "format" not in raw:
        return {str(name): _one(item).to_dict() for name, item in raw.items()}
    return _one(raw).to_dict()


def _local_path(ref):
    if isinstance(ref, Artifact):
        ref = ref.uri
    if isinstance(ref, os.PathLike):
        ref = os.fspath(ref)
    if not isinstance(ref, str):
        return None, ref
    if ref.startswith("file://"):
        return ref[len("file://"):], ref
    return (None, ref) if ref.startswith(("s3://", "gs://")) else (ref, ref)


class _Reader:
    """A file here, or an object of a store, read only as far as needed."""

    def __init__(self, ref, store=None):
        self.path, self.ref = _local_path(ref)
        self.store = store
        if self.path is None and store is None:
            from zenthros.core.storage import store_for

            self.store = store_for(self.ref)

    def tail(self, size):
        if self.path is None:
            return self.store.tail(self.ref, size)
        with open(self.path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - size))
            return handle.read()

    def all(self):
        if self.path is None:
            return self.store.get(self.ref)
        with open(self.path, "rb") as handle:
            return handle.read()


def _footer(reader):
    """The Parquet file's footer: its schema, as raw bytes with their marks."""
    data = reader.tail(_FOOTER_GUESS)
    if len(data) < 12 or data[-4:] != _PARQUET_MAGIC:
        raise ContractError(f"{reader.ref} is not a Parquet file: it does not end in {_PARQUET_MAGIC.decode()}")
    length = struct.unpack("<I", data[-8:-4])[0]
    if length + 8 > len(data):
        data = reader.tail(length + 8)
    return data[-(length + 8):]


def _schema_of(footer, ref):
    """{column: type} of a footer. Its row groups are not touched: the bytes
    before the footer are not even there."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ContractError(f"{ref}: a contract on Parquet columns needs pyarrow where this runs: add it to the "
                            "environment, or drop columns=") from None
    import pyarrow as pa

    meta = pq.read_metadata(pa.BufferReader(_PARQUET_MAGIC + footer))
    return {schema.name: str(schema.type) for schema in meta.schema.to_arrow_schema()}


def _check_columns(footer, ref, columns, error):
    schema = _schema_of(footer, ref)
    missing = sorted(name for name in columns if name not in schema)
    if missing:
        raise error(f"{ref}: no column {', '.join(missing)}; it has {', '.join(sorted(schema))}")
    wrong = [f"{name} is {schema[name]}, not {kind}" for name, kind in columns.items() if schema[name] != kind]
    if wrong:
        raise error(f"{ref}: {'; '.join(sorted(wrong))}")


def _parquet_files(reader):
    if reader.path is not None and os.path.isdir(reader.path):
        names = sorted(n for n in os.listdir(reader.path) if n.endswith(".parquet"))
        if not names:
            raise ContractError(f"{reader.ref}: a folder with no .parquet file")
        return [_Reader(os.path.join(reader.path, name)) for name in names]
    if reader.path is None and reader.ref.endswith("/"):
        listed = [item.uri for item in reader.store.ls(reader.ref) if item.uri.endswith(".parquet")]
        if not listed:
            raise ContractError(f"{reader.ref}: a folder with no .parquet file")
        return [_Reader(uri, reader.store) for uri in listed]
    return [reader]


def check_ref(ref, contract, error=ContractError, store=None):
    """That ref (a path, a URI, an Artifact) has this format and these columns.
    Raises error when not; only what the check needs is read."""
    contract = _one(contract)
    reader = _Reader(ref, store)
    if not isinstance(reader.ref, str):
        raise error(f"expected a path or a URI of {contract.format}, got {type(reader.ref).__name__}")
    try:
        _check_format(reader, contract, error)
    except FileNotFoundError:
        raise error(f"{reader.ref} is not there") from None
    except ContractError as err:
        raise (err if type(err) is error else error(str(err))) from None


def _check_format(reader, contract, error):
    if reader.path is not None and not os.path.exists(reader.path):
        raise FileNotFoundError(reader.path)
    if contract.format == "dir":
        if reader.path is not None and not os.path.isdir(reader.path):
            raise error(f"{reader.ref} is not a folder")
        if reader.path is None and not reader.store.ls(reader.ref.rstrip("/") + "/"):
            raise FileNotFoundError(reader.ref)
    elif contract.format == "file":
        if reader.path is not None and not os.path.isfile(reader.path):
            raise error(f"{reader.ref} is not a file")
        if reader.path is None:
            reader.tail(1)
    elif contract.format == "json":
        _check_json(reader, error)
    else:
        for one in _parquet_files(reader):
            footer = _footer(one)
            if contract.columns:
                _check_columns(footer, one.ref, contract.columns, error)


def _check_json(reader, error):
    if reader.path is not None and os.path.getsize(reader.path) > _JSON_PARSED_BYTES:
        with open(reader.path, "rb") as handle:
            start = handle.read(4096).lstrip()
        if start[:1] not in (b"{", b"[", b'"') and not start[:1].isdigit() and start[:4] not in (b"true", b"null"):
            raise error(f"{reader.ref} does not start like JSON")
        return
    try:
        json.loads(reader.all())
    except ValueError as err:
        raise error(f"{reader.ref} is not JSON: {err}") from None


def _plain_json(value):
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def _check_value(value, contract, error, store):
    contract = _one(contract)
    if contract.format == "json" and not (isinstance(value, str) and _is_ref(value)) and not isinstance(value, Artifact):
        if not _plain_json(value):
            raise error(f"expected JSON, got a {type(value).__name__}")
        return
    check_ref(value, contract, error, store)


def _is_ref(text):
    return text.startswith(("s3://", "gs://", "file://")) or os.path.exists(text)


def check_output(contract, value, output_dir=None, store=None):
    """What a function returned against its outputs=: one contract for the value
    itself, or by name: a key of the dict it returned, else a file of that name
    among its outputs."""
    if contract is None:
        return
    if "format" in contract:
        _check_value(value, contract, ContractError, store)
        return
    for name, one in contract.items():
        if isinstance(value, dict) and name in value:
            try:
                _check_value(value[name], one, ContractError, store)
            except ContractError as err:
                raise ContractError(f"{name}: {err}") from None
            continue
        found = _output_named(name, output_dir)
        if found is None:
            raise ContractError(f"{name}: the function neither returned it nor left it in its output")
        try:
            check_ref(found, one, ContractError, store)
        except ContractError as err:
            raise ContractError(f"{name}: {err}") from None


def _output_named(name, output_dir):
    if not output_dir or "://" in output_dir and not output_dir.startswith("file://"):
        return None
    folder = output_dir[len("file://"):] if output_dir.startswith("file://") else output_dir
    if not os.path.isdir(folder):
        return None
    for entry in sorted(os.listdir(folder)):
        if entry == name or os.path.splitext(entry)[0] == name:
            return os.path.join(folder, entry)
    return None


def check_inputs(contract, arguments, store=None):
    """A call's arguments, by name, against its inputs=. A single contract is
    for its only argument."""
    if contract is None:
        return
    if "format" in contract:
        if len(arguments) != 1:
            raise InputContractError("one inputs= contract, for a call with one argument")
        contract = {next(iter(arguments)): contract}
    for name, one in contract.items():
        if name not in arguments:
            continue
        try:
            _check_value(arguments[name], one, InputContractError, store)
        except ContractError as err:
            raise InputContractError(f"{name}: {err}") from None


def connects(upper, lower):
    """Whether what the step above declares leaving can be what the step below
    declares taking: (True or False, or None when either says nothing; why)."""
    if upper is None or lower is None:
        return None, "not compared: only a contract on both sides can be"
    if "format" not in upper or "format" not in lower:
        return None, "not compared: named contracts are checked when the step runs"
    if upper["format"] != lower["format"]:
        return False, f"the step above leaves {upper['format']} and this one takes {lower['format']}"
    missing = [name for name, kind in (lower.get("columns") or {}).items() if (upper.get("columns") or {}).get(name) != kind]
    if missing and upper.get("columns"):
        return False, f"the step above does not declare {', '.join(sorted(missing))}"
    return (None, "not compared: the step above declares no columns") if missing else (True, "they fit")
