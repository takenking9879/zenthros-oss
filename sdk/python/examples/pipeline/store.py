# SPDX-License-Identifier: Apache-2.0

"""Where the pipeline's tables live: a local directory or s3://bucket/prefix.

One place knows how each library is pointed at it. On S3 the credentials
are the process's own (the machine's role in the cloud, the job's secret on
your computer); AWS_ENDPOINT_URL moves every library to another S3 (an
emulator) at once.
"""

import json
import os
import urllib.parse

import pyarrow.parquet as pq
from pyarrow import fs

S3 = "s3://"


def join(base, *parts):
    return "/".join([base.rstrip("/"), *parts])


def _endpoint():
    return os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or ""


def filesystem(uri):
    """The pyarrow filesystem of uri, and uri's path in it."""
    if not uri.startswith(S3):
        return fs.LocalFileSystem(), os.path.abspath(uri)
    options = {}
    if _endpoint():
        parts = urllib.parse.urlsplit(_endpoint())
        options.update(endpoint_override=parts.netloc, scheme=parts.scheme)
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        options["region"] = region
    return fs.S3FileSystem(**options), uri[len(S3):]


def polars_options(uri):
    """storage_options for polars' own S3 client; None for a local path."""
    if not uri.startswith(S3):
        return None
    options = {}
    if _endpoint():
        options["aws_endpoint_url"] = _endpoint()
        if _endpoint().startswith("http://"):
            options["aws_allow_http"] = "true"
    return options


def spark_uri(uri):
    """uri as Spark's S3 connector names it."""
    return "s3a://" + uri[len(S3):] if uri.startswith(S3) else os.path.abspath(uri)


def _make_parent(filesystem_, path, uri):
    """A local file's directory. S3 has none to make, and pyarrow would ask
    HeadBucket first: ListBucket on the whole bucket, which credentials for
    one prefix (zenthros.aws) do not have."""
    if not uri.startswith(S3):
        filesystem_.create_dir(os.path.dirname(path), recursive=True)


def write_parquet(table, uri):
    filesystem_, path = filesystem(uri)
    _make_parent(filesystem_, path, uri)
    pq.write_table(table, path, filesystem=filesystem_, compression="zstd")


def parquet_files(uri):
    """The Parquet files under a directory, sorted; [] when there is none."""
    filesystem_, path = filesystem(uri)
    found = filesystem_.get_file_info(fs.FileSelector(path, recursive=True, allow_not_found=True))
    return sorted(info.path for info in found if info.is_file and info.path.endswith(".parquet"))


def parquet_rows(uri):
    """Rows of a Parquet table, from its files' footers: no data is read."""
    filesystem_, _ = filesystem(uri)
    return sum(pq.read_metadata(path, filesystem=filesystem_).num_rows for path in parquet_files(uri))


def parquet_kinds(uri):
    """A Parquet table's columns and their kinds, in order, from a footer.
    A kind is what the value is (int64, int32, float64, string), not how an
    engine chose to lay it out: Polars and Spark write strings differently."""
    filesystem_, _ = filesystem(uri)
    schema = pq.read_schema(parquet_files(uri)[0], filesystem=filesystem_)
    names = {"double": "float64", "large_string": "string", "string_view": "string"}
    return {field.name: names.get(str(field.type), str(field.type)) for field in schema}


def remove_dir(uri):
    filesystem_, path = filesystem(uri)
    if filesystem_.get_file_info(path).type != fs.FileType.NotFound:
        filesystem_.delete_dir(path)


def write_json(uri, value):
    filesystem_, path = filesystem(uri)
    _make_parent(filesystem_, path, uri)
    with filesystem_.open_output_stream(path) as out:
        out.write(json.dumps(value, indent=2, sort_keys=True).encode())


def read_json(uri):
    """The JSON at uri; None when there is no such object."""
    filesystem_, path = filesystem(uri)
    if filesystem_.get_file_info(path).type == fs.FileType.NotFound:
        return None
    with filesystem_.open_input_stream(path) as source:
        return json.loads(source.read())
