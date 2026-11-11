# SPDX-License-Identifier: Apache-2.0

"""Spark's S3 connector, for a PySpark installed from PyPI.

pip's pyspark brings Spark and Hadoop but not the jars that read s3a://:
hadoop-aws, of the same version as its Hadoop, and what hadoop-aws is built
against (its POM's compile dependencies: the AWS SDK and, since Hadoop 3.4.2,
the S3 analytics accelerator), at the versions hadoop-project pins. ensure()
puts them in pyspark's own jars directory, where Spark finds them with no
setting, and checks each against the SHA-1 Maven Central publishes beside it.

    python spark_jars.py          # at image build, so a job downloads nothing

On a machine whose pyspark came from a pool's python toolchain the jars are
not there: for_s3() downloads them once into a cache of the machine (its
/tmp, kept between jobs) for the Spark stage to name on its class path.

The SDK is one jar of several hundred MB: it is the price of s3a://.
"""

import glob
import hashlib
import os
import re
import sys
import tempfile
import urllib.request

MAVEN = "https://repo1.maven.org/maven2"


def jars_dir():
    import pyspark

    return os.path.join(os.path.dirname(pyspark.__file__), "jars")


def hadoop_version(directory):
    """The Hadoop this pyspark carries, from its client jar's name."""
    found = glob.glob(os.path.join(directory, "hadoop-client-api-*.jar"))
    if len(found) != 1:
        raise RuntimeError(f"{directory}: expected one hadoop-client-api jar, found {len(found)}")
    return re.search(r"hadoop-client-api-(.+)\.jar$", found[0]).group(1)


def _tag(xml, name):
    match = re.search(rf"<{name}>([^<]*)</{name}>", xml)
    return match.group(1).strip() if match else ""


def _dependencies(pom):
    """(groupId, artifactId, scope, optional) of each <dependency> in pom."""
    for block in re.findall(r"<dependency>(.*?)</dependency>", pom, re.S):
        yield _tag(block, "groupId"), _tag(block, "artifactId"), _tag(block, "scope") or "compile", _tag(block, "optional")


def dependencies(aws_pom, project_pom):
    """What hadoop-aws needs at run time and Hadoop's client jars do not
    carry: its POM's compile dependencies, not optional, as (groupId,
    artifactId, version) at the version hadoop-project manages."""
    properties = dict(re.findall(r"<([\w.-]+)>([^<]*)</\1>", project_pom.split("<properties>", 1)[-1].split("</properties>")[0]))
    managed = {}
    for block in re.findall(r"<dependency>(.*?)</dependency>", project_pom, re.S):
        version = _tag(block, "version")
        managed[(_tag(block, "groupId"), _tag(block, "artifactId"))] = re.sub(
            r"\$\{([^}]+)\}", lambda m: properties.get(m.group(1), m.group(0)), version)
    found = []
    for group, artifact, scope, optional in _dependencies(aws_pom):
        if scope != "compile" or optional == "true":
            continue
        version = managed.get((group, artifact), "")
        if not version or "${" in version:
            raise RuntimeError(f"hadoop-project pins no version of {group}:{artifact}")
        found.append((group, artifact, version))
    return found


def _jar_url(group, artifact, version):
    return f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar"


def _fetch(url):
    with urllib.request.urlopen(url, timeout=120) as resp:
        return resp.read()


def _download(url, path):
    """url to path, refused unless it is what Maven Central says it is."""
    expected = _fetch(url + ".sha1").decode().split()[0]
    digest = hashlib.sha1()
    # This process's own: two jobs on one machine may fetch the same jar.
    scratch = f"{path}.{os.getpid()}.part"
    with urllib.request.urlopen(url, timeout=600) as resp, open(scratch, "wb") as out:
        for chunk in iter(lambda: resp.read(1 << 20), b""):
            digest.update(chunk)
            out.write(chunk)
    if digest.hexdigest() != expected:
        os.unlink(scratch)
        raise RuntimeError(f"{url}: SHA-1 {digest.hexdigest()}, Maven Central says {expected}")
    os.replace(scratch, path)


def ensure(directory=None, into=None):
    """Download the jars that are not in into (default: directory, pyspark's
    jars) for the Hadoop in directory. Returns their paths."""
    directory = directory or jars_dir()
    into = into or directory
    hadoop = hadoop_version(directory)
    aws_pom = _fetch(f"{MAVEN}/org/apache/hadoop/hadoop-aws/{hadoop}/hadoop-aws-{hadoop}.pom").decode()
    project_pom = _fetch(f"{MAVEN}/org/apache/hadoop/hadoop-project/{hadoop}/hadoop-project-{hadoop}.pom").decode()
    wanted = {f"hadoop-aws-{hadoop}.jar": _jar_url("org.apache.hadoop", "hadoop-aws", hadoop)}
    for group, artifact, version in dependencies(aws_pom, project_pom):
        wanted[f"{artifact}-{version}.jar"] = _jar_url(group, artifact, version)
    os.makedirs(into, exist_ok=True)
    for name, url in wanted.items():
        path = os.path.join(into, name)
        if not os.path.exists(path):
            _download(url, path)
    return [os.path.join(into, name) for name in wanted]


def cache_dir():
    return os.environ.get("PIPELINE_JARS_DIR") or os.path.join(tempfile.gettempdir(), "zenthros-pipeline-jars")


def for_s3(directory=None, cache=None):
    """The jars to add to Spark's class path for s3a://: none when pyspark's
    own jars have the connector (the image's), else the cache's."""
    directory = directory or jars_dir()
    if glob.glob(os.path.join(directory, "hadoop-aws-*.jar")):
        return []
    return ensure(directory, into=cache or cache_dir())


if __name__ == "__main__":
    for jar in ensure(sys.argv[1] if len(sys.argv) > 1 else None):
        print(jar, os.path.getsize(jar) >> 20, "MiB")
