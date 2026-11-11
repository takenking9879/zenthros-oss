# SPDX-License-Identifier: Apache-2.0

"""zenthros.env: what an environment is, what a build and a publish decide, with
an engine that only answers (the real ones: tests/e2e/test_env_e2e.py)."""

import hashlib
import io
import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import cli, errors, imagebuild, tools
from zenthros import env as environments
from zenthros.jobs import local as local_module
from zenthros.jobs import localk8s
from zenthros.remote import call as remote_call

BASE = "python:3.12-slim-bookworm"


def base_raw(platforms):
    return json.dumps({"mediaType": "application/vnd.oci.image.index.v1+json", "manifests": [
        {"digest": f"sha256:{name.rpartition('/')[2]}", "platform": {"architecture": name.rpartition("/")[2], "os": "linux"}}
        for name in platforms] + [{"digest": "sha256:attestation", "platform": {"architecture": "unknown", "os": "unknown"}}]})


BASE_DIGEST = "sha256:" + hashlib.sha256(base_raw(["linux/amd64", "linux/arm64"]).encode()).hexdigest()
PROJECT = "proj"
GOOD_CONFIG = {"Env": ["PATH=/usr/bin"], "Cmd": ["python3"]}
LAYERS = ["sha256:base", "sha256:packages"]


class BuildingEngine:
    """Docker with a BuildKit and registries that do what the test says."""

    name, binary = "docker", "/fake/docker"

    def __init__(self):
        self.calls = []
        self.images = {}
        self.registry = {}
        self.base_image = {"linux/amd64": {"config": {"User": ""}, "rootfs": {"diff_ids": []}},
                           "linux/arm64": {"config": {"User": ""}, "rootfs": {"diff_ids": []}}}
        self.resolved = "numpy==2.0.0\npolars==1.0.0\n"
        self.resolve_error = ""
        self.build_error = ""
        self.push_error = ""
        self.content = "sha256:content1"
        self.push_content = None
        self.read_layers = LAYERS
        self.read_config = GOOD_CONFIG
        self.contexts = []
        self.installed = {"numpy": "2.0.0", "polars": "1.0.0", "pip": "25.0"}
        self.python = "3.12"
        self.musl = False
        self.home_builder = True
        self.single_arch = "amd64"

    def image_name(self, image):
        return image

    def builds(self):
        return [c for c in self.calls if c[:2] == ["buildx", "build"]]

    def outputs(self):
        return [c[c.index("--output") + 1] for c in self.builds()]

    def run(self, args, stdin=None, timeout=600):
        self.calls.append(list(args))
        if args[:2] == ["version", "--format"]:
            return 0, "amd64\n", ""
        if args[:2] == ["buildx", "inspect"]:
            return (0, "", "") if self.home_builder else (1, "", "no builder")
        if args[:2] == ["buildx", "create"]:
            self.home_builder = True
            return 0, "", ""
        if args[:2] == ["buildx", "rm"]:
            self.home_builder = False
            return 0, "", ""
        if args[:2] == ["buildx", "build"]:
            return self._build(args)
        if args[:3] == ["buildx", "imagetools", "inspect"]:
            return self._inspect(args)
        if args[:3] == ["buildx", "imagetools", "create"]:
            self.registry[args[args.index("-t") + 1]] = self.registry[args[-1]]
            self.registry[args[args.index("-t") + 1].rpartition(":")[0] + "@" + self.registry[args[-1]]["manifest"]] = self.registry[args[-1]]
            return 0, "", ""
        if args[:2] == ["image", "inspect"]:
            if args[2] in self.images:
                return 0, json.dumps([{"RootFS": {"Layers": LAYERS}, "Config": GOOD_CONFIG}]), ""
            return 1, "", "No such image"
        if args[:2] == ["image", "ls"]:
            return 0, "\n".join(self.images) + "\nother/thing:1\n", ""
        if args[:2] == ["image", "rm"]:
            self.images.pop(args[2])
            return 0, "", ""
        if args[0] == "run":
            return 0, json.dumps({"python": self.python, "musl": self.musl, "installed": self.installed}) + "\n", ""
        raise AssertionError(f"unexpected {args}")

    def _build(self, args):
        output = args[args.index("--output") + 1]
        context = Path(args[-1])
        self.contexts.append({p.name: p.read_text() for p in context.iterdir() if p.is_file()})
        if output.startswith("type=local"):
            if self.resolve_error:
                return 1, "", self.resolve_error
            (Path(output.split("dest=")[1]) / "resolved.txt").write_text(self.resolved)
            return 0, "", ""
        if self.build_error:
            return 1, "", self.build_error
        attrs = dict(part.split("=", 1) for part in output.split(",") if "=" in part)
        content = self.content
        manifest = "sha256:manifest-gzip" if attrs.get("compression", "gzip") == "gzip" else "sha256:manifest-zstd"
        if attrs["type"] == "docker":
            self.images[attrs["name"]] = True
            return 0, "", "#9 0.3 Installed 3 packages\n"
        if attrs.get("push") == "false" and self.push_content:
            content = self.push_content
        if attrs.get("push") == "true":
            if self.push_error:
                return 1, "", self.push_error
            content = self.push_content or content
            found = {"content": content, "manifest": manifest}
            self.registry[attrs["name"]] = found
            self.registry[attrs["name"].rpartition(":")[0] + "@" + manifest] = found
        if "--metadata-file" in args:
            Path(args[args.index("--metadata-file") + 1]).write_text(
                json.dumps({"containerimage.config.digest": content, "containerimage.digest": manifest}))
        return 0, "", ""

    def _inspect(self, args):
        ref = next(a for a in args[3:] if not a.startswith("-") and a not in ("zenthros", "{{json .Image}}"))
        wants_raw = "--raw" in args
        if ref.partition("@")[0].partition(":")[0] == "python":
            if wants_raw:
                return 0, base_raw(self.base_image), ""
            arch = ref.rpartition("sha256:")[2]
            return 0, json.dumps({"architecture": arch, **self.base_image[f"linux/{arch}"]}), ""
        if ref.startswith("localhost:5111/base"):
            if wants_raw:
                return 0, json.dumps({"config": {"digest": "sha256:c"}, "layers": []}), ""
            return 0, json.dumps({"architecture": self.single_arch, "config": {"User": "kept"}, "rootfs": {"diff_ids": []}}), ""
        found = self.registry.get(ref)
        if found is None:
            return 1, "", "not found"
        if wants_raw:
            # Its digest is what the fake registry says: the test reads it back.
            return 0, json.dumps({"config": {"digest": found["content"]}, "manifest_of": found["manifest"]}), ""
        return 0, json.dumps({"rootfs": {"diff_ids": self.read_layers}, "config": self.read_config}), ""


class EnvCase(unittest.TestCase):
    def setUp(self):
        here = tempfile.TemporaryDirectory()
        self.addCleanup(here.cleanup)
        self.root = Path(here.name).resolve()
        (self.root / ".git").mkdir()
        before = os.getcwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, before)
        self.engine = BuildingEngine()
        for patcher in (mock.patch.object(environments, "_engine", return_value=self.engine),
                        mock.patch.object(environments, "_project_id", return_value=PROJECT),
                        mock.patch.dict(os.environ, {"ZENTHROS_LOCAL_NETWORK": ""})):
            patcher.start()
            self.addCleanup(patcher.stop)
        # What the registry says the manifest digest is, without the digest of the fake's text.
        real_read = imagebuild.Buildx.read

        def read(builder, ref, arch=""):
            seen = real_read(builder, ref, arch)
            found = self.engine.registry.get(ref)
            if seen is not None and found is not None:
                seen.manifest = found["manifest"]
            return seen

        patcher = mock.patch.object(imagebuild.Buildx, "read", read)
        patcher.start()
        self.addCleanup(patcher.stop)

    def lock(self, name="train"):
        return json.loads((self.root / "envs" / name / "env.lock.json").read_text())

    def built(self, *packages, **options):
        environments.new("train", BASE, **options)
        if packages:
            environments.add("train", *packages)
        return environments.build("train", out=io.StringIO())


class TheLock(EnvCase):
    def test_new_fixes_the_base_by_its_digest_and_builds_nothing(self):
        environments.new("train", BASE, registry="r.example/team/")
        lock = self.lock()
        self.assertEqual(lock["base"], BASE)
        self.assertEqual(lock["base_digest"], BASE_DIGEST)
        self.assertEqual(lock["registry"], "r.example/team")
        self.assertEqual(lock["platform"], "linux/amd64")
        self.assertEqual(self.engine.builds(), [])

    def test_new_refuses_a_name_taken_or_a_bad_one(self):
        environments.new("train", BASE)
        with self.assertRaisesRegex(errors.ValidationError, "already here"):
            environments.new("train", BASE)
        with self.assertRaisesRegex(errors.ValidationError, "name"):
            environments.new("../up", BASE)

    def test_new_refuses_a_base_this_computers_architecture_cannot_run(self):
        self.engine.base_image = {"linux/arm64": self.engine.base_image["linux/arm64"]}
        with self.assertRaisesRegex(errors.ValidationError, "only arm64.*amd64"):
            environments.new("train", BASE)
        self.assertFalse((self.root / "envs" / "train").exists())

    def test_new_spends_few_requests_of_the_registrys_limit(self):
        # Docker Hub counts each manifest, and an index may list ten platforms.
        environments.new("train", BASE)
        asked = [c for c in self.engine.calls if c[:3] == ["buildx", "imagetools", "inspect"]]
        self.assertEqual(len(asked), 2, asked)
        self.assertIn("--raw", asked[0])
        self.assertIn(f"{BASE.partition(':')[0]}@sha256:amd64", asked[1])

    def test_new_takes_a_base_of_one_platform(self):
        environments.new("train", "localhost:5111/base/heavy:1")
        self.assertEqual(self.lock()["base_user"], "kept")
        self.engine.single_arch = "arm64"
        with self.assertRaisesRegex(errors.ValidationError, "only arm64.*amd64"):
            environments.new("other", "localhost:5111/base/heavy:1")

    def test_a_registrys_limit_says_what_to_do(self):
        original = self.engine.run

        def run(args, stdin=None, timeout=600):
            if args[:3] == ["buildx", "imagetools", "inspect"]:
                return 1, "", "ERROR: unexpected status from GET request: 429 Too Many Requests"
            return original(args, stdin, timeout)

        self.engine.run = run
        with self.assertRaisesRegex(errors.ValidationError, "too many requests.*docker login"):
            environments.new("train", BASE)

    def test_add_only_writes_the_lock(self):
        environments.new("train", BASE)
        environments.add("train", "polars", "xgboost==2.1")
        self.assertEqual(self.lock()["packages"], ["polars", "xgboost==2.1"])
        self.assertEqual(self.engine.builds(), [])

    def test_add_again_replaces_the_same_package_however_it_is_written(self):
        environments.new("train", BASE)
        environments.add("train", "scikit_learn==1.0")
        environments.add("train", "Scikit-Learn==1.5")
        self.assertEqual(self.lock()["packages"], ["Scikit-Learn==1.5"])

    def test_add_reads_a_requirements_file_without_its_comments(self):
        environments.new("train", BASE)
        (self.root / "r.txt").write_text("# heavy\npolars  # fast\n\nnumpy==2.0\n")
        environments.add("train", requirements="r.txt")
        self.assertEqual(self.lock()["packages"], ["numpy==2.0", "polars"])

    def test_add_refuses_what_is_not_a_package(self):
        environments.new("train", BASE)
        with self.assertRaisesRegex(errors.ValidationError, "not a package"):
            environments.add("train", "--index-url=http://evil")

    def test_a_missing_environment_says_how_to_make_it(self):
        with self.assertRaisesRegex(errors.ValidationError, "env new nope"):
            environments.build("nope")

    def test_a_name_never_reaches_outside_envs(self):
        (self.root / "up").mkdir()
        (self.root / "up" / "env.lock.json").write_text("{}")
        with self.assertRaisesRegex(errors.ValidationError, "no environment"):
            environments.image_for("../up", "local")

    def test_the_lock_never_holds_a_credential_or_a_computers_path(self):
        self.built("polars", registry="r.example/team")
        text = (self.root / "envs" / "train" / "env.lock.json").read_text()
        self.assertNotIn(str(self.root), text)
        self.assertNotIn(PROJECT, text)
        self.assertNotRegex(text.lower(), "password|token|auth")

    def test_a_project_is_found_by_its_environments_from_a_subfolder(self):
        environments.new("train", BASE)
        (self.root / ".git").rmdir()
        (self.root / "sub").mkdir()
        os.chdir(self.root / "sub")
        self.assertEqual(local_module.project_root(), self.root)


class TheBuild(EnvCase):
    def test_a_build_writes_the_recipe_pinned_and_keeps_both_digests(self):
        env = self.built("polars")
        recipe = self.engine.contexts[-1]["Dockerfile"]
        self.assertIn(f"FROM {BASE}@{BASE_DIGEST}", recipe)
        self.assertIn(f"COPY --from={environments.UV_IMAGE} /uv", recipe)
        self.assertIn("--mount=type=cache,id=zenthros-uv", recipe)
        self.assertIn("--no-deps", recipe)
        self.assertEqual(self.engine.contexts[-1]["packages.txt"], "numpy==2.0.0\npolars==1.0.0\n")
        self.assertEqual(env.content, "sha256:content1")
        self.assertEqual(env.build["manifest"], "sha256:manifest-gzip")
        self.assertEqual(env.python, "3.12")
        self.assertEqual(env.installed["polars"], "1.0.0")
        self.assertTrue(any(o.startswith("type=docker,name=localhost/zenthros-env/proj/train:") for o in self.engine.outputs()))

    def test_resolving_keeps_what_the_image_has_as_it_is(self):
        self.built("polars")
        recipe = self.engine.contexts[0]["Dockerfile"]
        self.assertIn("uv pip freeze --system > image.txt", recipe)
        self.assertIn("-c image.txt -c previous.txt", recipe)

    def test_nothing_changed_does_not_call_the_builder(self):
        self.built("polars")
        before = len(self.engine.builds())
        with mock.patch.object(imagebuild.Buildx, "_ensure_builder", side_effect=AssertionError("called")):
            environments.build("train", out=io.StringIO())
        self.assertEqual(len(self.engine.builds()), before)

    def test_the_lock_from_another_computer_builds_the_same_versions_without_resolving(self):
        self.built("polars")
        self.engine.images.clear()
        before = len(self.engine.outputs())
        self.engine.resolved = "numpy==9.9.9\n"
        environments.build("train", out=io.StringIO())
        made = self.engine.outputs()[before:]
        self.assertFalse([o for o in made if o.startswith("type=local")], "it asked for versions again")
        self.assertEqual(self.engine.contexts[-1]["packages.txt"], "numpy==2.0.0\npolars==1.0.0\n")

    def test_adding_a_package_keeps_the_versions_it_had(self):
        self.built("polars")
        environments.add("train", "lightgbm")
        self.engine.resolved = "lightgbm==4.0.0\nnumpy==2.0.0\npolars==1.0.0\n"
        environments.build("train", out=io.StringIO())
        resolving = [c for c in self.engine.contexts if "previous.txt" in c][-1]
        self.assertEqual(resolving["previous.txt"], "numpy==2.0.0\npolars==1.0.0\n")
        self.assertEqual(self.lock()["pins"], ["lightgbm==4.0.0", "numpy==2.0.0", "polars==1.0.0"])

    def test_a_version_asked_for_replaces_its_pin(self):
        self.built("polars")
        environments.add("train", "polars==0.9")
        self.engine.resolved = "numpy==2.0.0\npolars==0.9\n"
        environments.build("train", out=io.StringIO())
        resolving = [c for c in self.engine.contexts if "previous.txt" in c][-1]
        self.assertEqual(resolving["previous.txt"], "numpy==2.0.0\n")

    def test_upgrade_lets_every_version_move(self):
        self.built("polars")
        environments.add("train", upgrade=True)
        self.assertEqual(self.lock()["pins"], [])
        environments.build("train", out=io.StringIO())
        resolving = [c for c in self.engine.contexts if "previous.txt" in c][-1]
        self.assertEqual(resolving["previous.txt"], "\n")

    def test_a_base_that_runs_as_someone_else_stays_that_way(self):
        self.engine.base_image["linux/amd64"]["config"]["User"] = "jovyan"
        self.built("polars")
        recipe = self.engine.contexts[-1]["Dockerfile"]
        self.assertLess(recipe.index("USER root"), recipe.index("uv pip install"))
        self.assertTrue(recipe.rstrip().endswith("USER jovyan"))

    def test_no_network_says_so_and_the_old_image_stays(self):
        self.built("polars")
        tag_before = environments.here("train")
        environments.add("train", "lightgbm")
        self.engine.resolve_error = "#7 0.2 error: Failed to fetch `https://pypi.org/simple/lightgbm/`\n#7 0.2 dns error\n"
        with self.assertRaisesRegex(errors.ValidationError, "no network.*still runs"):
            environments.build("train", out=io.StringIO())
        self.assertEqual(self.lock()["build"]["input"][:12], tag_before.rpartition(":")[2])
        self.assertEqual(self.lock()["pins"], ["numpy==2.0.0", "polars==1.0.0"])

    def test_a_package_that_would_replace_the_images_is_named(self):
        environments.new("train", BASE)
        environments.add("train", "numpy==1.0")
        self.engine.resolve_error = ("#7 0.5 × No solution found when resolving dependencies:\n"
                                     "#7 0.5 ╰─▶ Because you require numpy==1.0 and numpy==2.0, your requirements are unsatisfiable.\n")
        with self.assertRaisesRegex(errors.ValidationError, r"replace what the image has[\s\S]*numpy==1\.0 and numpy==2\.0"):
            environments.build("train", out=io.StringIO())

    def test_a_failed_build_leaves_the_lock_and_names_its_log(self):
        self.built("polars")
        built_before = self.lock()["build"]
        environments.add("train", "lightgbm")
        self.engine.resolved = "lightgbm==4.0.0\nnumpy==2.0.0\npolars==1.0.0\n"
        self.engine.build_error = "#9 1.0 error: no space left\n"
        with self.assertRaisesRegex(errors.ValidationError, "no space left"):
            environments.build("train", out=io.StringIO())
        self.assertEqual(self.lock()["build"], built_before)

    def test_musl_is_refused_since_the_agent_needs_glibc(self):
        self.engine.musl = True
        with self.assertRaisesRegex(errors.ValidationError, "glibc"):
            self.built("polars")

    def test_the_builder_is_made_once_with_its_disk_capped(self):
        self.engine.home_builder = False
        seen = []
        original = self.engine.run

        def run(args, stdin=None, timeout=600):
            if args[:2] == ["buildx", "create"]:
                seen.append(Path(args[args.index("--buildkitd-config") + 1]).read_text())
            return original(args, stdin, timeout)

        self.engine.run = run
        self.built("polars")
        self.assertEqual(len(seen), 1)
        self.assertIn('maxUsedSpace = "20GB"', seen[0])
        created = next(c for c in self.engine.calls if c[:2] == ["buildx", "create"])
        self.assertIn("network=host", created)
        self.assertEqual(created[created.index("--name") + 1], "zenthros")

    def test_the_uv_image_is_the_uv_of_the_upstream_lock(self):
        lock = Path(__file__).resolve().parents[3] / "install" / "upstream.lock"
        pinned = re.search(r"^uv@(\S+)", lock.read_text(), re.M).group(1)
        self.assertIn(f":{pinned}@sha256:", environments.UV_IMAGE)


class Advanced(EnvCase):
    def setUp(self):
        super().setUp()
        environments.new("train", BASE)
        self.folder = self.root / "envs" / "train"
        (self.folder / "Dockerfile").write_text("FROM scratch\nCOPY hello /\n")
        (self.folder / "hello").write_text("hi\n")

    def test_its_dockerfile_is_what_builds_by_the_same_builder(self):
        environments.build("train", out=io.StringIO())
        self.assertEqual(self.engine.contexts[-1]["Dockerfile"], "FROM scratch\nCOPY hello /\n")
        self.assertEqual(self.engine.contexts[-1]["hello"], "hi\n")
        self.assertFalse([o for o in self.engine.outputs() if o.startswith("type=local")])

    def test_credentials_and_the_lock_stay_out_of_the_build(self):
        (self.folder / ".env").write_text("SECRET=1\n")
        (self.folder / "cloud.key").write_text("k\n")
        environments.build("train", out=io.StringIO())
        self.assertEqual(sorted(self.engine.contexts[-1]), ["Dockerfile", "hello"])

    def test_a_changed_file_builds_again_and_an_unchanged_one_does_not(self):
        environments.build("train", out=io.StringIO())
        count = len(self.engine.builds())
        environments.build("train", out=io.StringIO())
        self.assertEqual(len(self.engine.builds()), count)
        (self.folder / "hello").write_text("hello\n")
        environments.build("train", out=io.StringIO())
        self.assertGreater(len(self.engine.builds()), count)

    def test_add_says_where_the_package_goes(self):
        with self.assertRaisesRegex(errors.ValidationError, "its own Dockerfile"):
            environments.add("train", "polars")


class ThePush(EnvCase):
    def pushed(self, *args, **options):
        return environments.push("train", *args, out=io.StringIO(), **options)

    def test_it_publishes_and_records_what_the_registry_says(self):
        self.built("polars", registry="r.example/team")
        entry = self.pushed()
        self.assertEqual(entry["ref"], "r.example/team/train@sha256:manifest-gzip")
        self.assertEqual(entry["content"], "sha256:content1")
        self.assertEqual(entry["verdict"], "identical")
        self.assertEqual(self.lock()["published"], [entry])

    def test_it_reads_what_it_published_before_saying_anything(self):
        self.built("polars", registry="r.example/team")
        self.pushed()
        reads = [c for c in self.engine.calls if c[:3] == ["buildx", "imagetools", "inspect"] and "--raw" in c]
        self.assertIn("r.example/team/train@sha256:manifest-gzip", reads[-1])

    def test_with_zstd_the_manifest_changes_and_the_content_does_not(self):
        self.built("polars", registry="r.example/team")
        gzip, zstd = self.pushed(), self.pushed(zstd=True)
        self.assertNotEqual(gzip["ref"], zstd["ref"])
        self.assertEqual(gzip["content"], zstd["content"])
        self.assertEqual(zstd["verdict"], "same content")
        self.assertEqual(len(self.lock()["published"]), 2)
        output = self.engine.outputs()[-1]
        self.assertIn("compression=zstd", output)
        self.assertIn("force-compression=true", output)

    def test_gzip_does_not_force_a_recompression(self):
        self.built("polars", registry="r.example/team")
        self.pushed()
        output = self.engine.outputs()[-1]
        self.assertIn("compression=gzip", output)
        self.assertNotIn("force-compression", output)

    def test_only_a_registry_on_this_computer_may_be_plain_http(self):
        self.built("polars", registry="r.example/team")
        self.pushed()
        self.assertNotIn("registry.insecure", self.engine.outputs()[-1])
        self.pushed("localhost:5111/train")
        self.assertIn("registry.insecure=true", self.engine.outputs()[-1])

    def test_another_destination_changes_only_the_destination(self):
        self.built("polars", registry="r.example/team")
        self.pushed()
        entry = self.pushed("docker.io/me/train:v1")
        self.assertEqual(entry["ref"], "docker.io/me/train@sha256:manifest-gzip")
        self.assertIn("name=docker.io/me/train:v1,", self.engine.outputs()[-1])
        self.assertEqual(len(self.lock()["published"]), 2)

    def test_publishing_again_replaces_its_own_entry(self):
        self.built("polars", registry="r.example/team")
        self.pushed()
        self.pushed()
        self.assertEqual(len(self.lock()["published"]), 1)

    def test_configure_says_where_when_the_environment_does_not(self):
        self.built("polars")
        with zenthros.configure(registry="r.example/mine/"):
            entry = self.pushed()
        self.assertEqual(entry["ref"], "r.example/mine/train@sha256:manifest-gzip")

    def test_the_environments_registry_wins_over_configure(self):
        self.built("polars", registry="r.example/team")
        with zenthros.configure(registry="r.example/mine"):
            self.assertEqual(self.pushed()["ref"], "r.example/team/train@sha256:manifest-gzip")

    def test_not_built_is_refused_with_nothing_sent(self):
        environments.new("train", BASE, registry="r.example/team")
        with self.assertRaisesRegex(errors.ValidationError, "not built"):
            self.pushed()
        self.assertEqual(self.engine.builds(), [])

    def test_changed_since_the_build_is_refused_with_nothing_sent(self):
        self.built("polars", registry="r.example/team")
        environments.add("train", "lightgbm")
        count = len(self.engine.builds())
        with self.assertRaisesRegex(errors.ValidationError, "not what you tried"):
            self.pushed()
        self.assertEqual(len(self.engine.builds()), count)

    def test_no_destination_says_how_to_name_one(self):
        self.built("polars")
        with self.assertRaisesRegex(errors.ValidationError, "where to"):
            self.pushed()

    def test_a_lost_cache_is_refused_before_anything_is_published(self):
        self.built("polars", registry="r.example/team")
        self.engine.push_content = "sha256:another"
        with self.assertRaisesRegex(errors.ValidationError, "--force"):
            self.pushed()
        self.assertFalse([o for o in self.engine.outputs() if "push=true" in o])
        self.assertEqual(self.lock()["published"], [])

    def test_force_takes_the_new_content_as_the_environment(self):
        self.built("polars", registry="r.example/team")
        self.engine.content = "sha256:another"
        env = environments.build("train", force=True, out=io.StringIO())
        self.assertEqual(env.content, "sha256:another")
        self.assertEqual(self.pushed()["content"], "sha256:another")

    def test_a_registry_that_changed_the_config_with_the_same_files_and_run_is_said(self):
        self.built("polars", registry="r.example/team")
        with mock.patch.object(imagebuild.Buildx, "would_publish", return_value=imagebuild.Built(content="sha256:content1")):
            self.engine.push_content = "sha256:other-config"
            self.engine.read_config = imagebuild.run_config(GOOD_CONFIG)
            self.engine.read_config = GOOD_CONFIG
            entry = self.pushed()
        self.assertEqual(entry["verdict"], "same files, only metadata differs")

    def test_the_same_files_that_would_run_differently_are_refused(self):
        self.built("polars", registry="r.example/team")
        with mock.patch.object(imagebuild.Buildx, "would_publish", return_value=imagebuild.Built(content="sha256:content1")):
            self.engine.push_content = "sha256:other-config"
            self.engine.read_config = {**GOOD_CONFIG, "User": "someone"}
            with self.assertRaisesRegex(errors.ValidationError, "run differently"):
                self.pushed()
        self.assertEqual(self.lock()["published"], [])

    def test_other_files_are_refused(self):
        self.built("polars", registry="r.example/team")
        with mock.patch.object(imagebuild.Buildx, "would_publish", return_value=imagebuild.Built(content="sha256:content1")):
            self.engine.push_content = "sha256:other"
            self.engine.read_layers = ["sha256:base", "sha256:evil"]
            with self.assertRaisesRegex(errors.ValidationError, "not what you tried"):
                self.pushed()
        self.assertEqual(self.lock()["published"], [])

    def test_another_architecture_is_said_to_be_other_bytes(self):
        self.built("polars", registry="r.example/team")
        self.engine.push_content = "sha256:arm"
        self.engine.read_layers = ["sha256:armbase"]
        entry = self.pushed(platform="linux/arm64")
        self.assertEqual(entry["verdict"], "same versions, not the same bytes")
        self.assertEqual(entry["platform"], "linux/arm64")
        build = next(c for c in reversed(self.engine.builds()))
        self.assertEqual(build[build.index("--platform") + 1], "linux/arm64")

    def test_not_logged_in_gives_that_registrys_command(self):
        for dest, command in (("us-central1-docker.pkg.dev/p/r/train", "gcloud auth configure-docker us-central1-docker.pkg.dev"),
                              ("123.dkr.ecr.us-east-1.amazonaws.com/train", "aws ecr get-login-password"),
                              ("docker.io/me/train", "docker login"),
                              ("me/train", "docker login"),
                              ("ghcr.io/me/train", "docker login ghcr.io")):
            with self.subTest(dest=dest):
                self.assertIn(command, imagebuild.login_hint(dest))
        self.built("polars")
        self.engine.push_error = "ERROR: failed to push: unauthorized: authentication required\n"
        with self.assertRaisesRegex(errors.ValidationError, "Log in: docker login ghcr.io"):
            self.pushed("ghcr.io/me/train")
        self.assertEqual(self.lock()["published"], [])

    def test_with_no_build_here_it_copies_what_was_published(self):
        self.built("polars", registry="r.example/team")
        self.pushed()
        self.engine.images.clear()
        entry = self.pushed("docker.io/me/train:v1")
        self.assertEqual(entry["content"], "sha256:content1")
        create = next(c for c in self.engine.calls if c[:3] == ["buildx", "imagetools", "create"])
        self.assertEqual(create[-1], "r.example/team/train@sha256:manifest-gzip")
        self.assertIn("--prefer-index=false", create)

    def test_with_no_build_here_and_nothing_published_it_asks_to_build(self):
        self.built("polars", registry="r.example/team")
        self.engine.images.clear()
        with self.assertRaisesRegex(errors.ValidationError, "not built on this computer"):
            self.pushed()


class Podman(EnvCase):
    def test_it_reads_a_registry_by_pulling_what_it_published(self):
        # manifest inspect takes only a list of platforms, and refuses docker:// too.
        engine = mock.Mock()
        engine.name = "podman"
        found = {"Id": "abc", "RootFS": {"Layers": LAYERS}, "Config": {**GOOD_CONFIG, "Labels": {"a": "b"}}}
        engine.run.side_effect = [(0, "", ""), (0, json.dumps([found]), "")]
        seen = imagebuild.PodmanBuild(engine).read("localhost:5111/a/b@sha256:m")
        self.assertEqual(engine.run.call_args_list[0].args[0], ["pull", "--tls-verify=false", "localhost:5111/a/b@sha256:m"])
        self.assertEqual(engine.run.call_args_list[1].args[0], ["image", "inspect", "localhost:5111/a/b@sha256:m"])
        self.assertEqual((seen.manifest, seen.content, seen.layers), ("sha256:m", "sha256:abc", LAYERS))
        self.assertEqual(seen.config, imagebuild.run_config(GOOD_CONFIG))

    def test_a_registry_without_it_is_none(self):
        engine = mock.Mock()
        engine.name = "podman"
        engine.run.return_value = (125, "", "Error: reading manifest: manifest unknown")
        self.assertIsNone(imagebuild.PodmanBuild(engine).read("r.example/a/b:1"))

    def test_a_copy_says_the_digest_it_published(self):
        engine = mock.Mock()
        engine.name = "podman"

        def run(args, stdin=None, timeout=600):
            if args[0] == "push":
                Path(args[args.index("--digestfile") + 1]).write_text("sha256:copied\n")
            return 0, "", ""

        engine.run.side_effect = run
        built = imagebuild.PodmanBuild(engine).copy("r.example/a@sha256:s", "localhost:5112/b:1")
        self.assertEqual(built.manifest, "sha256:copied")

    def test_zstd_is_refused_on_podman_with_nothing_built(self):
        self.engine.name = "podman"
        lock = {"name": "train", "base": BASE, "base_digest": BASE_DIGEST, "build": {"input": "x"}}
        folder = self.root / "envs" / "train"
        folder.mkdir(parents=True)
        (folder / "env.lock.json").write_text(json.dumps(lock))
        with self.assertRaisesRegex(errors.ValidationError, "drop --zstd"):
            environments.push("train", "r.example/train", zstd=True, out=io.StringIO())


class WhereItRuns(EnvCase):
    def test_on_this_computer_it_is_the_built_image(self):
        self.built("polars")
        image = environments.image_for("train", "local")
        self.assertTrue(image.startswith("localhost/zenthros-env/proj/train:"))
        self.assertEqual(image, environments.here("train"))

    def test_not_built_says_how(self):
        environments.new("train", BASE)
        with self.assertRaisesRegex(errors.ValidationError, "env build train"):
            environments.image_for("train", "local")

    def test_a_change_not_built_still_runs_what_was_built_and_says_so(self):
        self.built("polars")
        before = environments.here("train")
        environments.add("train", "lightgbm")
        with mock.patch("sys.stderr") as said:
            self.assertEqual(environments.here("train"), before)
        self.assertIn("changed and is not built", "".join(str(c.args[0]) for c in said.write.call_args_list))

    def test_elsewhere_it_is_the_published_digest(self):
        self.built("polars", registry="r.example/team")
        environments.push("train", out=io.StringIO())
        self.assertEqual(environments.image_for("train", "gcp-gpu"), "r.example/team/train@sha256:manifest-gzip")
        self.assertEqual(environments.packages_for("train", "gcp-gpu"), [])

    def test_elsewhere_unpublished_with_a_public_base_it_is_the_base_and_its_packages(self):
        self.built("polars", registry="r.example/team")
        env = environments._load("train")
        with mock.patch("zenthros.registry.anyone_can_read", return_value=True):
            self.assertEqual(environments.image_for("train", "gcp-gpu"), f"{env.base}@{env.base_digest}")
        wanted = environments.packages_for("train", "gcp-gpu")
        self.assertEqual(len(wanted), 1)
        self.assertTrue(wanted[0].startswith("polars"))
        self.assertIn("==", wanted[0], "the lock's version, not whatever is new there")
        self.assertEqual(environments.packages_for("train", "local"), [])

    def test_elsewhere_unpublished_with_a_private_base_says_to_publish(self):
        self.built("polars", registry="r.example/team")
        with mock.patch("zenthros.registry.anyone_can_read", return_value=False), \
                self.assertRaisesRegex(errors.ValidationError, "env push train"):
            environments.image_for("train", "gcp-gpu")

    def test_elsewhere_unpublished_with_its_own_dockerfile_says_to_publish(self):
        self.built("polars", registry="r.example/team")
        (environments._load("train").folder / environments.DOCKERFILE).write_text("FROM x\n")
        with self.assertRaisesRegex(errors.ValidationError, "env push train"):
            environments.image_for("train", "gcp-gpu")

    def test_a_function_needs_the_same_python_on_both_sides(self):
        self.built("polars")
        with self.assertRaisesRegex(errors.ValidationError, "3.12 and this is 3.11"):
            environments.image_for("train", "local", python="3.11")
        environments.image_for("train", "local", python="3.12")
        environments.image_for("train", "local")

    def test_a_call_with_env_runs_in_its_image_and_image_wins(self):
        self.built("polars")
        made = remote_call._named_env({"env": "train", "image": ""}, "local", None)
        self.assertEqual(made["image"], environments.here("train"))
        self.assertIsNone(made["env"])
        self.assertEqual(remote_call._named_env({"env": "train", "image": "mine:1"}, "local", None)["image"], "mine:1")

    def test_a_dict_of_env_is_still_values(self):
        made = remote_call._named_env({"env": {"A": "1"}, "image": ""}, "local", None)
        self.assertEqual(made["env"], {"A": "1"})
        self.assertFalse(made["image"])

    def test_in_use_is_for_local_calls_with_no_image(self):
        self.built("polars")
        (self.root / ".zenthros" / "envs").mkdir(exist_ok=True, parents=True)
        (self.root / ".zenthros" / "envs" / "in-use").write_text("train\n")
        image = environments.here("train")
        self.assertEqual(remote_call._named_env({"image": ""}, "local", None)["image"], image)
        self.assertEqual(remote_call._named_env({"image": ""}, None, None)["image"], image)
        self.assertEqual(remote_call._named_env({"image": "mine:1"}, "local", None)["image"], "mine:1")
        self.assertFalse(remote_call._named_env({"image": ""}, "gcp-gpu", None)["image"])

    def test_use_prepares_it_before_it_says_it_is_in_use(self):
        self.built("polars")
        image = environments.here("train")
        with mock.patch.object(remote_call, "run") as run, mock.patch("zenthros.machines.python_minor", return_value="3.12"):
            environments.use("train")
        self.assertEqual(run.call_args.kwargs["image"], image)
        self.assertEqual(run.call_args.kwargs["on"], "local")
        self.assertTrue(callable(run.call_args.args[0]))
        run.return_value.result.assert_called_once()
        self.assertEqual(environments.using(), "train")

    def test_use_failing_leaves_nothing_in_use(self):
        self.built("polars")
        with mock.patch.object(remote_call, "run", side_effect=errors.ValidationError("no")):
            with self.assertRaises(errors.ValidationError):
                environments.use("train")
        self.assertEqual(environments.using(), "")

    def test_use_with_another_python_prepares_with_a_command(self):
        self.built("polars")
        with mock.patch.object(remote_call, "run") as run, mock.patch("zenthros.machines.python_minor", return_value="3.11"):
            environments.use("train")
        self.assertEqual(run.call_args.args[0], ["true"])

    def test_use_none_goes_back_to_the_default(self):
        self.built("polars")
        with mock.patch.object(remote_call, "run"):
            environments.use("train")
        environments.use(None)
        self.assertEqual(environments.using(), "")


class OnACluster(EnvCase):
    HOSTING = "help: https://k3d.io\nhost: localhost:5111\nhostFromClusterNetwork: reg:5000\nhostFromContainerRuntime: reg:5000\n"

    def client(self, hosting=HOSTING, arch="amd64"):
        kube = mock.Mock()
        kube.name = "kubectl"
        def run(args, stdin=None, timeout=600):
            if "configmap" in args:
                return 0, hosting, ""
            if args[:2] == ["get", "nodes"]:
                return 0, json.dumps({"items": [{"status": {"nodeInfo": {"architecture": arch}, "allocatable": {}}}]}), ""
            raise AssertionError(args)
        kube.run = run
        return localk8s.LocalK8sJobClient(root=str(self.root), kube=kube, helm="/fake/helm")

    def test_it_goes_to_the_clusters_registry_from_here_and_is_pulled_from_inside(self):
        env = self.built("polars")
        image = self.client().image_of_env(env)
        self.assertEqual(image, "reg:5000/zenthros-env/proj/train@sha256:manifest-gzip")
        pushed = [o for o in self.engine.outputs() if "push=true" in o][-1]
        self.assertIn(f"name=localhost:5111/zenthros-env/proj/train:{env.build['input'][:12]},", pushed)
        self.assertIn("registry.insecure=true", pushed)

    def test_it_goes_once_for_each_build(self):
        env = self.built("polars")
        client = self.client()
        client.image_of_env(env)
        count = len(self.engine.builds())
        client.image_of_env(env)
        self.assertEqual(len(self.engine.builds()), count)

    def test_it_is_not_built_there_again(self):
        env = self.built("polars")
        count = len([o for o in self.engine.outputs() if o.startswith("type=local") or o.startswith("type=docker")])
        self.client().image_of_env(env)
        self.assertEqual(len([o for o in self.engine.outputs() if o.startswith("type=local") or o.startswith("type=docker")]), count)

    def test_a_cluster_with_no_registry_gets_the_engines_tag(self):
        env = self.built("polars")
        image = self.client(hosting="").image_of_env(env)
        self.assertEqual(image, environments.here("train"))
        self.assertFalse([o for o in self.engine.outputs() if "push=true" in o])

    def test_a_cluster_of_another_architecture_is_refused_before_a_pod(self):
        env = self.built("polars")
        client = self.client(arch="arm64")
        image = client.image_of_env(env)
        with self.assertRaisesRegex(errors.ValidationError, "only amd64.*arm64"):
            client._fits(image, False)


    def test_an_image_it_cannot_pull_is_not_called_a_lack_of_network(self):
        said = ('pod zr-f: ErrImagePull failed to pull and unpack image "localhost/zenthros-env/1c/two:63": failed to do '
                'request: Head "https://localhost/v2/zenthros-env/1c/two/manifests/63": dial tcp 127.0.0.1:443: connect: '
                "connection refused")
        client = self.client(hosting="")
        status = local_module._explained({"message": said}, "c", client)
        self.assertIn("--registry-create", status["message"])
        self.assertNotIn("no network", status["message"])

    def test_another_image_it_cannot_pull_may_still_be_the_network(self):
        said = 'pod zr-f: ErrImagePull failed to pull "python:3.13": dial tcp: lookup registry: no such host'
        status = local_module._explained({"message": said}, "c", self.client(hosting=""))
        self.assertIn("no network", status["message"])


class ForAnAgent(EnvCase):
    def rows(self):
        def row(name):
            found = mock.Mock(costs=False)
            found.name = name
            found.to_dict.return_value = {"name": name}
            return found

        with mock.patch("zenthros.targetlist.targets", lambda *a, **k: [row("local"), row("local-k8s"), row("gcp-gpu")]):
            return {found["name"]: found["environments"] for found in tools.targets()}

    def test_targets_say_which_environments_run_where(self):
        environments.new("empty", BASE)
        self.built("polars", registry="r.example/team")
        self.assertEqual(self.rows(), {"local": ["train"], "local-k8s": ["train"], "gcp-gpu": []})
        environments.push("train", out=io.StringIO())
        self.assertEqual(self.rows(), {"local": ["train"], "local-k8s": ["train"], "gcp-gpu": ["train"]})

    def test_an_agents_run_names_an_environment(self):
        self.built("polars")
        with mock.patch.object(remote_call, "run") as run:
            run.return_value.to_dict.return_value = {}
            tools.run(["true"], env="train", dry_run=True)
        self.assertEqual(run.call_args.kwargs["env"], "train")


class Cleaning(EnvCase):
    def test_only_this_projects_images_and_the_builder_go(self):
        self.built("polars")
        tag = environments.here("train")
        self.engine.images["localhost/zenthros-env/another/x:1"] = True
        self.assertEqual(environments.clean_images(dry_run=True), [tag, "builder zenthros"])
        self.assertIn(tag, self.engine.images)
        self.assertTrue(self.engine.home_builder)
        self.assertEqual(environments.clean_images(), [tag, "builder zenthros"])
        self.assertEqual(sorted(self.engine.images), ["localhost/zenthros-env/another/x:1"])
        self.assertFalse(self.engine.home_builder)

    def test_an_image_in_use_stays_out_of_the_list(self):
        self.built("polars")
        original = self.engine.run

        def run(args, stdin=None, timeout=600):
            if args[:2] == ["image", "rm"]:
                return 1, "", "image is being used by a container"
            return original(args, stdin, timeout)

        self.engine.run = run
        self.assertEqual(environments.clean_images(), ["builder zenthros"])


class TheCommands(EnvCase):
    def run_cli(self, *args):
        out = io.StringIO()
        code = cli.main(list(args), out=out)
        return code, out.getvalue()

    def test_the_whole_walk_by_command(self):
        code, text = self.run_cli("env", "new", "train", "--from", BASE, "--registry", "r.example/team")
        self.assertEqual(code, 0, text)
        self.assertIn(BASE_DIGEST[:19], text)
        self.assertEqual(self.run_cli("env", "add", "train", "polars")[0], 0)
        with mock.patch("sys.stderr"):
            self.assertEqual(self.run_cli("env", "build", "train")[0], 0)
            code, text = self.run_cli("env", "push", "train")
        self.assertEqual(code, 0, text)
        self.assertIn("nothing rebuilt", text)
        self.assertIn("identical", text)
        code, text = self.run_cli("env", "list", "--json")
        rows = json.loads(text)["environments"]
        self.assertEqual(rows[0]["name"], "train")
        self.assertEqual(rows[0]["published"], ["r.example/team/train@sha256:manifest-gzip"])

    def test_a_refusal_is_a_line_and_not_a_trace(self):
        code, text = self.run_cli("env", "build", "nope")
        self.assertEqual(code, 1)
        self.assertTrue(text.startswith("✗ no environment"))

    def test_use_needs_a_name_or_off(self):
        self.assertEqual(self.run_cli("env", "use")[0], 1)

    def test_clean_images_is_asked_for_by_name(self):
        self.built("polars")
        tag = environments.here("train")
        with mock.patch("zenthros.remote.options.local_client") as client:
            client.return_value.clean.return_value = []
            self.run_cli("clean", "--dry-run")
            self.assertIn(tag, self.engine.images)
            code, text = self.run_cli("clean", "--images", "--dry-run", "--json")
        self.assertEqual(code, 0, text)
        self.assertIn(tag, json.loads(text)["images"])

    def test_the_package_exposes_it(self):
        self.assertIs(zenthros.env, environments)


if __name__ == "__main__":
    unittest.main()
