# SPDX-License-Identifier: Apache-2.0

"""Pipeline, contracts and reach: what a chain of steps does, and when it stops."""

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import zenthros
from zenthros import cache as entries
from zenthros import contracts, errors, reach, remote
from zenthros.artifact import Artifact
from zenthros.core.storage import MemoryBlobStore
from zenthros.pipeline import RESUME, REUSE, RUN, SKIP, YOUR_FILE, Pipeline

from test_remote import STORE, ExecutingClient


def prep(path):
    return f"prep({path})"


def features(rows):
    return f"features({rows})"


def train(feats, depth=1):
    return f"train({feats}, depth={depth})"


def broken(rows):
    raise ValueError("this step fails")


class PipelineBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.project = self.root / "project"
        (self.project / ".zenthros").mkdir(parents=True)
        (self.project / "main.py").write_text("x = 1\n")
        cwd = os.getcwd()
        os.chdir(self.project)
        self.addCleanup(os.chdir, cwd)
        env = mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": str(self.root / "config" / "config.yaml"),
                                           "XDG_CACHE_HOME": str(self.root / "c")})
        env.start()
        self.addCleanup(env.stop)
        self.store = MemoryBlobStore()
        self.client = ExecutingClient(self.store)
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo-ml")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        placed = mock.Mock(made=None)
        placed.name = "demo"
        placed.options = {"pool": "demo", "backend": "", "acquire": None, "client": None}
        place = mock.patch("zenthros.machines.place", return_value=placed)
        place.start()
        self.addCleanup(place.stop)
        # What a place says of itself has tests of its own (test_targets).
        fits = mock.patch("zenthros.targetlist.plan_call", return_value=mock.Mock(problems=[]))
        self.plan_call = fits.start()
        self.addCleanup(fits.stop)
        self.pipe = Pipeline([("prep", prep), ("features", features), ("train", train)], name="churn")
        self.stderr = io.StringIO()
        out = contextlib.redirect_stderr(self.stderr)
        out.__enter__()
        self.addCleanup(out.__exit__, None, None, None)

    def submits(self):
        return len(self.client._jobs)


class WithCache(PipelineBase):
    def test_three_runs_send_2_1_1_only_what_changed(self):
        """C5: prep and features are reused; only train runs, and says why."""
        sent = []
        for depth in (4, 6, 8):
            before = self.submits()
            run = self.pipe.run("datos", cache=True, params={"train": {"depth": depth}})
            self.assertEqual(run.result(), f"train(features(prep(datos)), depth={depth})")
            sent.append(self.submits() - before)
            if depth != 4:
                self.assertEqual([run["prep"].action, run["features"].action, run["train"].action], [REUSE, REUSE, RUN])
                self.assertEqual(run["train"].why, "depth changed")
                self.assertIn("train: run: depth changed", run.status())
        self.assertEqual(sent, [3, 1, 1])

    def test_the_first_run_sends_2_and_a_new_file_invalidates_from_prep(self):
        data = self.root / "datos.csv"
        data.write_text("a\n")
        path = str(data)
        # This place shares the path with this computer, as a folder both mount.
        answers = mock.patch("zenthros.reach.answers", return_value=[])
        answers.start()
        self.addCleanup(answers.stop)
        self.pipe.run(path, cache=True).result()
        data.write_text("b\n")
        run = self.pipe.run(path, cache=True)
        self.assertEqual(run["prep"].action, RUN)
        self.assertEqual(run["prep"].why, "datos.csv changed")

    def test_a_failure_stops_and_says_so(self):
        pipe = Pipeline([("prep", prep), ("features", broken), ("train", train)], name="f")
        run = pipe.run("x", cache=True)
        self.assertEqual(run["train"].action, SKIP)
        self.assertIn("features failed", run["train"].why)
        with self.assertRaises(remote.RemoteError):
            run.result()


class WithoutCache(PipelineBase):
    def test_it_chains_and_returns_at_once(self):
        run = self.pipe.run("x")
        jobs = [self.client.submitted(step.job) for step in run.steps]
        self.assertEqual(jobs[1].spec.depends_on, [run.steps[0].job])
        self.assertEqual(jobs[2].spec.depends_on, [run.steps[1].job])
        self.assertEqual(run.result(), "train(features(prep(x)), depth=1)")

    def test_each_job_has_its_pipeline_step_and_key_labels(self):
        run = self.pipe.run("x")
        job = self.client.submitted(run["features"].job)
        self.assertEqual(job.labels["zenthros.ai/pipeline"], "churn")
        self.assertEqual(job.labels["zenthros.ai/step"], "features")
        self.assertIn(entries.KEY_LABEL, job.labels)
        self.assertFalse(json.loads(job.annotations[entries.PARTS_ANNOTATION])["mode"])

    def test_the_key_of_a_step_follows_what_is_above_it(self):
        one = self.pipe.run("x")
        two = self.pipe.run("x")
        three = self.pipe.run("y")
        label = lambda run: self.client.submitted(run["train"].job).labels[entries.KEY_LABEL]  # noqa: E731
        self.assertEqual(label(one), label(two))
        self.assertNotEqual(label(one), label(three))

    def test_resume_reuses_what_ended_and_runs_the_rest(self):
        pipe = Pipeline([("prep", prep), ("features", broken), ("train", train)], name="r")
        first = pipe.run("x")
        with self.assertRaises(Exception):
            first.result()
        before = self.submits()
        pipe.steps[1].fn = features
        again = pipe.resume()
        self.assertEqual(again["prep"].action, REUSE)
        self.assertEqual(again["prep"].job, first["prep"].job)
        self.assertEqual(again.result(), "train(features(prep(x)), depth=1)")
        self.assertEqual(self.submits() - before, 2)

    def test_nothing_to_resume_says_so(self):
        with self.assertRaisesRegex(errors.NotFound, "no run on record"):
            Pipeline([("a", prep)], name="never").resume()


class Rule3(PipelineBase):
    """A step cut short, with the same key: a new job from its last checkpoint."""

    def cut_short(self, run, step="train", seq=14):
        job = self.client._jobs[run[step].job]
        job["phase"] = "Failed"
        job["progress"] = {"uri": "s3://b/ck/14", "phase": "fit", "seq": seq, "sha256": "aa"}
        self.client.exit_codes[run[step].job] = "WORKER_LOST"

    def test_it_takes_the_checkpoint_of_the_step_with_the_same_key(self):
        first = self.pipe.run("x")
        first["features"].call.result()
        self.cut_short(first)
        before = self.submits()
        again = self.pipe.resume()
        self.assertEqual(again["prep"].action, REUSE)
        self.assertEqual(again["features"].action, REUSE)
        self.assertEqual(again["train"].action, RESUME)
        self.assertIn(f"checkpoint 14 of {first['train'].job}", again["train"].why)
        self.assertEqual(self.submits() - before, 1)
        spec = self.client.submitted(again["train"].job).spec
        self.assertEqual(spec.resume_from.job, first["train"].job)
        self.assertEqual(spec.resume_from.uid, self.client.job_object(first["train"].job)["metadata"]["uid"])

    def test_with_other_code_it_starts_over_and_says_so(self):
        first = self.pipe.run("x")
        first["features"].call.result()
        self.cut_short(first)
        self.pipe.steps[2].fn = _other_train
        again = self.pipe.resume()
        self.assertEqual(again["train"].action, RUN)
        self.assertIn("starts over", again["train"].why)
        self.assertIn("code", again["train"].why)
        self.assertIsNone(self.client.submitted(again["train"].job).spec.resume_from)

    def test_with_other_params_it_starts_over(self):
        first = self.pipe.run("x", params={"train": {"depth": 4}})
        first["features"].call.result()
        self.cut_short(first)
        again = self.pipe.resume(params={"train": {"depth": 8}})
        self.assertEqual(again["train"].action, RUN)
        self.assertIn("depth changed", again["train"].why)

    def test_a_source_with_no_checkpoint_starts_over_and_says_so(self):
        first = self.pipe.run("x")
        first["features"].call.result()
        self.cut_short(first)
        del self.client._jobs[first["train"].job]["progress"]
        again = self.pipe.resume()
        self.assertEqual(again["train"].action, RUN)
        self.assertIn("no confirmed checkpoint", again["train"].why)

    def test_a_source_that_is_gone_starts_over_and_says_so(self):
        first = self.pipe.run("x")
        first["features"].call.result()
        name = first["train"].job
        self.client._jobs.pop(name)
        self.client._order.remove(name)
        again = self.pipe.resume()
        self.assertEqual(again["train"].action, RUN)
        self.assertIn("is gone", again["train"].why)

    def test_a_source_still_running_is_not_resumed(self):
        first = self.pipe.run("x")
        first["features"].call.result()
        self.client._jobs[first["train"].job].update(phase="Running", progress={"uri": "s3://b/1", "seq": 1})
        again = self.pipe.resume()
        self.assertEqual(again["train"].action, RUN)
        self.assertIn("has not ended", again["train"].why)

    def test_a_good_entry_comes_before_a_checkpoint(self):
        first = self.pipe.run("x", cache=True)
        first.result()
        again = self.pipe.resume()
        self.assertEqual([step.action for step in again.steps], [REUSE, REUSE, REUSE])

    def test_with_the_cache_a_step_with_no_entry_takes_its_checkpoint(self):
        pipe = Pipeline([("prep", prep), ("features", features), ("train", broken)], name="cut")
        first = pipe.run("x", cache=True)
        with self.assertRaises(Exception):
            first.result()
        self.client._jobs[first["train"].job]["progress"] = {"uri": "s3://b/ck/2", "seq": 2, "sha256": "aa"}
        again = pipe.resume()
        self.assertEqual([step.action for step in again.steps], [REUSE, REUSE, RESUME])
        self.assertEqual(self.client.submitted(again["train"].job).spec.resume_from.job, first["train"].job)

    def test_a_plan_of_the_resume_says_it_and_makes_nothing(self):
        first = self.pipe.run("x")
        first["features"].call.result()
        self.cut_short(first)
        before = self.submits()
        plan = self.pipe.plan(resume=True)
        self.assertEqual(self.submits(), before)
        self.assertEqual([row.action for row in plan.steps], [REUSE, REUSE, RESUME])
        self.assertIn("checkpoint 14", plan.steps[2].reason)
        self.assertIn(first["train"].job, plan.steps[2].origin)

    def test_a_job_without_cache_never_enters_the_cache(self):
        run = self.pipe.run("x")
        run.result()
        objects = [self.client.job_object(step.job) for step in run.steps]
        self.assertEqual(entries.notice_finished(objects, [entries.Home(STORE, self.store)]), [])
        self.assertEqual(list(entries.entries(entries.Home(STORE, self.store))), [])


def _other_train(feats, depth=1):
    return f"other({feats}, depth={depth})"


class KeepFor(PipelineBase):
    def test_a_job_is_kept_a_week_unless_you_say(self):
        run = self.pipe.run("x")
        self.assertEqual(self.client.submitted(run["prep"].job).spec.gc_policy.ttl_seconds_after_finished, 7 * 86400)
        run = self.pipe.run("x", keep_for="1h")
        self.assertEqual(self.client.submitted(run["prep"].job).spec.gc_policy.ttl_seconds_after_finished, 3600)


class ByHand(PipelineBase):
    def test_start_at_makes_no_jobs_for_what_is_before(self):
        run = self.pipe.run(start_at="features", upstream={"prep": "hecho"})
        self.assertEqual(run["prep"].action, SKIP)
        self.assertEqual(run.result(), "train(features(hecho), depth=1)")
        self.assertEqual(self.submits(), 2)

    def test_upstream_from_a_call_is_its_value(self):
        first = remote.run(prep, "x", code_dir=None, on="demo-ml")
        run = self.pipe.run(start_at="features", upstream={"prep": first}, cache=True)
        self.assertEqual(run.result(), "train(features(prep(x)), depth=1)")

    def test_upstream_of_the_wrong_step_is_refused(self):
        with self.assertRaisesRegex(errors.ValidationError, "takes what features leaves"):
            self.pipe.run(start_at="train", upstream={"prep": "x"})
        with self.assertRaisesRegex(errors.ValidationError, "not before"):
            self.pipe.run(start_at="prep", upstream={"train": "x"})
        with self.assertRaisesRegex(errors.ValidationError, "goes with start_at"):
            self.pipe.run("x", upstream={"prep": "x"})

    def test_unknown_names_are_refused(self):
        with self.assertRaisesRegex(errors.ValidationError, "no step"):
            self.pipe.run(start_at="nada")
        with self.assertRaisesRegex(errors.ValidationError, "params names"):
            self.pipe.run("x", params={"nada": {}})
        with self.assertRaises(errors.ValidationError):
            Pipeline([("a", prep), ("a", train)])

    def test_a_cloud_function_keeps_its_options(self):
        @zenthros.cloud(memory="2Gi")
        def step(x):
            return x

        run = Pipeline([("s", step)], name="o").run("x")
        self.assertEqual(self.client.submitted(run["s"].job).spec.compute.memory, "2Gi")


def parquet_at(path, columns):
    try:
        pa = __import__("pyarrow")
        pq = __import__("pyarrow.parquet", fromlist=["x"])
    except ImportError:
        raise unittest.SkipTest("pyarrow writes the test's parquet: pip install pyarrow") from None
    pq.write_table(pa.table({name: pa.array([1, 2], type=kind) for name, kind in columns.items()}), path)
    return str(path)


class Contracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_parquet_columns_come_from_its_footer(self):
        path = parquet_at(self.root / "a.parquet", {"user_id": "int64", "score": "float32"})
        contract = contracts.parquet(columns={"user_id": "int64", "score": "float"})
        contracts.check_ref(path, contract)

    def test_only_the_footer_is_read(self):
        path = parquet_at(self.root / "a.parquet", {"user_id": "int64"})
        store = mock.Mock()
        real = Path(path).read_bytes()
        store.tail.side_effect = lambda uri, size: real[-size:]
        contracts.check_ref("s3://b/a.parquet", contracts.parquet(columns={"user_id": "int64"}), store=store)
        store.get.assert_not_called()
        self.assertTrue(all(call.args[1] < (2 << 20) for call in store.tail.call_args_list))

    def test_a_missing_column_is_named(self):
        path = parquet_at(self.root / "a.parquet", {"user_id": "int64"})
        with self.assertRaisesRegex(contracts.ContractError, "no column score"):
            contracts.check_ref(path, contracts.parquet(columns={"score": "float"}))

    def test_a_wrong_type_is_named(self):
        path = parquet_at(self.root / "a.parquet", {"score": "int64"})
        with self.assertRaisesRegex(contracts.ContractError, "score is int64, not float"):
            contracts.check_ref(path, contracts.parquet(columns={"score": "float"}))

    def test_not_parquet(self):
        path = self.root / "a.parquet"
        path.write_text("hola, soy csv")
        with self.assertRaisesRegex(contracts.ContractError, "not a Parquet file"):
            contracts.check_ref(str(path), "parquet")

    def test_a_folder_of_parquet(self):
        (self.root / "d").mkdir()
        parquet_at(self.root / "d" / "a.parquet", {"x": "int64"})
        contracts.check_ref(str(self.root / "d"), contracts.parquet(columns={"x": "int64"}))
        (self.root / "e").mkdir()
        with self.assertRaisesRegex(contracts.ContractError, "no .parquet"):
            contracts.check_ref(str(self.root / "e"), "parquet")

    def test_json_file_and_dir(self):
        good = self.root / "a.json"
        good.write_text('{"a": 1}')
        contracts.check_ref(str(good), "json")
        good.write_text("no")
        with self.assertRaisesRegex(contracts.ContractError, "not JSON"):
            contracts.check_ref(str(good), "json")
        contracts.check_ref(str(good), "file")
        with self.assertRaises(contracts.ContractError):
            contracts.check_ref(str(self.root), "file")
        contracts.check_ref(str(self.root), "dir")
        with self.assertRaises(contracts.ContractError):
            contracts.check_ref(str(good), "dir")
        with self.assertRaisesRegex(contracts.ContractError, "not there"):
            contracts.check_ref(str(self.root / "none"), "file")

    def test_a_json_value_needs_no_file(self):
        contracts.check_output({"format": "json"}, {"a": 1})
        with self.assertRaisesRegex(contracts.ContractError, "expected JSON"):
            contracts.check_output({"format": "json"}, object())

    def test_a_named_output_may_be_returned_or_left(self):
        contracts.check_output({"model": {"format": "json"}}, {"model": {"a": 1}})
        out = self.root / "out"
        out.mkdir()
        (out / "model.json").write_text("{}")
        contracts.check_output({"model": {"format": "json"}}, None, str(out))
        with self.assertRaisesRegex(contracts.ContractError, "neither returned"):
            contracts.check_output({"other": {"format": "file"}}, None, str(out))

    def test_unknown_format_is_refused(self):
        with self.assertRaises(errors.ValidationError):
            contracts.declared("csv")

    def test_an_artifact_is_a_reference(self):
        path = parquet_at(self.root / "a.parquet", {"x": "int64"})
        contracts.check_ref(Artifact(path), contracts.parquet(columns={"x": "int64"}))

    def test_connections(self):
        self.assertIs(contracts.connects(None, {"format": "file"})[0], None)
        self.assertFalse(contracts.connects({"format": "file"}, {"format": "parquet"})[0])
        self.assertTrue(contracts.connects({"format": "parquet"}, {"format": "parquet"})[0])
        self.assertFalse(contracts.connects({"format": "parquet", "columns": {"a": "int64"}},
                                            {"format": "parquet", "columns": {"b": "int64"}})[0])


class ContractsInAJob(PipelineBase):
    def _parquet(self, columns):
        return parquet_at(self.root / "f.parquet", columns)

    def test_an_output_that_fails_reaches_neither_the_next_step_nor_the_cache(self):
        """D6: OUTPUT_CONTRACT."""
        path = self._parquet({"user_id": "int64"})

        @zenthros.cloud(outputs=zenthros.parquet(columns={"score": "float"}))
        def make(x):
            return path

        ran = []

        def after(feats):
            ran.append(feats)
            return feats

        run = Pipeline([("features", make), ("train", after)], name="c").run("x", cache=True, code_dir=str(self.project))
        self.assertEqual(run["train"].action, SKIP)
        with self.assertRaises(remote.RemoteError) as caught:
            run.result()
        self.assertEqual(caught.exception.reason, "OUTPUT_CONTRACT")
        self.assertIn("no column score", str(caught.exception))
        self.assertIn("fix what the function leaves", str(caught.exception))
        self.assertEqual(ran, [])
        self.assertFalse([uri for uri in self.store.data if "/cache/" in uri])

    def test_inputs_fail_before_the_function_runs(self):
        ran = []

        @zenthros.cloud(inputs={"path": "file"})
        def take(path):
            ran.append(path)

        with self.assertRaises(remote.RemoteError) as caught:
            take.remote("/nowhere/at/all.json", code_dir=None)
        self.assertEqual(caught.exception.reason, "INPUT_CONTRACT")
        self.assertEqual(ran, [])

    def test_what_you_give_fails_before_a_job_exists(self):
        """D5."""
        path = self._parquet({"user_id": "int64"})

        @zenthros.cloud(inputs={"feats": zenthros.parquet(columns={"score": "float"})})
        def step(feats):
            return feats

        pipe = Pipeline([("features", prep), ("train", step)], name="d5")
        with self.assertRaisesRegex(contracts.ContractError, "no column score"):
            pipe.run(start_at="train", upstream={"features": path})
        self.assertEqual(self.submits(), 0)

    def test_outs_list_what_it_left_with_their_digests(self):
        out = self.root / "out"
        out.mkdir()
        (out / "model.onnx").write_bytes(b"model")
        from zenthros.core.storage import FileBlobStore
        from zenthros.remote.runner import _outs

        (out / "result.pkl").write_bytes(b"not an output")
        listed = _outs(str(out), FileBlobStore())
        self.assertEqual([os.path.basename(item["uri"]) for item in listed], ["model.onnx"])
        self.assertEqual(listed[0]["sha256"], __import__("hashlib").sha256(b"model").hexdigest())


class Reach(unittest.TestCase):
    def test_a_local_path(self):
        with tempfile.NamedTemporaryFile() as handle:
            self.assertEqual(reach.of(handle.name, "local").answer, reach.POSSIBLE)
            self.assertEqual(reach.of(handle.name, "gcp-gpu").answer, reach.IMPOSSIBLE)
            self.assertEqual(reach.of(handle.name, "local-k8s", shared_folder="/nowhere").answer, reach.IMPOSSIBLE)
            self.assertEqual(reach.of(handle.name, "local-k8s", shared_folder=os.path.dirname(handle.name)).answer,
                             reach.POSSIBLE)

    def test_a_folder_that_only_starts_like_the_shared_one_is_not_under_it(self):
        self.assertEqual(reach.of("/home/a/k8s-other/x", "local-k8s", shared_folder="/home/a/k8s").answer, reach.IMPOSSIBLE)
        self.assertIn("do not have", reach.of("/home/a/x", "local-k8s", shared_folder="").why, "no folder to put it in")

    def test_the_store_of_the_place_is_possible_and_others_unchecked(self):
        self.assertEqual(reach.of("gs://b/z/x", "vm", store="gs://b/z").answer, reach.POSSIBLE)
        self.assertEqual(reach.of("gs://other/x", "vm", store="gs://b/z").answer, reach.UNCHECKED)
        self.assertEqual(reach.of("gs://b/zz/x", "vm", store="gs://b/z").answer, reach.UNCHECKED)
        self.assertEqual(reach.of("https://x.y/z", "vm").answer, reach.UNCHECKED)

    def test_with_no_network_nothing_remote_is_reached(self):
        for uri in ("gs://b/x", "s3://b/x", "https://x.y/z"):
            self.assertEqual(reach.of(uri, "local", offline=True).answer, reach.IMPOSSIBLE)


class Plans(PipelineBase):
    def test_plan_creates_nothing_and_says_each_step(self):
        plan = self.pipe.plan("x", cache=True)
        self.assertEqual(self.submits(), 0)
        self.assertEqual([step.action for step in plan.steps], [RUN, RUN, RUN])
        self.assertIn("nothing was created", str(plan))
        self.assertIn("has not run before", plan.steps[0].reason)
        self.assertIn("runs after prep", plan.steps[1].reason)
        self.assertEqual(plan.steps[2].key, "")

    def test_it_follows_good_entries_step_by_step(self):
        self.pipe.run("x", cache=True, params={"train": {"depth": 4}}).result()
        before = self.submits()
        plan = self.pipe.plan("x", cache=True, params={"train": {"depth": 6}})
        self.assertEqual(self.submits(), before)
        self.assertEqual([step.action for step in plan.steps], [REUSE, REUSE, RUN])
        self.assertEqual(plan.steps[2].reason, "depth changed")
        self.assertEqual(plan.steps[0].integrity, "same bytes")
        self.assertEqual(plan.steps[0].origin, "signed by you")

    def test_what_you_gave_has_its_four_answers(self):
        path = parquet_at(self.root / "limpio.parquet", {"score": "float"})

        @zenthros.cloud(inputs={"feats": zenthros.parquet(columns={"score": "float"})})
        def step(feats):
            return feats

        pipe = Pipeline([("a", prep), ("limpiar", prep), ("train", step)], name="p")
        plan = pipe.plan(start_at="train", upstream={"limpiar": path}, on="local")
        self.assertEqual([row.action for row in plan.steps], [SKIP, YOUR_FILE, RUN])
        row = plan.steps[1]
        self.assertEqual(row.available, "it is there")
        self.assertEqual(row.compatible, "it has the shape asked")
        self.assertIn("nobody", row.integrity)
        self.assertIn("nobody checks how it was made", row.origin)
        self.assertTrue(plan.ok)

    def test_a_contract_that_does_not_hold_is_a_problem(self):
        path = parquet_at(self.root / "limpio.parquet", {"other": "int64"})

        @zenthros.cloud(inputs={"feats": zenthros.parquet(columns={"score": "float"})})
        def step(feats):
            return feats

        plan = Pipeline([("a", prep), ("b", step)], name="p").plan(start_at="b", upstream={"a": path})
        self.assertFalse(plan.ok)
        self.assertIn("no column score", plan.problems[0])

    def test_what_the_place_says_is_a_problem_of_the_step(self):
        self.plan_call.return_value = mock.Mock(problems=["no node of it has 64Gi"])
        plan = self.pipe.plan("x", on="local-k8s")
        self.assertIn("prep: no node of it has 64Gi", plan.problems)

    def test_a_connection_that_cannot_fit_is_a_problem(self):
        @zenthros.cloud(outputs="file")
        def up(x):
            return x

        @zenthros.cloud(inputs="parquet")
        def down(x):
            return x

        plan = Pipeline([("up", up), ("down", down)], name="p").plan("x")
        self.assertIn("leaves file and this one takes parquet", plan.problems[0])

    def test_a_connection_without_contracts_is_not_compared_and_says_so(self):
        self.assertIn("not compared", self.pipe.plan("x").steps[1].compatible)

    def test_a_local_path_on_a_remote_place_is_stopped(self):
        data = self.root / "d.csv"
        data.write_text("x")
        plan = self.pipe.plan(str(data), on="gcp-gpu")
        self.assertIn("only on this computer", plan.problems[0])
        with self.assertRaisesRegex(errors.ValidationError, "only on this computer"):
            self.pipe.run(str(data), on="gcp-gpu")
        self.assertEqual(self.submits(), 0)

    def test_with_no_network_a_uri_is_stopped(self):
        """D9."""
        with mock.patch.dict(os.environ, {"ZENTHROS_LOCAL_NETWORK": "none"}):
            self.assertFalse(self.pipe.plan("gs://b/x").ok)
            with self.assertRaisesRegex(errors.ValidationError, "no network"):
                self.pipe.run("gs://b/x")

    def test_what_would_move_and_what_is_not_checked_are_said(self):
        plan = self.pipe.plan("gs://b/x", on="gcp-gpu")
        text = str(plan)
        self.assertIn("prep reads gs://b/x from gcp-gpu", text)
        self.assertIn("not checked until it runs", text)
        self.assertEqual(json.loads(json.dumps(plan.to_dict()))["steps"][0]["step"], "prep")

    def test_the_question_to_a_place_carries_none_of_the_directory(self):
        # Like zenthros attach's probe (hub.sh smoke, 2026-11-06): the SDK's
        # own function needs none of the person's files, and a project over
        # what a job without a bucket carries would refuse the question.
        from zenthros import pipeline

        rows = mock.Mock()
        rows.get.return_value = mock.Mock(template=False, state="Available")
        row = pipeline.StepPlan(step="prep", action=RUN, unchecked=["gs://b/x"])
        with mock.patch("zenthros.targetlist.targets", return_value=rows), \
                mock.patch.object(pipeline, "run") as run:
            run.return_value.result.return_value = {"gs://b/x": 1}
            pipeline._probed(row, ["gs://b/x"], "gcp-gpu")
        self.assertIsNone(run.call_args.kwargs.get("code_dir", "."))
        self.assertIn("1 objects seen from gcp-gpu", row.available)
        # From a wheel the function travels by name: its module must be one
        # the call ships, and pipeline.py is not (ModuleNotFoundError there,
        # told as "cannot read them").
        from zenthros.remote import bundle

        asked = run.call_args.args[0]
        self.assertIn(asked.__module__.replace(".", "/") + ".py", bundle.sdk_files())

    def test_a_place_that_is_not_up_is_not_asked(self):
        rows = mock.Mock()
        rows.get.return_value = mock.Mock(template=True, state="Available")
        with mock.patch("zenthros.targetlist.targets", return_value=rows):
            plan = self.pipe.plan("gs://b/x", on="gcp-gpu", probe=True)
        self.assertEqual(self.submits(), 0)
        self.assertIn("not asked", plan.steps[0].unchecked[0])


class Elsewhere(ExecutingClient):
    """A place with no hub in common with the other: a job there cannot wait for one here."""


class AcrossPlaces(PipelineBase):
    def setUp(self):
        super().setUp()
        self.here = Elsewhere(self.store)
        clients = {"here": self.here, "there": self.client}

        def place(on, *_, **__):
            placed = mock.Mock(made=None)
            placed.name = on
            placed.options = {"pool": on, "backend": "", "acquire": None, "client": clients[on]}
            return placed

        for target, side_effect in (("zenthros.machines.place", place),
                                    ("zenthros.remote.call._client_for", clients.__getitem__)):
            patched = mock.patch(target, side_effect=side_effect)
            patched.start()
            self.addCleanup(patched.stop)
        self.two = Pipeline([("prep", prep), ("train", train)], name="churn")

    def test_a_value_settles_here_before_a_job_goes_there(self):
        run = self.two.run("x", on={"prep": "here", "train": "there"})
        self.assertEqual(run.result(), train(prep("x")))
        self.assertEqual(len(self.here._jobs), 1)
        there = next(iter(self.client._jobs.values()))
        # A job waits only for jobs of its own place.
        self.assertEqual(there["job"].spec.depends_on, [])

    def test_on_the_same_place_the_job_waits_for_the_one_above(self):
        run = self.two.run("x", on={"prep": "there", "train": "there"})
        self.assertEqual(run.result(), train(prep("x")))
        waits = [job["job"].spec.depends_on for job in self.client._jobs.values()]
        self.assertEqual(waits, [[], [run["prep"].job]])

    def test_a_step_that_fails_here_sends_nothing_there(self):
        broken_first = Pipeline([("prep", broken), ("train", train)], name="churn")
        run = broken_first.run("x", on={"prep": "here", "train": "there"})
        self.assertEqual(run["train"].action, SKIP)
        self.assertEqual(len(self.client._jobs), 0)
        with self.assertRaises(Exception):
            run.result()


class PlansOfWhatGoesUp(PipelineBase):
    def setUp(self):
        super().setUp()
        self.data = self.root / "elsewhere.csv"
        self.data.write_text("a,b\n1,2\n")

    def test_a_path_only_here_stops_the_plan(self):
        plan = self.pipe.plan(str(self.data))
        self.assertFalse(plan.ok)
        self.assertEqual(self.submits(), 0)

    def test_with_upload_it_says_what_it_would_send_and_sends_nothing(self):
        plan = self.pipe.plan(str(self.data), upload=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertIn(str(self.data), str(plan))
        self.assertIn("would send", plan.steps[0].transfers[0])
        self.assertEqual(self.submits(), 0)
        self.assertFalse([item for item in self.store.ls(STORE) if "/data/" in item.uri])


    def test_run_with_upload_sends_what_the_plan_said(self):
        # Found on Vast: the plan said it would send the file, and run refused it.
        with self.assertRaisesRegex(errors.ValidationError, "upload=True"):
            self.pipe.run(str(self.data))
        self.assertEqual(self.submits(), 0)
        run = self.pipe.run(str(self.data), upload=True)
        self.assertEqual(self.submits(), 3)
        self.assertTrue([item for item in self.store.ls(STORE) if "/data/" in item.uri], "nothing went to the store")
        self.assertNotIn(str(self.data), run.result())


class Cleaning(PipelineBase):
    def _workspace(self):
        from zenthros import view

        ws = view.Workspace.__new__(view.Workspace)
        ws._client = self.client
        ws._blobs = lambda root: self.store
        store = mock.patch.object(view.Workspace, "store", new_callable=mock.PropertyMock, return_value=STORE)
        store.start()
        self.addCleanup(store.stop)
        return ws

    def test_what_a_good_entry_pins_stays_and_a_forged_one_protects_nothing(self):
        """D11."""
        import datetime

        from zenthros.remote.call import RESULT

        call = remote.run(prep, "x", on="demo-ml", code_dir=None, cache=True)
        call.result()
        other = remote.run(prep, "y", on="demo-ml", code_dir=None)
        other.result()
        for name in list(self.client._jobs):
            del self.client._jobs[name]
        forged = entries.signed({"key": "f" * 64, "result": {"uri": f"{other.prefix}/{RESULT}", "sha256": "0"}, "outs": []})
        forged["signature"] = "hmac-sha256:" + "0" * 64
        self.store.put(f"{STORE}/cache/{'f' * 64}.json", json.dumps(forged).encode())
        ws = self._workspace()
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30)
        with mock.patch.object(type(ws), "_now", staticmethod(lambda: future)), \
                mock.patch.object(self.store, "ls", _dated(self.store)):
            report = ws.clean("1d", dry_run=True)
            self.assertIn(f"{call.prefix}/{RESULT}", report.kept_cached)
            self.assertIn(other.name, report.calls)
            self.assertIn(f"{call.prefix}/{RESULT}", self.store.data)
            report = ws.clean("1d")
        self.assertIn(f"{call.prefix}/{RESULT}", self.store.data)
        self.assertNotIn(f"{other.prefix}/{RESULT}", self.store.data)

    def test_a_hit_that_lost_its_bytes_in_a_clean_runs_again(self):
        call = remote.run(prep, "x", on="demo-ml", code_dir=None, cache=True)
        call.result()
        del self.store.data[f"{call.prefix}/result.pkl"]
        again = remote.run(prep, "x", on="demo-ml", code_dir=None, cache=True)
        self.assertFalse(again.cached)
        self.assertEqual(again.result(), "prep(x)")


class OnAMachine(unittest.TestCase):
    """As the agent runs it: a process of its own, which is given no store object."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        env = mock.patch.dict(os.environ, {"ZENTHROS_CONFIG": f"{self.tmp.name}/config/config.yaml"})
        env.start()
        self.addCleanup(env.stop)

    def test_a_cached_call_lists_its_outs_and_is_reused(self):
        from test_remote_machine import machine

        with machine() as (client, _):
            # What the agent sets on every job, and this client does not.
            output = {"ZENTHROS_DATA_OUTPUT": "s3://zr-machine/calls/out"}
            call = remote.run(_returns_three, on="demo-ml", code_dir=None, cache=True, env=output)
            try:
                self.assertEqual(call.result(stream_logs=False), 3)
            except Exception:
                print("\n".join(client.outputs))
                raise
            self.assertTrue(remote.run(_returns_three, on="demo-ml", code_dir=None, cache=True, env=output).cached)

    def test_a_contract_is_checked_where_it_runs(self):
        from test_remote_machine import machine

        with machine() as (_, _store):
            with self.assertRaises(remote.RemoteError) as caught:
                remote.run(_returns_three, on="demo-ml", code_dir=None, outputs="file").result(stream_logs=False)
        self.assertEqual(caught.exception.reason, "OUTPUT_CONTRACT")

    def test_a_contract_on_a_uri_reads_it_with_the_store_of_the_machine(self):
        from test_remote_machine import machine

        with machine() as (_, store):
            store.put("s3://zr-machine/calls/data.json", b"{}")
            value = remote.run(_returns_uri, "s3://zr-machine/calls/data.json", on="demo-ml", code_dir=None, outputs="json",
                               inputs={"uri": "json"}, env={"ZENTHROS_DATA_OUTPUT": "s3://zr-machine/calls/out"})
            self.assertEqual(value.result(stream_logs=False), "s3://zr-machine/calls/data.json")


def _returns_three():
    return 3


def _returns_uri(uri):
    return uri


def _dated(store):
    import datetime

    from zenthros.core.storage import StoredObject

    def ls(prefix):
        then = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        return [StoredObject(uri, len(raw), then, "v") for uri, raw in sorted(store.data.items()) if uri.startswith(str(prefix))]

    return ls


if __name__ == "__main__":
    unittest.main()
