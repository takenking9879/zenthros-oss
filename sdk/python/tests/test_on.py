#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""f.remote(on=, params=, keep=): where a decorated function runs and with
what arguments.

Pinned: params are the function's arguments, even one called like an
option (memory, timeout); a machine made for the call goes after its
result is collected, never before, and stays with keep=True; a call that
could not be sent, or that is interrupted, leaves no machine behind.
"""
import io
import unittest
from unittest import mock

from zenthros import errors, machines, remote
from zenthros.core.storage import MemoryBlobStore


def sized(rows, memory="4Gi", timeout=10):
    return {"rows": rows, "memory": memory, "timeout": timeout}


class Placed(unittest.TestCase):
    """A Workspace whose calls run here, and machines.place and delete
    recorded: no tests of its own, so a subclass runs only its own."""

    def setUp(self):
        from test_remote import STORE, ExecutingClient

        self.store = MemoryBlobStore()
        self.events = []
        self.client = ExecutingClient(self.store, events=self.events)
        configured = remote.configure(store=STORE, client=self.client, blob_store=self.store, on="demo")
        configured.__enter__()
        self.addCleanup(configured.__exit__, None, None, None)
        self.made = machines.Machine(name="ct-9", id="ct-9", target="local")
        self.deleted = []
        patcher = mock.patch.object(machines, "delete", lambda m, **kw: (self.deleted.append(m.id),
                                                                         self.events.append(("delete", m.id))))
        patcher.start()
        self.addCleanup(patcher.stop)

    def placed(self, made=True):
        return mock.patch.object(machines, "place", return_value=machines.Placement({"backend": "ct-9"},
                                                                                    made=self.made if made else None))

    def spec(self, call):
        return self.client.submitted(call.name).spec


class OnTest(Placed):
    def test_params_are_the_functions_arguments_even_one_named_like_an_option(self):
        fn = remote.cloud(memory="2Gi")(sized)
        with self.placed(made=False):
            value = fn.remote(on="mi-pc", params={"rows": 5, "memory": "16Gi", "timeout": 99})
        self.assertEqual(value, {"rows": 5, "memory": "16Gi", "timeout": 99})

    def test_on_a_backend_goes_there_and_nowhere_else(self):
        # Whether its image applies there is the placement's word (it
        # refuses image= on a machine that runs no images).
        fn = remote.cloud(image="python:3.13")(sized)
        with self.placed(made=False):
            call = fn.submit(on="mi-pc", params={"rows": 1})
        spec = self.spec(call)
        self.assertEqual((spec.target.backends, spec.pool_ref, spec.workload.template.image),
                         (["ct-9"], "", "python:3.13"))

    def test_a_machine_made_for_the_call_goes_after_its_result(self):
        fn = remote.cloud(sized)
        read = self.store.get

        def get(uri):
            self.events.append(("get", uri))
            return read(uri)

        with self.placed(), mock.patch.object(self.store, "get", get):
            self.assertEqual(fn.remote(on="local", params={"rows": 2})["rows"], 2)
        self.assertEqual(self.deleted, ["ct-9"])
        results = [i for i, e in enumerate(self.events) if e[0] == "get" and e[1].endswith("/result.pkl")]
        deleted = [i for i, e in enumerate(self.events) if e[0] == "delete"]
        self.assertTrue(results and deleted and max(results) < deleted[0], f"the machine went before the result: {self.events}")

    def test_keep_keeps_it(self):
        fn = remote.cloud(sized)
        with self.placed():
            fn.remote(on="local", params={"rows": 2}, keep=True)
        self.assertEqual(self.deleted, [])

    def test_submit_leaves_it_until_the_result_is_read(self):
        fn = remote.cloud(sized)
        with self.placed():
            call = fn.submit(on="local", params={"rows": 3})
        self.assertEqual(self.deleted, [])
        self.assertEqual(call.made, self.made)
        call.result(stream_logs=False)
        call.result(stream_logs=False)
        call.release()
        self.assertEqual(self.deleted, ["ct-9"], "deleted once, after the result")

    def test_a_call_that_could_not_be_sent_leaves_no_machine(self):
        fn = remote.cloud(sized)
        with self.placed(), mock.patch.object(self.client, "submit", side_effect=ConnectionError("api down")):
            with self.assertRaises(ConnectionError):
                fn.remote(on="local", params={"rows": 1})
        self.assertEqual(self.deleted, ["ct-9"])

    def test_a_call_that_may_have_been_sent_keeps_its_machine(self):
        """A submit whose answer was lost may have made the job: its machine
        stays for it, and the error names the machine to delete if the job
        is not there. Deleted, a job that was made lost its machine (third
        review of Ola J, zone C P3)."""
        from zenthros.jobs import http

        fn = remote.cloud(sized)
        lost = http._maybe_created("job-1", "connection reset")
        with self.placed(), mock.patch.object(self.client, "submit", side_effect=lost):
            with self.assertRaises(errors.TransientError) as caught:
                fn.remote(on="local", params={"rows": 1})
        self.assertEqual(self.deleted, [])
        self.assertIn("ct-9", str(caught.exception))
        self.assertIs(caught.exception.made, self.made)

    def test_an_interrupted_call_is_cancelled_and_leaves_no_machine(self):
        fn = remote.cloud(sized)
        with self.placed(), mock.patch.object(self.client, "wait", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                fn.remote(on="local", params={"rows": 1})
        self.assertEqual(self.deleted, ["ct-9"])
        from zenthros.jobs.handle import CANCELLED

        self.assertEqual([j["phase"] for j in self.client._jobs.values()], [CANCELLED], "the job was left running")

    def test_the_error_that_stopped_the_wait_is_raised_not_the_cancels(self):
        # The hub went away mid-wait: cancelling fails as well, and that
        # failure once replaced the error the caller had to see.
        fn = remote.cloud(sized)
        with self.placed(), mock.patch.object(self.client, "wait", side_effect=errors.TransientError("api down")), \
                mock.patch.object(self.client, "cancel", side_effect=errors.TransientError("still down")), \
                mock.patch("sys.stderr", io.StringIO()) as said:
            with self.assertRaisesRegex(errors.TransientError, "api down"):
                fn.remote(on="local", params={"rows": 1})
        self.assertIn("could not cancel", said.getvalue())
        self.assertEqual(self.deleted, ["ct-9"])

    def test_leaving_a_with_block_that_raised_raises_its_error(self):
        fn = remote.cloud(sized)
        with self.placed():
            call = fn.submit(on="local", params={"rows": 1})
        with mock.patch.object(self.client, "job_status", side_effect=errors.TransientError("api down")), \
                mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaisesRegex(ValueError, "the block's"):
                with call:
                    raise ValueError("the block's")
        self.assertEqual(self.deleted, ["ct-9"])

    def test_on_a_template_asks_its_pool_for_a_new_machine(self):
        fn = remote.cloud(sized)
        placed = machines.Placement({"pool": "aws-spot", "acquire": ["Create"]})
        with mock.patch.object(machines, "place", return_value=placed) as place:
            call = fn.submit(on="aws-spot", params={"rows": 1}, keep=False)
        spec = self.spec(call)
        self.assertEqual((spec.pool_ref, spec.target, spec.capacity.acquire), ("aws-spot", None, ["Create"]))
        self.assertEqual(place.call_args.kwargs["keep"], False)


class RunAndServeOnTest(Placed):
    """zenthros.run and serve take on= as submit does: where, never an
    argument of the function. Before, run(f, on="x") called f(on="x")."""

    def test_run_places_a_function_and_never_passes_on_to_it(self):
        with self.placed(made=False):
            call = remote.run(sized, on="mi-pc", params={"rows": 4})
        self.assertEqual(self.spec(call).target.backends, ["ct-9"])
        self.assertEqual(call.result(stream_logs=False), {"rows": 4, "memory": "4Gi", "timeout": 10})

    def test_run_places_a_command_on_a_template(self):
        placed = machines.Placement({"pool": "aws-spot", "acquire": ["Create"]})
        with mock.patch.object(machines, "place", return_value=placed):
            call = remote.run("python train.py --epochs 10", on="aws-spot", code_dir=None)
        spec = self.spec(call)
        self.assertEqual((spec.pool_ref, spec.capacity.acquire), ("aws-spot", ["Create"]))
        self.assertEqual(spec.workload.template.command[-1], "python train.py --epochs 10")

    def test_run_releases_a_machine_made_for_it_after_its_result(self):
        with self.placed():
            call = remote.run(sized, on="local", params={"rows": 1})
            self.assertEqual(self.deleted, [])
            call.result(stream_logs=False)
        self.assertEqual(self.deleted, ["ct-9"])

    def test_run_dry_run_makes_nothing(self):
        with mock.patch("zenthros.targetlist.plan_call", return_value="the plan") as plan, \
                mock.patch.object(machines, "place", side_effect=AssertionError("placed")):
            self.assertEqual(remote.run("python x.py", on="aws-spot", cpu=2, dry_run=True), "the plan")
        self.assertEqual(plan.call_args.args[0], "aws-spot")
        self.assertEqual(plan.call_args.args[1]["cpu"], 2)
        self.assertEqual(self.client._jobs, {})

    def test_a_function_says_its_python_to_the_placement_and_a_command_does_not(self):
        from zenthros.remote import bundle

        with mock.patch.object(machines, "place", return_value=machines.Placement({"backend": "ct-9"})) as place:
            remote.run(sized, on="mi-pc", params={"rows": 1})
            remote.cloud(sized).submit(on="mi-pc", params={"rows": 1})
            remote.cloud(sized).map([{"rows": 1}], on="mi-pc")
            remote.run("python x.py", on="mi-pc", code_dir=None)
            remote.serve("python -m http.server", on="mi-pc", port=8000, secrets={}, code_dir=None)
        sent = [c.kwargs.get("python") for c in place.call_args_list]
        here = bundle.python_version()
        self.assertEqual(sent, [here, here, here, None, None])

    def test_serve_on_a_machine(self):
        with self.placed(made=False):
            api = remote.serve("python -m http.server 8000", on="mi-pc", port=8000, secrets={}, code_dir=None)
        spec = self.spec(api)
        self.assertEqual(spec.target.backends, ["ct-9"])
        self.assertEqual([p.port for p in spec.workload.template.service.ports], [8000])

    def test_serve_says_its_ports_to_the_placement_and_run_does_not(self):
        with mock.patch.object(machines, "place", return_value=machines.Placement({"backend": "ct-9"})) as place:
            remote.serve("python -m http.server", on="mi-pc", ports={"api": 8000, "metrics": 9090}, secrets={},
                         code_dir=None)
            remote.run("python x.py", on="mi-pc", code_dir=None)
        self.assertEqual([c.kwargs.get("ports") for c in place.call_args_list], [[8000, 9090], None])

    def test_serve_on_keeps_its_stop_after(self):
        with self.placed(made=False):
            api = remote.serve("python -m http.server 8000", on="mi-pc", port=8000, stop_after="2h", secrets={},
                               code_dir=None)
        self.assertEqual(self.spec(api).workload.template.service.stop_after_seconds, 7200)

    def test_serve_checks_its_ports_before_making_a_machine(self):
        from zenthros import errors

        with mock.patch.object(machines, "place", side_effect=AssertionError("placed")):
            with self.assertRaises(errors.ValidationError):
                remote.serve("python -m http.server", on="local")
            with self.assertRaises(errors.ValidationError):
                remote.serve("python -m http.server", on="local", port=8000, timeout=5)

    def test_a_cancelled_or_deleted_call_releases_its_machine(self):
        # A machine made for a call bills until it is gone: cancel(),
        # delete() and leaving a with block remove it, once (audit
        # 2026-10-16 AUD-041). Before, only result() and a Service's
        # cancel did.
        fn = remote.cloud(sized)
        with self.placed():
            call = fn.submit(on="local", params={"rows": 1})
        self.assertEqual(self.deleted, [])
        call.cancel()
        self.assertEqual(self.deleted, ["ct-9"])
        call.cancel()
        self.assertEqual(self.deleted, ["ct-9"], "released once")

        self.deleted.clear()
        with self.placed():
            call = fn.submit(on="local", params={"rows": 1})
        call.delete()
        self.assertEqual(self.deleted, ["ct-9"])

        self.deleted.clear()
        with self.placed():
            with fn.submit(on="local", params={"rows": 1}):
                pass
        self.assertEqual(self.deleted, ["ct-9"])

        # keep=True keeps the machine whatever happens to the call.
        self.deleted.clear()
        with self.placed():
            call = fn.submit(on="local", params={"rows": 1}, keep=True)
        call.cancel()
        self.assertEqual(self.deleted, [])

    def test_a_function_serves_on_a_machine_made_for_it_until_cancelled(self):
        def app():
            return None

        with self.placed():
            api = remote.cloud(app).serve(on="local", port=8000)
        self.assertEqual(self.deleted, [], "the machine went while it serves")
        api.cancel()
        self.assertEqual(self.deleted, ["ct-9"])
        api.cancel()
        self.assertEqual(self.deleted, ["ct-9"], "released once")


class KeepForTest(Placed):
    """keep_for= keeps a finished job (and so its result's digest, its logs
    and zenthros.get): seven days unless it says otherwise."""

    def gc(self, call):
        return self.client.submitted(call.name).to_manifest()["spec"].get("gcPolicy")

    def test_keep_for_is_the_jobs_ttl(self):
        call = remote.run(sized, params={"rows": 1}, keep_for="7d")
        self.assertEqual(self.gc(call), {"ttlSecondsAfterFinished": 7 * 86400})
        with self.placed(made=False):
            placed = remote.cloud(sized).submit(on="mi-pc", params={"rows": 1}, keep_for="36h")
        self.assertEqual(self.gc(placed), {"ttlSecondsAfterFinished": 36 * 3600})

    def test_without_it_a_job_stays_seven_days(self):
        self.assertEqual(self.gc(remote.run(sized, params={"rows": 1})), {"ttlSecondsAfterFinished": 7 * 86400})

    def test_its_bounds_and_words(self):
        from zenthros import errors

        for bad in ("91d", "soon", 0):
            with self.assertRaises(errors.ValidationError, msg=bad) as caught:
                remote.run(sized, params={"rows": 1}, keep_for=bad)
            self.assertIn("keep_for", str(caught.exception))
        self.assertEqual(self.client._jobs, {})

    def test_a_collected_job_says_keep_for(self):
        from zenthros import errors

        call = remote.run(sized, params={"rows": 1})
        self.store.put(f"{call.prefix}/result.pkl", b"x")
        self.assertIn("keep_for=", str(call._collected(errors.JobNotFound("gone"))))


if __name__ == "__main__":
    unittest.main()
