#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

"""ws.secrets: a token (Hugging Face, W&B) set once in the Workspace, for
machines that do not exist yet. The hub keeps it in Kubernetes; the
controller seals it to the machine each job goes to (a template's, one that
wakes later). A value goes in and never comes back: not in a listing, not
in a repr, not in the job's spec.
"""
import json
import os
import unittest
from unittest import mock

import zenthros
from zenthros import errors, view
from zenthros.jobs.http import HTTPJobClient
from zenthros.remote.options import secret_sources

from support import FakeAPI, status


class WorkspaceSecretsTest(unittest.TestCase):
    def setUp(self):
        self.api = FakeAPI()
        self.addCleanup(self.api.close)
        self.kept = {}

        def put(name):
            def answer(body):
                value = json.loads(body)["value"]
                code = 200 if name in self.kept else 201
                self.kept[name] = value
                return code, {"name": name, "updatedAt": "2026-10-15T10:00:00Z"}
            return answer

        def delete(name):
            def answer(_):
                if self.kept.pop(name, None) is None:
                    return status(404, f"Workspace risk has no secret {name}", "NotFound")
                return 200, {"name": name}
            return answer

        for name in ("hf-token", "wandb", "gone"):
            self.api.routes[("PUT", f"/v1/workspaces/risk/secrets/{name}")] = put(name)
            self.api.routes[("DELETE", f"/v1/workspaces/risk/secrets/{name}")] = delete(name)
        self.api.routes[("GET", "/v1/workspaces/risk/secrets")] = lambda _: (200, {"secrets": [
            {"name": name, "updatedAt": "2026-10-15T10:00:00Z"} for name in sorted(self.kept, reverse=True)]})
        self.ws = view.Workspace(HTTPJobClient(self.api.url, "risk", "bob-token"))

    def test_set_once_listed_by_name_and_deleted(self):
        self.ws.secrets["hf-token"] = "hf_s3cr3t"
        self.ws.secrets["wandb"] = b"wb_value"
        self.assertEqual(self.kept, {"hf-token": "hf_s3cr3t", "wandb": "wb_value"})
        puts = [r for r in self.api.requests if r["method"] == "PUT"]
        self.assertEqual([r["path"] for r in puts], ["/v1/workspaces/risk/secrets/hf-token",
                                                     "/v1/workspaces/risk/secrets/wandb"])
        self.assertEqual(puts[0]["auth"], "Bearer bob-token")
        self.assertEqual(list(self.ws.secrets), ["hf-token", "wandb"])
        self.assertEqual(len(self.ws.secrets), 2)
        self.assertIn("wandb", self.ws.secrets)
        self.assertNotIn("nope", self.ws.secrets)
        shown = repr(self.ws.secrets)
        self.assertIn("hf-token", shown)
        self.assertIn("2026-10-15T10:00:00Z", shown)
        self.assertNotIn("hf_s3cr3t", shown)
        del self.ws.secrets["hf-token"]
        self.assertEqual(list(self.ws.secrets), ["wandb"])
        with self.assertRaises(KeyError):
            del self.ws.secrets["gone"]

    def test_a_job_names_it_and_never_carries_its_value(self):
        self.ws.secrets["hf-token"] = "hf_s3cr3t"
        secret = self.ws.secrets["hf-token"]
        self.assertEqual((secret.type, secret.name), ("workspace-secret", "hf-token"))
        self.assertEqual(secret, zenthros.Secret.workspace("hf-token"))
        env, file = secret_sources({"HF_TOKEN": secret, "/run/secrets/hf": secret})[::-1]
        self.assertEqual((env.type, env.name, env.materialization.mode, env.materialization.env_name),
                         ("workspace-secret", "hf-token", "env", "HF_TOKEN"))
        self.assertEqual((file.materialization.mode, file.materialization.path), ("file", "/run/secrets/hf"))
        self.assertNotIn("hf_s3cr3t", repr(env) + repr(file))
        with self.assertRaises(KeyError) as caught:
            self.ws.secrets["wandb"]
        self.assertIn("ws.secrets['wandb'] = ", str(caught.exception))

    def test_a_name_the_hub_cannot_keep_is_refused_here(self):
        for name in ("HF_TOKEN", "hf token", "-x", "x-", "", "a" * 48, 7):
            with self.assertRaises(errors.ValidationError, msg=name):
                zenthros.Secret.workspace(name)
            with self.assertRaises(errors.ValidationError, msg=name):
                self.ws.secrets[name] = "v"
        zenthros.Secret.workspace("a" * 47)
        for value in ("", b"", None, 3, b"\xff\xfe"):
            with self.assertRaises(errors.ValidationError, msg=repr(value)):
                self.ws.secrets["hf-token"] = value
        self.assertEqual([r for r in self.api.requests if r["method"] == "PUT"], [])

    def test_a_refusal_is_the_apis_words(self):
        self.api.routes[("PUT", "/v1/workspaces/risk/secrets/hf-token")] = status(403, "bob cannot create zenthrossecrets",
                                                                                  "Forbidden")
        with self.assertRaises(errors.WorkspaceForbidden) as caught:
            self.ws.secrets["hf-token"] = "v"
        self.assertIn("zenthrossecrets", str(caught.exception))


class WithoutAnAPITest(unittest.TestCase):
    def test_the_workspace_of_the_connection_in_effect(self):
        api = FakeAPI()
        self.addCleanup(api.close)
        api.routes[("GET", "/v1/workspaces/risk/secrets")] = (200, {"secrets": [{"name": "db"}]})
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ZENTHROS_")}
        with mock.patch.dict(os.environ, {**clean, "ZENTHROS_CONFIG": os.devnull + ".none", "ZENTHROS_SERVER": api.url,
                                          "ZENTHROS_TOKEN": "t", "ZENTHROS_WORKSPACE": "risk"}, clear=True):
            self.assertEqual(list(zenthros.workspace().secrets), ["db"])


if __name__ == "__main__":
    unittest.main()
