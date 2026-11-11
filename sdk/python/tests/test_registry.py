# SPDX-License-Identifier: Apache-2.0

"""What a registry answers with no login, as a real one does it: a 401 that
names where to get an anonymous token, and the token that may or may not open it."""

import io
import json
import unittest
import urllib.error
from unittest import mock

from zenthros import registry


def _denied(url, challenge=""):
    return urllib.error.HTTPError(url, 401, "Unauthorized", {"WWW-Authenticate": challenge}, io.BytesIO())


class _Answer:
    def __init__(self, status=200, body=b"{}"):
        self.status, self.body = status, body

    def read(self, *_):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


CHALLENGE = 'Bearer realm="https://auth.example/token",service="r",scope="repository:me/x:pull"'


class AnyoneCanRead(unittest.TestCase):
    def serve(self, answer):
        self.asked = []

        def opened(request, timeout=None):
            url = request if isinstance(request, str) else request.full_url
            self.asked.append((url, None if isinstance(request, str) else request.get_header("Authorization")))
            return answer(url, None if isinstance(request, str) else request.get_header("Authorization"))

        patch = mock.patch("urllib.request.urlopen", opened)
        patch.start()
        self.addCleanup(patch.stop)

    def test_a_registry_that_answers_with_no_login(self):
        self.serve(lambda url, auth: _Answer())
        self.assertTrue(registry.anyone_can_read("ghcr.io/me/x@sha256:abc"))
        self.assertEqual(self.asked[0][0], "https://ghcr.io/v2/me/x/manifests/sha256:abc")

    def test_docker_hub_with_its_anonymous_token(self):
        def answer(url, auth):
            if url.startswith("https://auth.example/"):
                return _Answer(body=json.dumps({"token": "t"}).encode())
            if auth == "Bearer t":
                return _Answer()
            raise _denied(url, CHALLENGE)

        self.serve(answer)
        self.assertTrue(registry.anyone_can_read("python:3.12-slim"))
        self.assertEqual(self.asked[0][0], "https://registry-1.docker.io/v2/library/python/manifests/3.12-slim")
        self.assertIn("service=r", self.asked[1][0])
        self.assertIn("scope=repository", self.asked[1][0])

    def test_a_private_image_keeps_saying_no_with_the_anonymous_token(self):
        def answer(url, auth):
            if url.startswith("https://auth.example/"):
                return _Answer(body=b'{"token": "t"}')
            raise _denied(url, CHALLENGE)

        self.serve(answer)
        self.assertFalse(registry.anyone_can_read("me/private:1"))

    def test_a_registry_that_gives_no_anonymous_token(self):
        def answer(url, auth):
            raise _denied(url, CHALLENGE)

        self.serve(answer)
        self.assertFalse(registry.anyone_can_read("us-docker.pkg.dev/p/r/x:1"))

    def test_a_missing_image_is_not_readable(self):
        def answer(url, auth):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO())

        self.serve(answer)
        self.assertFalse(registry.anyone_can_read("ghcr.io/me/x:1"))

    def test_with_no_network_it_cannot_be_told(self):
        def answer(url, auth):
            raise urllib.error.URLError("no route")

        self.serve(answer)
        self.assertIsNone(registry.anyone_can_read("ghcr.io/me/x:1"))

    def test_a_registry_on_this_computer_is_not_for_anyone(self):
        self.serve(lambda url, auth: self.fail("asked"))
        self.assertFalse(registry.anyone_can_read("localhost:5111/x:1"))


if __name__ == "__main__":
    unittest.main()
