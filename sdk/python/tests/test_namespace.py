#!/usr/bin/env python3

# SPDX-License-Identifier: Apache-2.0

import unittest


class NamespaceTest(unittest.TestCase):
    def test_namespaces_split(self):
        import zenthros.core
        import zenthros.jobs

        self.assertTrue(hasattr(zenthros, "report"))
        self.assertTrue(hasattr(zenthros.jobs, "JobHandle"))
        self.assertTrue(hasattr(zenthros.jobs, "FakeJobClient"))
        self.assertFalse(hasattr(zenthros, "JobHandle"))
        self.assertFalse(hasattr(zenthros.core, "JobHandle"))


if __name__ == "__main__":
    unittest.main()
