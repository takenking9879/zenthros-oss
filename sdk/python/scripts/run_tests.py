# SPDX-License-Identifier: Apache-2.0

"""make test: the unit suite under DIR, then how many tests skipped for each reason.

A package missing from whichever interpreter ran the tests used to skip
about 30 of them without anyone seeing it; the reasons printed here make
each skip visible.
"""

import collections
import sys
import unittest


def main(start: str) -> int:
    suite = unittest.defaultTestLoader.discover(start, top_level_dir=start)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    reasons = collections.Counter(reason for _, reason in result.skipped)
    if reasons:
        print(f"\nskipped {len(result.skipped)}, by reason:")
        for reason, count in reasons.most_common():
            print(f"  {count:4d}  {reason}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
