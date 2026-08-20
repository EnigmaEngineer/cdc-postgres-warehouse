"""Run every check in this directory.

    python tests/run_all.py

Plain functions named check_* rather than a framework. Nothing here needs a database, so
this runs on a clean machine with only the requirements installed. The parts that do need
a running server are exercised by scripts/replica_identity_probe.py instead, and that gap
is named in the README.
"""

import importlib
import os
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)


def modules():
    for name in sorted(os.listdir(HERE)):
        if name.startswith("test_") and name.endswith(".py"):
            yield "tests." + name[:-3]


def main():
    passed = 0
    failed = []
    for modname in modules():
        mod = importlib.import_module(modname)
        for attr in sorted(dir(mod)):
            if not attr.startswith("check_"):
                continue
            fn = getattr(mod, attr)
            try:
                fn()
                passed += 1
            except Exception:
                failed.append((modname, attr, traceback.format_exc()))

    for modname, attr, tb in failed:
        print("FAIL {}.{}".format(modname, attr))
        print(tb)

    print("{} passed, {} failed".format(passed, len(failed)))
    if passed == 0:
        # A run that executed nothing is not a pass. This is the whole reason the count
        # gets printed rather than a bare word.
        print("no checks were collected, that is a failure")
        return 2
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
