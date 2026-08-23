"""Every third party import in this repo has to be declared in requirements.txt.

This check exists because a repo that cannot be installed is worse than a repo with a
thin README, and because reading the import list by eye finds the packages you remember
and misses the ones you do not. It walks the source with ast rather than importing
anything, so it runs on a machine where none of the requirements are installed.
"""

import ast
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Distribution name on the left, imported module name on the right. They differ often
# enough that mapping them by stripping a suffix is guesswork.
DIST_TO_MODULE = {
    "psycopg2-binary": "psycopg2",
}

LOCAL_PACKAGES = {"cdc", "load", "tests", "scripts", "db", "warehouse"}


def python_files():
    out = []
    for base, dirs, names in os.walk(ROOT):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]
        for n in names:
            if n.endswith(".py"):
                out.append(os.path.join(base, n))
    return sorted(out)


def top_level_imports(path):
    with open(path) as fh:
        tree = ast.parse(fh.read(), filename=path)
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                found.add(a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                found.add(node.module.split(".")[0])
    return found


def declared():
    out = set()
    with open(os.path.join(ROOT, "requirements.txt")) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            dist = line.split("==")[0].split(">=")[0].split("<")[0].strip()
            out.add(DIST_TO_MODULE.get(dist, dist.replace("-", "_")))
    return out


def third_party(names):
    out = set()
    for n in names:
        if n in LOCAL_PACKAGES:
            continue
        if n in sys.stdlib_module_names:
            continue
        out.add(n)
    return out


def check_at_least_one_file_was_scanned():
    # A check that can pass on zero inputs will eventually be pointed at zero inputs.
    files = python_files()
    assert len(files) >= 8, "expected the repo's modules, found {}".format(len(files))


def check_every_third_party_import_is_declared():
    imported = set()
    for path in python_files():
        imported |= top_level_imports(path)
    missing = third_party(imported) - declared()
    assert not missing, "undeclared imports: {}".format(sorted(missing))


def check_nothing_is_declared_that_nobody_imports():
    imported = set()
    for path in python_files():
        imported |= top_level_imports(path)
    unused = declared() - third_party(imported)
    assert not unused, "declared but never imported: {}".format(sorted(unused))


def check_the_scan_would_notice_a_new_import():
    # The check above passes when the repo is correct, which is also what it would do if
    # top_level_imports returned nothing at all. This feeds it a file it must not miss.
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as fh:
        fh.write("import pandas\nfrom sqlalchemy import text\nimport os\n")
        path = fh.name
    try:
        found = third_party(top_level_imports(path))
        assert found == {"pandas", "sqlalchemy"}, found
    finally:
        os.unlink(path)
