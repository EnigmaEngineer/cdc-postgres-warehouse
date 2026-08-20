"""Compare two replica identity settings on the same workload.

The database plumbing lives in scripts/replica_identity_probe.py. What lives here is the
arithmetic that turns two arms into a claim, because a claim is the thing that ends up in
a README and it should be under test.
"""


def ratio(a, b):
    """b relative to a. Returns None rather than raising when a is zero.

    A ratio against nothing is not infinity, it is a missing measurement, and the two
    should not print the same way.
    """
    if a == 0:
        return None
    return round(b / a, 4)


def before_coverage(summary):
    """Fraction of updates and deletes that carry a before image.

    Returns None when there were no updates or deletes at all. A coverage of 1.0 measured
    over zero rows is the shape of every uninformative metric this repo is trying to avoid.
    """
    total = summary["before_present"] + summary["before_absent"]
    if total == 0:
        return None
    return round(summary["before_present"] / total, 4)


def compare(arms):
    """arms is a dict of name to {'wal_bytes': int, 'summary': dict}.

    The baseline is whichever arm is named 'default', because that is the setting a table
    has when nobody chose one, and the interesting question is what changing it costs.
    """
    if "default" not in arms:
        raise KeyError("compare needs an arm named 'default' to measure against")
    base = arms["default"]
    out = {}
    for name, arm in sorted(arms.items()):
        out[name] = {
            "wal_bytes": arm["wal_bytes"],
            "wal_ratio_vs_default": ratio(base["wal_bytes"], arm["wal_bytes"]),
            "changes": arm["summary"]["changes"],
            "before_coverage": before_coverage(arm["summary"]),
            "mean_before_fields": arm["summary"]["mean_before_fields"],
            "mean_after_fields": arm["summary"]["mean_after_fields"],
        }
    return out
