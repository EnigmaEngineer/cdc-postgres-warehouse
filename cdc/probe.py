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


def spread(values):
    """Range across repeats of the same measurement.

    A difference smaller than this is not a result. Returns None for relative spread when
    the smallest value is zero, for the same reason ratio does.
    """
    if not values:
        raise ValueError("spread needs at least one value")
    lo, hi = min(values), max(values)
    return {
        "n": len(values),
        "min": lo,
        "max": hi,
        "range": hi - lo,
        "relative": None if lo == 0 else round((hi - lo) / lo, 6),
    }


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
        s = arm["summary"]
        out[name] = {
            "wal_bytes": arm["wal_bytes"],
            "wal_ratio_vs_default": ratio(base["wal_bytes"], arm["wal_bytes"]),
            "changes": s["changes"],
            "before_coverage": before_coverage(s),
            # The raw counts go out beside the fraction on purpose. A coverage of 0.1599
            # is not readable until you can see it is 188 out of 1176, and writing that
            # division into a sentence by hand is how a corrected numerator leaves a
            # stale total behind it.
            "before_present": s["before_present"],
            "before_absent": s["before_absent"],
            "updates_with_before_image": s["updates_with_before_image"],
            "updates": s["by_action"].get("UPDATE", 0),
            "deletes": s["by_action"].get("DELETE", 0),
            "mean_before_fields": s["mean_before_fields"],
            "mean_after_fields": s["mean_after_fields"],
            "pgoutput_messages": arm.get("pgoutput_messages"),
            "pgoutput_bytes": arm.get("pgoutput_bytes"),
            # The write ahead log ratio is what a DBA looks at. This is what the consumer
            # actually receives, and the two are not the same number.
            "pgoutput_ratio_vs_default": ratio(base.get("pgoutput_bytes", 0),
                                               arm.get("pgoutput_bytes", 0)),
        }
    return out
