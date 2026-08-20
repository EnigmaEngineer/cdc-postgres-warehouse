"""Checks over the arithmetic that turns two measured arms into a claim."""

from cdc import probe


def summary(present, absent, before=0.0, after=0.0, changes=None):
    return {
        "changes": changes if changes is not None else present + absent,
        "before_present": present,
        "before_absent": absent,
        "mean_before_fields": before,
        "mean_after_fields": after,
        "by_action": {},
        "by_table": {},
    }


def check_ratio_against_zero_is_missing_not_infinite():
    assert probe.ratio(0, 100) is None
    assert probe.ratio(100, 0) == 0.0


def check_ratio_is_the_right_way_round():
    assert probe.ratio(100, 250) == 2.5


def check_coverage_over_zero_rows_is_none():
    assert probe.before_coverage(summary(0, 0)) is None


def check_coverage_is_a_fraction():
    assert probe.before_coverage(summary(3, 1)) == 0.75
    assert probe.before_coverage(summary(4, 0)) == 1.0
    assert probe.before_coverage(summary(0, 4)) == 0.0


def check_compare_refuses_without_a_baseline():
    try:
        probe.compare({"full": {"wal_bytes": 10, "summary": summary(1, 0)}})
    except KeyError:
        return
    raise AssertionError("compare must refuse when there is no default arm to measure against")


def check_compare_measures_every_arm_against_default():
    arms = {
        "default": {"wal_bytes": 1000, "summary": summary(0, 10, 0.0, 6.0)},
        "full": {"wal_bytes": 1600, "summary": summary(10, 0, 6.0, 6.0)},
    }
    out = probe.compare(arms)
    assert out["default"]["wal_ratio_vs_default"] == 1.0
    assert out["full"]["wal_ratio_vs_default"] == 1.6
    assert out["default"]["before_coverage"] == 0.0
    assert out["full"]["before_coverage"] == 1.0


def check_an_arm_with_no_updates_reports_none_rather_than_a_perfect_score():
    # The failure this guards is a third arm that happens to contain only inserts
    # scoring 1.0 on before coverage and looking like the best setting available.
    arms = {
        "default": {"wal_bytes": 100, "summary": summary(1, 1)},
        "inserts_only": {"wal_bytes": 90, "summary": summary(0, 0, changes=40)},
    }
    out = probe.compare(arms)
    assert out["inserts_only"]["before_coverage"] is None
    assert out["inserts_only"]["changes"] == 40
