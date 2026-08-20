"""Checks over the arithmetic that turns two measured arms into a claim."""

from cdc import probe


def summary(present, absent, before=0.0, after=0.0, changes=None, updates_with_before=0):
    return {
        "changes": changes if changes is not None else present + absent,
        "before_present": present,
        "before_absent": absent,
        "updates_with_before_image": updates_with_before,
        "mean_before_fields": before,
        "mean_after_fields": after,
        "by_action": {},
        "by_table": {},
    }


def check_spread_of_one_value_is_zero():
    s = probe.spread([100])
    assert s["range"] == 0 and s["relative"] == 0.0 and s["n"] == 1


def check_spread_reports_the_range_not_the_mean():
    s = probe.spread([665672, 665488, 665672])
    assert s["min"] == 665488
    assert s["max"] == 665672
    assert s["range"] == 184
    assert s["relative"] == round(184 / 665488, 6)


def check_spread_relative_against_zero_is_none():
    assert probe.spread([0, 5])["relative"] is None


def check_spread_refuses_an_empty_list():
    try:
        probe.spread([])
    except ValueError:
        return
    raise AssertionError("spread over nothing must refuse rather than return a number")


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


def check_the_two_ratios_are_reported_separately():
    # The failure this guards is publishing one ratio and letting a reader assume it
    # covers both. The stream a consumer reads can grow at a different rate from the
    # write ahead log it was decoded from.
    arms = {
        "default": {"wal_bytes": 1000, "pgoutput_bytes": 500,
                    "summary": summary(1, 9, 2.0, 6.0)},
        "full": {"wal_bytes": 1080, "pgoutput_bytes": 650,
                 "summary": summary(10, 0, 6.0, 6.0, updates_with_before=9)},
    }
    out = probe.compare(arms)
    assert out["full"]["wal_ratio_vs_default"] == 1.08
    assert out["full"]["pgoutput_ratio_vs_default"] == 1.3
    assert out["full"]["wal_ratio_vs_default"] != out["full"]["pgoutput_ratio_vs_default"]


def check_a_missing_pgoutput_measurement_does_not_become_a_ratio():
    arms = {"default": {"wal_bytes": 10, "summary": summary(1, 0)}}
    out = probe.compare(arms)
    assert out["default"]["pgoutput_bytes"] is None
    assert out["default"]["pgoutput_ratio_vs_default"] is None


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
