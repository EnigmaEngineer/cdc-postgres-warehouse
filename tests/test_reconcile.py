"""Checks over the keyed reconciliation.

Nothing here needs a database. The reconciliation takes two row sets and a column list,
which is exactly what makes it testable without one.

Two fixture rules this repo learned the hard way and this file inherits. A fixture with one
row per key cannot test any rule about choosing between rows, so the shared fixture carries
a key covering two. And a fixture where every row agrees cannot tell a comparison that
looks at values from one that only looks at keys, so the shared fixture also carries a row
that differs on exactly one column.
"""

from cdc import decode
from cdc import events
from warehouse import reconcile as rec

COLS = ("order_id", "line_no", "qty", "status")
KEY = ("order_id", "line_no")


def _rows(*tuples):
    return {tuple(str(v) for v in t) for t in tuples}


def check_a_row_that_moved_one_column_is_one_finding_not_two():
    # The reason this module exists. Set subtraction sees the source tuple missing from
    # the target and the target tuple missing from the source, so it reports two.
    source = _rows((1, 1, 2, "shipped"))
    target = _rows((1, 1, 2, "paid"))
    got = rec.reconcile(source, target, COLS, KEY)
    assert got["differing"] == 1, got
    assert got["missing"] == 0 and got["extra"] == 0, got
    assert got["columns_differing"] == {"status": 1}, got


def check_missing_and_extra_stay_separate_from_differing():
    source = _rows((1, 1, 2, "paid"), (2, 1, 5, "paid"))
    target = _rows((1, 1, 9, "paid"), (3, 1, 1, "paid"))
    got = rec.reconcile(source, target, COLS, KEY)
    assert got["differing"] == 1, got
    assert got["missing"] == 1, got
    assert got["extra"] == 1, got
    assert got["matching"] == 0, got


def check_the_column_that_moved_is_named_and_the_ones_that_did_not_are_not():
    source = _rows((1, 1, 7, "shipped"))
    target = _rows((1, 1, 2, "paid"))
    got = rec.reconcile(source, target, COLS, KEY)
    assert got["columns_differing"] == {"qty": 1, "status": 1}, got


def check_two_live_rows_under_one_key_in_the_target_is_a_finding():
    # A warehouse holding two live rows for one primary key is broken whatever else the
    # comparison says, and a set difference against a source holding both would call it
    # a match.
    source = _rows((1, 1, 2, "paid"), (1, 1, 3, "paid"))
    target = _rows((1, 1, 2, "paid"), (1, 1, 3, "paid"))
    got = rec.reconcile(source, target, COLS, KEY)
    assert got["duplicate_keys_in_target"] == 1, got
    assert got["duplicate_keys_in_source"] == 1, got
    assert got["matching"] == 1, got
    assert got["verdict"] == "drift", got


def check_a_key_in_flight_is_left_out_of_the_verdict():
    source = _rows((1, 1, 2, "shipped"), (2, 1, 5, "paid"))
    target = _rows((1, 1, 2, "paid"), (2, 1, 5, "paid"))
    loud = rec.reconcile(source, target, COLS, KEY)
    assert loud["verdict"] == "drift" and loud["differing"] == 1, loud
    quiet = rec.reconcile(source, target, COLS, KEY, in_flight={("1", "1")})
    assert quiet["verdict"] == "clean_partial", quiet
    assert quiet["excluded_in_flight"] == 1, quiet
    assert quiet["checked"] == 1, quiet


def check_coverage_falls_as_the_exclusion_grows():
    source = _rows((1, 1, 2, "paid"), (2, 1, 5, "paid"), (3, 1, 5, "paid"),
                   (4, 1, 5, "paid"))
    got = rec.reconcile(source, source, COLS, KEY, in_flight={("1", "1"), ("2", "1")})
    assert got["coverage"] == 0.5, got
    assert got["checked"] == 2 and got["excluded_in_flight"] == 2, got


def check_excluding_everything_is_not_a_pass():
    # The whole point of a third verdict. Every key is in flight, so the comparison agrees
    # with the source about everything it looked at, and it looked at nothing.
    source = _rows((1, 1, 2, "shipped"))
    target = _rows((1, 1, 2, "paid"))
    got = rec.reconcile(source, target, COLS, KEY, in_flight={("1", "1")})
    assert got["checked"] == 0, got
    assert got["verdict"] == "no_coverage", got
    assert got["verdict"] != "clean"


def check_two_empty_sides_report_no_coverage_rather_than_clean():
    got = rec.reconcile(set(), set(), COLS, KEY)
    assert got["verdict"] == "no_coverage", got
    assert got["coverage"] is None, got


def check_an_exclusion_naming_a_key_nobody_holds_changes_nothing():
    source = _rows((1, 1, 2, "paid"))
    plain = rec.reconcile(source, source, COLS, KEY)
    odd = rec.reconcile(source, source, COLS, KEY, in_flight={("9", "9")})
    assert plain == odd, (plain, odd)


def check_a_clean_table_reports_clean_with_full_coverage():
    source = _rows((1, 1, 2, "paid"), (2, 1, 5, "paid"))
    got = rec.reconcile(source, source, COLS, KEY)
    assert got["verdict"] == "clean" and got["coverage"] == 1.0, got
    assert got["matching"] == 2, got


def check_index_by_key_keeps_both_rows_under_a_repeated_key():
    got = rec.index_by_key(_rows((1, 1, 2, "paid"), (1, 1, 3, "paid")), COLS, KEY)
    assert list(got) == [("1", "1")], got
    assert len(got[("1", "1")]) == 2, got


def check_the_rollup_takes_the_worst_verdict_and_not_the_common_one():
    tables = {"a": {"verdict": "clean"}, "b": {"verdict": "clean"},
              "c": {"verdict": "drift"}}
    assert rec.rollup(tables) == "drift"
    tables["c"]["verdict"] = "no_coverage"
    assert rec.rollup(tables) == "no_coverage"
    tables["c"]["verdict"] = "clean_partial"
    assert rec.rollup(tables) == "clean_partial"
    tables["c"]["verdict"] = "clean"
    assert rec.rollup(tables) == "clean"


def check_the_rollup_refuses_a_verdict_it_does_not_know():
    try:
        rec.rollup({"a": {"verdict": "probably_fine"}})
    except ValueError as exc:
        assert "probably_fine" in str(exc), str(exc)
    else:
        raise AssertionError("rollup accepted a verdict that is not in ORDER")


def check_no_tables_at_all_is_no_coverage():
    assert rec.rollup({}) == "no_coverage"


def _event(order_id, line_no, action="UPDATE"):
    image = {"order_id": str(order_id), "line_no": str(line_no), "qty": "1"}
    row = (decode.Row("shop.order_item", "DELETE", image, None) if action == "DELETE"
           else decode.Row("shop.order_item", action, None, image))
    return row


def _stream(rows, tombstones=True):
    keyed = {"shop.order_item": ("order_id", "line_no")}
    key_columns = {t: events.key_columns_for(t, keyed) for t in keyed}
    return events.stream([(i + 1, r) for i, r in enumerate(rows)], key_columns, "shopcdc",
                         tombstones_on_delete=tombstones)


def check_keys_in_flight_reads_the_after_image_of_a_change():
    evs = _stream([_event(1, 1), _event(2, 3)])
    by_table, counts = rec.keys_in_flight(evs, {"shop.order_item": ("order_id", "line_no")})
    assert by_table["shop.order_item"] == {("1", "1"), ("2", "3")}, by_table
    assert counts == {"tombstones": 0, "changes_with_no_key": 0}, counts


def check_keys_in_flight_reads_the_before_image_of_a_delete():
    # A delete carries no after image. Reading the wrong one leaves the key out of the
    # exclusion set, and a deleted row is exactly the case a reconciliation calls extra.
    evs = [e for e in _stream([_event(4, 2, "DELETE")], tombstones=False)]
    by_table, counts = rec.keys_in_flight(evs, {"shop.order_item": ("order_id", "line_no")})
    assert by_table["shop.order_item"] == {("4", "2")}, by_table
    assert counts["changes_with_no_key"] == 0, counts


def check_a_tombstone_names_no_key_and_is_counted_apart_from_a_defect():
    # These two counts have to stay apart. A mutant deleting the tombstone branch survived
    # the whole suite while they were one number, because identity() returns None on a
    # tombstone's absent image and the count landed in the same place either way. The
    # branch was doing nothing. Splitting the counter is what gave it something to do.
    evs = _stream([_event(4, 2, "DELETE")])
    assert any(e.tombstone for e in evs), "fixture produced no tombstone"
    by_table, counts = rec.keys_in_flight(evs, {"shop.order_item": ("order_id", "line_no")})
    assert counts["tombstones"] == 1, counts
    assert counts["changes_with_no_key"] == 0, counts
    # The delete envelope ahead of it still put the key in the set, which is the only
    # reason the gap costs nothing under the default key.
    assert by_table["shop.order_item"] == {("4", "2")}, by_table


def check_a_change_whose_image_lacks_the_merge_key_is_not_counted_as_a_tombstone():
    # The defect the split exists for, and it took two tries to write. The first fixture
    # built a change record with no key columns at all, and events.stream refuses that
    # before keys_in_flight ever sees it. What is reachable is the merge key naming a
    # column the record key does not, which is the whole point of message.key.columns.
    # The image here carries the record key and not the merge key.
    evs = _stream([_event(4, 2)])
    by_table, counts = rec.keys_in_flight(evs, {"shop.order_item": ("shipped_at",)})
    assert counts["changes_with_no_key"] == 1, counts
    assert counts["tombstones"] == 0, counts
    assert by_table == {}, by_table


def check_a_pass_over_part_of_the_table_is_not_the_same_word_as_a_pass():
    # The verdict that had to be added after the probe ran. Every key agrees, and three
    # quarters of them were never looked at, and the first version called that clean.
    source = _rows((1, 1, 2, "paid"), (2, 1, 5, "paid"), (3, 1, 5, "paid"),
                   (4, 1, 5, "paid"))
    full = rec.reconcile(source, source, COLS, KEY)
    part = rec.reconcile(source, source, COLS, KEY,
                         in_flight={("2", "1"), ("3", "1"), ("4", "1")})
    assert full["verdict"] == "clean", full
    assert part["verdict"] == "clean_partial", part
    assert part["coverage"] == 0.25, part


def check_drift_outranks_partial_coverage_on_the_same_report():
    # A key that disagreed and a key that was skipped in one table. The disagreement is
    # the answer. Reporting partial coverage here would bury a real failure under a
    # caveat about the ones that were not checked.
    source = _rows((1, 1, 2, "shipped"), (2, 1, 5, "paid"))
    target = _rows((1, 1, 2, "paid"), (2, 1, 9, "paid"))
    got = rec.reconcile(source, target, COLS, KEY, in_flight={("2", "1")})
    assert got["verdict"] == "drift", got
    assert got["coverage"] == 0.5, got
