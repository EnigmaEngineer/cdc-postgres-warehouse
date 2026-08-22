"""Checks over the apply rules.

Two rows share a record key in the shared fixture and two do not in the default one. Every
rule about tombstones is a rule about what happens when a key covers more than one row, and
a fixture where it never does turns all of them green without running them.
"""

from cdc import consumer
from cdc import decode
from cdc import events

PK = {"shop.order_item": ("order_id", "line_no")}
TOPIC = "shopcdc.shop.order_item"
COLS = ("order_id", "line_no", "qty")


def _row(order_id, line_no, action="INSERT", qty="1"):
    image = {"order_id": str(order_id), "line_no": str(line_no), "qty": qty}
    if action == "DELETE":
        return decode.Row("shop.order_item", "DELETE", image, None)
    return decode.Row("shop.order_item", action, None, image)


def _stream(pairs, columns=("order_id", "line_no")):
    return events.stream(pairs, {"shop.order_item": columns}, "shopcdc")


def _applied(result):
    return consumer.as_row_set(result.rows, TOPIC, COLS)


def check_an_insert_lands():
    r = consumer.apply_events(_stream([(10, _row(1, 1))]), merge_keys=PK)
    assert _applied(r) == {("1", "1", "1")}
    assert r.counters["inserts"] == 1


def check_an_update_replaces_the_row():
    r = consumer.apply_events(
        _stream([(10, _row(1, 1)), (20, _row(1, 1, "UPDATE", "9"))]), merge_keys=PK)
    assert _applied(r) == {("1", "1", "9")}


def check_a_delete_removes_the_row():
    r = consumer.apply_events(
        _stream([(10, _row(1, 1)), (20, _row(1, 1, "DELETE"))]), merge_keys=PK)
    assert _applied(r) == set()
    assert r.counters["deletes"] == 1


def check_a_stale_change_is_dropped():
    late = list(_stream([(10, _row(1, 1)), (20, _row(1, 1, "UPDATE", "9"))]))
    r = consumer.apply_events(list(reversed(late)), merge_keys=PK)
    assert _applied(r) == {("1", "1", "9")}, _applied(r)
    assert r.counters["stale_dropped"] == 1, r.counters


def check_the_guard_can_be_taken_off_and_then_the_stale_change_wins():
    # The mirror of the check above. Without it the guard could be a no-op and the first
    # check would pass because the last write happened to be the newest one anyway.
    late = list(_stream([(10, _row(1, 1)), (20, _row(1, 1, "UPDATE", "9"))]))
    r = consumer.apply_events(list(reversed(late)), guard=False, merge_keys=PK)
    assert _applied(r) == {("1", "1", "1")}, _applied(r)
    assert r.counters["stale_dropped"] == 0


def check_a_deleted_key_keeps_its_position_and_cannot_be_resurrected():
    # The rule that looks like an optimisation to remove. If the mark is dropped with the
    # row then a delayed insert older than the delete puts the row back.
    stream = list(_stream([(10, _row(1, 1)), (20, _row(1, 1, "DELETE"))]))
    delayed = stream[0]
    r = consumer.apply_events(stream[1:] + [delayed] , merge_keys=PK)
    assert _applied(r) == set(), _applied(r)
    assert r.counters["stale_dropped"] == 1, r.counters


def check_two_records_at_one_position_are_both_applied():
    # Dropping these as stale would lose real changes. They are counted so that a stream
    # full of them is visible rather than silently thinned.
    r = consumer.apply_events(
        _stream([(10, _row(1, 1)), (10, _row(1, 1, "UPDATE", "9"))]), merge_keys=PK)
    assert _applied(r) == {("1", "1", "9")}
    assert r.counters["same_position"] == 1, r.counters


def check_an_update_for_a_row_never_seen_is_applied_and_counted():
    r = consumer.apply_events(_stream([(20, _row(1, 1, "UPDATE", "9"))]), merge_keys=PK)
    assert _applied(r) == {("1", "1", "9")}
    assert r.counters["updates_of_a_row_not_held"] == 1


def check_a_delete_for_a_row_never_seen_is_counted_and_not_fatal():
    r = consumer.apply_events(_stream([(20, _row(1, 1, "DELETE"))]), merge_keys=PK)
    assert _applied(r) == set()
    assert r.counters["deletes_of_a_row_not_held"] == 1


def check_a_tombstone_under_the_primary_key_removes_exactly_one_row():
    stream = _stream([(10, _row(1, 1)), (20, _row(1, 2)), (30, _row(1, 1, "DELETE"))])
    r = consumer.apply_events(stream, merge_keys=PK)
    assert _applied(r) == {("1", "2", "1")}, _applied(r)
    assert r.counters["tombstones"] == 1


def check_a_tombstone_under_a_shared_key_takes_the_other_row_with_it():
    # This is the finding. Two lines of one order share a record key, so the tombstone for
    # the deleted line names both and the surviving line goes too.
    stream = _stream([(10, _row(1, 1)), (20, _row(1, 2)), (30, _row(1, 1, "DELETE"))],
                     columns=("order_id",))
    r = consumer.apply_events(stream, merge_keys=PK)
    assert _applied(r) == set(), _applied(r)
    assert r.counters["rows_removed_by_tombstones"] == 1, r.counters


def check_ignoring_the_tombstone_saves_the_surviving_row():
    stream = _stream([(10, _row(1, 1)), (20, _row(1, 2)), (30, _row(1, 1, "DELETE"))],
                     columns=("order_id",))
    r = consumer.apply_events(stream, merge_keys=PK, on_tombstone="ignore")
    assert _applied(r) == {("1", "2", "1")}, _applied(r)
    assert r.counters["rows_removed_by_tombstones"] == 0


def check_an_unknown_tombstone_policy_is_refused():
    try:
        consumer.apply_events([], on_tombstone="maybe")
    except ValueError as exc:
        assert "on_tombstone" in str(exc), exc
        return
    raise AssertionError("an unknown tombstone policy was accepted")


def check_merging_on_the_record_key_collapses_two_rows_into_one():
    # The shortcut. Both lines land in one slot and the second overwrites the first, so
    # the target is short a row that nothing ever deleted.
    stream = _stream([(10, _row(1, 1)), (20, _row(1, 2))], columns=("order_id",))
    r = consumer.apply_events(stream, merge_keys=None)
    assert len(_applied(r)) == 1, _applied(r)


def check_merging_on_the_primary_key_keeps_both():
    stream = _stream([(10, _row(1, 1)), (20, _row(1, 2))], columns=("order_id",))
    r = consumer.apply_events(stream, merge_keys=PK)
    assert _applied(r) == {("1", "1", "1"), ("1", "2", "1")}, _applied(r)


def check_a_change_missing_a_merge_column_is_counted_rather_than_guessed():
    narrow = decode.Row("shop.order_item", "DELETE", {"order_id": "1"}, None)
    stream = events.stream([(10, narrow)], {"shop.order_item": ("order_id",)}, "shopcdc")
    r = consumer.apply_events(stream, merge_keys=PK)
    assert r.counters["changes_with_no_merge_key"] == 1, r.counters


def check_a_row_short_a_column_shows_up_as_a_mismatch_rather_than_vanishing():
    # as_row_set could skip a row missing one of the compared columns, which would make a
    # consumer that lost a column look like a consumer with fewer rows.
    rows = {(TOPIC, ("1", "1")): {"order_id": "1", "line_no": "1"}}
    assert consumer.as_row_set(rows, TOPIC, COLS) == {("1", "1", None)}


def check_as_row_set_reads_one_topic_and_not_the_others():
    rows = {
        (TOPIC, ("1", "1")): {"order_id": "1", "line_no": "1", "qty": "1"},
        ("shopcdc.shop.customer", ("3",)): {"order_id": "9", "line_no": "9", "qty": "9"},
    }
    assert consumer.as_row_set(rows, TOPIC, COLS) == {("1", "1", "1")}


def check_compare_separates_missing_from_extra():
    # Two missing against one extra on purpose. One of each is symmetric, so swapping the
    # two subtractions passes it, and missing against extra is the whole difference
    # between a merge that loses rows and one that keeps dead ones.
    truth = {("1", "1", "1"), ("1", "2", "1"), ("1", "4", "1")}
    applied = {("1", "1", "1"), ("1", "3", "1")}
    out = consumer.compare(applied, truth)
    assert out["missing"] == 2, out
    assert out["extra"] == 1, out
    assert out["matching"] == 1
    assert out["exact"] is False


def check_compare_calls_an_identical_pair_exact():
    truth = {("1", "1", "1")}
    assert consumer.compare(set(truth), truth)["exact"] is True


def check_an_unknown_op_is_refused():
    bad = events.Event(TOPIC, "shop.order_item", {}, b"{}", "z", None, {}, 1, False)
    try:
        consumer.apply_events([bad])
    except ValueError as exc:
        assert "unknown op" in str(exc), exc
        return
    raise AssertionError("a record with an unknown op was applied")
