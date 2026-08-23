"""Checks over the SQL merge, the reduction in front of it, and the soft delete.

These run a real duckdb, in process, with no server and no file on disk. Nothing here
needs the Postgres bring-up, so this stays in the suite that runs on a clean machine.

Two fixture rules the rest of this repo learned the hard way and this file inherits. A
fixture where one record key covers one row cannot test any rule about choosing between
rows, so the shared fixture has a key covering two. And a fixture with one clean row cannot
tell a rule that walks forward from one that walks backward, so where direction matters
there are three.
"""

import duckdb

from cdc import decode
from cdc import events
from warehouse import merge as wh
from warehouse import sql as W

TOPIC = "shopcdc.shop.order_item"
COLS = ("order_id", "line_no", "qty")
PK = {"shop.order_item": ("order_id", "line_no")}


def _row(order_id, line_no, action="INSERT", qty="1"):
    image = {"order_id": str(order_id), "line_no": str(line_no), "qty": qty}
    if action == "DELETE":
        return decode.Row("shop.order_item", "DELETE", image, None)
    return decode.Row("shop.order_item", action, None, image)


def _stream(pairs, key_columns=("order_id", "line_no")):
    return events.stream(pairs, {"shop.order_item": key_columns}, "shopcdc")


def _target():
    return wh.Target("wh_order_item", "shop.order_item", TOPIC, COLS,
                     ("order_id", "line_no"))


def _warehouse(versioned=False):
    house = wh.Warehouse(duckdb.connect(), [_target()], versioned=versioned)
    house.create()
    return house


def _live(house):
    return house.live_rows("wh_order_item", COLS)


def _all_rows(house):
    return house.con.execute(
        "select order_id, line_no, qty, {}, {} from wh_order_item order by 1, 2".format(
            W.quote(W.LSN), W.quote(W.DELETED))).fetchall()


def check_an_insert_lands_and_carries_its_position():
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1))]), 1, merge_keys=PK)
    assert _live(house) == {("1", "1", "1")}
    assert _all_rows(house) == [('1', '1', '1', 10, False)], _all_rows(house)


def check_an_update_replaces_the_row_and_advances_the_position():
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1)), (20, _row(1, 1, "UPDATE", "9"))]),
                      1, merge_keys=PK)
    assert _all_rows(house) == [('1', '1', '9', 20, False)], _all_rows(house)


def check_a_delete_leaves_the_row_behind_holding_its_position():
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1)), (20, _row(1, 1, "DELETE"))]),
                      1, merge_keys=PK)
    assert _live(house) == set()
    assert _all_rows(house) == [('1', '1', '1', 20, True)], _all_rows(house)


def check_the_held_position_refuses_a_late_insert_after_a_delete():
    """The whole argument for the soft delete, as one case."""
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1)), (20, _row(1, 1, "DELETE"))]),
                      1, merge_keys=PK)
    house.apply_batch(_stream([(15, _row(1, 1, "INSERT", "7"))]), 2, merge_keys=PK)
    assert _live(house) == set(), _all_rows(house)


def check_a_hard_delete_lets_the_same_late_insert_resurrect_the_row():
    """The mirror. Without it the check above passes whether or not the position is read.

    The row is removed rather than flagged, which is the only difference, and the late
    insert then finds nothing to compare against.
    """
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1)), (20, _row(1, 1, "DELETE"))]),
                      1, merge_keys=PK)
    house.con.execute("delete from wh_order_item where {} = true".format(W.quote(W.DELETED)))
    house.apply_batch(_stream([(15, _row(1, 1, "INSERT", "7"))]), 2, merge_keys=PK)
    assert _live(house) == {("1", "1", "7")}, _all_rows(house)


def check_the_guard_off_lets_a_stale_change_overwrite_a_newer_one():
    house = _warehouse()
    house.apply_batch(_stream([(20, _row(1, 1, "INSERT", "9"))]), 1, merge_keys=PK)
    house.apply_batch(_stream([(10, _row(1, 1, "UPDATE", "1"))]), 2, merge_keys=PK,
                      guard=False)
    assert _live(house) == {("1", "1", "1")}, _all_rows(house)


def check_the_guard_on_refuses_that_same_change():
    house = _warehouse()
    house.apply_batch(_stream([(20, _row(1, 1, "INSERT", "9"))]), 1, merge_keys=PK)
    house.apply_batch(_stream([(10, _row(1, 1, "UPDATE", "1"))]), 2, merge_keys=PK)
    assert _live(house) == {("1", "1", "9")}, _all_rows(house)


def check_the_reduction_keeps_the_highest_position_in_a_batch():
    house = _warehouse()
    batch = _stream([(10, _row(1, 1, "INSERT", "1")),
                     (30, _row(1, 1, "UPDATE", "3")),
                     (20, _row(1, 1, "UPDATE", "2"))])
    counters = house.apply_batch(batch, 1, merge_keys=PK)
    assert _all_rows(house) == [('1', '1', '3', 30, False)], _all_rows(house)
    assert counters["superseded"] == 2, counters
    assert counters["staged_rows"] == 1, counters


def check_a_tie_at_one_position_is_broken_by_delivery_order():
    """Two changes at the same log position, which the stream really does produce.

    Highest wins is not a rule here, because neither is higher. What is left is the order
    they arrived in, and the count is reported rather than assumed to be zero.
    """
    house = _warehouse()
    batch = _stream([(10, _row(1, 1, "INSERT", "1")), (10, _row(1, 1, "UPDATE", "2"))])
    counters = house.apply_batch(batch, 1, merge_keys=PK)
    assert _all_rows(house) == [('1', '1', '2', 10, False)], _all_rows(house)
    assert counters["same_position"] == 1, counters


def check_the_batch_boundary_does_not_change_the_answer():
    """The same three changes in one batch and in three. Same table, both ways.

    In one batch they meet in the reduction. Split up they meet in the merge's guard. Two
    code paths, one required answer.
    """
    together = _warehouse()
    changes = [(10, _row(1, 1, "INSERT", "1")),
               (30, _row(1, 1, "UPDATE", "3")),
               (20, _row(1, 1, "UPDATE", "2"))]
    together.apply_batch(_stream(changes), 1, merge_keys=PK)

    apart = _warehouse()
    for i, one in enumerate(changes, start=1):
        apart.apply_batch(_stream([one]), i, merge_keys=PK)

    assert _all_rows(together) == _all_rows(apart), (_all_rows(together), _all_rows(apart))
    assert together.fingerprint(include_batch=False) == apart.fingerprint(include_batch=False)


def check_replaying_a_batch_changes_nothing():
    house = _warehouse()
    batch = _stream([(10, _row(1, 1)), (20, _row(1, 2)), (30, _row(1, 1, "UPDATE", "5"))])
    house.apply_batch(batch, 1, merge_keys=PK)
    once = house.fingerprint()
    house.apply_batch(batch, 1, merge_keys=PK)
    assert house.fingerprint() == once, _all_rows(house)


def check_a_tombstone_soft_deletes_every_row_under_the_record_key():
    """A record key covering two rows, which is the only fixture this rule can be seen in.

    Keyed on order_id alone, so lines 1 and 2 of order 1 share a record key. Deleting line
    1 puts out a tombstone that names the key and cannot name the line.
    """
    house = _warehouse()
    batch = _stream([(10, _row(1, 1)), (20, _row(1, 2)), (30, _row(1, 1, "DELETE"))],
                    key_columns=("order_id",))
    counters = house.apply_batch(batch, 1, merge_keys=PK)
    assert _live(house) == set(), _all_rows(house)
    assert counters["rows_removed_by_tombstones"] == 1, counters


def check_a_tombstone_arriving_behind_a_newer_change_leaves_the_row_alone():
    """The gate on the tombstone, and the divergence that found it.

    The in-memory consumer used to remove every row under the key with no position
    comparison at all. On a shuffled delivery order that is 37 rows on the real stream.
    """
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1))], key_columns=("order_id",)),
                      1, merge_keys=PK)
    house.apply_batch(_stream([(40, _row(1, 1, "UPDATE", "9"))], key_columns=("order_id",)),
                      2, merge_keys=PK)
    stale = [e for e in _stream([(20, _row(1, 1, "DELETE"))], key_columns=("order_id",))
             if e.tombstone]
    counters = house.apply_batch(stale, 3, merge_keys=PK)
    assert _live(house) == {("1", "1", "9")}, _all_rows(house)
    assert counters["rows_removed_by_tombstones"] == 0, counters


def check_ignoring_the_tombstone_is_a_different_answer():
    house = _warehouse()
    batch = _stream([(10, _row(1, 1)), (20, _row(1, 2)), (30, _row(1, 1, "DELETE"))],
                    key_columns=("order_id",))
    house.apply_batch(batch, 1, merge_keys=PK, on_tombstone="ignore")
    assert _live(house) == {("1", "2", "1")}, _all_rows(house)


def check_an_unknown_tombstone_setting_is_refused():
    house = _warehouse()
    try:
        house.apply_batch([], 1, merge_keys=PK, on_tombstone="soft")
    except ValueError as exc:
        assert "on_tombstone" in str(exc), str(exc)
    else:
        raise AssertionError("a bad on_tombstone was accepted")


def check_keying_the_target_by_the_record_key_collapses_rows():
    """merge_keys=None, which is the shortcut. Two source rows, one target row."""
    house = _warehouse()
    batch = _stream([(10, _row(1, 1)), (20, _row(1, 2))], key_columns=("order_id",))
    house.apply_batch(batch, 1, merge_keys=None)
    assert len(_live(house)) == 1, _all_rows(house)


def check_the_ledger_records_the_batch_inside_the_same_transaction():
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1)), (20, _row(1, 2))]), 7, merge_keys=PK)
    assert house.last_batch() == {"batch": 7, "records_in_batch": 2}, house.last_batch()


def check_a_failed_batch_leaves_neither_the_rows_nor_the_ledger_behind():
    """The rollback, shown with a failure the engine really produces.

    An unreduced batch carrying two changes to one key hits the not-matched branch with a
    duplicate and the primary key refuses it. Both halves of the transaction have to go.
    """
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(2, 1))]), 1, merge_keys=PK)
    before = _all_rows(house)
    batch = _stream([(20, _row(1, 1)), (30, _row(1, 1, "UPDATE", "2"))])
    try:
        house.apply_batch(batch, 2, merge_keys=PK, reduce=False)
    except duckdb.Error:
        pass
    else:
        raise AssertionError("an unreduced duplicate batch was accepted")
    assert _all_rows(house) == before, _all_rows(house)
    assert house.last_batch()["batch"] == 1, house.last_batch()


def check_the_versioned_target_keeps_a_row_per_position():
    """The literal reading of "merge on the primary key plus the position".

    Every change is a new key, so nothing ever matches and the table is a history rather
    than a copy of the source.

    Three batches and not one. Inside a single batch the target starts empty, so every
    staged row goes down the not-matched branch and the join is never read. A mutant
    dropping the position from the join survived the one batch version of this check and
    collapsed three rows into one the moment a second batch arrived.
    """
    house = _warehouse(versioned=True)
    for i, change in enumerate([(10, _row(1, 1, "INSERT", "1")),
                                (20, _row(1, 1, "UPDATE", "2")),
                                (30, _row(1, 1, "UPDATE", "3"))], start=1):
        house.apply_batch(_stream([change]), i, merge_keys=PK)
    assert len(_live(house)) == 3, _all_rows(house)


def check_the_versioned_reduction_does_not_collapse_inside_one_batch():
    """The other half, and it needs one batch rather than three.

    Split across batches the reduction only ever sees one record, so collapsing on the
    wrong key costs nothing and a mutant doing it survives. In one batch the three
    changes meet the reduction and it has to keep all three.
    """
    house = _warehouse(versioned=True)
    house.apply_batch(_stream([(10, _row(1, 1, "INSERT", "1")),
                               (20, _row(1, 1, "UPDATE", "2")),
                               (30, _row(1, 1, "UPDATE", "3"))]), 1, merge_keys=PK)
    assert len(_live(house)) == 3, _all_rows(house)


def check_the_unversioned_target_keeps_one_row_across_the_same_batches():
    """The mirror. Without it the check above passes on a target that never merges."""
    house = _warehouse()
    for i, change in enumerate([(10, _row(1, 1, "INSERT", "1")),
                                (20, _row(1, 1, "UPDATE", "2")),
                                (30, _row(1, 1, "UPDATE", "3"))], start=1):
        house.apply_batch(_stream([change]), i, merge_keys=PK)
    assert _all_rows(house) == [('1', '1', '3', 30, False)], _all_rows(house)


def check_the_highest_of_two_tombstones_in_one_batch_is_the_one_kept():
    """Two deletes under one record key in one batch, delivered newest first.

    The reduction has to carry the higher position forward. Keeping the first one seen
    leaves a gate that a row written between the two positions slips through.
    """
    house = _warehouse()
    house.apply_batch(_stream([(10, _row(1, 1)), (40, _row(1, 2))],
                              key_columns=("order_id",)), 1, merge_keys=PK)
    # Oldest first, which is the order that separates the two rules. Newest first and
    # "keep the first one seen" is the same answer as "keep the highest", so the fixture
    # says nothing. That is how the first version of this check let the mutant through.
    tombs = ([e for e in _stream([(20, _row(1, 1, "DELETE"))], key_columns=("order_id",))
              if e.tombstone]
             + [e for e in _stream([(50, _row(1, 2, "DELETE"))], key_columns=("order_id",))
                if e.tombstone])
    counters = house.apply_batch(tombs, 2, merge_keys=PK)
    assert _live(house) == set(), _all_rows(house)
    assert counters["rows_removed_by_tombstones"] == 2, counters


def check_the_fingerprint_reads_the_batch_column_only_when_asked():
    one = _warehouse()
    two = _warehouse()
    one.apply_batch(_stream([(10, _row(1, 1))]), 1, merge_keys=PK)
    two.apply_batch(_stream([(10, _row(1, 1))]), 99, merge_keys=PK)
    assert one.fingerprint() != two.fingerprint()
    assert one.fingerprint(include_batch=False) == two.fingerprint(include_batch=False)


def check_the_fingerprint_notices_a_swapped_column():
    """A hash that reads a row as a set of values would call these two warehouses equal.

    The rows are written straight into the table rather than pushed through a batch, so
    the record key column is identical on both sides and the only difference is which
    column holds which value. Going through the pipeline instead makes the record key
    differ too, and then the check passes whether or not the hash respects column order.
    That is what let a mutant sorting each row survive.
    """
    one = _warehouse()
    two = _warehouse()
    row = "insert into wh_order_item values (?, ?, ?, 'k', 10, false, 1)"
    one.con.execute(row, ["1", "2", "3"])
    two.con.execute(row, ["1", "3", "2"])
    assert one.fingerprint() != two.fingerprint()


def check_duckdb_takes_the_first_matching_staged_row_and_the_order_decides_it():
    """The measurement the reduction exists because of, on this engine, today."""
    got = wh.engine_duplicate_source_behaviour(duckdb.connect())
    assert got["stage_ascending"]["at_position"] == 20, got
    assert got["stage_descending"]["at_position"] == 30, got
    assert got["stage_ascending"]["highest_offered"] == 30, got
    assert got["order_decides_the_answer"] is True, got
    assert got["empty_target"]["raised"] == "ConstraintException", got


def check_a_batch_size_below_one_is_refused():
    try:
        wh.batches([1, 2, 3], 0)
    except ValueError as exc:
        assert "at least 1" in str(exc), str(exc)
    else:
        raise AssertionError("a batch size of zero was accepted")


def check_batches_covers_every_record_once():
    order = list(range(17))
    for size in (1, 5, 17, 40):
        chunks = wh.batches(order, size)
        assert [x for c in chunks for x in c] == order, (size, chunks)
        assert all(len(c) <= size for c in chunks), (size, chunks)


def check_every_dialect_note_says_whether_it_was_measured():
    """A claim about an engine nothing here has run is worth having and it is not worth
    confusing with one that was measured."""
    for dialect in W.DIALECTS:
        notes = W.DIALECT_NOTES[dialect]
        assert notes, dialect
        for key, text in notes.items():
            assert text.startswith(("measured:", "unverified:")), (dialect, key, text)


def check_every_dialect_carries_the_same_notes():
    keys = [set(W.DIALECT_NOTES[d]) for d in W.DIALECTS]
    assert all(k == keys[0] for k in keys), [sorted(k) for k in keys]


def check_snowflake_qualifies_the_target_column_and_duckdb_does_not():
    """Not cosmetic. duckdb raises a parser error on the qualified form."""
    duck = W.merge_sql("duckdb", "t1", "s1", COLS, ("order_id",), 1)
    snow = W.merge_sql("snowflake", "t1", "s1", COLS, ("order_id",), 1)
    assert 't."qty" = s."qty"' in snow, snow
    assert 't."qty" = s."qty"' not in duck, duck
    assert '"qty" = s."qty"' in duck, duck


def check_the_guard_is_the_only_difference_the_guard_flag_makes():
    on = W.merge_sql("duckdb", "t1", "s1", COLS, ("order_id",), 1, guard=True)
    off = W.merge_sql("duckdb", "t1", "s1", COLS, ("order_id",), 1, guard=False)
    assert 's."cdc_lsn" > t."cdc_lsn"' in on
    assert 's."cdc_lsn" > t."cdc_lsn"' not in off
    assert on.replace(' and s."cdc_lsn" > t."cdc_lsn"', "") == off


def check_an_unknown_dialect_is_refused():
    try:
        W.merge_sql("bigquery", "t1", "s1", COLS, ("order_id",), 1)
    except ValueError as exc:
        assert "bigquery" in str(exc), str(exc)
    else:
        raise AssertionError("an unknown dialect was accepted")


def check_an_identifier_carrying_a_quote_is_refused():
    try:
        W.quote('order";drop')
    except ValueError as exc:
        assert "double quote" in str(exc), str(exc)
    else:
        raise AssertionError("a quoted identifier was accepted")


def check_a_key_column_outside_the_column_list_is_refused():
    for build in (lambda: W.target_ddl("duckdb", "t1", COLS, ("missing_id",)),
                  lambda: wh.Target("t1", "shop.x", TOPIC, COLS, ("missing_id",))):
        try:
            build()
        except ValueError as exc:
            assert "missing_id" in str(exc), str(exc)
        else:
            raise AssertionError("a key column outside the columns was accepted")


def check_the_versioned_ddl_puts_the_position_in_the_primary_key():
    plain = W.target_ddl("duckdb", "t1", COLS, ("order_id", "line_no"))
    versioned = W.target_ddl("duckdb", "t1", COLS, ("order_id", "line_no"), versioned=True)
    assert 'primary key ("order_id", "line_no")' in plain, plain
    assert 'primary key ("order_id", "line_no", "cdc_lsn")' in versioned, versioned


def check_a_record_off_an_unknown_topic_is_counted_rather_than_dropped_quietly():
    target = _target()
    stray = _stream([(10, _row(1, 1))])
    rows, tombs, counters = wh.reduce_batch(stray, {"some.other.topic": target})
    assert counters["records_off_a_known_topic"] == 1, counters
    assert rows == {} and tombs == {}
