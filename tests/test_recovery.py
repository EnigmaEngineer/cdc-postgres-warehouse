"""Checks over what a consumer does after it dies.

These run a real duckdb in process, same as the merge checks, and the pure batch arithmetic
runs on nothing at all.

The fixture rule this file cares about most is the one about a test whose subject is
"nothing changes". A resume that loses no records passes whether or not the resume logic
ran, so every rule here that can be stated as a loss is tested on a fixture that really
loses something, and the clean case is the control beside it rather than the evidence.
"""

import duckdb

from cdc import decode
from cdc import events
from warehouse import merge as wh
from warehouse import recovery
from warehouse import reconcile as rec

TOPIC = "shopcdc.shop.order_item"
COLS = ("order_id", "line_no", "qty")
PK = {"shop.order_item": ("order_id", "line_no")}


def _row(order_id, line_no, action="INSERT", qty="1"):
    image = {"order_id": str(order_id), "line_no": str(line_no), "qty": qty}
    if action == "DELETE":
        return decode.Row("shop.order_item", "DELETE", image, None)
    return decode.Row("shop.order_item", action, None, image)


def _stream(pairs):
    return events.stream(pairs, {"shop.order_item": ("order_id", "line_no")}, "shopcdc")


def _warehouse():
    target = wh.Target("wh_order_item", "shop.order_item", TOPIC, COLS,
                       ("order_id", "line_no"))
    out = wh.Warehouse(duckdb.connect(), [target])
    out.create()
    return out


def _six_batches():
    """Six single record batches, each touching a different row.

    One row per batch so a crash in batch four is visible as a specific row's absence
    rather than as a count nobody can point at.
    """
    evs = _stream([(100 + i, _row(i, 1, qty=str(i))) for i in range(1, 7)])
    return [[e] for e in evs]


# The ledger, which nothing read until now


def check_a_fresh_warehouse_says_it_has_applied_nothing():
    warehouse = _warehouse()
    assert recovery.resume_point(warehouse) == 0, recovery.resume_point(warehouse)


def check_the_ledger_names_the_last_batch_and_not_the_count_of_them():
    warehouse = _warehouse()
    batches = _six_batches()
    # Started at three, so four batches ran and the answer has to be six rather than
    # four. A ledger holding a count instead of a position passes the first check and
    # fails this one.
    recovery.drive(warehouse, batches, PK, start_at=3)
    assert recovery.resume_point(warehouse) == 6, recovery.resume_point(warehouse)


def check_two_consumers_keep_separate_positions():
    warehouse = _warehouse()
    batches = _six_batches()
    for i, batch in enumerate(batches[:4], start=1):
        warehouse.apply_batch(batch, i, merge_keys=PK, consumer="a")
    warehouse.apply_batch(batches[4], 5, merge_keys=PK, consumer="b")
    assert recovery.resume_point(warehouse, "a") == 4
    assert recovery.resume_point(warehouse, "b") == 5
    assert recovery.resume_point(warehouse, "c") == 0


# The crash points


def check_every_named_crash_point_really_stops_the_batch():
    # Not one assertion about rollback. The point is that all three names are wired to
    # something, because a name in a tuple that no branch reads is the padding this repo
    # keeps finding.
    for point in wh.FAILURE_POINTS:
        warehouse = _warehouse()
        raised = None
        try:
            warehouse.apply_batch(_six_batches()[0], 1, merge_keys=PK, fail_at=point)
        except wh.Crash as exc:
            raised = exc
        assert raised is not None, point
        assert raised.point == point and raised.batch == 1, (point, raised)


def check_a_crash_anywhere_in_the_transaction_leaves_no_row_behind():
    for point in wh.FAILURE_POINTS:
        warehouse = _warehouse()
        batches = _six_batches()
        run = recovery.drive(warehouse, batches, PK, fail_at=point, fail_in_batch=4)
        assert run["crashed"] == {"batch": 4, "point": point}, run
        assert run["ledger_says"] == 3, (point, run)
        got = warehouse.con.execute(
            'select count(*) from "wh_order_item" where "cdc_batch" = 4').fetchone()[0]
        assert got == 0, (point, got)


def check_an_unknown_crash_point_is_refused_rather_than_ignored():
    warehouse = _warehouse()
    raised = None
    try:
        warehouse.apply_batch(_six_batches()[0], 1, merge_keys=PK, fail_at="halfway")
    except ValueError as exc:
        raised = str(exc)
    assert raised is not None and "halfway" in raised, raised


def check_a_crash_is_not_reported_as_a_database_error():
    # `Crash` deriving from something the probe already catches would let an injected
    # death and a real duckdb failure land in one bucket, which is a blanket except
    # wearing a better name.
    assert not issubclass(wh.Crash, duckdb.Error)


def check_the_ledger_cannot_fall_behind_the_rows_it_committed():
    warehouse = _warehouse()
    recovery.drive(warehouse, _six_batches(), PK, fail_at="merged", fail_in_batch=5)
    got = recovery.ledger_agrees_with_the_data(warehouse)
    assert got["data_ahead_of_the_ledger"] is False, got
    assert got["ledger_batch"] == got["highest_batch_in_the_data"] == 4, got


def check_data_ahead_of_the_ledger_is_reported_when_it_happens():
    # The other half of the check above, which would otherwise only ever see the passing
    # case and could not tell a working comparison from a hardcoded False.
    warehouse = _warehouse()
    recovery.drive(warehouse, _six_batches(), PK)
    warehouse.con.execute(
        'update "cdc_applied_batch" set batch = 2 where consumer = \'cdc\'')
    got = recovery.ledger_agrees_with_the_data(warehouse)
    assert got["data_ahead_of_the_ledger"] is True, got


# Where a restarted consumer starts


def check_the_two_strategies_choose_different_batches():
    assert recovery.first_batch_to_apply(6, "ledger") == 7
    assert recovery.first_batch_to_apply(6, "replay_all") == 1
    assert recovery.first_batch_to_apply(0, "ledger") == 1


def check_an_unknown_strategy_is_refused():
    raised = None
    try:
        recovery.first_batch_to_apply(3, "whatever")
    except ValueError as exc:
        raised = str(exc)
    assert raised is not None and "whatever" in raised, raised


def check_a_negative_last_applied_is_refused():
    raised = None
    try:
        recovery.first_batch_to_apply(-1)
    except ValueError as exc:
        raised = str(exc)
    assert raised is not None, raised


def check_driving_from_a_batch_index_of_zero_is_refused():
    raised = None
    try:
        recovery.drive(_warehouse(), _six_batches(), PK, start_at=0)
    except ValueError as exc:
        raised = str(exc)
    assert raised is not None, raised


def check_a_crash_point_without_a_batch_is_refused():
    # Half a crash instruction is how an arm quietly runs clean while its name says it
    # crashed, which would make every recovery figure in the report a lie about the
    # arm above it rather than a wrong number.
    for kwargs in ({"fail_at": "merged"}, {"fail_in_batch": 3}):
        raised = None
        try:
            recovery.drive(_warehouse(), _six_batches(), PK, **kwargs)
        except ValueError as exc:
            raised = str(exc)
        assert raised is not None, kwargs


# The batch number gap, which is the day's finding


def check_the_same_batching_twice_loses_nothing():
    same = [[1, 2], [3, 4], [5, 6], [7, 8]]
    got = recovery.resume_gap(same, same, 3)
    assert got["dropped"] == 0 and got["applied_twice"] == 0, got


def check_a_batching_that_moved_under_the_resume_point_drops_records():
    # The control above passes whether or not anything is compared. This one cannot.
    # Four applied, and the restarted run's batch three onward starts at the sixth
    # record, so the fifth lands nowhere.
    before = [[1, 2], [3, 4], [5, 6], [7, 8]]
    after = [[1, 2, 3], [4, 5], [6, 7, 8]]
    got = recovery.resume_gap(before, after, 3)
    assert got["dropped"] == 1, got
    assert got["applied_twice"] == 0, got


def check_a_restart_that_batches_smaller_doubles_records_rather_than_dropping_them():
    before = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    after = [[1, 2], [3, 4], [5, 6], [7, 8], [9]]
    got = recovery.resume_gap(before, after, 3)
    assert got["dropped"] == 0, got
    assert got["applied_twice"] == 2, got


def check_dropped_and_doubled_are_one_number_when_the_prefixes_are_the_same_size():
    # Two batchings of equal shape make these arithmetically identical, so a report
    # quoting both is quoting one. This is the case the probe's default settings are in.
    before = [[1, 2], [3, 4], [5, 6]]
    after = [[1, 2], [3, 5], [4, 6]]
    got = recovery.resume_gap(before, after, 3)
    assert got["dropped"] == got["applied_twice"] == 1, got
    assert recovery.resume_gap_is_two_numbers(got) is False, got


def check_dropped_and_doubled_come_apart_when_the_prefixes_differ():
    before = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    after = [[1, 2], [3, 4], [5, 6], [7, 8], [9]]
    got = recovery.resume_gap(before, after, 3)
    assert got["dropped"] != got["applied_twice"], got
    assert recovery.resume_gap_is_two_numbers(got) is True, got


def check_two_batchings_of_different_streams_are_refused():
    raised = None
    try:
        recovery.resume_gap([[1, 2], [3]], [[1, 2], [4]], 2)
    except ValueError as exc:
        raised = str(exc)
    assert raised is not None, raised


def check_a_resume_index_below_one_is_refused():
    raised = None
    try:
        recovery.resume_gap([[1]], [[1]], 0)
    except ValueError as exc:
        raised = str(exc)
    assert raised is not None, raised


def check_replaying_everything_covers_a_batching_that_moved():
    before = [[1, 2], [3, 4], [5, 6], [7, 8]]
    after = [[1, 2, 3], [4, 5], [6, 7, 8]]
    got = recovery.resume_gap(before, after, recovery.first_batch_to_apply(2, "replay_all"))
    assert got["dropped"] == 0, got


# The offset kept outside the transaction


def check_committing_the_offset_after_the_merge_replays_rather_than_losing():
    warehouse = _warehouse()
    offsets = recovery.SeparateOffsetStore(commit_before=False)
    recovery.drive(warehouse, _six_batches(), PK, offsets=offsets,
                   fail_at="merged", fail_in_batch=4)
    assert offsets.committed == 3, offsets.committed
    assert recovery.resume_point(warehouse) == 3


def check_committing_the_offset_before_the_merge_leaves_it_ahead_of_the_data():
    warehouse = _warehouse()
    offsets = recovery.SeparateOffsetStore(commit_before=True)
    recovery.drive(warehouse, _six_batches(), PK, offsets=offsets,
                   fail_at="merged", fail_in_batch=4)
    # The offset says four and the fourth batch is not in the warehouse. A consumer
    # trusting it starts at five and that batch is gone for good.
    assert offsets.committed == 4, offsets.committed
    assert recovery.resume_point(warehouse) == 3
    got = warehouse.con.execute(
        'select count(*) from "wh_order_item" where "cdc_batch" = 4').fetchone()[0]
    assert got == 0, got


def check_the_default_offset_ordering_is_the_safe_one():
    # Every check above names the ordering, so the default was untested and a mutant
    # flipping it survived the suite. The default is the one a caller who has not read
    # this file gets, which makes it the ordering most worth pinning.
    warehouse = _warehouse()
    offsets = recovery.SeparateOffsetStore()
    recovery.drive(warehouse, _six_batches(), PK, offsets=offsets,
                   fail_at="merged", fail_in_batch=4)
    assert offsets.commit_before is False
    assert offsets.committed == 3, offsets.committed


def check_the_ledger_and_the_offset_store_agree_when_nothing_crashes():
    # Both orderings are correct on a clean run, which is exactly why the wrong one
    # survives into production.
    for before in (True, False):
        warehouse = _warehouse()
        offsets = recovery.SeparateOffsetStore(commit_before=before)
        run = recovery.drive(warehouse, _six_batches(), PK, offsets=offsets)
        assert run["offset_store_says"] == run["ledger_says"] == 6, (before, run)


# The rolled up reconciliation line, moved out of the probe


def _report(*coverages):
    """A reconciliation report with the per table coverages named, and nothing else real.

    The rollup divides one sum by another, so what matters is the keys and the checked
    counts. Every table here holds a different coverage on purpose, because a fixture
    where they are all equal makes the range check pass on any implementation.
    """
    tables = {}
    for i, (checked, keys) in enumerate(coverages):
        tables["t{}".format(i)] = {
            "checked": checked, "keys_either_side": keys,
            "excluded_in_flight": keys - checked, "matching": checked,
            "missing": 0, "extra": 0, "differing": 0, "duplicate_keys_in_target": 0,
            "coverage": None if not keys else round(checked / keys, 4),
        }
    return {"tables": tables, "verdict": "clean_partial"}


def check_the_rolled_up_line_sums_the_tables():
    got = rec.one_line(_report((10, 20), (30, 30), (1, 50)))
    assert got["checked"] == 41, got
    assert got["keys_either_side"] == 100, got
    assert got["coverage"] == 0.41, got


def check_the_rolled_up_coverage_sits_inside_the_per_table_range():
    report = _report((10, 20), (30, 30), (1, 50))
    # 0.02, 0.5 and 1.0 on the tables. A rollup outside that range would mean the two
    # definitions of coverage in this repo are not the same quantity at all.
    got = rec.coverage_is_between_the_tables(report, rec.one_line(report))
    assert got is True, got


def check_a_rollup_outside_the_range_is_reported_as_such():
    # Without this the check above passes on a function that returns True. Handing it a
    # line whose coverage no weighting could produce is the only way to see it work.
    report = _report((10, 20), (30, 30), (1, 50))
    assert rec.coverage_is_between_the_tables(report, {"coverage": 0.001}) is False
    assert rec.coverage_is_between_the_tables(report, {"coverage": 1.5}) is False


def check_a_table_holding_no_keys_is_left_out_rather_than_counted_as_zero():
    report = _report((10, 20), (0, 0))
    line = rec.one_line(report)
    assert line["coverage"] == 0.5, line
    assert rec.coverage_is_between_the_tables(report, line) is True


def check_a_report_with_no_measurable_table_answers_neither_way():
    report = _report((0, 0))
    assert rec.coverage_is_between_the_tables(report, rec.one_line(report)) is None


def check_the_empty_table_guard_stays_even_though_nothing_reaches_it():
    # Every table reporting no coverage means the sum of the denominators is zero, which
    # makes the rolled up coverage None too, so the second half of that condition already
    # covers the first. Removing the first would let `min` raise out of a function whose
    # answers are True, False and None. It stays and this is the check that says why.
    got = rec.coverage_is_between_the_tables({"tables": {}}, {"coverage": 0.5})
    assert got is None, got


class _Cursor(object):
    """Enough of a cursor for `read_source`, which executes one select and fetches it.

    A real one needs Postgres and this suite runs on a clean machine, which is how
    `rec.run` came to be the entry point of the whole reconciliation job with no check
    over it. Twenty checks covered `reconcile` and none covered the thing that calls it.
    """

    def __init__(self, by_table):
        self.by_table = by_table
        self.held = None

    def execute(self, statement):
        table = statement.rsplit(" from ", 1)[1].strip()
        self.held = self.by_table[table]

    def fetchall(self):
        return list(self.held)


def _one_table_warehouse(rows):
    warehouse = _warehouse()
    for i, (order_id, line_no, qty) in enumerate(rows, start=1):
        warehouse.apply_batch(_stream([(100 + i, _row(order_id, line_no, qty=str(qty)))]),
                              i, merge_keys=PK)
    return warehouse


def check_the_reconciliation_job_reads_both_sides_and_finds_a_row_that_moved():
    warehouse = _one_table_warehouse([(1, 1, 5), (2, 1, 7)])
    cur = _Cursor({"shop.order_item": [("1", "1", "5"), ("2", "1", "9")]})
    got = rec.run(cur, warehouse, {"order_item": COLS},
                  {"order_item": ("order_id", "line_no")})
    assert got["verdict"] == "drift", got
    assert got["tables"]["order_item"]["differing"] == 1, got


def check_the_job_honours_the_keys_it_is_told_are_in_flight():
    # The mutant here turned `in_flight_by_table or {}` into `and {}`, which throws away
    # every exclusion a caller passes and leaves the default working. The job then reports
    # drift on rows the caller already said were mid stream. Nothing saw it because
    # nothing in the suite called `run` at all.
    warehouse = _one_table_warehouse([(1, 1, 5), (2, 1, 7)])
    cur = _Cursor({"shop.order_item": [("1", "1", "5"), ("2", "1", "9")]})
    got = rec.run(cur, warehouse, {"order_item": COLS},
                  {"order_item": ("order_id", "line_no")},
                  in_flight_by_table={"order_item": {("2", "1")}})
    assert got["verdict"] == "clean_partial", got
    assert got["tables"]["order_item"]["differing"] == 0, got
    assert got["tables"]["order_item"]["excluded_in_flight"] == 1, got
