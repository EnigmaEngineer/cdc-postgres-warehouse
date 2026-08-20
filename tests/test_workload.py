"""Checks over the workload planner.

The planner is where the change counts come from, so a mistake here is a mistake in every
number the project publishes. It needs no database, which is deliberate.
"""

from load import workload


def check_same_seed_gives_the_same_plan():
    a = workload.plan(11, 300)
    b = workload.plan(11, 300)
    assert a.ops == b.ops


def check_different_seeds_diverge():
    a = workload.plan(11, 300)
    b = workload.plan(12, 300)
    assert a.ops != b.ops


def check_step_count_is_exact():
    p = workload.plan(3, 457)
    assert len(p.ops) == 457
    assert sum(workload.action_totals(p).values()) == 457


def check_an_impossible_delete_becomes_an_insert_and_is_counted():
    # Every table starts empty, so the first op on each table cannot be an update or a
    # delete. The planner is not allowed to drop those silently.
    p = workload.plan(5, 200)
    assert p.deflected["empty_table"] > 0
    for table in workload.TABLES:
        first = next((op for op in p.ops if op.table == table), None)
        if first is not None:
            assert first.action == "insert", (table, first)


def check_seeding_the_dimensions_removes_their_deflections():
    empty = workload.plan(5, 400)
    seeded = workload.plan(5, 400, seed_rows={"customer": 50, "product": 50,
                                              "order_header": 0, "order_item": 0})
    assert seeded.deflected["empty_table"] < empty.deflected["empty_table"]


def check_row_counts_never_go_negative():
    for seed in range(6):
        p = workload.plan(seed, 500)
        for table, n in p.final_rows.items():
            assert n >= 0, (seed, table, n)


def check_deletes_only_land_on_the_table_that_takes_them():
    p = workload.plan(9, 1500)
    deleted = {op.table for op in p.ops if op.action == "delete"}
    assert deleted == {"order_item"}, deleted


def check_the_mix_is_update_heavy():
    # This is a claim the README makes, so it is pinned rather than left to a comment.
    p = workload.plan(7, 2000, seed_rows={"customer": 200, "product": 80,
                                          "order_header": 0, "order_item": 0})
    t = workload.action_totals(p)
    assert t["update"] > t["delete"]
    assert t["insert"] > 0


def check_weights_are_respected_when_overridden():
    only_inserts = {t: (1.0, 0.0, 0.0) for t in workload.TABLES}
    p = workload.plan(1, 100, weights=only_inserts)
    assert workload.action_totals(p) == {"insert": 100, "update": 0, "delete": 0}
    assert p.deflected["empty_table"] == 0


def check_counts_and_action_totals_agree():
    p = workload.plan(4, 600)
    by_key = workload.counts(p)
    totals = workload.action_totals(p)
    for action in totals:
        summed = sum(v for k, v in by_key.items() if k.endswith(":" + action))
        assert summed == totals[action], (action, summed, totals[action])
