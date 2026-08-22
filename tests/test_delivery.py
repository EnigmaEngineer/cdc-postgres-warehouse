"""Checks over what a consumer is handed.

The fixture has more than one record under some keys and exactly one under others,
because every rule here is about which pairs keep their order and a fixture where no key
repeats cannot tell a correct answer from a lucky one.
"""

from cdc import decode
from cdc import delivery
from cdc import events

PK = {"shop.order_item": ("order_id", "line_no")}


def _row(order_id, line_no, action="INSERT", qty="1"):
    image = {"order_id": str(order_id), "line_no": str(line_no), "qty": qty}
    if action == "DELETE":
        return decode.Row("shop.order_item", "DELETE", image, None)
    return decode.Row("shop.order_item", action, None, image)


def _stream(pairs, columns=("order_id", "line_no")):
    return events.stream(pairs, {"shop.order_item": columns}, "shopcdc")


# Six rows, two of them touched twice, one deleted. 30 orders wide so the routing has
# something to spread.
BUSY = _stream([
    (10, _row(1, 1)),
    (20, _row(1, 2)),
    (30, _row(1, 1, "UPDATE", "9")),
    (40, _row(2, 1)),
    (50, _row(2, 1, "UPDATE", "4")),
    (60, _row(3, 1)),
    (70, _row(3, 1, "DELETE")),
])


def check_routing_is_stable():
    a = delivery.route(BUSY, 6)
    b = delivery.route(BUSY, 6)
    assert {k: [e.lsn for e in v] for k, v in a.items()} == \
           {k: [e.lsn for e in v] for k, v in b.items()}


def check_every_record_lands_somewhere_and_nothing_is_duplicated():
    routed = delivery.route(BUSY, 6)
    landed = sum(len(v) for v in routed.values())
    assert landed == len(BUSY), (landed, len(BUSY))


def check_one_partition_is_a_legal_answer():
    routed = delivery.route(BUSY, 1)
    assert len(routed[0]) == len(BUSY)


def check_zero_partitions_is_refused_even_with_nothing_to_route():
    # Handed records, this passes whether or not route checks anything, because the
    # partitioner refuses a zero partition count with the same exception. Handed an empty
    # list the partitioner is never called, so only route's own check can refuse. That is
    # the input the check is most likely to be wrong on.
    try:
        delivery.route([], 0)
    except ValueError as exc:
        assert "at least 1" in str(exc), exc
        return
    raise AssertionError("a topic with no partitions was accepted")


def check_records_under_one_key_stay_on_one_partition():
    routed = delivery.route(BUSY, 6)
    where = {}
    for p, evs in routed.items():
        for e in evs:
            where.setdefault(e.key_bytes, set()).add(p)
    assert all(len(v) == 1 for v in where.values()), where


def check_an_interleaving_delivers_every_record_once():
    order = delivery.interleave(delivery.route(BUSY, 6), 3)
    assert sorted(e.lsn for e in order) == sorted(e.lsn for e in BUSY)
    assert len(order) == len(BUSY)


def check_an_interleaving_never_reorders_two_records_under_one_key():
    # This is Kafka's whole promise and it is the reason the consumer needs no ordering
    # help. Asserting it on the fixture means a routing change that breaks it is loud.
    for seed in range(8):
        order = delivery.interleave(delivery.route(BUSY, 6), seed)
        assert delivery.disorder(order)["same_key_inversions"] == 0, seed


def check_an_interleaving_really_moves_records():
    # Without this the check above passes on a delivery order that is just the log. Six
    # partitions and eight seeds, and at least one has to disturb the overall order.
    moved = [delivery.disorder(delivery.interleave(delivery.route(BUSY, 6), s))["inversions"]
             for s in range(8)]
    assert max(moved) > 0, moved


def check_a_shuffle_does_break_the_per_key_order():
    # And this is why the shuffle arm is unfair. It is the only arm where the ordering
    # guard has anything to do, and a guard measured only on the fair arms is a guard
    # measured on an input that cannot exercise it.
    broken = [delivery.disorder(delivery.shuffle(BUSY, s))["same_key_inversions"]
              for s in range(20)]
    assert max(broken) > 0, broken


def check_disorder_counts_the_log_as_ordered():
    d = delivery.disorder(list(BUSY))
    assert d["inversions"] == 0, d
    assert d["same_key_inversions"] == 0, d


def check_disorder_counts_a_reversal_as_every_pair_that_is_not_a_tie():
    # A delete and the tombstone behind it sit at the same log position, so the stream
    # always carries ties and reversing it cannot invert those pairs. Writing n choose 2
    # here would be a test of a fixture with no delete in it.
    lsns = [e.lsn for e in BUSY]
    strict = sum(1 for i in range(len(lsns)) for j in range(i + 1, len(lsns))
                 if lsns[i] < lsns[j])
    assert strict < len(lsns) * (len(lsns) - 1) // 2, "the fixture has no tied pair in it"
    d = delivery.disorder(list(reversed(BUSY)))
    assert d["inversions"] == strict, (d, strict)


def check_a_key_carrying_exactly_two_records_is_counted():
    # A key with three records under it is enough to make same_key_inversions non zero, so
    # a version that skips keys of two passes every check above. This one has exactly one
    # key and exactly two records, so the answer is one or the rule is not running.
    pair = _stream([(10, _row(9, 1)), (20, _row(9, 1, "UPDATE", "3"))])
    assert len(pair) == 2, pair
    d = delivery.disorder(list(reversed(pair)))
    assert d["same_key_inversions"] == 1, d


def check_cross_key_inversions_are_the_remainder():
    d = delivery.disorder(list(reversed(BUSY)))
    assert d["cross_key_inversions"] == d["inversions"] - d["same_key_inversions"], d


def check_compaction_keeps_the_last_record_for_a_key():
    kept = delivery.compact(list(BUSY))
    by_key = {}
    for e in kept:
        by_key.setdefault(e.key_bytes, []).append(e)
    assert all(len(v) == 1 for v in by_key.values()), by_key
    # order 1 line 1 was written at 10 and updated at 30. The survivor is the update.
    surviving = {e.lsn for e in kept}
    assert 30 in surviving and 10 not in surviving, surviving
    assert 50 in surviving and 40 not in surviving, surviving


def check_compaction_removes_a_key_whose_last_record_is_a_tombstone():
    kept = delivery.compact(list(BUSY))
    # order 3 line 1 was inserted at 60 and deleted at 70, and the tombstone at 70 is the
    # last record under that key. Nothing about order 3 survives.
    assert not any(e.key.get("order_id") == "3" for e in kept), [e.key for e in kept]


def check_compaction_keeps_the_survivors_in_log_order():
    kept = delivery.compact(list(BUSY))
    assert [e.lsn for e in kept] == sorted(e.lsn for e in kept), [e.lsn for e in kept]


def check_compaction_under_a_shared_key_loses_a_row_that_is_still_there():
    # The same seven changes keyed by order_id alone. Order 1 has two live lines and
    # compaction keeps one record for the key, so a line that was never deleted is gone.
    shared = _stream([(10, _row(1, 1)), (20, _row(1, 2))], columns=("order_id",))
    kept = delivery.compact(shared)
    assert len(kept) == 1, kept
    assert kept[0].after["line_no"] == "2", kept[0].after


def check_partition_spread_reports_empty_partitions():
    spread = delivery.partition_spread(delivery.route(BUSY, 64))
    assert spread["records"] == len(BUSY)
    assert spread["empty_partitions"] > 0, spread
