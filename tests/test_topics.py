"""Checks over the topic layout and the partitioner.

The murmur2 port is the load bearing part. Everything this module claims about where a
record lands is wrong if the hash is wrong, and a hash ported from a description is a
hash nobody has checked. The vectors below came out of
org.apache.kafka.common.utils.Utils.murmur2 running on this machine against the real
kafka-clients jar, and scripts/dump_connector_configdef.sh documents how to regenerate
them.
"""

from cdc import decode
from cdc import topics

# key, unsigned murmur2, partition at 6. Taken from the Java client, not from this port.
JAVA_VECTORS = [
    ('{"order_id":1,"line_no":1}', 2017569160, 4),
    ('{"order_id":1,"line_no":2}', 573077348, 2),
    ('{"order_id":1,"line_no":3}', 1280944159, 1),
    ('{"customer_id":1}', 151059933, 3),
    ('{"customer_id":40}', 179988587, 5),
    # These three have the sign bit set, which is the only case where masking and taking
    # an absolute value disagree. Every vector above has it clear, so without these the
    # toPositive rule the docstring makes a point of is not tested at all. A mutant
    # widening the mask to 0xFFFFFFFF survived one pass and was killed on the next two,
    # by a check that happened to flip, which is worse than a clean survival.
    ('{"order_id":2,"line_no":1}', 3325209962, 0),
    ('{"order_id":4,"line_no":1}', 2834121711, 1),
    ('{"order_id":5,"line_no":1}', 3108692482, 2),
]


def check_murmur2_matches_the_java_client():
    for key, expected_hash, expected_partition in JAVA_VECTORS:
        raw = key.encode("utf-8")
        assert topics.murmur2(raw) == expected_hash, key
        assert topics.partition_for(raw, 6) == expected_partition, key


def check_the_sign_bit_is_masked_and_not_absolute():
    # Stated directly rather than left to the vector table, so the rule is findable by
    # name. Java's toPositive is a mask. Taking the hash modulo the partition count
    # without it gives a different answer on every one of these.
    for key, raw, expected in JAVA_VECTORS:
        if not raw & 0x80000000:
            continue
        assert topics.partition_for(key.encode("utf-8"), 6) == expected, key
        assert raw % 6 != expected, ("this vector no longer separates the two rules", key)
        return
    raise AssertionError("no vector in JAVA_VECTORS has the sign bit set")


def check_murmur2_is_sensitive_to_the_tail():
    # The tail handling is the part of murmur2 a port gets wrong, because it is a switch
    # with deliberate fallthrough in the original. Three lengths mod 4 exercise all of it.
    seen = {topics.murmur2(b"a" * n) for n in (1, 2, 3, 5, 6, 7)}
    assert len(seen) == 6, seen


def check_partition_count_must_be_positive():
    for bad in (0, -1):
        try:
            topics.partition_for(b"x", bad)
        except ValueError:
            continue
        raise AssertionError("partition_for accepted {}".format(bad))


def check_key_json_uses_key_order_not_row_order():
    row = {"line_no": 2, "order_id": 41, "qty": 3}
    assert topics.key_json(row, ("order_id", "line_no")) == b'{"order_id":41,"line_no":2}'
    # The other order really is different bytes, which is the whole point of pinning it.
    assert topics.key_json(row, ("line_no", "order_id")) != topics.key_json(
        row, ("order_id", "line_no"))


def check_key_json_refuses_a_row_missing_a_key_column():
    try:
        topics.key_json({"order_id": 1}, ("order_id", "line_no"))
    except KeyError as exc:
        assert "line_no" in str(exc)
        return
    raise AssertionError("key_json built a key from an incomplete row")


def check_key_json_refuses_an_empty_key():
    try:
        topics.key_json({"a": 1}, ())
    except ValueError:
        return
    raise AssertionError("key_json accepted a key with no columns")


def check_topic_name_refuses_a_dotted_prefix():
    assert topics.topic_name("shopcdc", "shop", "customer") == "shopcdc.shop.customer"
    for bad in ("", "a.b"):
        try:
            topics.topic_name(bad, "shop", "customer")
        except ValueError:
            continue
        raise AssertionError("topic_name accepted prefix {!r}".format(bad))


def check_split_rate_ignores_groups_that_cannot_split():
    # Two orders. One has three lines and one has a single line. A single line order can
    # never split, so counting it in the denominator would halve the rate for a reason
    # that has nothing to do with partitioning.
    rows = [{"order_id": 1, "line_no": n} for n in (1, 2, 3)]
    rows.append({"order_id": 2, "line_no": 1})
    out = topics.split_rate(rows, ("order_id", "line_no"), "order_id", 6)
    assert out["groups"] == 2, out
    assert out["splittable"] == 1, out


def check_split_rate_reports_nothing_rather_than_zero_when_nothing_can_split():
    rows = [{"order_id": n, "line_no": 1} for n in range(5)]
    out = topics.split_rate(rows, ("order_id", "line_no"), "order_id", 6)
    assert out["splittable"] == 0, out
    assert out["rate"] is None, out


def check_routing_on_the_group_column_cannot_split_a_group():
    rows = [{"order_id": o, "line_no": n} for o in range(1, 30) for n in (1, 2, 3)]
    composite = topics.split_rate(rows, ("order_id", "line_no"), "order_id", 6)
    routed = topics.split_rate(rows, ("order_id",), "order_id", 6)
    assert routed["split"] == 0, routed
    # And the composite key really does split, otherwise the comparison proves nothing
    # and the check would pass against a partitioner that returned a constant.
    assert composite["split"] > 0, composite


def check_split_cost_counts_framing_separately_from_rows():
    # Every row kind appears here on purpose. A fixture carrying inserts and updates only
    # cannot notice a row kind dropped from the tally, and the real stream has deletes in
    # it. A mutant removing delete from ROW_KINDS survived an earlier version of this.
    single = {"begin": 10, "commit": 10, "relation": 2,
              "insert": 4, "update": 3, "delete": 2, "truncate": 1}
    per_table = [
        {"begin": 10, "commit": 10, "relation": 1, "insert": 4, "truncate": 1},
        {"begin": 10, "commit": 10, "relation": 1, "update": 3, "delete": 2},
    ]
    out = topics.split_cost(single, per_table)
    assert out["single_framing"] == 22, out
    assert out["single_rows"] == 10, out
    assert out["split_framing"] == 42, out
    assert out["split_rows"] == 10, out
    assert out["rows_agree"] is True, out


def check_every_row_kind_is_counted_as_a_row():
    # Driven off the kind list rather than off a fixture beside it, so a kind added later
    # cannot be silently left out of both.
    for kind in topics.ROW_KINDS:
        out = topics.split_cost({kind: 1}, [{kind: 1}])
        assert out["single_rows"] == 1, (kind, out)
    for kind in topics.FRAMING_KINDS:
        out = topics.split_cost({kind: 1}, [{kind: 1}])
        assert out["single_framing"] == 1, (kind, out)
        assert out["single_rows"] == 0, (kind, out)


def check_split_cost_notices_when_the_split_loses_a_row_change():
    single = {"begin": 5, "commit": 5, "insert": 4}
    per_table = [{"begin": 5, "commit": 5, "insert": 3}]
    out = topics.split_cost(single, per_table)
    assert out["rows_agree"] is False, out


def check_split_cost_refuses_an_empty_split():
    try:
        topics.split_cost({"begin": 1}, [])
    except ValueError:
        return
    raise AssertionError("split_cost accepted a split with no connectors in it")


def check_pgoutput_types_read_the_leading_byte():
    frames = [b"B\x00\x01", b"I\x00", b"I\x00", b"U\x00", b"C\x00"]
    counts = decode.pgoutput_message_types(frames)
    assert counts == {"begin": 1, "insert": 2, "update": 1, "commit": 1}, counts


def check_pgoutput_types_report_an_unknown_byte_as_unknown():
    counts = decode.pgoutput_message_types([b"\xfe\x00"])
    assert counts == {"unknown:fe": 1}, counts
    assert decode.pgoutput_message_types([b""]) == {"empty": 1}


def check_row_change_share_separates_empty_from_zero():
    assert decode.row_change_share({}) is None
    assert decode.row_change_share({"begin": 3, "commit": 3}) == 0.0
    assert decode.row_change_share({"begin": 1, "commit": 1, "insert": 2}) == 0.5


def check_frame_split_agrees_with_split_cost_on_the_same_counts():
    # Two functions in this module answer the same question about one input, so something
    # has to assert they agree. Otherwise a change to one of them is invisible.
    counts = {"begin": 7, "commit": 7, "relation": 2, "insert": 5, "delete": 1}
    one = topics.frame_split(counts)
    both = topics.split_cost(counts, [counts])
    assert one["framing"] == both["single_framing"], (one, both)
    assert one["rows"] == both["single_rows"], (one, both)
    assert one["frames"] == both["single_messages"], (one, both)


def check_frame_split_separates_an_empty_stream_from_a_zero_share():
    assert topics.frame_split({})["row_change_share"] is None
    assert topics.frame_split({"begin": 2, "commit": 2})["row_change_share"] == 0.0
