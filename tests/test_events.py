"""Checks over the record shape a topic carries.

The fixture is built so that two rows share a key under the order_id strategy and do not
under the default. A fixture with one row per group cannot test a rule about what happens
when a key covers more than one row, and every rule here is about exactly that.
"""

from cdc import decode
from cdc import events

PK = {
    "shop.customer": ("customer_id",),
    "shop.order_item": ("order_id", "line_no"),
}

OVERRIDE = {"shop.order_item": ("order_id",)}

ITEM_ONE = decode.Row("shop.order_item", "INSERT", None,
                      {"order_id": "7", "line_no": "1", "qty": "2"})
ITEM_TWO = decode.Row("shop.order_item", "INSERT", None,
                      {"order_id": "7", "line_no": "2", "qty": "5"})
ITEM_TWO_GONE = decode.Row("shop.order_item", "DELETE",
                           {"order_id": "7", "line_no": "2"}, None)
CUSTOMER = decode.Row("shop.customer", "UPDATE", None,
                      {"customer_id": "3", "country": "GB"})

STREAM = [(10, ITEM_ONE), (20, ITEM_TWO), (30, CUSTOMER), (40, ITEM_TWO_GONE)]


def _columns(override=None):
    return {t: events.key_columns_for(t, PK, override) for t in PK}


def check_the_default_key_is_the_primary_key():
    assert events.key_columns_for("shop.order_item", PK) == ("order_id", "line_no")


def check_message_key_columns_moves_a_table_off_its_primary_key():
    assert events.key_columns_for("shop.order_item", PK, OVERRIDE) == ("order_id",)
    # The override is per table. A table it does not name keeps its primary key, and a
    # version of this that applied the override everywhere would pass every check below.
    assert events.key_columns_for("shop.customer", PK, OVERRIDE) == ("customer_id",)


def check_a_table_with_no_known_primary_key_is_an_error():
    try:
        events.key_columns_for("shop.nowhere", PK)
    except KeyError:
        return
    raise AssertionError("a table with no primary key produced a key anyway")


def check_a_delete_takes_its_key_from_the_before_image():
    evs = events.envelope(ITEM_TWO_GONE, 40, ("order_id", "line_no"), "shopcdc")
    assert evs[0].key == {"order_id": "7", "line_no": "2"}
    assert evs[0].op == events.OP_DELETE


def check_a_delete_is_followed_by_a_tombstone_under_the_same_key():
    evs = events.envelope(ITEM_TWO_GONE, 40, ("order_id", "line_no"), "shopcdc")
    assert len(evs) == 2, evs
    assert evs[1].tombstone is True
    assert evs[1].key_bytes == evs[0].key_bytes
    # The tombstone carries no value at all. A consumer reading it as a delete envelope
    # would find None where the before image should be.
    assert evs[1].before is None and evs[1].after is None


def check_tombstones_can_be_turned_off():
    evs = events.envelope(ITEM_TWO_GONE, 40, ("order_id", "line_no"), "shopcdc",
                          tombstones_on_delete=False)
    assert len(evs) == 1
    assert evs[0].tombstone is False


def check_an_insert_and_an_update_produce_one_record_each():
    assert len(events.envelope(ITEM_ONE, 10, ("order_id", "line_no"), "shopcdc")) == 1
    assert len(events.envelope(CUSTOMER, 30, ("customer_id",), "shopcdc")) == 1


def check_a_key_column_the_change_does_not_carry_is_an_error():
    # This is what a narrow replica identity does. The delete carries the identity columns
    # and nothing else, so keying on a column outside them has nothing to read. Producing
    # a record with a partial key would put every such row on one partition.
    try:
        events.envelope(ITEM_TWO_GONE, 40, ("order_id", "qty"), "shopcdc")
    except KeyError as exc:
        assert "qty" in str(exc), exc
        return
    raise AssertionError("a key was built out of a column the change does not carry")


def check_the_topic_name_comes_from_the_schema_and_the_table():
    evs = events.envelope(ITEM_ONE, 10, ("order_id", "line_no"), "shopcdc")
    assert evs[0].topic == "shopcdc.shop.order_item"


def check_key_bytes_change_when_the_key_columns_do():
    default = events.envelope(ITEM_ONE, 10, ("order_id", "line_no"), "shopcdc")[0]
    routed = events.envelope(ITEM_ONE, 10, ("order_id",), "shopcdc")[0]
    assert default.key_bytes != routed.key_bytes


def check_two_lines_of_one_order_share_a_key_only_under_the_override():
    one = events.envelope(ITEM_ONE, 10, ("order_id",), "shopcdc")[0]
    two = events.envelope(ITEM_TWO, 20, ("order_id",), "shopcdc")[0]
    assert one.key_bytes == two.key_bytes

    one_pk = events.envelope(ITEM_ONE, 10, ("order_id", "line_no"), "shopcdc")[0]
    two_pk = events.envelope(ITEM_TWO, 20, ("order_id", "line_no"), "shopcdc")[0]
    assert one_pk.key_bytes != two_pk.key_bytes


def check_the_stream_keeps_the_log_order_and_grows_by_the_tombstones():
    evs = events.stream(STREAM, _columns(), "shopcdc")
    assert [e.lsn for e in evs] == [10, 20, 30, 40, 40]
    assert sum(1 for e in evs if e.tombstone) == 1


def check_key_reuse_is_one_under_the_primary_key():
    evs = events.stream(STREAM, _columns(), "shopcdc")
    reuse = events.key_reuse(evs, PK)
    assert reuse["keys_carrying_more_than_one_row"] == 0, reuse
    assert reuse["max_rows_under_one_key"] == 1, reuse


def check_key_reuse_finds_the_collision_the_override_creates():
    evs = events.stream(STREAM, _columns(OVERRIDE), "shopcdc")
    reuse = events.key_reuse(evs, PK)
    assert reuse["keys_carrying_more_than_one_row"] == 1, reuse
    assert reuse["max_rows_under_one_key"] == 2, reuse


def check_key_reuse_counts_rows_and_not_versions_of_a_row():
    # An update rewrites the image. Counting distinct images would report a row that was
    # updated once as two rows, and every key would then look reused.
    updated = decode.Row("shop.customer", "UPDATE", None,
                         {"customer_id": "3", "country": "FR"})
    evs = events.stream([(30, CUSTOMER), (35, updated)], _columns(), "shopcdc")
    reuse = events.key_reuse(evs, PK)
    assert reuse["max_rows_under_one_key"] == 1, reuse
