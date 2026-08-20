"""Checks over the decoded change reader.

The fixtures are real test_decoding output copied out of a running server, not lines
written from memory. A parser tested against invented input tests the author's memory of
a format rather than the format.
"""

from cdc import decode

INSERT = "table shop.customer: INSERT: customer_id[bigint]:1 email[text]:'a@b.c' full_name[text]:'User 1' country[character]:'GB' created_at[timestamp with time zone]:'2026-08-20 11:00:00+00' updated_at[timestamp with time zone]:'2026-08-20 11:00:00+00'"

UPDATE_DEFAULT = "table shop.product: UPDATE: product_id[bigint]:4 sku[text]:'SKU-000004' name[text]:'Product 4' price_cents[integer]:2211 active[boolean]:true updated_at[timestamp with time zone]:'2026-08-20 11:00:01+00'"

UPDATE_FULL = "table shop.product: UPDATE: old-key: product_id[bigint]:4 sku[text]:'SKU-000004' name[text]:'Product 4' price_cents[integer]:1900 active[boolean]:true updated_at[timestamp with time zone]:'2026-08-20 11:00:00+00' new-tuple: product_id[bigint]:4 sku[text]:'SKU-000004' name[text]:'Product 4' price_cents[integer]:2211 active[boolean]:true updated_at[timestamp with time zone]:'2026-08-20 11:00:01+00'"

DELETE_DEFAULT = "table shop.order_item: DELETE: order_id[bigint]:12 line_no[smallint]:2"

DELETE_FULL = "table shop.order_item: DELETE: order_id[bigint]:12 line_no[smallint]:2 product_id[bigint]:9 qty[smallint]:3 unit_cents[integer]:499"

DELETE_NOTHING = "table shop.order_item: DELETE: (no-tuple-data)"


def check_begin_and_commit_are_not_changes():
    assert decode.parse_change("BEGIN 742") is None
    assert decode.parse_change("COMMIT 742") is None


def check_insert_has_no_before_image():
    c = decode.parse_change(INSERT)
    assert c.table == "shop.customer"
    assert c.action == "INSERT"
    assert c.before is None
    assert len(c.after) == 6, c.after


def check_default_update_carries_only_the_new_row():
    c = decode.parse_change(UPDATE_DEFAULT)
    assert c.before is None
    assert len(c.after) == 6


def check_full_update_splits_old_from_new():
    c = decode.parse_change(UPDATE_FULL)
    assert len(c.before) == 6, c.before
    assert len(c.after) == 6, c.after
    # The two halves must not be the same object or the same list. If old and new came
    # back identical the parser has read one fragment twice, which is the failure this
    # check exists for and it is invisible when both sides have the same width.
    assert c.before is not c.after


def check_delete_tuple_is_read_as_a_before_image():
    c = decode.parse_change(DELETE_DEFAULT)
    assert c.after is None
    assert c.before == ["order_id", "line_no"]


def check_replica_identity_widens_the_delete():
    narrow = decode.parse_change(DELETE_DEFAULT)
    wide = decode.parse_change(DELETE_FULL)
    assert len(wide.before) > len(narrow.before)
    assert len(wide.before) == 5


def check_no_tuple_data_is_empty_and_not_missing():
    c = decode.parse_change(DELETE_NOTHING)
    # An empty list and None mean different things. Empty is 'the server sent nothing'.
    # None is 'this change shape has no such side'. summarise counts them differently.
    assert c.before == []
    assert c.before is not None


def check_summarise_counts_before_images():
    stream = [INSERT, UPDATE_DEFAULT, UPDATE_FULL, DELETE_DEFAULT, DELETE_NOTHING]
    s = decode.summarise(decode.parse_stream(stream))
    assert s["changes"] == 5
    assert s["by_action"] == {"INSERT": 1, "UPDATE": 2, "DELETE": 2}
    assert s["before_present"] == 2, s
    assert s["before_absent"] == 2, s


def check_summarise_counts_updates_with_a_before_image_separately():
    # before_present pools updates and deletes. Under the default replica identity every
    # one of them is a delete, and that is the whole finding. Pooling hides it.
    stream = [UPDATE_DEFAULT, UPDATE_FULL, DELETE_DEFAULT, DELETE_FULL]
    s = decode.summarise(decode.parse_stream(stream))
    assert s["before_present"] == 3
    assert s["updates_with_before_image"] == 1, s


def check_summarise_separates_tables():
    s = decode.summarise(decode.parse_stream([INSERT, UPDATE_DEFAULT, DELETE_DEFAULT]))
    assert s["by_table"]["shop.customer:INSERT"] == 1
    assert s["by_table"]["shop.product:UPDATE"] == 1
    assert s["by_table"]["shop.order_item:DELETE"] == 1


def check_field_counting_is_not_fooled_by_a_value_containing_a_colon():
    line = ("table shop.customer: INSERT: customer_id[bigint]:1 "
            "full_name[text]:'Smith: the second' country[character]:'GB'")
    c = decode.parse_change(line)
    assert len(c.after) == 3, c.after


def check_the_known_limit_of_field_counting_is_pinned():
    # A text value that itself contains name[type]: does inflate the count. Nothing this
    # repo writes produces one. Pinning it here means the limit is documented by a
    # failing assertion the day someone changes the parser, rather than by a comment.
    line = "table shop.customer: INSERT: full_name[text]:'a weird[thing]:value'"
    c = decode.parse_change(line)
    assert len(c.after) == 2, "the naive count is known to be 2 here, not 1"
