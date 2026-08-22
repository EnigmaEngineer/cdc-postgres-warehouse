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


def check_a_value_that_looks_like_a_field_no_longer_inflates_the_count():
    # This check used to assert the opposite. The counter matched name[type]: anywhere in
    # the fragment, so a text value carrying that shape was read as a second field, and the
    # limit was pinned at 2 rather than fixed. Reading values meant reading where a value
    # ends, and once the scanner does that the quoted run is skipped whole.
    line = "table shop.customer: INSERT: full_name[text]:'a weird[thing]:value'"
    c = decode.parse_change(line)
    assert len(c.after) == 1, c.after
    assert decode.parse_row(line).after == {"full_name": "a weird[thing]:value"}


def check_values_come_back_with_the_quotes_taken_off():
    r = decode.parse_row(INSERT)
    assert r.table == "shop.customer"
    assert r.before is None
    assert r.after["customer_id"] == "1"
    assert r.after["email"] == "a@b.c"
    assert r.after["full_name"] == "User 1"


def check_a_value_can_contain_a_space_and_a_colon():
    line = ("table shop.customer: INSERT: customer_id[bigint]:1 "
            "full_name[text]:'Smith: the second' country[character]:'GB'")
    r = decode.parse_row(line)
    assert r.after["full_name"] == "Smith: the second", r.after
    assert r.after["country"] == "GB"


def check_a_doubled_quote_is_one_quote():
    # Postgres writes O'Brien as 'O''Brien'. A reader that stops at the first quote
    # truncates the name and then the next field name is read out of the remainder.
    line = "table shop.customer: INSERT: full_name[text]:'O''Brien' country[character]:'IE'"
    r = decode.parse_row(line)
    assert r.after["full_name"] == "O'Brien", r.after
    assert r.after["country"] == "IE", r.after


def check_null_is_none_and_not_the_word():
    line = "table shop.customer: INSERT: customer_id[bigint]:1 country[character]:null"
    r = decode.parse_row(line)
    assert r.after["country"] is None
    assert "country" in r.after


def check_an_unterminated_quote_is_an_error_and_not_a_short_value():
    line = "table shop.customer: INSERT: full_name[text]:'never closed"
    try:
        decode.parse_row(line)
    except ValueError as exc:
        assert "unterminated" in str(exc), exc
    else:
        raise AssertionError("a truncated line came back as if it were whole")


def check_a_full_update_splits_values_the_same_way_it_splits_names():
    r = decode.parse_row(UPDATE_FULL)
    assert r.before["price_cents"] == "1900", r.before
    assert r.after["price_cents"] == "2211", r.after


def check_a_delete_carries_its_values_as_a_before_image():
    r = decode.parse_row(DELETE_DEFAULT)
    assert r.after is None
    assert r.before == {"order_id": "12", "line_no": "2"}


def check_no_tuple_data_is_an_empty_image_and_not_a_missing_one():
    r = decode.parse_row(DELETE_NOTHING)
    assert r.before == {}
    assert r.before is not None
