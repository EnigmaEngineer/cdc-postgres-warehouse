"""Checks over db/schema.sql that do not need a database.

The one that matters is publication coverage. Postgres 14 has no FOR ALL TABLES IN SCHEMA,
so the publication names its tables. A table added to the schema and not to the publication
does not error and does not warn. It just never appears in the change stream, and the first
sign is a warehouse table that is silently short.
"""

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA = os.path.join(ROOT, "db", "schema.sql")


def sql():
    with open(SCHEMA) as fh:
        return fh.read()


def strip_comments(text):
    return re.sub(r"--[^\n]*", "", text)


def declared_tables(text):
    return set(re.findall(r"create table\s+(shop\.[a-z_]+)", text, re.I))


def published_tables(text):
    m = re.search(r"create publication\s+\w+\s+for table\s+(.*?);", text, re.I | re.S)
    if not m:
        return set()
    return {t.strip() for t in m.group(1).split(",") if t.strip()}


def check_the_schema_file_is_not_empty():
    assert len(sql()) > 500, "schema.sql looks truncated"


def check_the_parser_finds_the_tables():
    # If this drops to zero the coverage check below passes on an empty set, which is the
    # shape of every gate that reports clean because it looked at nothing.
    assert len(declared_tables(strip_comments(sql()))) >= 4


def check_every_table_is_published():
    text = strip_comments(sql())
    missing = declared_tables(text) - published_tables(text)
    assert not missing, "tables in the schema and not in the publication: {}".format(
        sorted(missing))


def check_nothing_is_published_that_does_not_exist():
    text = strip_comments(sql())
    extra = published_tables(text) - declared_tables(text)
    assert not extra, "published but not declared: {}".format(sorted(extra))


def check_the_coverage_check_would_notice_a_missing_table():
    # Feed the same functions a schema with a table left out of the publication. Without
    # this, the check above proves the schema is correct today and proves nothing about
    # whether it can fail.
    broken = "\n".join([
        "create table shop.a (id int primary key);",
        "create table shop.b (id int primary key);",
        "create publication cdc_shop for table shop.a;",
    ])
    text = strip_comments(broken)
    assert declared_tables(text) - published_tables(text) == {"shop.b"}


def check_every_table_has_a_primary_key():
    # Logical decoding under the default replica identity uses the primary key. A table
    # without one cannot be updated at all once it is published.
    text = strip_comments(sql())
    for block in re.findall(r"create table\s+(shop\.[a-z_]+)\s*\((.*?)\n\);", text, re.S | re.I):
        name, body = block
        assert "primary key" in body.lower(), "{} has no primary key".format(name)
