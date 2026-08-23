"""The statements the merge runs, in the two dialects this project targets.

Held as Python strings rather than in a `.sql` file. Two dialects that have to stay in step
want one place a single test can walk both of them.

DuckDB is the one that runs here. Every DuckDB behaviour recorded below came off this
machine. The Snowflake statements are written alongside and have never executed, and the
notes about Snowflake are marked as such rather than blended in with the measured ones.

## The merge key is the primary key. The log position is a predicate, not part of the key.

Worth stating because the obvious phrasing is "merge on primary key plus position", and
taken literally that produces a different table. If the position joins the key then no
incoming change ever matches an existing row, every change inserts, and what you have is a
history table wearing a merge's clothes. `versioned=True` builds exactly that, so the cost
of the literal reading is a number rather than an argument.

## Metadata columns

    cdc_record_key   what the topic keyed this row under, and the only thing a tombstone
                     can address. Stored because a tombstone arrives carrying nothing else.
    cdc_lsn          the source log position this row was last written at. This is the
                     high water mark and it is why the delete is soft. A hard delete throws
                     the mark away with the row.
    cdc_deleted      soft delete flag. A deleted row stays, holding its position.
    cdc_batch        which batch last wrote this row, written inside the merge transaction.
"""

RECORD_KEY = "cdc_record_key"
LSN = "cdc_lsn"
DELETED = "cdc_deleted"
BATCH = "cdc_batch"

DIALECTS = ("duckdb", "snowflake")

# Two engine behaviours this module's callers have to reason about, and neither is a syntax
# difference. Every entry says where it came from. A claim about an engine nothing here has
# run is worth having and it is not worth confusing with one that was measured, so
# tests/test_warehouse.py fails on a note carrying neither marker.
DIALECT_NOTES = {
    "duckdb": {
        "primary_key_enforced": (
            "measured: a duplicate key from the not-matched branch raises "
            "ConstraintException on duckdb 1.5.5"),
        "duplicate_source_rows": (
            "measured: duckdb 1.5.5 applies the FIRST matching source row and silently "
            "discards the rest, so an unreduced batch is quietly wrong on the matched "
            "branch and loud on the not-matched one"),
    },
    "snowflake": {
        "primary_key_enforced": (
            "unverified: Snowflake accepts a primary key constraint and does not enforce "
            "it, so the not-matched branch would not raise. Nothing here has run against "
            "Snowflake"),
        "duplicate_source_rows": (
            "unverified: Snowflake documents ERROR_ON_NONDETERMINISTIC_MERGE for this "
            "case. Not run here, so the reduction is done before the statement rather "
            "than left to the engine"),
    },
}


def _check(dialect):
    if dialect not in DIALECTS:
        raise ValueError("unknown dialect {!r}, expected one of {}".format(
            dialect, ", ".join(DIALECTS)))


def quote(name):
    """Double quote an identifier, and refuse one carrying a quote of its own.

    Table and column names here come from a Postgres catalog read, so they are not user
    input in any interesting sense. Refusing is still cheaper than the argument about
    whether they are.
    """
    if '"' in name:
        raise ValueError("identifier contains a double quote: " + repr(name))
    return '"' + name + '"'


def target_ddl(dialect, table, columns, key_columns, versioned=False):
    """The warehouse table for one source table.

    Every business column is text. The decoded change stream carries whatever the type's
    output function printed and the source comparison casts to text on the other side, so
    parsing an integer here would only create somewhere for the two to disagree.
    """
    _check(dialect)
    missing = [c for c in key_columns if c not in columns]
    if missing:
        raise ValueError("key column not in columns: " + ", ".join(missing))
    lines = ["    {} varchar".format(quote(c)) for c in columns]
    lines.append("    {} varchar not null".format(quote(RECORD_KEY)))
    lines.append("    {} bigint not null".format(quote(LSN)))
    lines.append("    {} boolean not null".format(quote(DELETED)))
    lines.append("    {} bigint not null".format(quote(BATCH)))
    pk = list(key_columns) + ([LSN] if versioned else [])
    lines.append("    primary key ({})".format(", ".join(quote(c) for c in pk)))
    return "create table {} (\n{}\n)".format(quote(table), ",\n".join(lines))


def stage_ddl(dialect, table, columns):
    """The staging table one batch lands in before the merge reads it.

    No primary key. The stage holds whatever the reduction produced and the reduction is
    what guarantees one row per key. Putting a constraint here would turn a reduction bug
    into a constraint error, which sounds like a good idea until you notice it means the
    reduction is never really tested.
    """
    _check(dialect)
    lines = ["    {} varchar".format(quote(c)) for c in columns]
    lines.append("    {} varchar not null".format(quote(RECORD_KEY)))
    lines.append("    {} bigint not null".format(quote(LSN)))
    lines.append("    {} boolean not null".format(quote(DELETED)))
    return "create table {} (\n{}\n)".format(quote(table), ",\n".join(lines))


def merge_sql(dialect, target, stage, columns, key_columns, batch,
              guard=True, versioned=False):
    """One batch into one target table.

    guard=False removes the position comparison from the matched branch, so a late change
    overwrites a newer one. It exists to be measured against.
    """
    _check(dialect)
    join_columns = list(key_columns) + ([LSN] if versioned else [])
    on = " and ".join("t.{c} = s.{c}".format(c=quote(c)) for c in join_columns)

    # duckdb 1.5.5 refuses a qualified target column on the left of a SET with "Qualified
    # column names in UPDATE .. SET not supported". Snowflake's own examples write the
    # alias. So the target side of an assignment is the one place the two dialects really
    # do need different text, and it is a parser error rather than anything subtle.
    lhs = "t.{}" if dialect == "snowflake" else "{}"
    set_columns = list(columns) + [RECORD_KEY, LSN, DELETED]
    assignments = [(lhs + " = s.{}").format(quote(c), quote(c)) for c in set_columns]
    assignments.append((lhs + " = {:d}").format(quote(BATCH), batch))

    insert_columns = list(columns) + [RECORD_KEY, LSN, DELETED, BATCH]
    values = ["s.{}".format(quote(c)) for c in list(columns) + [RECORD_KEY, LSN, DELETED]]
    values.append("{:d}".format(batch))

    matched = "when matched"
    if guard:
        matched += " and s.{lsn} > t.{lsn}".format(lsn=quote(LSN))
    matched += " then update set\n        " + ",\n        ".join(assignments)

    return (
        "merge into {target} t\nusing {stage} s\n  on {on}\n{matched}\n"
        "when not matched then insert ({cols})\n    values ({vals})".format(
            target=quote(target), stage=quote(stage), on=on, matched=matched,
            cols=", ".join(quote(c) for c in insert_columns),
            vals=", ".join(values)))


def tombstone_sql(dialect, target, batch):
    """Soft delete every row the topic holds under one record key.

    A tombstone carries a record key and no value at all, so this is the widest thing it
    can say. Under the default key that is one row. Under a key that covers several it is
    several, which is the whole cost of widening one.

    The position comparison is here for the same reason it is on the merge. A row already
    written past the tombstone's position has been changed since, so the tombstone is stale
    for it. The in-memory consumer does not make that distinction and the probe counts
    where the two disagree rather than assuming they never do.
    """
    _check(dialect)
    return (
        "update {target} set {deleted} = true, {batch_col} = {batch:d}\n"
        " where {key} = ? and {lsn} <= ? and {deleted} = false".format(
            target=quote(target), deleted=quote(DELETED), batch_col=quote(BATCH),
            batch=batch, key=quote(RECORD_KEY), lsn=quote(LSN)))


LEDGER = "cdc_applied_batch"


def ledger_ddl(dialect):
    """Which batch was last applied, in the warehouse rather than beside it.

    The reason this table exists at all. A consumer's committed offset lives in Kafka and
    the data lives here, and nothing makes the two agree. Writing the batch id inside the
    merge transaction means the warehouse can be asked what it has already seen, and the
    answer cannot be a batch whose rows did not land.
    """
    _check(dialect)
    return (
        "create table {} (\n"
        "    consumer varchar not null,\n"
        "    batch bigint not null,\n"
        "    records_in_batch bigint not null,\n"
        "    primary key (consumer)\n)".format(quote(LEDGER)))


def record_batch_sql(dialect):
    _check(dialect)
    if dialect == "duckdb":
        return (
            "insert into {} (consumer, batch, records_in_batch) values (?, ?, ?)\n"
            "  on conflict (consumer) do update set batch = excluded.batch, "
            "records_in_batch = excluded.records_in_batch".format(quote(LEDGER)))
    # Snowflake has no ON CONFLICT. The same intent is a merge, and there is exactly one
    # row per consumer so the nondeterminism this file is otherwise about cannot arise.
    return (
        "merge into {} t using (select ? as consumer, ? as batch, ? as records_in_batch) s\n"
        "  on t.consumer = s.consumer\n"
        "when matched then update set t.batch = s.batch, t.records_in_batch = s.records_in_batch\n"
        "when not matched then insert (consumer, batch, records_in_batch)\n"
        "    values (s.consumer, s.batch, s.records_in_batch)".format(quote(LEDGER)))
