"""Thin helpers over a Postgres connection. Nothing here is clever.

The one thing worth knowing is that write ahead log positions are text in the wire
protocol and bytes in arithmetic, so every size in this repo goes through
pg_wal_lsn_diff rather than through anything that parses the '0/1A2B3C4D' form by hand.
"""

import psycopg2

DEFAULT_DSN = "host=/tmp port=55432 dbname=shop user=postgres"


def connect(dsn=DEFAULT_DSN, autocommit=True):
    con = psycopg2.connect(dsn)
    con.autocommit = autocommit
    return con


def current_lsn(cur):
    cur.execute("select pg_current_wal_lsn()::text")
    return cur.fetchone()[0]


def lsn_bytes(cur, start, end):
    """Bytes of write ahead log between two positions. Postgres does the arithmetic."""
    cur.execute("select pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)::bigint", (end, start))
    return int(cur.fetchone()[0])


def create_slot(cur, name, plugin="test_decoding"):
    cur.execute("select pg_create_logical_replication_slot(%s, %s)", (name, plugin))
    return cur.fetchone()[0]


def drop_slot_if_exists(cur, name):
    cur.execute("select 1 from pg_replication_slots where slot_name = %s", (name,))
    if cur.fetchone():
        cur.execute("select pg_drop_replication_slot(%s)", (name,))


def peek_changes(cur, slot):
    """Read the slot without consuming it. Peek leaves the slot where it was.

    Consuming is pg_logical_slot_get_changes. Peek is the right call for a measurement,
    because a measurement that moves the thing it measures cannot be repeated.
    """
    cur.execute("select data from pg_logical_slot_peek_changes(%s, null, null)", (slot,))
    return [r[0] for r in cur.fetchall()]


def peek_changes_with_lsn(cur, slot):
    """Every change with the log position it sits at, as a number.

    The position arrives as '0/1A2B3C4D' text and that does not sort correctly as a string
    once either half changes width. pg_wal_lsn_diff against the origin turns it into bytes,
    which is the quantity a consumer's ordering rule compares.
    """
    cur.execute(
        "select pg_wal_lsn_diff(lsn, '0/0')::bigint, data"
        " from pg_logical_slot_peek_changes(%s, null, null)",
        (slot,),
    )
    return [(int(r[0]), r[1]) for r in cur.fetchall()]


def peek_binary_changes(cur, slot, publication, proto_version="1"):
    """Message count and total bytes from a pgoutput slot.

    The frames are not parsed here. The question this answers is whether the binary plugin
    a real consumer reads carries the same extra data the readable one shows, and a byte
    total answers that without a frame decoder.
    """
    cur.execute(
        "select data from pg_logical_slot_peek_binary_changes(%s, null, null,"
        " 'proto_version', %s, 'publication_names', %s)",
        (slot, proto_version, publication),
    )
    rows = [bytes(r[0]) for r in cur.fetchall()]
    return len(rows), sum(len(r) for r in rows)


def peek_binary_frames(cur, slot, publication, proto_version="1"):
    """The raw pgoutput frames, so something can look at what kind they are.

    peek_binary_changes returns a count and a byte total, which answers how big the
    stream is and not what is in it. Those are different questions and the second one is
    the one that explains the first.
    """
    cur.execute(
        "select data from pg_logical_slot_peek_binary_changes(%s, null, null,"
        " 'proto_version', %s, 'publication_names', %s)",
        (slot, proto_version, publication),
    )
    return [bytes(r[0]) for r in cur.fetchall()]


def slot_retention(cur):
    """What each slot is holding on to, in bytes.

    An unconsumed slot is the thing that takes a Postgres source down. It pins the write
    ahead log from its restart position forward, and the log then grows until the disk
    fills. Nothing warns. The database is healthy right up until it is not.
    """
    cur.execute(
        "select slot_name, plugin, active, wal_status,"
        " pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint"
        " from pg_replication_slots order by slot_name"
    )
    return [
        {"slot": r[0], "plugin": r[1], "active": r[2], "wal_status": r[3],
         "retained_bytes": int(r[4]) if r[4] is not None else None}
        for r in cur.fetchall()
    ]


def consume_changes(cur, slot):
    """Read the slot and advance it. This is what a real consumer does.

    peek_changes is the right call for a measurement. This is the right call when the
    question is what happens to the retained log after somebody reads it, because that
    only moves when the slot is consumed.
    """
    cur.execute("select data from pg_logical_slot_get_changes(%s, null, null)", (slot,))
    return [r[0] for r in cur.fetchall()]


def consume_binary_changes(cur, slot, publication, proto_version="1"):
    """Consume a pgoutput slot, which is the binary path and needs its options.

    The text function refuses a pgoutput slot with 'client sent proto_version=0'. It is
    not that the slot is unreadable. It is that pgoutput takes required options and the
    text function has nowhere to put them.
    """
    cur.execute(
        "select data from pg_logical_slot_get_binary_changes(%s, null, null,"
        " 'proto_version', %s, 'publication_names', %s)",
        (slot, proto_version, publication),
    )
    rows = [bytes(r[0]) for r in cur.fetchall()]
    return len(rows), sum(len(r) for r in rows)


def publication_tables(cur, publication):
    """The tables a publication really covers, read from the catalog.

    Not the list in db/schema.sql. The catalog is what the connector will stream and the
    file is what somebody meant. A connector config built from the file cannot notice
    that the two have drifted.
    """
    cur.execute(
        "select schemaname, tablename from pg_publication_tables"
        " where pubname = %s order by schemaname, tablename",
        (publication,),
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


def primary_key_columns(cur, table):
    """Primary key columns in key order, which is the order a Debezium key uses."""
    cur.execute(
        "select a.attname from pg_index i"
        " join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)"
        " where i.indrelid = %s::regclass and i.indisprimary"
        " order by array_position(i.indkey, a.attnum)",
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def try_create_slot(cur, name, plugin):
    """Create a slot and return the error text instead of raising.

    A probe that reports what a setting costs has to be able to report a refusal as a
    refusal. Swallowing the exception and returning False loses the message, and here
    the message is the finding.
    """
    try:
        cur.execute("select pg_create_logical_replication_slot(%s, %s)", (name, plugin))
        return None
    except Exception as exc:  # the driver raises several types for this
        cur.connection.rollback()
        return str(exc).strip().splitlines()[0]


def slot_positions(cur, slot):
    """Both positions a slot carries, and they are not the same thing.

    confirmed_flush_lsn is how far the consumer says it has got. restart_lsn is how far
    back the server still has to keep the log for this slot. A consumer that reads
    everything moves the first one. Only the server moves the second, and it does that on
    its own schedule.

    Reporting one of these as 'the slot position' is how a retention problem gets missed.
    """
    cur.execute(
        "select restart_lsn::text, confirmed_flush_lsn::text,"
        " pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint,"
        " pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint"
        " from pg_replication_slots where slot_name = %s",
        (slot,),
    )
    row = cur.fetchone()
    if row is None:
        raise KeyError("no slot named " + slot)
    return {
        "restart_lsn": row[0],
        "confirmed_flush_lsn": row[1],
        "retained_bytes": int(row[2]),
        "unconfirmed_bytes": int(row[3]),
    }


def checkpoint(cur):
    """Force a checkpoint. Needs superuser, which the bootstrap user is.

    A slot's restart position is recalculated when the server writes a running
    transactions record, and that happens at a checkpoint. So the answer to 'I read
    everything and the disk is still full' is usually 'no checkpoint has happened yet'.
    """
    cur.execute("checkpoint")


def set_replica_identity(cur, table, mode):
    if mode not in ("DEFAULT", "FULL", "NOTHING"):
        raise ValueError("replica identity must be DEFAULT, FULL or NOTHING, got " + repr(mode))
    cur.execute("alter table {} replica identity {}".format(table, mode))


def replica_identity(cur, table):
    """Read back what the catalog says rather than what was asked for.

    One letter per setting. d for default and f for full. n for nothing and i for index.
    """
    cur.execute("select relreplident from pg_class where oid = %s::regclass", (table,))
    return cur.fetchone()[0]
