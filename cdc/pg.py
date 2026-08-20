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
