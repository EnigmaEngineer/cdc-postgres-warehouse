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
