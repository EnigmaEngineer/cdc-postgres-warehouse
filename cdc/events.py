"""Turn decoded row changes into the records a Debezium topic actually carries.

Debezium does not put a row on a topic. It puts an envelope on a topic, under a key, and
on a delete it puts a second record under the same key with no value at all. That second
record is the tombstone and it is the part that decides what the key choice costs.

Three things live here and nothing else.

    key           which columns become the record key, and the bytes they serialise to
    envelope      the op and the two images and the position the consumer orders by
    tombstone     the null valued record that follows a delete

The source position is the write ahead log position of the change. It comes back from
Postgres as a number rather than as the '0/1A2B3C4D' text, because the text form does not
compare correctly as a string once the first half rolls over.
"""

from collections import namedtuple

from cdc import topics

# key_bytes is carried rather than recomputed. The partitioner and the compactor and the
# consumer all key on it, and three functions serialising the same dict independently is
# three chances to disagree about field order.
Event = namedtuple("Event", "topic table key key_bytes op before after lsn tombstone")

OP_INSERT = "c"
OP_UPDATE = "u"
OP_DELETE = "d"

_OP_FOR = {"INSERT": OP_INSERT, "UPDATE": OP_UPDATE, "DELETE": OP_DELETE}


def key_columns_for(table, primary_keys, overrides=None):
    """The columns Debezium keys this table by.

    primary_keys is what the catalog says. overrides is message.key.columns, which is the
    setting that moves a table off its primary key and is the whole argument of this repo's
    key decision.
    """
    if overrides and table in overrides:
        return tuple(overrides[table])
    if table not in primary_keys:
        raise KeyError("no primary key known for " + table)
    return tuple(primary_keys[table])


def _key_of(row, columns):
    source = row.after if row.after else row.before
    if source is None:
        raise ValueError("change on {} has neither a before nor an after image".format(row.table))
    missing = [c for c in columns if c not in source]
    if missing:
        # This is the failure that a replica identity setting causes and it has to be loud.
        # A delete under a narrow identity carries the identity columns and nothing else, so
        # keying on a non key column produces a record with no key rather than an error.
        raise KeyError("{} on {} carries no {}".format(row.action, row.table, ", ".join(missing)))
    return {c: source[c] for c in columns}


def envelope(row, lsn, columns, prefix, tombstones_on_delete=True):
    """One decoded row change into the one or two records Debezium would publish.

    A delete becomes two records. The envelope carries the before image so a consumer can
    tell what went away, and the tombstone that follows carries a key and nothing else so
    that a compacted topic can drop the key entirely.
    """
    if row.action not in _OP_FOR:
        raise ValueError("unknown action " + repr(row.action))
    schema, _, table_only = row.table.partition(".")
    topic = topics.topic_name(prefix, schema, table_only)
    key = _key_of(row, columns)
    key_bytes = topics.key_json(key, columns)
    op = _OP_FOR[row.action]

    out = [Event(topic, row.table, key, key_bytes, op, row.before, row.after, lsn, False)]
    if op == OP_DELETE and tombstones_on_delete:
        out.append(Event(topic, row.table, key, key_bytes, op, None, None, lsn, True))
    return out


def stream(rows_with_lsn, key_columns, prefix, tombstones_on_delete=True):
    """Every change in log order, flattened into the records the topics would carry.

    rows_with_lsn is (lsn, Row) pairs in the order the slot handed them over, which is the
    order the log holds them in. That order is the only total order in this system and
    every arm below is a rearrangement of it.
    """
    out = []
    for lsn, row in rows_with_lsn:
        columns = key_columns[row.table]
        out.extend(envelope(row, lsn, columns, prefix, tombstones_on_delete))
    return out


def key_reuse(events, primary_keys):
    """How many distinct source rows share each record key.

    One when the key is the primary key. More than one the moment a key column is dropped,
    and that is exactly when a tombstone stops meaning what the consumer thinks it means.
    A row's identity here is its primary key and not its whole image, because an update
    rewrites the image and the row is still the same row.
    """
    rows_under = {}
    for e in events:
        if e.tombstone:
            continue
        image = e.after if e.after else e.before
        if image is None:
            continue
        pk = primary_keys[e.table]
        if any(c not in image for c in pk):
            # A narrow replica identity can leave the primary key out of an image. Counting
            # such a change as a distinct row would inflate the answer, and dropping it
            # silently would deflate it. It is neither, so it is reported.
            rows_under.setdefault((e.topic, e.key_bytes), set())
            continue
        rows_under.setdefault((e.topic, e.key_bytes), set()).add(
            tuple(image[c] for c in pk))
    sizes = [len(v) for v in rows_under.values()]
    return {
        "keys": len(sizes),
        "keys_carrying_more_than_one_row": sum(1 for n in sizes if n > 1),
        "max_rows_under_one_key": max(sizes) if sizes else 0,
    }
