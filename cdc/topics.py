"""Topic layout for the change stream, and the partitioner that decides what it buys.

Debezium puts one table on one topic. The topic name is the connector's prefix, then the
schema, then the table. That part is settled and there is nothing to decide.

What is not settled is the message key, and the key is the whole of the ordering story.
The default key is the table's primary key. On a single column key that gives you what
everybody assumes: every change to one row lands on one partition, so a consumer sees
those changes in order. On a composite key it does not, and shop.order_item has a
composite key on purpose.

So this module carries the partitioner rather than a sentence claiming co-location. The
claim is arithmetic and it is computed.
"""

import json

# Kafka's default partitioner is in the Java client, so a Python port is the only way to
# predict where a record lands without a broker. This port came out of the clickstream
# project, where scripts/probe_partitioner.py ran a real console producer against it.
# It is re-checked here against org.apache.kafka.common.utils.Utils directly.
MURMUR2_SEED = 0x9747B28C


def topic_name(prefix, schema, table):
    """Debezium's default topic naming, which is prefix.schema.table.

    There is a topic routing transform that can collapse these into one topic. It is not
    used here and the reason is in the README.
    """
    if not prefix:
        raise ValueError("topic prefix must not be empty")
    if "." in prefix:
        raise ValueError("topic prefix must not contain a dot, got " + repr(prefix))
    return "{}.{}.{}".format(prefix, schema, table)


def key_json(row, key_columns):
    """The key bytes a JSON converter writes with schemas turned off.

    Column order is the primary key's own order and not the row's. Two producers that
    disagree about field order produce different bytes for the same logical key, which
    is a different partition for the same row.
    """
    if not key_columns:
        raise ValueError("a key needs at least one column")
    missing = [c for c in key_columns if c not in row]
    if missing:
        raise KeyError("row is missing key columns: " + ", ".join(missing))
    ordered = {c: row[c] for c in key_columns}
    return json.dumps(ordered, separators=(",", ":")).encode("utf-8")


def murmur2(data):
    """Kafka's Java client hash. Returns the unsigned 32 bit value.

    Not a general murmur2. It is the exact variant in
    org.apache.kafka.common.utils.Utils, seed 0x9747b28c.
    """
    m = 0x5BD1E995
    h = (MURMUR2_SEED ^ len(data)) & 0xFFFFFFFF
    whole = len(data) // 4 * 4
    for i in range(0, whole, 4):
        k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> 24
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = len(data) - whole
    if tail == 3:
        h ^= data[whole + 2] << 16
    if tail >= 2:
        h ^= data[whole + 1] << 8
    if tail >= 1:
        h ^= data[whole]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def partition_for(key_bytes, partitions):
    """Where the Java producer's default partitioner puts a keyed record.

    Java's toPositive masks the sign bit rather than taking an absolute value, so it is
    & 0x7fffffff and not abs. The two differ on one input and that input is reachable.
    """
    if partitions < 1:
        raise ValueError("partitions must be at least 1, got {}".format(partitions))
    return (murmur2(key_bytes) & 0x7FFFFFFF) % partitions


def fanout(rows, key_columns, group_column, partitions):
    """How many partitions the rows of one logical group are spread across.

    This is the question a composite key raises. If a warehouse merge wants to see every
    line of an order together, and the key carries the line number, then the lines of one
    order are hashed independently and land wherever they land.

    Returns the per group partition counts, so a caller reports a distribution rather
    than an average that hides the shape.
    """
    groups = {}
    for row in rows:
        target = groups.setdefault(row[group_column], set())
        target.add(partition_for(key_json(row, key_columns), partitions))
    return {g: len(p) for g, p in groups.items()}


def split_rate(rows, key_columns, group_column, partitions):
    """Fraction of groups whose rows do not all land on one partition.

    A group of one row can never split, so it is counted separately. A rate computed
    over groups that cannot split is a rate diluted by cases the question does not
    apply to, which is how a real effect gets reported as a small one.
    """
    counts = fanout(rows, key_columns, group_column, partitions)
    sizes = {}
    for row in rows:
        sizes[row[group_column]] = sizes.get(row[group_column], 0) + 1
    splittable = [g for g, n in sizes.items() if n > 1]
    if not splittable:
        return {"groups": len(counts), "splittable": 0, "split": 0, "rate": None}
    split = [g for g in splittable if counts[g] > 1]
    return {
        "groups": len(counts),
        "splittable": len(splittable),
        "split": len(split),
        "rate": len(split) / len(splittable),
    }


FRAMING_KINDS = ("begin", "commit", "relation", "origin", "type")
ROW_KINDS = ("insert", "update", "delete", "truncate")


def frame_split(counts):
    """One stream's frames divided into transaction framing and row changes.

    Kept here rather than worked out beside a table, because a number derived from other
    published numbers and written into a document is a number nothing re-runs.
    """
    framing = sum(counts.get(k, 0) for k in FRAMING_KINDS)
    rows = sum(counts.get(k, 0) for k in ROW_KINDS)
    total = framing + rows
    return {
        "frames": total,
        "framing": framing,
        "rows": rows,
        "row_change_share": rows / total if total else None,
    }


def split_cost(single, per_connector):
    """What splitting one connector into several costs on the source side.

    A pgoutput stream carries a frame per transaction boundary as well as a frame per row
    change, and the boundary frames are not filtered by the publication. A transaction
    that touches a table the connector does not care about still produces a BEGIN and a
    COMMIT on its stream.

    So splitting does not divide the work between connectors. Each one re-reads the whole
    transaction framing. This computes the totals rather than leaving them to be worked
    out in a sentence, because arithmetic over published numbers written into prose is
    arithmetic nothing re-runs.
    """
    if not per_connector:
        raise ValueError("split_cost needs at least one per connector breakdown")

    def framing(counts):
        return sum(counts.get(k, 0) for k in FRAMING_KINDS)

    def rows(counts):
        return sum(counts.get(k, 0) for k in ROW_KINDS)

    single_framing = framing(single)
    single_rows = rows(single)
    split_framing = sum(framing(c) for c in per_connector)
    split_rows = sum(rows(c) for c in per_connector)
    single_total = single_framing + single_rows
    split_total = split_framing + split_rows
    return {
        "connectors": len(per_connector),
        "single_framing": single_framing,
        "single_rows": single_rows,
        "single_messages": single_total,
        "split_framing": split_framing,
        "split_rows": split_rows,
        "split_messages": split_total,
        "framing_multiple": split_framing / single_framing if single_framing else None,
        "message_multiple": split_total / single_total if single_total else None,
        # A split that delivers a different number of row changes is not the same work
        # done differently, it is a bug. This is the check that says so.
        "rows_agree": split_rows == single_rows,
    }


def layout(prefix, tables, partitions):
    """The whole topic layout as data, so a test can read it and a README can print it.

    tables is an ordered mapping of (schema, table) to the primary key columns.
    """
    if partitions < 1:
        raise ValueError("partitions must be at least 1, got {}".format(partitions))
    out = []
    for (schema, table), key_columns in tables.items():
        out.append({
            "schema": schema,
            "table": table,
            "topic": topic_name(prefix, schema, table),
            "key_columns": list(key_columns),
            "partitions": partitions,
            "composite_key": len(key_columns) > 1,
        })
    return out
