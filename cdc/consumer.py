"""Apply a change stream to a keyed target, in whatever order it turns up.

The target here is a dict. The warehouse merge comes later and it will be the same rules
written in SQL. Doing it in memory first is deliberate, because the rules are the part
worth getting wrong cheaply.

**The record key and the merge key are two different things and conflating them is the
mistake this module is shaped to avoid.** The record key is what Kafka hashes to pick a
partition and what a tombstone addresses. The merge key is what identifies a row in the
target. Debezium makes them the same by default and they stop being the same the moment
`message.key.columns` moves a table off its primary key. A consumer that treats the record
key as the row identity then merges several source rows into one and never says so.

Three rules and each one is a decision somebody has to make.

**A change older than what the key already holds is dropped.** The comparison is the write
ahead log position, which is the source's own clock and the only ordering both sides agree
on. Comparing timestamps compares two clocks. Comparing Kafka offsets compares one per
partition.

**The high water mark outlives the row.** A key that has been deleted keeps the position it
was deleted at, so a delayed insert that predates the delete cannot resurrect it. Holding
the mark only for live rows is the version of this that looks correct and loses the case
the guard exists for.

**A tombstone carries a record key and nothing else.** It cannot name a row when the record
key covers more than one. Applying it is then a decision about which rows go, and the
honest answer for a consumer merging on the primary key is that it does not know. It is
also subject to the same position comparison as everything else, because a tombstone that
turns up behind a newer change to the row is stale in exactly the way a delete envelope
would be.
"""

from collections import namedtuple

Result = namedtuple("Result", "rows counters")


def identity(image, columns):
    """The merge key of one row image, or None when the image does not carry it.

    Public because warehouse/merge.py has to derive the same key the same way. Two
    definitions of what identifies a row is how the SQL merge and the in-memory reference
    end up disagreeing for a reason neither of them reports.
    """
    if image is None:
        return None
    if any(c not in image for c in columns):
        return None
    return tuple(image[c] for c in columns)


def apply_events(events, guard=True, on_tombstone="delete", merge_keys=None):
    """Fold a delivery order into the target state.

    merge_keys maps a source table to the columns that identify one row. Leave it None to
    key the target by the record key, which is the shortcut a consumer takes when nobody
    has separated the two ideas.

    guard=False is the same consumer with the ordering rule taken out. It exists to be
    measured against rather than to be used.

    on_tombstone='delete' removes every row held under the record key. 'ignore' treats the
    tombstone as the framing it also is, on the argument that the delete envelope in front
    of it already named the row. Which is right depends entirely on the key.
    """
    if on_tombstone not in ("delete", "ignore"):
        raise ValueError("on_tombstone must be delete or ignore, got " + repr(on_tombstone))

    rows = {}
    under_key = {}
    watermark = {}
    counters = {
        "records": 0, "inserts": 0, "updates": 0, "deletes": 0, "tombstones": 0,
        "stale_dropped": 0, "same_position": 0,
        "deletes_of_a_row_not_held": 0, "updates_of_a_row_not_held": 0,
        "rows_removed_by_tombstones": 0, "changes_with_no_merge_key": 0,
        "rows_kept_from_a_stale_tombstone": 0,
    }

    for e in events:
        counters["records"] += 1

        if e.tombstone:
            counters["tombstones"] += 1
            if on_tombstone == "delete":
                held = under_key.get((e.topic, e.key_bytes), ())
                # The guard applies here too. This path used to pop every row under the
                # key with no position comparison at all, which meant a tombstone that
                # arrived after a newer change removed a row that had moved past it. The
                # SQL merge gated the same operation and the two implementations then
                # disagreed by 37 rows on the shuffled arm. That disagreement is what
                # found this.
                for slot in sorted(held):
                    at = watermark.get(slot)
                    if guard and at is not None and at > e.lsn:
                        counters["rows_kept_from_a_stale_tombstone"] += 1
                        continue
                    if rows.pop(slot, None) is not None:
                        counters["rows_removed_by_tombstones"] += 1
                    _forget(under_key, e, slot)
            continue

        image = e.after if e.op != "d" else e.before
        if merge_keys is None:
            slot = (e.topic, e.key_bytes)
        else:
            ident = identity(image, merge_keys[e.table])
            if ident is None:
                # A replica identity narrow enough to leave a merge column out makes this
                # change unaddressable. Counting it is the point. Guessing is what a
                # consumer does when it treats the record key as the row.
                counters["changes_with_no_merge_key"] += 1
                continue
            slot = (e.topic, ident)

        held_at = watermark.get(slot)
        if held_at is not None and e.lsn == held_at:
            # Two records at one log position. Dropping these as stale would lose real
            # changes, so they are applied and counted, and the count is reported rather
            # than assumed to be zero.
            counters["same_position"] += 1
        elif guard and held_at is not None and e.lsn < held_at:
            counters["stale_dropped"] += 1
            continue

        # Not max(). Anything that reaches this line is at or past the mark, because the
        # guard dropped everything below it, and with the guard off the mark is never read
        # for a decision. A max() here survives every mutation because it cannot do
        # anything, which is a good sign it should not be here.
        watermark[slot] = e.lsn

        if e.op == "d":
            counters["deletes"] += 1
            if slot not in rows:
                counters["deletes_of_a_row_not_held"] += 1
            rows.pop(slot, None)
            _forget(under_key, e, slot)
        elif e.op in ("c", "u"):
            if e.op == "c":
                counters["inserts"] += 1
            else:
                counters["updates"] += 1
                if slot not in rows:
                    # Under the default replica identity an update carries the whole new
                    # row, so this is recoverable rather than fatal. It is counted because
                    # a stream that starts mid life produces a lot of these.
                    counters["updates_of_a_row_not_held"] += 1
            rows[slot] = dict(e.after or {})
            under_key.setdefault((e.topic, e.key_bytes), set()).add(slot)
        else:
            raise ValueError("unknown op " + repr(e.op))

    return Result(rows, counters)


def _forget(under_key, event, slot):
    holder = under_key.get((event.topic, event.key_bytes))
    if holder is not None:
        holder.discard(slot)
        if not holder:
            del under_key[(event.topic, event.key_bytes)]


def as_row_set(rows, topic, columns):
    """The applied state of one topic as a set of tuples, for comparing against the source.

    Values are compared as text on both sides. The source read casts to text and the
    decoded stream is text already, so nothing here turns a string into a number and then
    argues with Postgres about how many decimal places that has.

    A row missing one of the columns is not skipped. It comes back with None in that
    position, so a consumer that lost a column shows up as a mismatch rather than as a row
    that quietly stopped being counted.
    """
    out = set()
    for slot, row in rows.items():
        if slot[0] != topic:
            continue
        out.add(tuple(None if row.get(c) is None else str(row[c]) for c in columns))
    return out


def compare(applied, truth):
    """Applied state against the source, reported as separate numbers.

    Missing and extra are different failures. A merge that loses rows and a merge that
    keeps dead ones both come back as 'does not match', and only one of them is the
    tombstone problem.
    """
    return {
        "source_rows": len(truth),
        "applied_rows": len(applied),
        "matching": len(truth & applied),
        "missing": len(truth - applied),
        "extra": len(applied - truth),
        "exact": truth == applied,
    }
