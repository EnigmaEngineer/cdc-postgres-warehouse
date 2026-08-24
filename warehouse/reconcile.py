"""Compare the source tables against the warehouse, one key at a time.

`cdc.consumer.compare` already answers whether two row sets are equal, and it does it by
subtracting one set of whole tuples from the other. That is the right answer to the
question it was asked and it is the wrong shape for a reconciliation report. A row whose
`status` moved from `paid` to `shipped` is one row that drifted. Set subtraction sees a
tuple in the source that is not in the target and a tuple in the target that is not in the
source, so it reports one missing row and one extra row. The count doubles and neither
half names what happened.

So this indexes both sides by the key columns first. Missing, extra and differing are then
three different findings rather than two of them wearing each other's clothes, and a
differing row can say which column moved.

## The part that matters more than the comparison

A reconciliation that compares a live source against a target fed by a log is comparing
two moments. Everything sitting in the log unapplied shows up as drift. It is not drift.
It is lag, and a report that cannot tell them apart wakes someone up every night until
they stop reading it.

The way out is to exclude any key with a change still in flight. That works, and it is not
free, and the price is the thing this module is careful to publish. A key excluded because
it is moving is a key nobody checked. Under lag the reconciliation reports clean over a
shrinking slice of the table, and if the slice is small enough then clean means nothing at
all. `coverage` goes out beside every verdict for that reason, and a report that checked
no keys returns `no_coverage` rather than `clean`.
"""

from cdc.consumer import identity


def read_source(cur, target, columns):
    """The live source rows behind one target, as text tuples.

    Reads through `target.source_table`, which is why a Target carries the name of the
    table it came from. Casting to text on this side matches the decoded change stream,
    which is text already, so nothing here turns a string into a number and then argues
    with Postgres about trailing zeros.
    """
    select = ", ".join("{}::text".format(c) for c in columns)
    cur.execute("select {} from {}".format(select, target.source_table))
    return {tuple(r) for r in cur.fetchall()}


def index_by_key(rows, columns, key_columns):
    """Group text tuples by their key columns.

    Returns key tuple to list of rows. A list rather than a single row because two live
    rows under one key is a finding, and a dict would quietly keep the last one.
    """
    positions = [columns.index(c) for c in key_columns]
    out = {}
    for row in rows:
        key = tuple(row[p] for p in positions)
        out.setdefault(key, []).append(row)
    return out


def keys_in_flight(events, key_columns_by_table):
    """The keys named by changes that have not been applied yet.

    Derived through `consumer.identity`, the same function the merge keys rows with, so
    the exclusion set and the merge cannot disagree about what identifies a row.

    A tombstone carries no row image, so it names no key here. That is a real gap and not
    an oversight. Under the default Debezium key a tombstone follows its own delete
    envelope, which does carry the before image, so the key is already in the set. Under a
    widened key it is not, and this is one of the places that costs something.

    The two counts come back separately and that is not tidiness. A tombstone naming no
    key is how tombstones work. A change record whose image does not carry the key columns
    is a defect in the decoder or the replica identity, and it is the one somebody needs
    to hear about. Folded into one number the second hides behind the first forever,
    because there are hundreds of the first.
    """
    out = {}
    counts = {"tombstones": 0, "changes_with_no_key": 0}
    for e in events:
        if e.tombstone:
            counts["tombstones"] += 1
            continue
        image = e.after if e.op != "d" else e.before
        ident = identity(image, key_columns_by_table[e.table])
        if ident is None:
            counts["changes_with_no_key"] += 1
            continue
        out.setdefault(e.table, set()).add(
            tuple(None if v is None else str(v) for v in ident))
    return out, counts


def reconcile(source, applied, columns, key_columns, in_flight=frozenset()):
    """One source table against one warehouse table, keyed.

    source and applied are sets of text tuples over the same `columns`, which is what
    `read_source` and `Warehouse.live_rows` already hand back. in_flight is a set of key
    tuples to leave out of the verdict.

    `coverage` is checked keys over every key either side holds. It is not a quality
    score. It is how much of the table the verdict is about, and a caller that prints the
    verdict without it is publishing a sentence with the subject removed.
    """
    src = index_by_key(source, columns, key_columns)
    tgt = index_by_key(applied, columns, key_columns)

    every_key = set(src) | set(tgt)
    excluded = {k for k in every_key if k in in_flight}
    checked = every_key - excluded

    report = {
        "source_rows": len(source),
        "applied_rows": len(applied),
        "keys_either_side": len(every_key),
        "checked": len(checked),
        "excluded_in_flight": len(excluded),
        "matching": 0,
        "missing": 0,
        "extra": 0,
        "differing": 0,
        "duplicate_keys_in_source": 0,
        "duplicate_keys_in_target": 0,
        "columns_differing": {},
    }

    for key in checked:
        left = src.get(key, [])
        right = tgt.get(key, [])
        if len(left) > 1:
            report["duplicate_keys_in_source"] += 1
        if len(right) > 1:
            report["duplicate_keys_in_target"] += 1
        if not right:
            report["missing"] += 1
            continue
        if not left:
            report["extra"] += 1
            continue
        # Sorted so a duplicated key compares its rows in a fixed order rather than in
        # whatever order the set iterated. Duplicates are already counted above.
        if sorted(left) == sorted(right):
            report["matching"] += 1
            continue
        report["differing"] += 1
        for column in _columns_that_moved(sorted(left)[0], sorted(right)[0], columns):
            report["columns_differing"][column] = \
                report["columns_differing"].get(column, 0) + 1

    report["coverage"] = (None if not every_key
                          else round(len(checked) / len(every_key), 4))
    report["verdict"] = verdict(report)
    return report


def _columns_that_moved(left, right, columns):
    """Which columns of two rows under one key disagree.

    This is the whole reason for keying the comparison. Knowing that 812 rows drifted is
    a page. Knowing that all 812 of them drifted on `status` and nothing else is a cause.
    """
    return [c for i, c in enumerate(columns) if left[i] != right[i]]


def verdict(report):
    """One of four answers. Drift beats no coverage beats a partial pass beats a pass.

    The first version of this had three verdicts and `no_coverage` fired only when the
    comparison checked exactly zero keys. That is the shape of guard this repo keeps
    finding: it is arithmetically incapable of firing on the case it exists for. Zero
    checked keys means every key in the table has a change in flight, which does not
    happen. What does happen is the run below, where holding back half a change stream
    left 20.4 percent of keys checkable and the report came back `clean`, and holding back
    three quarters left 7.7 percent and it still came back `clean`. The verdict that was
    supposed to stop a pass being believed over a slice of the table did not fire once.

    So a pass over part of the table is its own answer. The boundary is coverage below
    1.0, which is not a threshold anybody picked. A caller who genuinely does not care can
    treat `clean_partial` as a pass, and they have to write that down.
    """
    bad = (report["missing"] + report["extra"] + report["differing"]
           + report["duplicate_keys_in_target"])
    # These two are in an order and the order is not a rule. Every one of the four counts
    # is incremented inside the loop over the checked keys, so `bad` cannot be non zero
    # when `checked` is zero. Swapping the next four lines changes no reachable answer,
    # and a mutant that swaps them survives the suite for that reason rather than for a
    # missing test. Nothing here is worth pinning with a hand built report that the
    # comparison cannot produce.
    if bad:
        return "drift"
    if report["checked"] == 0:
        return "no_coverage"
    return "clean" if report["coverage"] == 1.0 else "clean_partial"


def run(cur, warehouse, compared_columns, key_columns, in_flight_by_table=None):
    """Reconcile every target in a warehouse against its source table.

    The job as a job. It reads the source through each Target, reads the target through
    `Warehouse.live_rows`, and rolls the per-table verdicts up into one. The rollup takes
    the worst verdict present rather than a majority, because a reconciliation that says
    clean while one of four tables has drifted is worse than no reconciliation.
    """
    in_flight_by_table = in_flight_by_table or {}
    tables = {}
    for target in warehouse.targets:
        source_table = target.source_table.split(".")[-1]
        columns = compared_columns[source_table]
        tables[source_table] = reconcile(
            read_source(cur, target, columns),
            warehouse.live_rows(target.name, columns),
            columns,
            key_columns[source_table],
            in_flight_by_table.get(source_table, frozenset()))
    return {"tables": tables, "verdict": rollup(tables)}


# Worst first. A rollup over four tables reports the worst verdict any of them holds,
# because a reconciliation that says clean while one table in four has drifted is worse
# than no reconciliation at all.
ORDER = ("drift", "no_coverage", "clean_partial", "clean")


def rollup(tables):
    if not tables:
        return "no_coverage"
    for name in ORDER:
        if any(t["verdict"] == name for t in tables.values()):
            return name
    raise ValueError("unknown verdict in " + repr(sorted(
        {t["verdict"] for t in tables.values()})))
