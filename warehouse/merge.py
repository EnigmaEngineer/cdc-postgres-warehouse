"""Apply a batch of change records to a warehouse table with a real MERGE.

`cdc/consumer.py` does this in memory and is the reference. This does it in SQL, against a
database, and is graded against that reference on every arm. Two implementations of one
rule that only ever meet in a summary line agree by accident, so they meet in a comparison
instead.

## The reduction is not an optimisation

A batch drained off six partitions holds several changes to one row. A MERGE joins the
staged batch to the target, and if two staged rows match one target row the statement has
two answers and no rule for choosing. What each engine does then is its own business.

    duckdb 1.5.5    takes the first matching source row and drops the rest, silently,
                    on the matched branch. On the not-matched branch the same input
                    raises a primary key violation instead.
    postgres 14.24  refuses `on conflict do update` outright with "cannot affect row a
                    second time"

Both measured here today. So the batch is reduced to one row per merge key before the
statement runs, and `reduce=False` exists to show what the alternative costs rather than
to be used.

## The delete is soft because the ordering guard needs somewhere to live

The guard compares the incoming log position against the position the target row already
holds. A hard delete removes the row and the position with it, so a delayed insert that
predates the delete finds nothing to compare against and puts the row back. The soft delete
is not a data retention feature here. It is the storage for the high water mark.
"""

import hashlib

from cdc import topics
from cdc.consumer import identity
from load import workload
from warehouse import sql as W

# The topic prefix every probe uses. It is the `topic.prefix` the generated Debezium config
# carries, so changing it here changes what the consumer is fed and what the config says.
TOPIC_PREFIX = "shopcdc"

# The columns a source row is compared on. Not every column in the table: `created_at` on
# order_header moves under the load generator and is not part of what a merge has to get
# right, so it is left out of the comparison rather than out of the schema.
COMPARED_COLUMNS = {
    "customer": ("customer_id", "email", "full_name", "country", "created_at", "updated_at"),
    "product": ("product_id", "sku", "name", "price_cents", "active", "updated_at"),
    "order_header": ("order_id", "customer_id", "status", "total_cents", "placed_at",
                     "updated_at"),
    "order_item": ("order_id", "line_no", "product_id", "qty", "unit_cents"),
}

PRIMARY_KEY_COLUMNS = {
    "customer": ("customer_id",),
    "product": ("product_id",),
    "order_header": ("order_id",),
    "order_item": ("order_id", "line_no"),
}


# The three moments inside `apply_batch`'s transaction where a consumer process can die.
# In statement order. `tombstoned` is the interesting one: everything the batch does to the
# data has happened and the ledger row has not been written, which is the state a consumer
# keeping its offset outside the transaction would be in permanently.
FAILURE_POINTS = ("staged", "merged", "tombstoned")


class Crash(Exception):
    """An injected process death. Not a database error and not caught like one.

    It is a distinct type on purpose. A blanket except in `merge_probe` once turned a
    parser error into eight arms quietly reporting `refused`, and a chaos arm that swallows
    a real duckdb failure while claiming to have injected one is the same defect with a
    worse story.
    """

    def __init__(self, point, batch):
        Exception.__init__(self, "crash at {} in batch {}".format(point, batch))
        self.point = point
        self.batch = batch


class Target(object):
    """One source table and the warehouse table it lands in."""

    def __init__(self, name, source_table, topic, columns, key_columns):
        missing = [c for c in key_columns if c not in columns]
        if missing:
            raise ValueError("key column not in columns: " + ", ".join(missing))
        self.name = name
        self.source_table = source_table
        self.topic = topic
        self.columns = tuple(columns)
        self.key_columns = tuple(key_columns)

    @property
    def stage(self):
        return self.name + "_stage"


def build_targets(prefix=TOPIC_PREFIX):
    """The four targets, built once, here.

    This used to live in `scripts/merge_probe.py` and three other scripts imported it from
    there. A script importing a script is worse than either importing a library, and the
    alternative that was avoided at the time was a fifth copy of the column lists. Four
    copies of the plumbing around it did exist, and their bodies agreed by luck rather than
    because anything compared them.
    """
    out = []
    for table in workload.TABLES:
        out.append(Target(
            name="wh_" + table,
            source_table="shop." + table,
            topic=topics.topic_name(prefix, "shop", table),
            columns=COMPARED_COLUMNS[table],
            key_columns=PRIMARY_KEY_COLUMNS[table],
        ))
    return out


def reduce_batch(events, targets_by_topic, merge_keys=None, versioned=False):
    """Collapse a delivery order into at most one staged row per merge key.

    Highest log position wins. A tie at one position is broken by delivery order, which is
    what the in-memory consumer does, and the tie count is reported rather than assumed to
    be zero.

    merge_keys maps a source table to the columns that identify a row. None keys the target
    by the record key, which is the shortcut that merges several source rows into one when
    the record key is wider than the primary key.

    versioned has to be threaded through here and not only into the statement. The
    reduction collapses on whatever the merge key is, so reducing on the primary key and
    then merging on the primary key plus the position gives one row per key per batch,
    which is neither of the two things anybody wanted. The first build did exactly that
    and the arm looked far more reasonable than the design it was supposed to indict.
    """
    rows = {}
    tombs = {}
    counters = {
        "records": 0, "tombstones": 0, "superseded": 0, "same_position": 0,
        "changes_with_no_merge_key": 0, "records_off_a_known_topic": 0,
    }

    for e in events:
        counters["records"] += 1
        if e.topic not in targets_by_topic:
            counters["records_off_a_known_topic"] += 1
            continue
        target = targets_by_topic[e.topic]

        if e.tombstone:
            counters["tombstones"] += 1
            held = tombs.get((e.topic, e.key_bytes))
            if held is None or e.lsn > held:
                tombs[(e.topic, e.key_bytes)] = e.lsn
            continue

        image = e.after if e.op != "d" else e.before
        if merge_keys is None:
            slot = (e.topic, e.key_bytes)
        else:
            ident = identity(image, merge_keys[e.table])
            if ident is None:
                counters["changes_with_no_merge_key"] += 1
                continue
            slot = (e.topic, ident)
        if versioned:
            slot = slot + (e.lsn,)

        held = rows.get(slot)
        if held is not None:
            if e.lsn < held["lsn"]:
                counters["superseded"] += 1
                continue
            if e.lsn == held["lsn"]:
                counters["same_position"] += 1
            counters["superseded"] += 1

        values = [None if image is None else image.get(c) for c in target.columns]
        rows[slot] = {
            "target": target,
            "record_key": e.key_bytes,
            "lsn": e.lsn,
            "deleted": e.op == "d",
            "values": values,
        }

    counters["staged_rows"] = len(rows)
    counters["tombstoned_keys"] = len(tombs)
    return rows, tombs, counters


class Warehouse(object):
    """A duckdb connection carrying the target tables and the applied batch ledger.

    dialect is here so the statement text can be generated for Snowflake and read, not so
    this class can talk to Snowflake. It cannot. Nothing in this repo has.
    """

    def __init__(self, con, targets, dialect="duckdb", versioned=False):
        self.con = con
        self.targets = list(targets)
        self.dialect = dialect
        self.versioned = versioned
        self.by_topic = {t.topic: t for t in self.targets}

    def create(self):
        for t in self.targets:
            self.con.execute(W.target_ddl(self.dialect, t.name, t.columns,
                                          t.key_columns, versioned=self.versioned))
            self.con.execute(W.stage_ddl(self.dialect, t.stage, t.columns))
        self.con.execute(W.ledger_ddl(self.dialect))

    def apply_batch(self, events, batch, merge_keys=None, on_tombstone="delete",
                    guard=True, reduce=True, consumer="cdc", fail_at=None):
        """One batch, one transaction, and the batch id written inside it.

        The ledger write is the point of the transaction. A consumer that merges and then
        commits its offset separately has two pieces of state in two systems and no way to
        find out they disagree. Here the warehouse can be asked what it last applied and
        the answer cannot name a batch whose rows are not in the table beside it.

        fail_at names a point inside the transaction and raises there. A consumer process
        can die at any of them and the three are not equivalent, so a chaos arm has to be
        able to pick one. It lives here rather than in a probe because the points are
        properties of this function, and a list of them kept somewhere else goes stale the
        first time the order of the statements changes.
        """
        if on_tombstone not in ("delete", "ignore"):
            raise ValueError("on_tombstone must be delete or ignore, got "
                             + repr(on_tombstone))
        if fail_at is not None and fail_at not in FAILURE_POINTS:
            raise ValueError("fail_at must be one of {}, got {!r}".format(
                ", ".join(FAILURE_POINTS), fail_at))

        if reduce:
            rows, tombs, counters = reduce_batch(events, self.by_topic, merge_keys,
                                                 versioned=self.versioned)
            staged = list(rows.values())
        else:
            staged, tombs, counters = _stage_unreduced(events, self.by_topic, merge_keys)

        self.con.begin()
        try:
            for t in self.targets:
                self.con.execute("delete from {}".format(W.quote(t.stage)))
            for row in staged:
                t = row["target"]
                placeholders = ", ".join("?" * (len(t.columns) + 3))
                self.con.execute(
                    "insert into {} values ({})".format(W.quote(t.stage), placeholders),
                    list(row["values"]) + [row["record_key"], row["lsn"], row["deleted"]])

            if fail_at == "staged":
                raise Crash("staged", batch)

            for t in self.targets:
                self.con.execute(W.merge_sql(self.dialect, t.name, t.stage, t.columns,
                                             t.key_columns, batch, guard=guard,
                                             versioned=self.versioned))

            if fail_at == "merged":
                raise Crash("merged", batch)

            removed = 0
            if on_tombstone == "delete":
                for (topic, record_key), lsn in sorted(tombs.items()):
                    t = self.by_topic[topic]
                    before = self._live_count(t.name)
                    self.con.execute(W.tombstone_sql(self.dialect, t.name, batch),
                                     [record_key, lsn])
                    removed += before - self._live_count(t.name)

            if fail_at == "tombstoned":
                raise Crash("tombstoned", batch)

            self.con.execute(W.record_batch_sql(self.dialect),
                             [consumer, batch, counters["records"]])
            self.con.commit()
        except Exception:
            self.con.rollback()
            raise

        counters["rows_removed_by_tombstones"] = removed
        return counters

    def _live_count(self, name):
        return self.con.execute("select count(*) from {} where {} = false".format(
            W.quote(name), W.quote(W.DELETED))).fetchone()[0]

    def live_rows(self, target_name, columns):
        """Live rows of one target as a set of text tuples, for the source comparison."""
        select = ", ".join(W.quote(c) for c in columns)
        got = self.con.execute("select {} from {} where {} = false".format(
            select, W.quote(target_name), W.quote(W.DELETED))).fetchall()
        return {tuple(None if v is None else str(v) for v in r) for r in got}

    def last_batch(self, consumer="cdc"):
        got = self.con.execute(
            "select batch, records_in_batch from {} where consumer = ?".format(W.quote(W.LEDGER)),
            [consumer]).fetchall()
        return None if not got else {"batch": got[0][0], "records_in_batch": got[0][1]}

    def counts(self):
        out = {}
        for t in self.targets:
            total, live = self.con.execute(
                "select count(*), sum(case when {d} then 0 else 1 end) from {n}".format(
                    d=W.quote(W.DELETED), n=W.quote(t.name))).fetchone()
            out[t.name] = {"rows": total, "live": live or 0, "soft_deleted": total - (live or 0)}
        return out

    def fingerprint(self, include_batch=True):
        """A hash of the whole warehouse, ordered so two warehouses can be compared.

        include_batch=False drops the batch id column, which is what makes two different
        batch sizes comparable. With it in, the same batches replayed have to produce the
        identical string or the merge is not idempotent.

        **This does not survive between runs and it is not supposed to.** The source rows
        carry `now()` timestamps, so a second workload at the same seed produces the same
        counts and different data. Two runs of the probe differ on the fingerprints and on
        nothing else, which was checked with `diff` rather than assumed. Callers report it
        under a name saying so, because a hash presented as a reproducibility check that
        changes every run teaches a reader to ignore it.
        """
        digest = hashlib.md5()
        for t in sorted(self.targets, key=lambda x: x.name):
            cols = list(t.columns) + [W.RECORD_KEY, W.LSN, W.DELETED]
            if include_batch:
                cols.append(W.BATCH)
            select = ", ".join(W.quote(c) for c in cols)
            order = ", ".join(str(i + 1) for i in range(len(cols)))
            got = self.con.execute("select {} from {} order by {}".format(
                select, W.quote(t.name), order)).fetchall()
            digest.update(t.name.encode("utf-8"))
            for row in got:
                digest.update(repr(row).encode("utf-8"))
        return digest.hexdigest()


def _stage_unreduced(events, targets_by_topic, merge_keys):
    """Every change record staged as it arrived, with nothing collapsed.

    The arm that shows what the reduction is worth. Kept next to the reduction rather than
    in a script, because what happens here is a property of the merge and the probe should
    not be the only thing that knows it.
    """
    staged = []
    tombs = {}
    counters = {
        "records": 0, "tombstones": 0, "superseded": 0, "same_position": 0,
        "changes_with_no_merge_key": 0, "records_off_a_known_topic": 0,
    }
    for e in events:
        counters["records"] += 1
        if e.topic not in targets_by_topic:
            counters["records_off_a_known_topic"] += 1
            continue
        if e.tombstone:
            counters["tombstones"] += 1
            held = tombs.get((e.topic, e.key_bytes))
            if held is None or e.lsn > held:
                tombs[(e.topic, e.key_bytes)] = e.lsn
            continue
        target = targets_by_topic[e.topic]
        image = e.after if e.op != "d" else e.before
        if merge_keys is not None and identity(image, merge_keys[e.table]) is None:
            counters["changes_with_no_merge_key"] += 1
            continue
        staged.append({
            "target": target,
            "record_key": e.key_bytes,
            "lsn": e.lsn,
            "deleted": e.op == "d",
            "values": [None if image is None else image.get(c) for c in target.columns],
        })
    counters["staged_rows"] = len(staged)
    counters["tombstoned_keys"] = len(tombs)
    return staged, tombs, counters


def engine_duplicate_source_behaviour(con):
    """What this engine does when two staged rows match one target row.

    The measurement the reduction exists because of. It lives here rather than in a report
    script because it decides a design, and a decision rule that only a script knows about
    is one no mutant can reach.

    Two staged rows, one target row, positions 20 and 30 against a target at 10. A merge
    with a rule for choosing would take 30 both times. What comes back instead is whichever
    row was inserted into the stage first, so the answer is a property of insertion order
    and nothing else.

    The same duplicate pair against an empty target goes down the not-matched branch and
    raises. So the defect is loud on one branch and silent on the other, which is the worst
    available arrangement, because the loud branch is the one people test.
    """
    out = {}
    for name, staged in (("stage_ascending", [(1, "b", 20), (1, "c", 30)]),
                         ("stage_descending", [(1, "c", 30), (1, "b", 20)])):
        con.execute("create or replace table probe_target "
                    "(k bigint primary key, v varchar, lsn bigint)")
        con.execute("create or replace table probe_stage (k bigint, v varchar, lsn bigint)")
        con.execute("insert into probe_target values (1, 'a', 10)")
        for row in staged:
            con.execute("insert into probe_stage values (?, ?, ?)", list(row))
        con.execute(
            "merge into probe_target t using probe_stage s on t.k = s.k "
            "when matched and s.lsn > t.lsn then update set v = s.v, lsn = s.lsn "
            "when not matched then insert (k, v, lsn) values (s.k, s.v, s.lsn)")
        kept = con.execute("select v, lsn from probe_target").fetchall()
        out[name] = {"rows": len(kept), "kept": kept[0][0], "at_position": kept[0][1],
                     "highest_offered": max(r[2] for r in staged)}

    con.execute("create or replace table probe_target "
                "(k bigint primary key, v varchar, lsn bigint)")
    con.execute("create or replace table probe_stage (k bigint, v varchar, lsn bigint)")
    for row in [(1, "b", 20), (1, "c", 30)]:
        con.execute("insert into probe_stage values (?, ?, ?)", list(row))
    try:
        con.execute(
            "merge into probe_target t using probe_stage s on t.k = s.k "
            "when matched and s.lsn > t.lsn then update set v = s.v, lsn = s.lsn "
            "when not matched then insert (k, v, lsn) values (s.k, s.v, s.lsn)")
        out["empty_target"] = {"raised": None,
                               "rows": con.execute(
                                   "select count(*) from probe_target").fetchone()[0]}
    except Exception as exc:
        out["empty_target"] = {"raised": type(exc).__name__, "rows": 0}

    con.execute("drop table probe_target")
    con.execute("drop table probe_stage")
    out["order_decides_the_answer"] = (
        out["stage_ascending"]["at_position"] != out["stage_descending"]["at_position"])
    return out


def batches(events, size):
    """Split a delivery order into batches of at most size records.

    A batch boundary is a real thing, not a formatting choice. Two changes to one row in
    one batch meet in the reduction. The same two split across a boundary meet in the
    merge's guard instead. Both paths have to give the same answer and the probe runs
    several sizes to check that they do.
    """
    if size < 1:
        raise ValueError("batch size must be at least 1, got {}".format(size))
    return [events[i:i + size] for i in range(0, len(events), size)]
