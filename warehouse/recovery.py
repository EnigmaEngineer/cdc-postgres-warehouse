"""What a consumer does after it dies, and whether the thing it asks is a resume point.

The merge is idempotent, so replaying a batch is free. That makes recovery look like a
solved problem and it is not, because the consumer still has to decide *where* to start,
and the only durable thing it can ask is the ledger `apply_batch` writes inside its own
transaction.

## The ledger stores a batch number and a batch number is not an offset

A batch is a slice of whatever the consumer drained. Drain six partitions twice and you get
two different interleavings, so the eleventh batch of the second run is not the eleventh
batch of the first. Resuming at `last_applied + 1` is only correct when the batching is
reproducible, and nothing about a broker makes it so. `resume_gap` counts what falls
through when it is not.

The fix is not a smarter batch number. It is to ask the ledger a different question, or to
replay from the start, which the idempotency makes cheap. Both are arms in the probe.

## The alternative design is worse in a way that does not show up in a passing test

A consumer that merges and then commits its offset to a separate store has two pieces of
state and no transaction over them. `SeparateOffsetStore` models both orderings.
Committing after the merge loses nothing and replays a batch. Committing before it loses
the batch permanently, and neither ordering fails on a run where nothing crashes.
"""

from warehouse import sql as W
from warehouse.merge import Crash  # noqa: F401  re-exported, the probes catch it from here

STRATEGIES = ("ledger", "replay_all")


def resume_point(warehouse, consumer="cdc"):
    """The last batch the warehouse itself says it applied. Zero when it has seen nothing.

    Reading this is the whole reason the ledger is written inside the merge transaction,
    and until now nothing read it. A column written by every batch and read by nobody is a
    claim about durability that has never been exercised.
    """
    held = warehouse.last_batch(consumer)
    return 0 if held is None else held["batch"]


def first_batch_to_apply(last_applied, strategy="ledger"):
    """Which batch index a restarted consumer starts at, under a named strategy.

    Split out from the driver because it is the decision, and a decision living inside a
    loop cannot be mutated for on its own.
    """
    if strategy not in STRATEGIES:
        raise ValueError("strategy must be one of {}, got {!r}".format(
            ", ".join(STRATEGIES), strategy))
    if last_applied < 0:
        raise ValueError("last applied batch cannot be negative, got {}".format(
            last_applied))
    return 1 if strategy == "replay_all" else last_applied + 1


def resume_gap(before, after, resume_at):
    """What a batch number resume covers when the batching moved under it.

    `before` is the batching the crashed run used and `after` is the batching the restarted
    run built. Both are lists of batches and a batch is a list of record ids. The restarted
    run applied `before[:resume_at - 1]` before it died and will now apply
    `after[resume_at - 1:]`.

    Every id is expected to appear once in each side. A record in neither the applied
    prefix nor the replayed suffix is lost, and one in both is merged twice, which the
    idempotency makes harmless and which is still worth counting because it is the price
    the safe strategy pays.

    **`dropped` and `applied_twice` are the same number whenever the two prefixes hold the
    same count of records**, which is what a fixed batch size gives you. Two sets of equal
    size drawn from one population overlap and miss by the same amount, so reporting both
    as if they were two observations is reporting one. They come apart the moment the
    restarted consumer batches at a different size, which is what a real one does, since it
    batches whatever the poll returned. `resume_gap_is_two_numbers` says which case a
    caller is in rather than leaving them to notice.
    """
    if resume_at < 1:
        raise ValueError("resume_at is a 1-based batch index, got {}".format(resume_at))

    applied = set()
    for batch in before[:resume_at - 1]:
        applied.update(batch)
    replayed = set()
    for batch in after[resume_at - 1:]:
        replayed.update(batch)

    every = set()
    for batch in after:
        every.update(batch)
    # `before` and `after` are two orderings of one stream, so a record in one and not the
    # other is a caller error rather than a finding. Saying so here keeps the counts below
    # from quietly absorbing it.
    only_before = set()
    for batch in before:
        only_before.update(batch)
    if only_before != every:
        raise ValueError("the two batchings do not hold the same records: {} vs {}".format(
            len(only_before), len(every)))

    return {
        "records": len(every),
        "applied_before_the_crash": len(applied),
        "replayed_after_it": len(replayed),
        "dropped": len(every - applied - replayed),
        "applied_twice": len(applied & replayed),
        "prefix_sizes_agree": len(applied) + len(replayed) == len(every),
    }


def resume_gap_is_two_numbers(gap):
    """Whether `dropped` and `applied_twice` are independent on this pair of batchings.

    False means they are one number wearing two names and a report quoting both is quoting
    it twice. The condition is entirely about the sizes: when the applied prefix and the
    replayed suffix add up to the whole stream, every record missed forces a record
    doubled, and no property of the data can separate them.
    """
    return not gap["prefix_sizes_agree"]


class SeparateOffsetStore(object):
    """An offset kept outside the merge transaction, which is the usual arrangement.

    `commit_before=True` writes the offset and then merges. `commit_before=False` merges
    and then writes it. The second is the one people mean by at-least-once and it is
    correct here. The first is not, and the run where nothing crashes cannot tell them
    apart, which is why it survives into production.
    """

    def __init__(self, commit_before=False):
        self.commit_before = commit_before
        self.committed = 0

    def commit(self, batch):
        self.committed = batch


def drive(warehouse, batches, merge_keys, start_at=1, fail_at=None, fail_in_batch=None,
          consumer="cdc", offsets=None):
    """Run a consumer over a list of batches, optionally dying inside one of them.

    Returns what happened rather than raising, because every caller here wants to look at
    the warehouse afterwards. A `Crash` is the injected death and anything else is a real
    failure and is left to propagate.

    `offsets` is a `SeparateOffsetStore` when the arm is modelling the alternative design.
    Without it the ledger is the only offset, which is this project's position.
    """
    if start_at < 1:
        raise ValueError("start_at is a 1-based batch index, got {}".format(start_at))
    if (fail_at is None) != (fail_in_batch is None):
        raise ValueError("fail_at and fail_in_batch are set together or not at all")

    applied = []
    crashed = None
    for i, batch in enumerate(batches, start=1):
        if i < start_at:
            continue
        point = fail_at if i == fail_in_batch else None
        if offsets is not None and offsets.commit_before:
            offsets.commit(i)
        try:
            warehouse.apply_batch(batch, i, merge_keys=merge_keys, fail_at=point)
        except Crash as exc:
            crashed = {"batch": exc.batch, "point": exc.point}
            break
        applied.append(i)
        if offsets is not None and not offsets.commit_before:
            offsets.commit(i)

    return {
        "batches_applied": applied,
        "last_applied": applied[-1] if applied else 0,
        "crashed": crashed,
        "ledger_says": resume_point(warehouse, consumer),
        "offset_store_says": None if offsets is None else offsets.committed,
    }


def ledger_agrees_with_the_data(warehouse, consumer="cdc"):
    """Whether the ledger names a batch the target tables can show rows for.

    The claim the in-transaction ledger exists to make. A batch id in the ledger with no
    row anywhere carrying it means the two committed apart, which is the failure the whole
    design is arranged to prevent, so it gets a check rather than a sentence.

    A batch whose every row was later overwritten by a higher one leaves no trace, so the
    absence of rows at exactly the last batch is not evidence on its own. What is checkable
    is the other direction: no row may carry a batch id the ledger has never reached.
    """
    said = resume_point(warehouse, consumer)
    highest = 0
    for target in warehouse.targets:
        got = warehouse.con.execute("select max({}) from {}".format(
            W.quote(W.BATCH), W.quote(target.name))).fetchone()[0]
        if got is not None:
            highest = max(highest, got)
    return {
        "ledger_batch": said,
        "highest_batch_in_the_data": highest,
        "data_ahead_of_the_ledger": highest > said,
    }
