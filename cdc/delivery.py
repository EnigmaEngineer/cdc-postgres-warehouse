"""What a consumer really sees, which is not the log.

The log has one total order. A topic with six partitions has six orders and no relation
between them. A consumer reading all six sees some interleaving of the six, and which one
it sees changes every run.

So the ordering question is not whether the consumer applies changes in order. It is which
pairs of changes the broker has promised to keep in order, and that promise is exactly one
sentence long: two records with the same key land on the same partition, so they arrive in
the order they were produced. Everything else is arbitrary.

This module builds the arbitrary part on purpose, so the consumer is tested against the
guarantee it really has rather than against the log it will never see.
"""

import random

from cdc import topics


def route(events, partitions):
    """Where each record lands, using the Java producer's default partitioner."""
    if partitions < 1:
        raise ValueError("partitions must be at least 1, got {}".format(partitions))
    out = {p: [] for p in range(partitions)}
    for e in events:
        out[topics.partition_for(e.key_bytes, partitions)].append(e)
    return out


def interleave(partitioned, seed):
    """One arbitrary delivery order that still respects every per partition order.

    Picking uniformly among the partitions that still have records is the point. Draining
    partition 0 and then partition 1 is also a valid interleaving and it is a gentle one,
    because it puts every record of a key next to its neighbours.
    """
    rng = random.Random(seed)
    queues = {p: list(evs) for p, evs in partitioned.items() if evs}
    out = []
    while queues:
        p = rng.choice(sorted(queues))
        out.append(queues[p].pop(0))
        if not queues[p]:
            del queues[p]
    return out


def shuffle(events, seed):
    """No order at all. Not something Kafka does.

    This arm exists because a guard that is never exercised is a guard nobody has tested.
    Under per row keying the broker's promise already delivers every change to a row in
    order, so the ordering guard cannot fire and its count is zero by construction. That
    is a fact about the routing and not evidence that the guard works.
    """
    out = list(events)
    random.Random(seed).shuffle(out)
    return out


def compact(events):
    """What log compaction leaves behind.

    The last record for each key survives and keeps its place in the log. A key whose last
    record is a tombstone loses everything, which is the entire reason tombstones exist.

    Debezium topics are not compacted by default and this repo does not compact them. The
    function is here because the key decision gets argued on compaction grounds, and an
    argument about compaction is cheaper to run than to have.
    """
    last_at = {}
    for i, e in enumerate(events):
        last_at[(e.topic, e.key_bytes)] = i
    kept = sorted(last_at.values())
    return [events[i] for i in kept if not events[i].tombstone]


def _inversions(values):
    """Pairs out of order in a sequence, counted by merge sort rather than by two loops."""
    def sort(xs):
        if len(xs) < 2:
            return xs, 0
        mid = len(xs) // 2
        left, a = sort(xs[:mid])
        right, b = sort(xs[mid:])
        merged = []
        count = a + b
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                j += 1
                count += len(left) - i
        merged.extend(left[i:])
        merged.extend(right[j:])
        return merged, count

    return sort(list(values))[1]


def disorder(ordered):
    """How badly a delivery order departs from the log, split by whether it matters.

    An arm that comes back exact proves nothing if the order it was handed was the log's.
    So this is the check on the check.

    same_key_inversions is the number that carries the argument. Kafka promises that two
    records under one key arrive in the order they were produced, so an interleaving of
    partitions must show zero of these however scrambled the rest of it is. A shuffle shows
    plenty, which is what makes it an unfair arm and a useful one.
    """
    lsns = [e.lsn for e in ordered]
    per_key = {}
    for e in ordered:
        per_key.setdefault((e.topic, e.key_bytes), []).append(e.lsn)
    same_key = sum(_inversions(v) for v in per_key.values() if len(v) > 1)
    total = _inversions(lsns)
    return {
        "records": len(ordered),
        "inversions": total,
        "same_key_inversions": same_key,
        "cross_key_inversions": total - same_key,
    }


def partition_spread(partitioned):
    counts = {p: len(v) for p, v in partitioned.items()}
    total = sum(counts.values())
    return {
        "partitions": len(counts),
        "records": total,
        "per_partition": counts,
        "empty_partitions": sum(1 for n in counts.values() if n == 0),
    }
