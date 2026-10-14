# Design notes

Moved out of the README so the front page stays short. Back to the [README](../README.md).

## The topic layout, and why it is one connector

Debezium puts one table on one topic and the obvious next thought is to run one connector
per table, so a slow table cannot hold up a fast one. On the Kafka side that is fine. On
the Postgres side it is not, and the reason is that a `pgoutput` stream carries a frame
per transaction boundary as well as a frame per row change, and the boundary frames are
not filtered by the publication.

One workload from an empty schema, 280 dimension rows and then 600 operations. One slot on
the four table publication against four slots each on a one table publication, all five
reading the same log.

```
python -m scripts.topic_layout_probe --steps 600 --seed 11 --passes 1 --markdown
```

| | frames | of which framing | of which row changes | row change share |
|---|---|---|---|---|
| one connector, 4 tables | 2,652 | 1,772 | 880 | 0.3318 |
| `shop.customer` alone | 2,026 | 1,767 | 259 | 0.1278 |
| `shop.product` alone | 1,910 | 1,765 | 145 | 0.0759 |
| `shop.order_header` alone | 1,962 | 1,763 | 199 | 0.1014 |
| `shop.order_item` alone | 2,038 | 1,761 | 277 | 0.1359 |
| **four connectors, totalled** | **7,936** | **7,056** | **880** | 0.1109 |

Splitting four ways delivers exactly the same 880 row changes and reads 3.98 times the
transaction framing to do it. Every connector sees a BEGIN and a COMMIT for every
transaction in the database, including the ones that touch nothing it publishes. The
`shop.product` connector reads 1,765 framing frames to deliver 145 row changes.

So the layout here is one connector on one slot reading one publication. Debezium routes
to four topics itself. That is also its default, and the point is that the default is now
a measurement rather than an assumption.

The counts reproduce exactly. Across three passes every frame count and the 3.9819
multiple had a range of zero. The stream byte total moved by 54 bytes at 0.045 percent and
the write ahead log volume by 208 bytes at 0.089 percent. Those are three kinds of
quantity and only the first is allowed to carry a claim without its spread beside it.

### The key is where the ordering story breaks

Debezium keys a record by the table's primary key, and `shop.order_item` has a composite
one. So the lines of an order are hashed independently and land wherever they land.

```
python -m scripts.topic_layout_probe --steps 4000 --seed 11 --passes 1
```

Measured over the 482 `order_item` rows that workload leaves behind, at 6 partitions,
using the Kafka Java client's own partitioner. Of 296 orders, 122 had more than one line,
and **104 of those 122 were spread across more than one partition**. That is 85.2 percent.
Setting `message.key.columns` to `shop.order_item:order_id` takes it to 0 of 122.

Orders with a single line are excluded from the denominator, because a group of one row
cannot split and counting it would report a smaller rate for a reason that has nothing to
do with partitioning.

That is not a free fix. Keying on `order_id` alone means two lines of one order share a
record key, and a tombstone names a record key. The next section runs both keys through the
consumer, and the 85.2 percent turns out to cost nothing.

The partitioner is a port. `scripts/dump_connector_configdef.sh` regenerates the vectors
in `tests/test_topics.py` from `org.apache.kafka.common.utils.Utils.murmur2` directly,
and 220 keys were compared against the real client with no mismatches.

## The consumer, and the ordering it actually needs

The 85.2 percent looks like a problem. It is not one, and the reason is that a merge and a
broker want different things.

A merge needs two changes to one row to arrive in the order they happened. Kafka promises
exactly that and nothing more. Two records under one key land on one partition, so they
arrive in order. Records under different keys have no relation at all.

`shop.order_item`'s primary key is `(order_id, line_no)`, so every change to one line
already carries the same key. The merge has the guarantee it needs. Wanting the lines of an
order together is a different wish and the merge never had it.

So the choice gets measured. One change stream out of one slot, read eight ways, under both
keys.

```
python -m scripts.consumer_probe --steps 4000 --seed 11 --partitions 6 --markdown
```

4,280 row changes over 12,840 decoded lines, each one at its own log position. 1,626
inserts, 2,004 updates and 370 deletes on top of 280 seeded dimension rows. Every delete is
followed by a tombstone, so the topics carry 4,650 records. The source is left holding 482
`order_item` rows and every applied row is compared against it column by column, timestamps
included.

`row` key, 0 of 1777 record keys carry more than one row

| delivery | order_item rows | missing | extra | all four tables |
|---|---|---|---|---|
| `log_order` | 482 | 0 | 0 | exact |
| `interleaved` | 482 | 0 | 0 | exact |
| `interleaved_open` | 482 | 0 | 0 | exact |
| `compacted` | 482 | 0 | 0 | exact |
| `interleaved_tombstone_ignored` | 482 | 0 | 0 | exact |
| `interleaved_key_as_row` | 482 | 0 | 0 | exact |
| `shuffled` | 482 | 0 | 0 | exact |
| `shuffled_open` | 561 | 128 | 207 | **wrong** |

`order` key, 188 of 1415 record keys carry more than one row

| delivery | order_item rows | missing | extra | all four tables |
|---|---|---|---|---|
| `log_order` | 381 | 101 | 0 | **wrong** |
| `interleaved` | 381 | 101 | 0 | **wrong** |
| `interleaved_open` | 381 | 101 | 0 | **wrong** |
| `compacted` | 258 | 224 | 0 | **wrong** |
| `interleaved_tombstone_ignored` | 482 | 0 | 0 | exact |
| `interleaved_key_as_row` | 258 | 224 | 0 | **wrong** |
| `shuffled` | 418 | 64 | 0 | **wrong** |
| `shuffled_open` | 458 | 199 | 175 | **wrong** |

**Under the default key every delivery order Kafka can produce is exact.** All four tables,
every row, including the compacted arm. The 104 orders spread across partitions cost
nothing, because nothing downstream ever needed them together.

**Under `order_id` the fix loses 101 of 482 rows in perfect log order.** Not under an
awkward interleaving. In the log's own order. A tombstone carries a record key and no
value, so a tombstone for one deleted line names every line of that order, and 285 live
rows are removed by tombstones over the run.

Ignoring the tombstone recovers all 482. That arm is exact, and it is exact because the
delete envelope in front of the tombstone still carries `line_no` under the default replica
identity, so the consumer can name the row without help. Which leaves the position that
`order_id` keying really requires. Keep the co-location, throw away the tombstone, and
accept that a compacted topic then loses 224 of 482.

So it is not a tradeoff between per order ordering and per row compaction. Per order
ordering buys nothing the merge wanted, and it costs the mechanism that lets the topic
forget a deleted row. **The layout stays on Debezium's default key.**

### The record key is not the merge key

`interleaved_key_as_row` is the same consumer with one shortcut in it. Instead of merging
on the source primary key it keys its target by the record key, which is what everyone
writes first because Debezium makes the two the same by default.

Under the default key that shortcut is invisible. It scores 482 of 482. Move one table off
its primary key and it loses 224 rows, and the two lines it silently merged were never
deleted by anyone. `cdc/consumer.py` therefore takes the merge key as an argument and the
record key comes off the record.

### The ordering guard does no work here, and that is the finding

The consumer drops any change older than the position its key already holds. That guard
fires zero times.

| key | delivery | pairs out of order | under one key | dropped as stale |
|---|---|---|---|---|
| `row` | `log_order` | 0 | 0 | 0 |
| `row` | `interleaved` | 421,968 | 0 | 0 |
| `row` | `shuffled` | 5,455,467 | 3,472 | 1,606 |
| `order` | `log_order` | 0 | 0 | 0 |
| `order` | `interleaved` | 227,216 | 0 | 0 |
| `order` | `shuffled` | 5,455,467 | 8,261 | 1,606 |

The interleaved arm scrambles 421,968 pairs and not one of them is a pair under the same
key. That is the broker guarantee written as a number, and it is why the guard cannot fire.
A firing count of zero measured on that arm says nothing about whether the guard works.

The shuffle arm is the one that exercises it. It breaks 3,472 same key pairs, which Kafka
never does, and there the guard is worth 207 wrong rows. Removing it takes the target from
482 rows with none wrong to 561 rows with 207 that should not be there.

### A tombstone gets the same comparison as everything else

The shuffle row above used to read 445 and 37 missing. That was not the shuffle. The
tombstone path applied the record key with no position comparison at all, so a tombstone
delivered behind a newer change to its own row removed a row that had already moved past
it. Under `order_id` it cost 152 rows rather than 64.

Nothing found it for a day. What found it was the SQL merge, which gated the same operation
because a gate is the obvious thing to write in an `update ... where`, and then the two
implementations were graded against each other on one real stream. They came apart by 37
`order_item` rows. One of them had to be wrong and it was the older one.

The gate is off when the guard is off, because leaving one half gated is two answers to one
question inside one function.

I am keeping it. It costs one comparison per record and it is the difference between a
consumer that is correct and one that is correct as long as nobody widens a partition, adds
a second writer, or replays a topic out of a snapshot. But calling it tested by the
interleaved arms would be a claim about an arm that cannot fail.

## The merge, and the two things in it that are not what they look like

`warehouse/merge.py` takes a batch of change records and puts them into a warehouse table
with a real `MERGE`. `warehouse/sql.py` builds the statements. DuckDB is what runs here.
The Snowflake statements are generated and read and have never executed, and the notes in
that file say which of the two each claim came from.

```
python -m scripts.merge_probe --steps 4000 --seed 11 --batch 250 --markdown
```

Same workload, same slot, same delivery orders as the consumer measurement. 4,280 row
changes into four tables holding 1,536 live source rows.

| arm | rows | live | soft deleted | missing | extra | four tables |
|---|---|---|---|---|---|---|
| `log_order` | 1,777 | 1,536 | 241 | 0 | 0 | exact |
| `interleaved` | 1,777 | 1,536 | 241 | 0 | 0 | exact |
| `shuffled` | 1,777 | 1,536 | 241 | 0 | 0 | exact |
| `unreduced` | 0 | 0 | 0 | 1,536 | 0 | **refused** |
| `hard_delete` | 1,536 | 1,536 | 0 | 0 | 0 | exact |
| `hard_delete_shuffled` | 1,634 | 1,634 | 0 | 0 | 98 | **wrong** |
| `versioned` | 4,280 | 3,301 | 979 | 0 | 1,709 | **wrong** |
| `open_shuffled` | 1,777 | 1,611 | 166 | 537 | 612 | **wrong** |

The first three arms are also graded against `cdc/consumer.py` rather than only against the
source, row by row. Two implementations of one rule that meet in a summary line agree by
accident.

### The batch has to be collapsed before the statement runs

A batch drained off six partitions holds several changes to one row. A `MERGE` joins the
staged batch to the target, and two staged rows matching one target row is a question the
statement has no rule for. So the reduction in front of it keeps one row per merge key at
the highest log position, and `reduce=False` is there to show what that is worth.

What each engine does with the unreduced version is its own business, and the two available
here do not agree.

| engine | two staged rows, one target row |
|---|---|
| postgres 14.24 | `on conflict do update` refuses. `cannot affect row a second time` |
| duckdb 1.5.5 | takes one and drops the other, no error |

Which one duckdb takes is decided by insertion order into the stage. Offer it positions 20
then 30 against a target at 10 and it keeps 20. Offer the same two the other way round and
it keeps 30. Neither is the highest by rule. Both are the first row it happened to see.
`warehouse.merge.engine_duplicate_source_behaviour` is what measures that, and it lives in
the library rather than in a script so a mutation pass can reach it.

The same duplicate pair against an **empty** target goes down the not-matched branch and
raises a primary key violation instead. So the defect is loud on the branch people test and
silent on the branch that runs for the rest of the table's life. That is why the
`unreduced` arm above says refused rather than wrong. It died on its first batch, on the
loud branch, and the quiet branch is the one that would have cost something.

### The delete is soft because the guard needs somewhere to live

Every merged row carries the source log position it was last written at. The guard compares
an incoming position against that one. A hard delete removes the row and takes the position
with it, so a delayed insert that predates the delete finds nothing to compare against and
puts the row back.

`hard_delete` and `hard_delete_shuffled` are the same warehouse with the flagged rows really
deleted after each batch. On the interleaved arm it is exact, and it is exact for the same
reason the ordering guard fires zero times there. On the shuffle it resurrects 98
`order_item` rows.

So the soft delete costs 241 retained rows out of 1,777, and on every delivery order Kafka
can produce it buys nothing measurable. That is the third mechanism in this consumer whose
value cannot be shown on a fair arm. I am keeping it, and I am not going to pretend the
interleaved arms tested it.

Worth writing down separately: under Debezium's default key the tombstone removes **zero**
rows across the whole run. Its own delete envelope names the row and removes it first. Every
row a tombstone has ever removed in this repo is a sibling under a widened key that the
tombstone could not name.

### "Keyed on the primary key plus the LSN" is not a merge key

That phrasing is the obvious way to describe this and taken literally it builds a different
table. If the position joins the key then no incoming change ever matches an existing row,
every change inserts, and what comes out is a history. The `versioned` arm is exactly that
reading, and it lands 4,280 rows for 1,536 source rows, with 1,709 live rows that are not in
the source.

The merge key is the primary key. The position is a predicate on the update.

### The batch boundary does not decide the answer

Two changes to one row in one batch meet in the reduction. The same two split across a
boundary meet in the merge's guard instead. Two code paths, and the warehouse has to come
out the same either way.

| batch size | batches | collapsed inside a batch | warehouse |
|---|---|---|---|
| 1 | 4,650 | 0 | identical |
| 37 | 126 | 115 | identical |
| 250 | 19 | 483 | identical |
| 4,650 | 1 | 2,503 | identical |

Replaying the 19 batches into the same warehouse a second time leaves it byte identical,
batch column included.

### The batch id goes in inside the merge transaction

A consumer that merges and then commits its Kafka offset has two pieces of state in two
systems and nothing that makes them agree. `cdc_applied_batch` is written in the same
transaction as the rows, so the warehouse can be asked what it last applied and the answer
cannot name a batch whose rows are not sitting beside it. A batch that raises takes the
ledger row with it, which `tests/test_warehouse.py` shows using a failure duckdb really
produces rather than a mocked one.

## The reconciliation, and the number it has to publish about itself

`warehouse/reconcile.py` compares the source tables against the warehouse a key at a time.
`python3 -m scripts.reconcile_probe --steps 4000 --seed 11 --markdown` prints both tables
below. Postgres 14.24, duckdb 1.5.5, 4,650 records over four tables holding 1,536 live
source rows.

The easy version of this job is a set difference, and `cdc/consumer.py` already has one.
It answers whether two row sets are equal, which is the right answer to a different
question. A row whose `status` moved from `paid` to `shipped` is one row that drifted. Set
subtraction sees a tuple in the source the target does not have and a tuple in the target
the source does not have, so it counts two, and it cannot say which column moved.

| arm | keyed: missing | extra | differing | set subtraction reports |
|---|---|---|---|---|
| `quiesced` | 0 | 0 | 0 | 0 |
| `lag_25` | 402 | 77 | 342 | 1,163 |
| `lag_50` | 755 | 88 | 443 | 1,729 |
| `lag_75` | 1,063 | 55 | 349 | 1,816 |

821 rows are involved on the `lag_25` arm and the set difference calls it 1,163, which is
821 plus the 342 that drifted counted a second time. The keyed report also says the 342
broke down as 140 `updated_at`, 105 `status` and the rest across four other columns. That
is a cause. The other number is a page.

### Comparing a live source against a lagging target measures the lag

This is the part that decides whether the job is worth running. Everything sitting in the
log unapplied looks exactly like drift. On the `lag_25` arm, a quarter of the stream held
back, the reconciliation reports 821 mismatched rows and every single one of them is lag.
Nothing is wrong. Run that nightly and somebody stops reading it inside a week.

The way out is to leave out any key with a change still in flight. It works. It takes
every arm to zero. And the price is in the column nobody would think to print.

| arm | held back | reported drift, no exclusion | left after exclusion | keys checked | coverage | verdict |
|---|---|---|---|---|---|---|
| `quiesced` | 0 | 0 | 0 | 1,536 | 100.0% | `clean` |
| `lag_25` | 1,162 | 821 | 0 | 786 | 48.7% | `clean_partial` |
| `lag_50` | 2,325 | 1,286 | 0 | 332 | 20.4% | `clean_partial` |
| `lag_75` | 3,488 | 1,467 | 0 | 122 | 7.7% | `clean_partial` |

Coverage falls much faster than the lag does. Half the stream held back leaves 20.4 percent
of keys checkable, not half. The reason is that one held back record removes a whole key
from the report, and 4,650 records land on 1,536 keys, so a backlog names far more distinct
keys than its own size suggests. At `lag_50` the 2,325 held back records name 1,292 keys of
1,624. A busy table is exactly where this is worst.

So a reconciliation with an exclusion in it can report a clean run over 7.7 percent of the
table, and 7.7 percent is not printed anywhere unless somebody decides to print it.

### What the exclusion gives up, shown rather than argued

The probe takes the `lag_50` warehouse, picks a row whose key the exclusion is about to
hide, and moves one column that has nothing to do with the change in flight. It sets
`email` on `customer` 1 to a string no workload produces. Then it reconciles again.

    with the exclusion       clean_partial
    without the exclusion    drift

The corruption is real, the reconciliation is looking straight at it, and the exclusion
tells it not to. That is not a bug in the exclusion. It is what the exclusion is, and a
job that hides real damage on the rows most likely to be damaged should say so out loud
rather than in a footnote.

### Three verdicts were not enough, and the fourth is not a threshold

The first version had `clean`, `drift`, and `no_coverage` for a report that checked zero
keys. `no_coverage` never fired. It cannot, because zero checked keys means every key in
the table has a change in flight, and that does not happen. Meanwhile the run above came
back `clean` at 20.4 percent coverage and again at 7.7 percent, which is the exact failure
that verdict was invented to prevent, arriving one step to the left of where it was
watching.

A pass over part of the table is now its own answer. The boundary is coverage below 1.0,
which is a fact about the run rather than a number anybody chose. Sitting it in the gap
between 7.7 and 100 would have meant picking it off the arms it was about to judge.

### The two counts that had to stay apart

`keys_in_flight` reports tombstones and unkeyable changes separately. A tombstone carries
no row image, so it names no key, and there are 104 of them held back on `lag_25`. A change
record whose image does not carry the merge key columns is a defect in the replica identity
and somebody needs to hear about it. Folded into one counter the second hides behind the
first forever.

They were one counter until a mutation pass. A mutant deleting the tombstone branch
entirely survived the whole suite, because `identity` returns `None` on a tombstone's
absent image and the count landed in the same place either way, so the branch was doing
nothing at all. Splitting the counter is what gave it something to do. That mutant and two
more about the split now die.

## Killing the consumer, and the resume point that is not one

`scripts/chaos_probe.py` runs one change stream through a consumer that dies partway
through a batch, restarts it, and grades the warehouse against the source. The crash is
injected at three named points inside `apply_batch`'s transaction. After the staging. After
the merge. After the tombstones with the ledger write still to come.

Those three all pass, and that is the problem with them. The transaction holds, the ledger
sits at six, no row anywhere carries batch seven. Replay is exact because the merge is
idempotent, which was already true and already measured. A check that cannot fail is not
evidence, so the interesting arms are the ones nobody asked for.

| arm | resumed at | records dropped | rows missing | rows extra | source |
|---|---|---|---|---|---|
| `clean` | n/a | 0 | 0 | 0 | exact |
| `resume_same_batching` | 7 | 0 | 0 | 0 | exact |
| `resume_new_batching` | 7 | 30 | 6 | 5 | **wrong** |
| `resume_new_batching_resized` | 7 | 0 | 0 | 0 | exact |
| `resume_new_batching_all` | 1 | 0 | 0 | 0 | exact |
| `offset_after_merge` | 7 | 0 | 0 | 0 | exact |
| `offset_before_merge` | 8 | 0 | 40 | 34 | **wrong** |

### A batch number is not an offset

The ledger stores which batch was last applied. A batch is a slice of whatever the consumer
drained, and a restarted consumer re-drains six partitions and gets a different
interleaving, so its seventh batch is not the seventh batch that died. Resuming at
`last_applied + 1` is correct only when the batching reproduces, and nothing about a broker
makes it so.

Same records and the same per partition order. Only the arrival moves. Thirty records land in
neither the applied prefix nor the replayed suffix, and the warehouse ends six rows short
and five rows wrong against the source.

The damage is much smaller than the loss and that is what makes it dangerous. Thirty lost
records produce eleven wrong rows, because most of them were superseded by a later change
to the same key that did get replayed. A reconciliation run afterwards sees eleven rows and
a live source, and the section above says exactly how a lagging target explains eleven rows
away.

### The size of the restart batch decides whether it is harmless or catastrophic

The variable a consumer controls least is how many records the poll handed it. Below the
original size the resume point lands short of what was applied, the overlap gets merged
twice, and the idempotency absorbs it. Above it, everything between the two lands nowhere.

| restart batch size | records dropped | merged twice |
|---|---|---|
| 100 | 0 | 900 |
| 180 | 0 | 420 |
| 250 | 30 | 30 |
| 320 | 420 | 0 |
| 400 | 900 | 0 |
| 600 | 2,100 | 0 |

Away from 250 the loss is the size difference times the six batches already applied, exactly,
and the interleaving contributes nothing measurable on top. At 250 the size term is zero and
what is left is the interleaving alone, which ran 23 to 68 records over eight restart orders
and was never zero.

**The two columns at 250 are one number, not two.** Two prefixes of equal length drawn from
one stream miss and overlap by the same amount, so `dropped` and `applied_twice` are
arithmetically forced to agree there and a report quoting both is quoting one.
`resume_gap_is_two_numbers` says which case a reader is in. Every row where the sizes differ
is a genuine pair.

The fix is not a cleverer batch number. It is `replay_all`, which the idempotency makes
cheap, and the last row of the first table is what says it costs nothing here.

### The offset outside the transaction, in both orderings

`SeparateOffsetStore` models the arrangement this project did not choose. Committing the
offset after the merge is correct, replays one batch, and matches the clean warehouse
exactly. Committing it before loses forty rows and gains thirty four, permanently, because
the offset names a batch whose rows never landed and the consumer starts past it.

Neither ordering fails on a run where nothing crashes. `check_the_ledger_and_the_offset_store_agree_when_nothing_crashes`
asserts that they agree there, which is the point: the wrong one survives into production
because the test everyone writes cannot see it.

## Three defaults that are wrong here

`cdc/connector.py` refuses to emit a config that leaves any of these alone, and
`tests/test_connector.py` asserts that each one really is the shipped default by reading
`db/debezium-configdef.tsv`, which is dumped out of the connector jar. A note about a
default that has changed is worse than no note.

| property | ships as | measured |
|---|---|---|
| `plugin.name` | `decoderbufs` | not available. `pg_create_logical_replication_slot` returns `library "decoderbufs" may not be used as an output plugin`. `wal2json` fails the same way. `pgoutput` and `test_decoding` are accepted. |
| `publication.autocreate.mode` | `all_tables` | would replace an explicit four table publication with one covering the whole database, and needs ownership of every table in it |
| `slot.name` | `debezium` | one fixed string for every connector. A second connector gets `replication slot "debezium" already exists`. |

The first two are the ones that matter. A misspelled property is accepted and ignored by
Kafka Connect, so the validator checks every key against the 100 the connector really
declares, and the first check in the suite is a list of legitimate properties it must not
refuse.

## The first thing worth knowing

A published table's replica identity decides what an update and a delete carry. Everyone
knows that. What is easy to get wrong is what each setting costs, and the usual way to
measure that cost is the wrong one.

Measured with `python -m scripts.replica_identity_probe --steps 2000 --seed 7 --passes 3`.
It applies 824 inserts, 988 updates and 188 deletes on top of 280 seeded dimension rows.
That is 2,280 decoded changes under every setting that runs. Nothing was skipped and
nothing was deflected.

| replica identity | updates carrying a before image | mean before fields | WAL bytes | WAL vs default | pgoutput bytes | pgoutput vs default |
|---|---|---|---|---|---|---|
| `DEFAULT` | 0 of 988 | 2.0 | 616,072 | 1.0 | 297,077 | 1.0 |
| `FULL` | 988 of 988 | 5.6284 | 665,488 | 1.0802 | 386,657 | 1.3015 |
| `NOTHING` | refused by the source | | | | | |

Three things fall out of that.

**The write ahead log ratio understates what the consumer pays by nearly four times.**
Turning on full before images adds 8 percent to the write ahead log and 30 percent to the
bytes the replication stream delivers. Those are the same rows. The message count is
identical at 6,844 either way, so every extra byte sits inside a message the consumer was
always going to receive. A capacity argument made off `pg_wal_lsn_diff` is an argument
about disk. The number that decides whether a consumer keeps up is the other one.

I have not measured why the two ratios differ. The obvious candidate is that the log
carries full page images and transaction metadata and every other write in the database,
so the same extra tuple is a smaller fraction of a larger total. That is an explanation I
find convincing and it is not a measurement, and decomposing the log by record type would
settle it.

**`NOTHING` is not a cheap option, it is not an option.** Postgres refuses the write. The
error is `cannot update table "customer" because it does not have a replica identity and
publishes updates`. That is the OLTP database rejecting an application write because of a
replication setting. Anyone reasoning about it as a downstream tradeoff has it backwards.

**Under `DEFAULT`, not one of the 988 updates carries a before image.** The 188 changes
that do carry one are all deletes, and each of those carries only its key. Mean before
width is exactly 2.0 because the only hard deleted table has a two column primary key. A
reconciliation job that wants to know what a row used to be has nothing to read.

## Reproducibility

The probe discards a warmup arm, then runs each setting three times. The first pass of
each is what the table above reports. The other two exist to say how much of any gap
between two settings is noise.

| arm | decoded changes | WAL bytes | pgoutput bytes |
|---|---|---|---|
| `DEFAULT` | range 0 | range 208, 0.0338 percent | range 58, 0.0195 percent |
| `FULL` | range 0 | range 24, 0.0036 percent | range 156, 0.0403 percent |

Decoded change counts reproduce exactly, both arms, every pass. Byte volumes do not. So a
byte volume here is neither a counted quantity nor a timing, and it has a small floor under
it. Two separate runs of the whole script gave a stream ratio of 1.3019 and 1.3015, so the
third decimal is not worth reading and the first two are.

The merge probe reports a warehouse fingerprint and it is named `within_run_fingerprint`
for a reason. Two runs of that script produce identical counts and different hashes, which
was checked with `diff` rather than assumed. The source tables carry `now()` timestamps, so
a second workload at the same seed gives the same shape and different data. The fingerprint
compares two warehouses inside one run, which is what the replay check and the batch size
check need. It says nothing across runs and the name is there so nobody reads it as if it
did.

## The failure mode nobody measures

A replication slot that nobody consumes pins the write ahead log. That is the incident
that takes a Postgres source down, and it is not a slow degradation. The disk fills and
the database stops.

The probe reads `pg_replication_slots` before dropping its slots, so the number is what
the slots were really holding.

```
probe_slot            test_decoding  active=false  wal_status=reserved  retained 616,128 bytes
probe_slot_pgoutput   pgoutput       active=false  wal_status=reserved  retained 616,128 bytes
```

The workload wrote 616,072 bytes of log. Each idle slot is pinning 616,128, which is all of
it. Two slots hold the same log rather than two copies of it, so the cost of a second
consumer is not a second copy. The cost of one consumer that stops reading is everything
written since it stopped.

`wal_status` is the column worth alerting on. It reads `reserved` on this instance, where
`max_slot_wal_keep_size` is `-1`, which is the Postgres 14 default and means no limit. With
no limit the slot never gives up and the disk is what runs out. Setting a limit trades that
for a slot that can reach `lost` and need reseeding from a snapshot. I have measured the
`reserved` state and the retained byte count. I have not run a source out of disk and I
have not driven a slot to `lost`.

### Reading the whole stream is not what releases it

This is the part I had wrong. A slot carries two positions and they are not the same
thing. `confirmed_flush_lsn` is how far the consumer says it has read.  `restart_lsn` is
how far back the server still has to keep the log. Only the second one is retention.

One connector slot, followed through a workload. From
`scripts/topic_layout_probe.py`, the `positions` block.

| after | retained bytes | unconfirmed bytes |
|---|---|---|
| the workload, nothing read | 240,072 | 240,016 |
| reading the entire stream | 238,368 | 0 |
| reading everything, all five slots | 238,368 | 0 |
| a `CHECKPOINT` | 238,672 | 304 |
| a `CHECKPOINT` and then one more read | **176** | 0 |

The consumer reached the end of the stream and 238,368 bytes stayed pinned. A checkpoint
on its own did not release them either, and moved the number up by 304 bytes because a
checkpoint writes log of its own. Only a checkpoint followed by another read dropped it to
176 bytes.

So a monitor watching consumer lag reports a healthy consumer while the disk fills. The
restart position is recalculated from a running transactions record, which the server
writes at a checkpoint, and that record has to be decoded before the slot moves. Two
things have to happen and neither of them is the one people name. On a quiet source both
can be a long way off, which is what `heartbeat.interval.ms` in the connector config is
for.

The byte figures move between passes and no ratio here is built on them. Across three
passes the write ahead log volume ranged 208 bytes at 0.089 percent while every frame
count had a range of zero. What reproduces is the shape of the table.

## Every published figure, re-measured

Every table above was written on the day it was measured. That is the day the number was
true. It says nothing about today.

```
python3 -m scripts.reproduction_report --markdown
```

This re-runs the commands this README prints, character for character, and grades what
comes back against what the README says. The published side is read out of the README and the
files in `docs/` at run time by `warehouse/reproduce.py`, addressed by heading and row and column. Nothing
transcribes a number. That matters more than it sounds: a checker holding a hand typed copy
of what the document used to say turns a wrong transcription into a figure that looks
reproduced, which is the exact failure the tool exists to catch.

A figure whose row the README has lost raises. So does an address matching two cells, which
is real here, because five rows in this file are labelled `log_order`.

Three kinds, not two. A count reproduces exactly or it is broken, with no tolerance band on
purpose. A byte volume out of a database has a floor under it that is not zero, measured at
0.0036 to 0.0403 percent across passes on identical work, so it gets a band of one tenth of
a percent. A timing may move and is reported as moved.

| figure | kind | published | re-measured | verdict | produced by |
|---|---|---|---|---|---|
| WAL bytes under DEFAULT | volume | 616,072 | 616,072 | EXACT | `replica_identity_probe` |
| WAL bytes under FULL | volume | 665,488 | 665,488 | EXACT | `replica_identity_probe` |
| pgoutput bytes under DEFAULT | volume | 297,077 | 297,018 | WITHIN_BAND | `replica_identity_probe` |
| pgoutput bytes under FULL | volume | 386,657 | 386,732 | WITHIN_BAND | `replica_identity_probe` |
| mean before fields under FULL | counted | 5.6284 | 5.6284 | EXACT | `replica_identity_probe` |
| frames, one connector | counted | 2,652 | 2,652 | EXACT | `topic_layout_probe` |
| framing, one connector | counted | 1,772 | 1,772 | EXACT | `topic_layout_probe` |
| row changes, one connector | counted | 880 | 880 | EXACT | `topic_layout_probe` |
| framing, four connectors | counted | 7,056 | 7,056 | EXACT | `topic_layout_probe` |
| framing, product alone | counted | 1,765 | 1,765 | EXACT | `topic_layout_probe` |
| retained after the workload | volume | 240,072 | 240,112 | WITHIN_BAND | `topic_layout_probe` |
| retained after reading it all | volume | 238,368 | 238,368 | EXACT | `topic_layout_probe` |
| missing on lag_25 | counted | 402 | 402 | EXACT | `reconcile_probe` |
| differing on lag_25 | counted | 342 | 342 | EXACT | `reconcile_probe` |
| set subtraction on lag_25 | counted | 1,163 | 1,163 | EXACT | `reconcile_probe` |
| keys checked on lag_25 | counted | 786 | 786 | EXACT | `reconcile_probe` |
| keys checked on lag_75 | counted | 122 | 122 | EXACT | `reconcile_probe` |
| drift reported on lag_50 | counted | 1,286 | 1,286 | EXACT | `reconcile_probe` |
| records dropped, new batching | counted | 30 | 30 | EXACT | `chaos_probe` |
| rows missing, new batching | counted | 6 | 6 | EXACT | `chaos_probe` |
| rows missing, offset before merge | counted | 40 | 40 | EXACT | `chaos_probe` |
| dropped at restart batch 320 | counted | 420 | 420 | EXACT | `chaos_probe` |
| dropped at restart batch 600 | counted | 2,100 | 2,100 | EXACT | `chaos_probe` |
| merged twice at restart batch 100 | counted | 900 | 900 | EXACT | `chaos_probe` |
| rows on log_order | counted | 1,777 | 1,777 | EXACT | `merge_probe` |
| soft deleted on log_order | counted | 241 | 241 | EXACT | `merge_probe` |
| rows on the versioned arm | counted | 4,280 | 4,280 | EXACT | `merge_probe` |
| extra on the hard delete shuffle | counted | 98 | 98 | EXACT | `merge_probe` |

Twenty five exact, three inside the band, nothing broken. All twenty three counted
quantities came back to the digit.

### The number that matters here is 12 percent

That table grades 28 figures. This README publishes 233 numeric cells. So a pass that reads
as "everything reproduces" covers one figure in eight, and I would rather print that than
let the run of EXACT verdicts imply otherwise.

The figure list is written by hand, so it grades what I remembered to put in it. The
denominator is not. `reproduce.numeric_cells` walks every table in the file and counts
every cell that parses as a number, so the coverage figure is derived from the document
rather than asserted about it. The report prints it on every run.

And the one wrong number in this README was outside the table entirely. A sentence under
"Running the checks" said the latest mutation pass was 22 mutants. That was true when it
was written and two passes had happened since. A figure in prose has no heading and no row and no column.
There is nothing for a grader to address. It stayed wrong until I read the sentence. The tool cannot see the place the defect actually was.
