# cdc-postgres-warehouse

Change data capture from Postgres into a warehouse, built so that a rerun can never
duplicate a row. A Postgres instance with logical decoding turned on and an OLTP schema
that produces the change shapes a consumer has to handle. A seeded load generator drives
it. A consumer applies the result and is graded against the source table.

```
./scripts/bootstrap-local.sh
python -m load.generate --seed 7 --steps 2000
```

No Docker needed for that. `bootstrap-local.sh` unpacks the Postgres 14 packages into a
prefix with `dpkg -x` and runs `initdb` there. No root, no package manager lock, nothing
written outside the prefix. `docker-compose.yml` is the laptop path and it is the less
tested of the two.

## What is here

```
  load/generate.py           seeded workload, inserts and updates and deletes
        |
        v
  Postgres 14, wal_level=logical
        |
        |  one logical replication slot, publication cdc_shop
        v
  cdc/decode.py              reads what the slot really emitted
        |
        v
  cdc/connector.py           the Debezium config, generated from the catalog
  cdc/topics.py              topic names, keys, and the Kafka partitioner
  cdc/events.py              the envelope, and the tombstone behind every delete
        |
        v
  shopcdc.shop.customer      shopcdc.shop.order_header
  shopcdc.shop.product       shopcdc.shop.order_item
        |
        |  cdc/delivery.py   routing, interleaving, compaction
        v
  cdc/consumer.py            apply, ordered by log position, keyed by primary key
```

Kafka and Kafka Connect are not running here yet. The connector config is generated and
validated against the real connector rather than pasted from a tutorial. The topic layout
decision is measured on the source side, where its cost actually lands. The consumer is fed
the records a topic would carry, routed by Kafka's own partitioner.
`docker-compose.yml` carries one service for that reason. A compose file listing services
that nothing in the repo ever contacts is an architecture claim the code cannot back.

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
| `shuffled` | 445 | 37 | 0 | **wrong** |
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
| `shuffled` | 330 | 152 | 0 | **wrong** |
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
445 rows with none wrong to 561 rows with 207 that should not be there.

I am keeping it. It costs one comparison per record and it is the difference between a
consumer that is correct and one that is correct as long as nobody widens a partition, adds
a second writer, or replays a topic out of a snapshot. But calling it tested by the
interleaved arms would be a claim about an arm that cannot fail.

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

## Running the checks

```
python tests/run_all.py
```

146 checks, no database required. Everything runs on fixtures. The workload planner and the
decoded change reader and the schema, then the connector config and the partitioner, then
the record shape and the delivery orders and the apply rules.
`tests/test_decode.py` uses real `test_decoding` output copied off a running server rather
than lines written from memory, because a parser tested against invented input tests the
author's memory of a format.

`tests/test_consumer.py` and `tests/test_delivery.py` build a fixture where two rows share
a record key. A fixture where every key covers one row turns every rule about tombstones
green without running any of them, and there are four such rules.

`tests/test_topics.py` pins the murmur2 port against vectors taken from
`org.apache.kafka.common.utils.Utils.murmur2`. Three of them have the sign bit set,
because that is the only input where masking and taking an absolute value disagree and
every other vector leaves the rule untested.

`tests/test_connector.py` opens with a list of legitimate Debezium properties the
validator must not refuse. A guardrail is judged first on what it wrongly blocks, because
one that refuses real configs gets switched off and then it protects nothing.

`tests/test_schema.py` reads `db/schema.sql` and fails when a table is declared and not
published. Postgres 14 has no `FOR ALL TABLES IN SCHEMA`, so the publication names its
tables one by one. Adding a table and forgetting the publication does not error and does
not warn. It just never appears in the change stream.

Mutants are run against the shipped code every time it changes, with a clean control
before and after. The one worth naming took the delete tuple and reported it as an after
image rather than a before image, which is a plausible reading of the plugin output and
would have made every delete look like an insert of a two column row.

The latest pass is 26 mutants over the apply rules and the delivery orders and the record
shape, and the four that survived the first attempt are worth more than the twenty two that
died.

One was dead code. Taking the high water mark to `max(held, incoming)` cannot change
anything, because the guard already dropped everything below the mark. A line no mutation
can reach is a line doing nothing, and it was deleted rather than tested.

One was a symmetric fixture. `compare` was checked with one missing row against one extra
row, so swapping the two subtractions passed. It is two against one now, and missing
against extra is exactly the difference between a merge that loses rows and one that keeps
dead ones.

One was a guard rescued by another module. `route` refuses a zero partition count, and so
does the partitioner underneath it, with the same exception and a message containing the
same words. The check only proves anything when it is handed an empty record list, because
then the partitioner is never called and only `route` can refuse.

One was a rule that only ran on keys carrying three records or more, and every fixture key
that mattered carried three, because a delete brings a tombstone with it. There is now a
key with exactly two.

`tests/test_deps.py` walks every `.py` file with `ast` and fails when a third party import
is missing from `requirements.txt`, or when `requirements.txt` declares something nothing
imports. It also feeds itself a file it must not miss, because a check that passes when
the repo is correct would do the same thing if it were scanning nothing.

## Known limitations

- **The consumer applies to a dictionary, not to a warehouse.** The rules are real and the
  target is memory. The same rules written as a MERGE, and the reconciliation that checks
  them, are the work ahead.
- **The consumer has never read a Kafka topic.** It reads a change stream out of a
  replication slot and the records are built here rather than by a connector. The routing
  and the interleaving are models of what a topic would deliver, using Kafka's own
  partitioner. Nothing has consumed from a broker.
- **The shuffle arm is not something Kafka does.** It is the only arm where the ordering
  guard has anything to do, and its numbers describe a delivery model this system does not
  have. The guard is kept on an argument about future shapes rather than on a measured need.
- **The high water mark is never forgotten.** A key that has been deleted keeps its
  position so a delayed insert cannot bring it back, and nothing expires the entry. On a
  long lived stream that grows without limit. The window it really needs is a function of
  how far out of order records can arrive, which is not measured here.
- **No change in this workload moves a primary key.** A primary key update is a delete and
  an insert under two different keys, so they land on two partitions and can be reordered
  against each other. That is the case where the guard would earn its place on a real
  topic, and the load generator does not produce one.
- **The measured stream is `test_decoding`, not `pgoutput`.** The two carry the same
  information about what is present and absent. A real connector reads the binary one and
  the record shapes here are built from the readable one.
- **No Kafka Connect worker has ever run this connector config.** It is generated from
  the catalog and validated against the key list dumped out of the connector jar, which
  catches a misspelled property and a dangerous default. It does not catch a value the
  connector accepts and then rejects at runtime, and nothing here has posted it to a REST
  endpoint. `scripts/dump_connector_configdef.sh` is the only thing in this repo that has
  loaded the connector class at all.
- **The topic layout figures are frame counts, not throughput.** Four connectors read
  3.98 times the transaction framing. Whether that costs a real source anything depends
  on decoding cost per frame, which is not measured here. The framing frames are small.
  The argument for one connector is that the extra work buys nothing, not that it is
  expensive.
- **The composite key split rate is measured at 6 partitions on one table.** It is a
  property of the partition count and the key, and 6 was chosen rather than measured. A
  different count moves the number and not the conclusion. A shorter workload of 600
  operations gives 18 of 20 rather than 104 of 122, and 20 splittable orders is too few to
  read a rate off.
- **The retention table is one slot's positions through one workload.** The order of
  operations in it was chosen after the first run came back flat, so it is a sequence
  built to expose a specific behaviour rather than a trace of a real deployment.
- **The database plumbing in `scripts/replica_identity_probe.py` has no tests.** The
  arithmetic it prints comes from `cdc/probe.py` and `cdc/decode.py`, which do. The part
  that opens a connection and resets a schema does not.
- **The server does not survive past the shell that starts it** in the sandbox this was
  built in. Bring-up and work have to sit in one script there. On a normal machine
  `pg_ctl` leaves a daemon running and this does not apply.
- **`docker-compose.yml` has never run.** There is no Docker daemon on the machine this
  was written on. It is written from the Postgres image documentation and not from a run.
- **`cdc/decode.py` still splits a full update on the literal `new-tuple:`.** A text value
  carrying that string would split the change in the wrong place. The field counting
  version of this limit is gone, because reading values means finding where a value ends,
  and the scanner now skips a quoted run whole. That check used to pin the wrong count at
  2 and now asserts the right one at 1.
- **The probe loads the schema by handing the whole file to `cur.execute`.** That works
  because `db/schema.sql` is plain SQL. A file carrying a psql meta-command would break it,
  and the failure would look like a syntax error in the schema rather than in the loader.

- **The `pgoutput` frames are counted and never parsed.** The probe opens a second slot on
  the same workload and reports the message count and the total bytes. That is enough to
  say the binary stream a consumer reads grows by 1.3019 while its message count does not
  move. It is not enough to say what is inside a frame, and no claim here rests on that.

- **The 1.3019 ratio is a property of these four tables.** They are narrow. A table with a
  text blob in it would push the before image cost higher, and I have not measured one.

## What I would do differently already

The schema puts the load generator's label counter in a database sequence. The first
version derived it from `max(customer_id)`, which read as obviously correct and was wrong,
because customers and products draw ids from separate sequences while sharing one name
counter. It only showed up on the second run against a populated database. Any counter
that has to be unique across runs belongs in the database, not in the process.
