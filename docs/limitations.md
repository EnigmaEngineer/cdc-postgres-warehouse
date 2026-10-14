# Limitations

Moved out of the README so the front page stays short. Back to the [README](../README.md).

## Known limitations

- **The reproduction pass covers 28 of 233 published numeric cells.** The figure list is
  hand written, so it grades what I thought to include. Every figure not in it is
  unverified in exactly the way the whole section is about, and the honest read of a table
  of EXACT verdicts is that a twelfth of the document reproduces.
- **A figure in prose is unreachable.** The grader addresses a cell by heading and row and
  column. A number in a sentence has none of those, and the only wrong number this pass
  found was in a sentence. Every published figure could carry a machine readable marker
  instead, which would take the coverage question off the table, and it would make the
  document worse to read.
- **The warehouse is duckdb and the Snowflake statements have never run.** Every merge
  figure here came off duckdb 1.5.5 in process. `warehouse/sql.py` generates the Snowflake
  text and a test asserts that each claim about Snowflake in that file is labelled
  unverified. Reading a statement is not running it. The reconciliation reads its source
  through Postgres and its target through duckdb, so it has never compared a real
  warehouse either.
- **The in flight exclusion is only as good as the pending set it is handed.** Here the
  probe knows exactly which records it held back. A real consumer would have to read that
  set off the broker's committed offsets, or off the slot, and both of those are behind by
  however long the read takes. A key that moves in that gap is checked against a source
  that has already changed.
- **The reconciliation is never run against a warehouse being written to.** The probe
  applies a prefix of the stream, stops, and then compares. That is lag standing still. A
  real run reads the two ends at two different instants while a merge is in progress, and
  nothing here measures what that costs.
- **`changes_with_no_key` has never been non zero outside a test.** It counts a change
  record whose image does not carry the merge key columns, which needs a replica identity
  that has stopped carrying them or a merge key wider than the record key. Both are real
  and neither is in this workload, so the counter is exercised by one check and nothing
  else.
- **The corruption arm moves one column in one row.** It is enough to show that the
  exclusion hides a real error, which is what it is for. It says nothing about how much
  damage would go unseen at a given lag, and that is the number somebody running this
  would actually want.
- **`scripts/reconcile_probe.py` has no tests over its database plumbing**, the same gap
  as the other three probes.
- **The soft delete buys nothing measurable on any arm Kafka can produce.** It costs 241
  retained rows out of 1,777 and it is worth 98 rows on a shuffle the broker does not do.
  Same shape as the ordering guard. Both are kept on an argument about future shapes.
- **Nothing expires a soft deleted row.** The position it holds is the only reason it is
  there and the table grows forever. The window it really needs is a function of how far
  out of order records can arrive, which is not measured here.
- **The batch id in the warehouse is not yet joined to a Kafka offset**, and the chaos
  arms are what say how much that matters. The ledger goes in inside the merge transaction
  and is now read, and what it hands back is a batch number this repo assigns. Resuming on
  one is wrong the moment the batching moves, which is measured above. The right thing to
  store is the per partition offset the batch ended at, and that needs a broker.
- **The crash is injected, not a process death.** `fail_at` raises inside the transaction
  and the handler rolls back, which is what a real death gets from the database rather than
  from a handler. It does not model the database itself dying, a half written page, or a
  connection that vanishes with the transaction still open on the server. Every crash arm
  here is one of the three cheap failures.
- **The restart never re-reads the source.** Both delivery orders come from one slot read,
  and a real restarted consumer reconnects and may see records the first one never got to.
  So the arms measure re-batching and not re-reading, and the two are different.
- **The batch size sweep varies one thing at a time and reality varies both.** A restarted
  consumer picks up a different interleaving and a different batch size together. The
  sweeps hold one fixed to say which term dominates, and the combined case is not run.
- **`scripts/merge_probe.py` has no tests over its database plumbing**, the same gap as the
  other two probes. Every figure it prints comes out of `warehouse/merge.py` or
  `cdc/consumer.py`, which are tested. The part that opens a connection and resets a schema
  is not.
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
  say the binary stream a consumer reads grows by about 1.30 while its message count does
  not move. It is not enough to say what is inside a frame, and no claim here rests on that.
  Three runs of the script gave 1.3015, 1.3019 and 1.3020, so the first two decimals are
  the ones worth quoting and I had been writing four.

- **The 1.30 ratio is a property of these four tables.** They are narrow. A table with a
  text blob in it would push the before image cost higher, and I have not measured one.

## What I would do next

Four things, in the order I would do them.

**Store the offset, not the batch number.** This is the big one and the chaos arms are what
say so. The ledger hands back a number this code assigned, and a restarted consumer that
re-drains six partitions gets a different interleaving, so resuming on it drops records the
moment the batching moves. The right thing to write inside the merge transaction is the per
partition offset the batch ended at. That needs a broker, which is why it is not here.

**Run a Kafka Connect worker.** The connector config is generated from the catalog and
validated against the 100 keys the connector really declares, which catches a misspelled
property and three dangerous defaults. It does not catch a value the worker accepts and
then rejects at runtime. Nothing here has posted the config to a REST endpoint.

**Give the probes tests over their database plumbing.** Five scripts open a connection and
reset a schema and none of that is covered. Everything they compute comes out of tested
library code. The part that talks to Postgres does not, and one of them ran without a reset
for a day and published a row count that grew on every run while the rate it printed held
steady, which is exactly the shape nobody looks at twice.

**Put a marker on every published figure.** The reproduction pass grades 28 of 233 numeric
cells and cannot see a number in a sentence at all, which is where the only wrong figure
turned out to be.

## What I would do differently already

The schema puts the load generator's label counter in a database sequence. The first
version derived it from `max(customer_id)`, which read as obviously correct and was wrong,
because customers and products draw ids from separate sequences while sharing one name
counter. It only showed up on the second run against a populated database. Any counter
that has to be unique across runs belongs in the database, not in the process.
