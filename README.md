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
        |                    in memory, and the reference the SQL is graded against
        v
  warehouse/merge.py         reduce the batch, MERGE it, soft delete, record the batch
  warehouse/sql.py           the statements, duckdb and Snowflake
        |
        v
  wh_customer  wh_product  wh_order_header  wh_order_item  cdc_applied_batch
        |
        |  warehouse/reconcile.py   reads both ends and compares them key by key
        v
  missing, extra, differing, and how much of the table the verdict is about

  warehouse/recovery.py    where a restarted consumer starts, and what that costs
  warehouse/reproduce.py   grades this README's own figures against a fresh run
```

Kafka and Kafka Connect are not running here yet. The connector config is generated and
validated against the real connector rather than pasted from a tutorial. The topic layout
decision is measured on the source side, where its cost actually lands. The consumer is fed
the records a topic would carry, routed by Kafka's own partitioner.
`docker-compose.yml` carries one service for that reason. A compose file listing services
that nothing in the repo ever contacts is an architecture claim the code cannot back.

## Results

All of this ran against a real Postgres 14 with logical decoding on. Counts
reproduce exactly between runs and byte volumes move by a few hundredths of a percent.

**Turning on `REPLICA IDENTITY FULL` costs the consumer nearly four times what the write
ahead log says.** It adds 8 percent to the log and 30 percent to the bytes the replication
stream delivers, over the same rows and the same 6,844 messages. Sizing a consumer off
`pg_wal_lsn_diff` sizes it for the disk.

**Under `DEFAULT`, none of 988 updates carries a before image.** A reconciliation job that
wants to know what a row used to be has nothing to read. `NOTHING` is not a cheaper option
either. Postgres refuses the application's write outright.

**Reading the whole stream does not release the log.** One slot kept 238,368 bytes pinned
after its consumer had read everything. A checkpoint alone did not free it. A checkpoint
followed by one more read dropped it to 176 bytes. A monitor watching consumer lag reports a
healthy consumer while the disk fills.

**A batch number is not an offset.** A consumer killed mid batch and restarted with a
different interleaving resumed at the right batch number and dropped 30 records. The
warehouse ended six rows short and five rows wrong, which is small enough for a lagging
target to explain away.

Twenty eight published figures are re-measured by one command and all twenty eight come back
exact or inside their band. That covers 28 of the 233 numeric cells in these docs, and the
report prints that ratio on every run rather than letting a column of EXACT imply more.

```
python3 -m scripts.reproduction_report --markdown
python tests/run_all.py
```

## Known limitations

- The warehouse is duckdb. The Snowflake statements are written beside it and have never
  run.
- The consumer has never read a Kafka topic, and no Kafka Connect worker has ever run the
  generated connector config.
- The crash is injected inside the transaction rather than being a real process death.
- The restart never re-reads the source.
- The reconciliation has never run against a warehouse being written to.
- The probes that open a database connection have no tests over that plumbing.
- `docker-compose.yml` has never run, because the machine this was built on has no Docker
  daemon.

The full list is in [docs/limitations.md](docs/limitations.md). The first thing I would
build next is storing the per partition offset inside the merge transaction instead of a
batch number, and that needs a broker.

## More detail

**[Design notes](docs/design-notes.md)**

- [The topic layout, and why it is one connector](docs/design-notes.md#the-topic-layout-and-why-it-is-one-connector)
- [The consumer, and the ordering it actually needs](docs/design-notes.md#the-consumer-and-the-ordering-it-actually-needs)
- [The merge, and the two things in it that are not what they look like](docs/design-notes.md#the-merge-and-the-two-things-in-it-that-are-not-what-they-look-like)
- [The reconciliation, and the number it has to publish about itself](docs/design-notes.md#the-reconciliation-and-the-number-it-has-to-publish-about-itself)
- [Killing the consumer, and the resume point that is not one](docs/design-notes.md#killing-the-consumer-and-the-resume-point-that-is-not-one)
- [Three defaults that are wrong here](docs/design-notes.md#three-defaults-that-are-wrong-here)
- [The first thing worth knowing](docs/design-notes.md#the-first-thing-worth-knowing)
- [Reproducibility](docs/design-notes.md#reproducibility)
- [The failure mode nobody measures](docs/design-notes.md#the-failure-mode-nobody-measures)
- [Every published figure, re-measured](docs/design-notes.md#every-published-figure-re-measured)

**[Running the checks](docs/running-the-checks.md)**
