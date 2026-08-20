# cdc-postgres-warehouse

Change data capture from Postgres into a warehouse, built so that a rerun can never
duplicate a row. This is the source side. A Postgres instance with logical decoding turned
on, an OLTP schema that produces the change shapes a consumer has to handle, and a seeded
load generator that drives it.

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
        |  logical replication slot, publication cdc_shop
        v
  cdc/decode.py              reads what the slot really emitted
        |
        v
  cdc/probe.py               turns two measured arms into a claim
```

Kafka, Kafka Connect and Debezium belong in this stack and none of them are here yet.
`docker-compose.yml` carries one service for that reason. A compose file listing services
that nothing in the repo ever contacts is an architecture claim the code cannot back.

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
| `DEFAULT` | 0 of 988 | 2.0 | 616,072 | 1.0 | 297,033 | 1.0 |
| `FULL` | 988 of 988 | 5.6284 | 665,488 | 1.0802 | 386,708 | 1.3019 |
| `NOTHING` | refused by the source | | | | | |

Three things fall out of that.

**The write ahead log ratio understates what the consumer pays by nearly four times.**
Turning on full before images adds 8 percent to the write ahead log and 30 percent to the
bytes the replication stream delivers. Those are the same rows. The gap is there because
the log is already carrying the full page images, the transaction metadata and everything
the other tables wrote, and the replication stream is carrying only the change. A capacity
argument made off `pg_wal_lsn_diff` is an argument about disk, and the number that decides
whether a consumer keeps up is the other one. The message count is identical at 6,844
either way, so every one of those extra bytes is inside a message the consumer was already
going to receive.

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
| `DEFAULT` | range 0 | range 208, 0.0338 percent | range 34, 0.0114 percent |
| `FULL` | range 0 | range 24, 0.0036 percent | range 5, 0.0013 percent |

Decoded change counts reproduce exactly, both arms, every pass. Byte volumes do not. So a
byte volume here is neither a counted quantity nor a timing, and it has a small floor under
it. The floor is three orders of magnitude below the 1.3019 ratio, which is the only reason
that ratio is worth stating.

## Running the checks

```
python tests/run_all.py
```

48 checks, no database required. The workload planner, the decoded change reader, the
schema and the comparison arithmetic all run on fixtures. `tests/test_decode.py` uses real
`test_decoding` output copied off a running server rather than lines written from memory,
because a parser tested against invented input tests the author's memory of a format.

`tests/test_schema.py` reads `db/schema.sql` and fails when a table is declared and not
published. Postgres 14 has no `FOR ALL TABLES IN SCHEMA`, so the publication names its
tables one by one. Adding a table and forgetting the publication does not error and does
not warn. It just never appears in the change stream.

Eight mutants were run against the shipped code and all eight were killed, with a clean
control before and after. The one worth naming took the delete tuple and reported it as an
after image rather than a before image, which is a plausible reading of the plugin output
and would have made every delete look like an insert of a two column row.

`tests/test_deps.py` walks every `.py` file with `ast` and fails when a third party import
is missing from `requirements.txt`, or when `requirements.txt` declares something nothing
imports. It also feeds itself a file it must not miss, because a check that passes when
the repo is correct would do the same thing if it were scanning nothing.

## Known limitations

- **The consumer does not exist yet.** Nothing reads the slot except a measurement. The
  merge, the ordering guarantees and the reconciliation are the work ahead.
- **The database plumbing in `scripts/replica_identity_probe.py` has no tests.** The
  arithmetic it prints comes from `cdc/probe.py` and `cdc/decode.py`, which do. The part
  that opens a connection and resets a schema does not.
- **The server does not survive past the shell that starts it** in the sandbox this was
  built in. Bring-up and work have to sit in one script there. On a normal machine
  `pg_ctl` leaves a daemon running and this does not apply.
- **`docker-compose.yml` has never run.** There is no Docker daemon on the machine this
  was written on. It is written from the Postgres image documentation and not from a run.
- **The field counter in `cdc/decode.py` counts `name[type]:` runs.** A text value that
  contains one inflates the count. Nothing written here produces such a value and
  `tests/test_decode.py` pins the behaviour so the limit fails loudly if the parser moves.
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
