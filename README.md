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
knows that. What is easy to get wrong is what each setting really costs, and what the
cheapest looking one actually does.

Measured with `python -m scripts.replica_identity_probe --steps 2000 --seed 7`, which
applies 824 inserts, 988 updates and 188 deletes on top of 280 seeded dimension rows. That
is 2,280 decoded changes under every setting that runs.

| replica identity | before image coverage | mean before fields | WAL bytes | vs default |
|---|---|---|---|---|
| `DEFAULT` | 0.1599 | 2.0 | 616,072 | 1.0 |
| `FULL` | 1.0000 | 5.6284 | 665,672 | 1.0805 |
| `NOTHING` | refused by the source | | | |

Three things fall out of that.

**`NOTHING` is not a cheap option, it is not an option.** Postgres refuses the write. The
error is `cannot update table "customer" because it does not have a replica identity and
publishes updates`. That is the OLTP database rejecting an application write because of a
replication setting. Anyone reasoning about it as a downstream tradeoff has it wrong.

**Under `DEFAULT`, 0 of 988 updates carry any before image at all.** The coverage of
0.1599 is 188 deletes out of 1,176 updates and deletes, and each of those deletes carries
only its key. Mean before width is exactly 2.0 because the only hard deleted table has a
two column primary key. A reconciliation job that wants to know what a row used to be has
nothing to read.

**Full before images cost about 8 percent more write ahead log on this workload.** Not
double. The reason is that a `FULL` update writes the old tuple in addition to the new one,
and the rows here are narrow. On a wide table with a text blob the ratio would be worse,
and I have not measured that.

The 8 percent is the number I expected to be large and it is small. The refusal is the
one I did not expect at all.

## Reproducibility

The probe runs a warmup arm and discards it. Then `DEFAULT` and `FULL` and `DEFAULT` again,
in that order. The repeat is there because two arms measured in a fixed order can differ
for reasons that have nothing to do with the setting.

Decoded change counts reproduce exactly. Both `DEFAULT` passes give 2,280 changes and
616,072 WAL bytes, in the same process and across two separate runs of the script. The
`FULL` arm does not. It gave 665,672 and 665,488 on two runs, which is 184 bytes and
0.028 percent apart. So a WAL byte count is not a counted quantity and it is not a timing
either. Treat it as a measurement with a small floor under it, and do not publish a
difference smaller than that floor as a result.

## Running the checks

```
python tests/run_all.py
```

32 checks, no database required. The workload planner, the decoded change reader and the
comparison arithmetic all run on fixtures. `tests/test_decode.py` uses real
`test_decoding` output copied off a running server rather than lines written from memory,
because a parser tested against invented input tests the author's memory of a format.

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
- **`test_decoding` is not the plugin a real consumer uses.** It is readable, which is why
  the measurement uses it. The consumer will read `pgoutput` binary frames. Both plugins
  agree about what is present and what is absent, which is the only question asked here.

## What I would do differently already

The schema puts the load generator's label counter in a database sequence. The first
version derived it from `max(customer_id)`, which read as obviously correct and was wrong,
because customers and products draw ids from separate sequences while sharing one name
counter. It only showed up on the second run against a populated database. Any counter
that has to be unique across runs belongs in the database, not in the process.
