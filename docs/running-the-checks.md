# Running the checks

Moved out of the README so the front page stays short. Back to the [README](../README.md).

## Running the checks

```
python tests/run_all.py
```

270 checks, no database required. duckdb runs in process, so the merge checks need no
server either. The order they run in:

```
workload planner    decoded change reader    schema
connector config    partitioner
record shape        delivery orders          apply rules
reduction           merge statements         soft delete
```
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

`tests/test_reconcile.py` builds a fixture carrying a key that covers two rows and a row
that differs on exactly one column. Without the first, no rule about choosing between rows
under one key ever runs. Without the second, a comparison that only looks at keys passes
every check a comparison that looks at values would.

`tests/test_recovery.py` states every recovery rule on a fixture that really loses
something. A resume that drops no records passes whether or not the resume logic ran at
all, so the clean case sits beside the losing one as a control rather than standing in as
the evidence. It also carries the first checks in this repo over `warehouse.reconcile.run`,
which is the reconciliation job's entry point and had none, because a real one needs
Postgres and this suite does not.

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

The latest pass is 34 mutants over the figure grader. All 34 die, control clean either
side. Nine survived the first attempt. Eight wanted a check and one wanted the code
changing, because the character set it tested against carried a space that could never
reach it, so an inclusive comparison and a strict one could not disagree.

The pass before it is 50 mutants over the recovery logic and the reconciliation and the
merge, all 50 killed. The one before that is 22 over the merge and the statement builders
and the tombstone gate, where five survived the first attempt and every one was a real gap.

Two of those five came from the same weak fixture. A delete envelope removes its own row, so its
tombstone then finds nothing under the key, and a fixture that keeps the envelope cannot
test the tombstone at all. Taking the gate off entirely and moving it off by one both
survived. The checks now deliver a tombstone with its envelope removed, which is not an
artificial shape, because under the default key that is the only shape in which a tombstone
removes anything.

One was a fixture ordered the wrong way round. Two tombstones under one key, delivered
newest first, makes "keep the first one seen" and "keep the highest" the same answer.

One was a check rescued by a column it was not about. The fingerprint was supposed to prove
it respects column order, and the two warehouses it compared also differed in the record key
column, so a mutant sorting each row survived. The rows go in through raw SQL now, with the
record key identical on both sides.

One was a check I had just improved. Moving the versioned check to three batches killed a
mutant about the join and revived one about the reduction, because a single record per batch
has nothing to collapse. Both checks exist now. They are two different rules.

The earlier pass is unchanged. It was 26 mutants over the apply rules, the delivery orders
and the record shape.

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
