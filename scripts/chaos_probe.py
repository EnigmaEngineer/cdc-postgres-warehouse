"""Kill the consumer partway through, restart it, and see what the warehouse holds.

One Postgres and one workload and one logical slot, same as every other probe here. Every
arm below is that one change stream applied by a consumer that dies somewhere.

    clean                    nothing crashes. The reference warehouse
    crash_staged             dies with the batch staged and not merged
    crash_merged             dies with the rows merged and the ledger not written
    crash_tombstoned         dies with everything done except the ledger write
    resume_same_batching     restarts and re-drains in the identical order
    resume_new_batching      restarts and re-drains in a different order, resumes by number
    resume_new_batching_all  same restart, replays from the first batch instead
    offset_after_merge       offset kept outside the transaction, committed after it
    offset_before_merge      offset kept outside the transaction, committed before it

The first four say whether a batch can land half applied. The middle three are the
interesting ones. Replaying the same batches into one warehouse passes on the first try,
because the merge is idempotent and that was already measured, so the arms worth running
are the ones where the batching moved underneath the resume point. The last two are the
design this project did not choose.

    python3 -m scripts.chaos_probe --steps 4000 --seed 11 --markdown
"""

import argparse
import json
import os
import sys

import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc import decode  # noqa: E402
from cdc import delivery  # noqa: E402
from cdc import events  # noqa: E402
from cdc import pg  # noqa: E402
from load import drive  # noqa: E402
from load import workload  # noqa: E402
from scripts.merge_probe import COMPARED, PRIMARY_KEY_COLUMNS, build_targets  # noqa: E402
from scripts.merge_probe import reset_schema, truth  # noqa: E402
from warehouse import merge as wh  # noqa: E402
from warehouse import recovery  # noqa: E402

SLOT = "chaos_probe"
PREFIX = "shopcdc"


def fresh(versioned=False):
    con = duckdb.connect()
    warehouse = wh.Warehouse(con, build_targets(), versioned=versioned)
    warehouse.create()
    return warehouse


def exactness(warehouse, source):
    """Whether every target table matches the source, and by how much it does not."""
    from cdc import consumer
    missing = extra = 0
    exact = True
    for table in workload.TABLES:
        got = consumer.compare(warehouse.live_rows("wh_" + table, COMPARED[table]),
                               source[table])
        missing += got["missing"]
        extra += got["extra"]
        exact = exact and got["exact"]
    return {"missing": missing, "extra": extra, "exact": exact}


def ids_per_batch(batches, index_of):
    """Each batch as a list of stream positions, which is what `resume_gap` compares.

    The events themselves are not hashable and two records really can carry the same log
    position, so identity has to be the position in the log order rather than anything read
    off the record.
    """
    return [[index_of[id(e)] for e in batch] for batch in batches]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=80)
    ap.add_argument("--batch", type=int, default=250)
    ap.add_argument("--crash-in-batch", type=int, default=7)
    ap.add_argument("--restart-seed", type=int, default=97)
    ap.add_argument("--restart-batch", type=int, default=180)
    ap.add_argument("--sweep", type=int, default=8)
    ap.add_argument("--dsn", default=pg.DEFAULT_DSN)
    ap.add_argument("--markdown", action="store_true")
    args = ap.parse_args()

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    con = pg.connect(args.dsn)
    cur = con.cursor()
    cur.execute("set timezone to 'UTC'")
    cur.execute("set datestyle to 'ISO, MDY'")

    report = {"settings": vars(args), "duckdb": duckdb.__version__}
    cur.execute("show server_version")
    report["postgres"] = cur.fetchone()[0]

    reset_schema(cur, os.path.join(here, "db", "schema.sql"))
    pg.drop_slot_if_exists(cur, SLOT)
    pg.create_slot(cur, SLOT, "test_decoding")
    report["workload"] = drive.run_workload(cur, args.seed, args.steps,
                                            args.customers, args.products)

    raw = pg.peek_changes_with_lsn(cur, SLOT)
    rows = [(lsn, decode.parse_row(data)) for lsn, data in raw]
    rows = [(lsn, r) for lsn, r in rows if r is not None]

    primary_keys = {}
    for table in workload.TABLES:
        primary_keys["shop." + table] = tuple(pg.primary_key_columns(cur, "shop." + table))
    source = {t: truth(cur, t, COMPARED[t]) for t in workload.TABLES}
    key_columns = {t: events.key_columns_for(t, primary_keys) for t in primary_keys}
    evs = events.stream(rows, key_columns, PREFIX)
    pg.drop_slot_if_exists(cur, SLOT)
    con.close()

    index_of = {id(e): i for i, e in enumerate(evs)}
    routed = delivery.route(evs, args.partitions)
    first_order = delivery.interleave(routed, args.seed)
    # A restart re-drains the same six partitions and gets a different interleaving. Same
    # records and the same per partition order. Only the arrival order moves, which is
    # the only thing a broker ever promised.
    second_order = delivery.interleave(delivery.route(evs, args.partitions),
                                       args.restart_seed)

    first_batches = wh.batches(first_order, args.batch)
    second_batches = wh.batches(second_order, args.batch)
    report["stream"] = {
        "records": len(evs),
        "batches": len(first_batches),
        "delivery_orders_identical": first_order == second_order,
        "batches_before_the_crash": args.crash_in_batch - 1,
    }

    arms = {}

    # The reference. Nothing crashes, so this is what every recovered warehouse has to
    # come back to.
    ref = fresh()
    recovery.drive(ref, first_batches, primary_keys)
    clean_fingerprint = ref.fingerprint()
    clean_ignoring_batch = ref.fingerprint(include_batch=False)
    arms["clean"] = {
        "ledger": recovery.resume_point(ref),
        "fingerprint_matches_clean": True,
        "ignoring_batch_column": True,
        "grade": exactness(ref, source),
        "ledger_vs_data": recovery.ledger_agrees_with_the_data(ref),
    }
    ref.con.close()

    # Three deaths inside one transaction. The question is whether the batch that was in
    # flight left anything behind.
    for point in wh.FAILURE_POINTS:
        warehouse = fresh()
        run = recovery.drive(warehouse, first_batches, primary_keys,
                             fail_at=point, fail_in_batch=args.crash_in_batch)
        rows_at_crashed_batch = 0
        for target in warehouse.targets:
            rows_at_crashed_batch += warehouse.con.execute(
                'select count(*) from "{}" where "cdc_batch" = ?'.format(target.name),
                [args.crash_in_batch]).fetchone()[0]
        arms["crash_" + point] = {
            "crashed": run["crashed"],
            "ledger": run["ledger_says"],
            "rows_carrying_the_crashed_batch": rows_at_crashed_batch,
            "ledger_vs_data": recovery.ledger_agrees_with_the_data(warehouse),
        }
        warehouse.con.close()

    # Restart and resume. The crash point is the one that has done everything except the
    # ledger write, which is the state a consumer with an external offset lives in.
    def restart(batches_after, strategy):
        warehouse = fresh()
        recovery.drive(warehouse, first_batches, primary_keys,
                       fail_at="tombstoned", fail_in_batch=args.crash_in_batch)
        last = recovery.resume_point(warehouse)
        start = recovery.first_batch_to_apply(last, strategy)
        recovery.drive(warehouse, batches_after, primary_keys, start_at=start)
        out = {
            "ledger_after_the_crash": last,
            "resumed_at": start,
            "fingerprint_matches_clean": warehouse.fingerprint() == clean_fingerprint,
            "ignoring_batch_column":
                warehouse.fingerprint(include_batch=False) == clean_ignoring_batch,
            "grade": exactness(warehouse, source),
            "ledger_vs_data": recovery.ledger_agrees_with_the_data(warehouse),
        }
        warehouse.con.close()
        return out, last, start

    same, _, _ = restart(first_batches, "ledger")
    arms["resume_same_batching"] = same

    new, last, start = restart(second_batches, "ledger")
    new["gap"] = recovery.resume_gap(ids_per_batch(first_batches, index_of),
                                     ids_per_batch(second_batches, index_of), start)
    arms["resume_new_batching"] = new

    new_all, _, start_all = restart(second_batches, "replay_all")
    new_all["gap"] = recovery.resume_gap(ids_per_batch(first_batches, index_of),
                                         ids_per_batch(second_batches, index_of),
                                         start_all)
    arms["resume_new_batching_all"] = new_all

    # A restarted consumer batches whatever the poll handed it, so the same size twice is
    # the unrealistic case and it is the one that makes `dropped` and `applied_twice`
    # arithmetically equal. This arm is here to break that.
    resized = wh.batches(second_order, args.restart_batch)
    small, _, start_small = restart(resized, "ledger")
    small["gap"] = recovery.resume_gap(ids_per_batch(first_batches, index_of),
                                       ids_per_batch(resized, index_of), start_small)
    small["batch_size"] = args.restart_batch
    arms["resume_new_batching_resized"] = small

    # How much of the loss is this one restart order. A single number off one seed reads
    # as a property of the design and is a property of a draw, so the same crash is run
    # against several restart orders and the spread is published beside it.
    sweep = []
    for seed in range(args.restart_seed, args.restart_seed + args.sweep):
        order = delivery.interleave(delivery.route(evs, args.partitions), seed)
        gap = recovery.resume_gap(ids_per_batch(first_batches, index_of),
                                  ids_per_batch(wh.batches(order, args.batch), index_of),
                                  args.crash_in_batch)
        sweep.append({"restart_seed": seed, "dropped": gap["dropped"],
                      "applied_twice": gap["applied_twice"],
                      "two_numbers": recovery.resume_gap_is_two_numbers(gap)})
    report["restart_order_sweep"] = {
        "runs": sweep,
        "dropped_low": min(s["dropped"] for s in sweep),
        "dropped_high": max(s["dropped"] for s in sweep),
        "never_zero": all(s["dropped"] > 0 for s in sweep),
    }

    # The restart batch size, which is the variable the consumer has least control over,
    # because it batches whatever the poll returned. Below the original size the resume
    # point lands short of what was applied and the over-replay covers the gap. Above it,
    # every record between the two lands nowhere.
    sizes = []
    for size in (100, 180, 250, 320, 400, 600):
        gap = recovery.resume_gap(
            ids_per_batch(first_batches, index_of),
            ids_per_batch(wh.batches(second_order, size), index_of),
            args.crash_in_batch)
        sizes.append({"restart_batch": size, "dropped": gap["dropped"],
                      "applied_twice": gap["applied_twice"],
                      "two_numbers": recovery.resume_gap_is_two_numbers(gap)})
    report["restart_batch_sweep"] = sizes

    # The design this project did not choose. An offset outside the transaction, in both
    # orderings, crashing at the same point.
    for name, before in (("offset_after_merge", False), ("offset_before_merge", True)):
        warehouse = fresh()
        offsets = recovery.SeparateOffsetStore(commit_before=before)
        run = recovery.drive(warehouse, first_batches, primary_keys, offsets=offsets,
                             fail_at="tombstoned", fail_in_batch=args.crash_in_batch)
        # This consumer trusts its offset store rather than the ledger, which is the whole
        # point of having one.
        recovery.drive(warehouse, first_batches, primary_keys,
                       start_at=offsets.committed + 1)
        arms[name] = {
            "offset_store_after_the_crash": run["offset_store_says"],
            "ledger_after_the_crash": run["ledger_says"],
            "resumed_at": offsets.committed + 1,
            "fingerprint_matches_clean": warehouse.fingerprint() == clean_fingerprint,
            "ignoring_batch_column":
                warehouse.fingerprint(include_batch=False) == clean_ignoring_batch,
            "grade": exactness(warehouse, source),
        }
        warehouse.con.close()

    report["arms"] = arms
    if args.markdown:
        print(markdown(report))
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


ARM_ORDER = ("clean", "resume_same_batching", "resume_new_batching",
             "resume_new_batching_resized", "resume_new_batching_all",
             "offset_after_merge", "offset_before_merge")


def markdown(report):
    """The tables the README publishes, printed by the thing that measured them."""
    out = ["| arm | resumed at | records dropped | rows missing | rows extra | source |",
           "|---|---|---|---|---|---|"]
    for name in ARM_ORDER:
        body = report["arms"][name]
        grade = body["grade"]
        gap = body.get("gap")
        out.append("| `{}` | {} | {} | {:,} | {:,} | {} |".format(
            name,
            body.get("resumed_at", "n/a"),
            "{:,}".format(gap["dropped"]) if gap else "0",
            grade["missing"], grade["extra"],
            "exact" if grade["exact"] else "**wrong**"))

    out.append("")
    sweep = report["restart_order_sweep"]
    out.append("| restart order | records dropped | merged twice | two numbers |")
    out.append("|---|---|---|---|")
    for run in sweep["runs"]:
        out.append("| seed {} | {:,} | {:,} | {} |".format(
            run["restart_seed"], run["dropped"], run["applied_twice"],
            "yes" if run["two_numbers"] else "no"))

    out.append("")
    out.append("| restart batch size | records dropped | merged twice | two numbers |")
    out.append("|---|---|---|---|")
    for run in report["restart_batch_sweep"]:
        out.append("| {:,} | {:,} | {:,} | {} |".format(
            run["restart_batch"], run["dropped"], run["applied_twice"],
            "yes" if run["two_numbers"] else "no"))

    out.append("")
    out.append("| crash point | ledger says | rows carrying that batch | data ahead |")
    out.append("|---|---|---|---|")
    for point in ("staged", "merged", "tombstoned"):
        body = report["arms"]["crash_" + point]
        out.append("| `{}` | {} | {:,} | {} |".format(
            point, body["ledger"], body["rows_carrying_the_crashed_batch"],
            "yes" if body["ledger_vs_data"]["data_ahead_of_the_ledger"] else "no"))
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
