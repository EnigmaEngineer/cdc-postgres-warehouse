"""Reconcile a warehouse against its source at several amounts of lag.

The quiesced arm is the easy one and it is not the point. Everything below it applies part
of the change stream and holds the rest back, which is what a reconciliation running
against a live system is really looking at.

    quiesced         every record applied, nothing in flight
    lag_25           three quarters applied
    lag_50           half applied
    lag_75           a quarter applied

Each lagged arm is reconciled twice. Once with no exclusion, which is what a naive report
does, and once excluding the keys named by the records still in the log. The first number
is the false alarm. The second is what is left, and the coverage beside it is how much of
the table the second number is about.

The last arm corrupts one row under an excluded key and reconciles again, because the
exclusion has to be shown giving something up rather than described as if it does not.

Bring-up and work sit in one script. The server does not survive the shell that started it
here.

    python3 -m scripts.reconcile_probe --steps 4000 --seed 11 --markdown
"""

import argparse
import json
import os
import sys

import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc import consumer  # noqa: E402
from cdc import decode  # noqa: E402
from cdc import delivery  # noqa: E402
from cdc import events  # noqa: E402
from cdc import pg  # noqa: E402
from load import drive  # noqa: E402
from load import workload  # noqa: E402
from warehouse import merge as wh  # noqa: E402
from warehouse import reconcile as rec  # noqa: E402
from warehouse import sql as W  # noqa: E402

SLOT = "reconcile_probe"


def apply_prefix(order, batch_size, merge_keys, fraction):
    """Apply the first `fraction` of a delivery order and return what was held back."""
    cut = int(round(len(order) * fraction))
    applied, held = order[:cut], order[cut:]
    con = duckdb.connect()
    warehouse = wh.Warehouse(con, wh.build_targets())
    warehouse.create()
    for i, batch in enumerate(wh.batches(applied, batch_size), start=1):
        warehouse.apply_batch(batch, i, merge_keys=merge_keys)
    return warehouse, held


def flat(report):
    """The pager line, plus the check that its coverage relates to the per table ones.

    `rec.one_line` used to live here. It computes coverage by summing both sides, and
    `rec.reconcile` computes it a table at a time, so the repo had two definitions of one
    published number with nothing tying them together. The rule that holds between them is
    now asserted on every arm this probe runs rather than being a property somebody
    believed.
    """
    line = rec.one_line(report)
    agrees = rec.coverage_is_between_the_tables(report, line)
    if agrees is False:
        raise ValueError("rolled up coverage {} is outside the per table range".format(
            line["coverage"]))
    line["coverage_is_between_the_tables"] = agrees
    return line


def set_difference_view(cur, warehouse):
    """The same arm read through cdc.consumer.compare, for the doubling comparison.

    Kept because the claim that set subtraction double counts a drifted row should be a
    measured pair of numbers from one warehouse rather than an argument about set theory.
    """
    missing = extra = 0
    for target in warehouse.targets:
        table = target.source_table.split(".")[-1]
        columns = wh.COMPARED_COLUMNS[table]
        got = consumer.compare(warehouse.live_rows(target.name, columns),
                               rec.read_source(cur, target, columns))
        missing += got["missing"]
        extra += got["extra"]
    return {"missing": missing, "extra": extra, "reported_rows": missing + extra}


def corrupt_one_excluded_row(warehouse, in_flight):
    """Break a row the exclusion is about to hide, and say whether it stays hidden.

    Picks a live target row whose key is in the in flight set, moves one non key column,
    and reconciles again with the same exclusion. The exclusion is a real tradeoff and the
    only way to show it is to hand it something it should catch and cannot.
    """
    for target in warehouse.targets:
        table = target.source_table.split(".")[-1]
        columns = wh.COMPARED_COLUMNS[table]
        keys = in_flight.get(table, set())
        if not keys:
            continue
        key_cols = wh.PRIMARY_KEY_COLUMNS[table]
        moved = [c for c in columns if c not in key_cols]
        if not moved:
            continue
        positions = [columns.index(c) for c in key_cols]
        for row in sorted(warehouse.live_rows(target.name, columns)):
            key = tuple(row[p] for p in positions)
            if key not in keys:
                continue
            where = " and ".join("{} = ?".format(W.quote(c)) for c in key_cols)
            warehouse.con.execute(
                "update {} set {} = 'corrupted-by-the-probe' where {}".format(
                    W.quote(target.name), W.quote(moved[0]), where), list(key))
            return {"table": table, "column": moved[0], "key": list(key)}
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=80)
    ap.add_argument("--batch", type=int, default=250)
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

    drive.reset_schema(cur, os.path.join(here, "db", "schema.sql"))
    pg.drop_slot_if_exists(cur, SLOT)
    pg.create_slot(cur, SLOT, "test_decoding")
    report["workload"] = drive.run_workload(cur, args.seed, args.steps, args.customers,
                                            args.products)

    raw = pg.peek_changes_with_lsn(cur, SLOT)
    rows = [(lsn, decode.parse_row(data)) for lsn, data in raw]
    rows = [(lsn, r) for lsn, r in rows if r is not None]
    primary_keys = {}
    for table in workload.TABLES:
        primary_keys["shop." + table] = tuple(pg.primary_key_columns(cur, "shop." + table))
    key_columns = {t: events.key_columns_for(t, primary_keys) for t in primary_keys}
    evs = events.stream(rows, key_columns, wh.TOPIC_PREFIX)
    pg.drop_slot_if_exists(cur, SLOT)

    order = delivery.interleave(delivery.route(evs, args.partitions), args.seed)
    report["stream"] = {"records": len(evs), "row_changes": len(rows),
                        "tombstones": sum(1 for e in evs if e.tombstone)}

    # The reconciliation only ever names a source table, never a schema qualified one, so
    # the in flight map is keyed the same way the COMPARED map is.
    short = {t.split(".")[-1]: v for t, v in primary_keys.items()}

    report["arms"] = {}
    for name, fraction in (("quiesced", 1.0), ("lag_25", 0.75), ("lag_50", 0.50),
                           ("lag_75", 0.25)):
        warehouse, held = apply_prefix(order, args.batch, primary_keys, fraction)
        by_table, key_counts = rec.keys_in_flight(held, primary_keys)
        by_table = {t.split(".")[-1]: v for t, v in by_table.items()}
        naive = rec.run(cur, warehouse, wh.COMPARED_COLUMNS, short)
        excluded = rec.run(cur, warehouse, wh.COMPARED_COLUMNS, short, in_flight_by_table=by_table)
        report["arms"][name] = {
            "records_applied": int(round(len(order) * fraction)),
            "records_held_back": len(held),
            "held_back_key_counts": key_counts,
            "no_exclusion": flat(naive),
            "with_exclusion": flat(excluded),
            "set_difference_view": set_difference_view(cur, warehouse),
            "per_table": {t: v["columns_differing"]
                          for t, v in naive["tables"].items()},
        }
        if name == "lag_50":
            broke = corrupt_one_excluded_row(warehouse, by_table)
            after = rec.run(cur, warehouse, wh.COMPARED_COLUMNS, short, in_flight_by_table=by_table)
            report["corruption_under_an_excluded_key"] = {
                "broke": broke,
                "verdict_with_exclusion": flat(after)["verdict"],
                "verdict_without_exclusion":
                    flat(rec.run(cur, warehouse, wh.COMPARED_COLUMNS, short))["verdict"],
            }
        warehouse.con.close()

    con.close()
    print(markdown(report) if args.markdown else json.dumps(report, indent=2,
                                                            sort_keys=True))
    return 0


ARM_ORDER = ("quiesced", "lag_25", "lag_50", "lag_75")


def markdown(report):
    """The tables the README publishes, printed by the thing that measured them."""
    out = ["| arm | held back | reported drift, no exclusion | left after exclusion "
           "| keys checked | coverage | verdict |", "|---|---|---|---|---|---|---|"]
    for arm in ARM_ORDER:
        body = report["arms"][arm]
        a, b = body["no_exclusion"], body["with_exclusion"]
        out.append("| `{}` | {:,} | {:,} | {:,} | {:,} | {} | {} |".format(
            arm, body["records_held_back"], a["mismatched"], b["mismatched"],
            b["checked"],
            "n/a" if b["coverage"] is None else "{:.1%}".format(b["coverage"]),
            b["verdict"]))

    out.append("")
    out.append("| arm | keyed: missing | extra | differing | set subtraction reports |")
    out.append("|---|---|---|---|---|")
    for arm in ARM_ORDER:
        a = report["arms"][arm]["no_exclusion"]
        sd = report["arms"][arm]["set_difference_view"]
        out.append("| `{}` | {:,} | {:,} | {:,} | {:,} |".format(
            arm, a["missing"], a["extra"], a["differing"], sd["reported_rows"]))
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
