"""Run a real change stream through the SQL merge and grade it three ways.

Against the source tables, which is the only thing that says the warehouse is right.
Against `cdc/consumer.py`, which applied the same rules in memory and is the reference.
Against itself replayed, which is what idempotent means.

The change stream is the same one the consumer measurement uses. One Postgres and one
workload and one logical slot. Every arm below is a rearrangement of what came out of it.

    log_order        the log's own order, which no consumer sees
    interleaved      six partitions drained in an arbitrary interleaving
    shuffled         no order at all, an arm Kafka cannot produce
    replayed         the interleaved batches applied a second time
    unreduced        the batch staged without collapsing it first
    hard_delete      soft delete removed, so the position goes with the row
    versioned        the merge key literally including the log position
    open             the ordering guard removed

Bring-up and work have to sit in one script, because the server does not survive the shell
that started it here.

    python3 -m scripts.merge_probe --steps 4000 --seed 11 --partitions 6 --markdown
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
from cdc import topics  # noqa: E402
from load import apply as applier  # noqa: E402
from load import workload  # noqa: E402
from warehouse import merge as wh  # noqa: E402
from warehouse import sql as W  # noqa: E402

SLOT = "merge_probe"
PREFIX = "shopcdc"

COMPARED = {
    "customer": ("customer_id", "email", "full_name", "country", "created_at", "updated_at"),
    "product": ("product_id", "sku", "name", "price_cents", "active", "updated_at"),
    "order_header": ("order_id", "customer_id", "status", "total_cents", "placed_at",
                     "updated_at"),
    "order_item": ("order_id", "line_no", "product_id", "qty", "unit_cents"),
}

PRIMARY_KEY_COLUMNS = {
    "customer": ("customer_id",),
    "product": ("product_id",),
    "order_header": ("order_id",),
    "order_item": ("order_id", "line_no"),
}


def reset_schema(cur, schema_path):
    cur.execute("drop schema if exists shop cascade")
    cur.execute("drop publication if exists cdc_shop")
    with open(schema_path) as handle:
        cur.execute(handle.read())


def truth(cur, table, columns):
    select = ", ".join("{}::text".format(c) for c in columns)
    cur.execute("select {} from shop.{}".format(select, table))
    return {tuple(r) for r in cur.fetchall()}


def run_workload(cur, seed, steps, customers, products):
    """The same workload the other two probes drive, built through the same call.

    seed_rows has to say the dimension tables are populated. Telling the planner they are
    empty deflects the first update on each into an insert, which moves every generator
    draw after it, and one seed then produces two different databases. Two probes in this
    repo already disagreed about a published rate for exactly that reason.
    """
    seed_rows = dict.fromkeys(workload.TABLES, 0)
    seed_rows["customer"] = customers
    seed_rows["product"] = products
    plan = workload.plan(seed, steps, seed_rows=seed_rows)
    a = applier.Applier(cur, seed)
    a.adopt_existing()
    seeded = a.seed_dimensions(customers, products)
    applied, skipped = a.run(plan.ops)
    return {"seeded": seeded, "applied": applied, "skipped": skipped,
            "deflected": plan.deflected}


def build_targets():
    out = []
    for table in workload.TABLES:
        out.append(wh.Target(
            name="wh_" + table,
            source_table="shop." + table,
            topic=topics.topic_name(PREFIX, "shop", table),
            columns=COMPARED[table],
            key_columns=PRIMARY_KEY_COLUMNS[table],
        ))
    return out


def load(order, batch_size, merge_keys, versioned=False, guard=True, reduce=True,
         on_tombstone="delete", soft_delete=True, allow_error=False):
    """Apply one delivery order to a fresh in-memory warehouse, batch by batch.

    soft_delete=False is the arm the whole soft delete argument rests on. The row is
    removed rather than flagged, which throws away the log position with it, so the
    ordering guard has nothing to compare a late change against.

    allow_error is off everywhere except the unreduced arm, which is the only one with a
    reason to raise. A blanket catch here would have turned the first parser error in a
    generated statement into eight arms quietly reporting `refused`, which is exactly what
    it did before this argument existed.
    """
    con = duckdb.connect()
    warehouse = wh.Warehouse(con, build_targets(), versioned=versioned)
    warehouse.create()
    totals = {}
    error = None
    batches = wh.batches(order, batch_size)
    for i, batch in enumerate(batches, start=1):
        try:
            counters = warehouse.apply_batch(batch, i, merge_keys=merge_keys,
                                             on_tombstone=on_tombstone, guard=guard,
                                             reduce=reduce)
        except duckdb.Error as exc:
            if not allow_error:
                raise
            error = "{}: {}".format(type(exc).__name__, str(exc).split("\n")[0])
            break
        for k, v in counters.items():
            totals[k] = totals.get(k, 0) + v
        if not soft_delete:
            for t in warehouse.targets:
                con.execute("delete from {} where {} = true".format(
                    W.quote(t.name), W.quote(W.DELETED)))
    return warehouse, totals, error, len(batches)


def grade(warehouse, source, reference):
    """Warehouse against the source, and warehouse against the in-memory consumer."""
    tables = {}
    agrees = {}
    for table in workload.TABLES:
        applied = warehouse.live_rows("wh_" + table, COMPARED[table])
        tables[table] = consumer.compare(applied, source[table])
        ref = reference[table] if reference is not None else None
        agrees[table] = None if ref is None else {
            "sql_only": len(applied - ref),
            "memory_only": len(ref - applied),
            "same": applied == ref,
        }
    return tables, agrees


def in_memory(order, merge_keys, guard=True, on_tombstone="delete"):
    result = consumer.apply_events(order, guard=guard, on_tombstone=on_tombstone,
                                   merge_keys=merge_keys)
    out = {}
    for table in workload.TABLES:
        topic = topics.topic_name(PREFIX, "shop", table)
        out[table] = consumer.as_row_set(result.rows, topic, COMPARED[table])
    return out, result.counters


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

    reset_schema(cur, os.path.join(here, "db", "schema.sql"))
    pg.drop_slot_if_exists(cur, SLOT)
    pg.create_slot(cur, SLOT, "test_decoding")
    report["workload"] = run_workload(cur, args.seed, args.steps, args.customers,
                                      args.products)

    raw = pg.peek_changes_with_lsn(cur, SLOT)
    rows = [(lsn, decode.parse_row(data)) for lsn, data in raw]
    rows = [(lsn, r) for lsn, r in rows if r is not None]

    primary_keys = {}
    for table in workload.TABLES:
        primary_keys["shop." + table] = tuple(pg.primary_key_columns(cur, "shop." + table))

    source = {t: truth(cur, t, COMPARED[t]) for t in workload.TABLES}
    report["source_rows"] = {t: len(v) for t, v in source.items()}
    key_columns = {t: events.key_columns_for(t, primary_keys) for t in primary_keys}
    evs = events.stream(rows, key_columns, PREFIX)
    report["stream"] = {
        "row_changes": len(rows),
        "records": len(evs),
        "tombstones": sum(1 for e in evs if e.tombstone),
        "positions_shared_by_more_than_one_change":
            len(rows) - len({lsn for lsn, _ in rows}),
    }

    pg.drop_slot_if_exists(cur, SLOT)
    con.close()

    routed = delivery.route(evs, args.partitions)
    interleaved = delivery.interleave(routed, args.seed)
    shuffled = delivery.shuffle(evs, args.seed)
    log_order = list(evs)

    arms = [
        ("log_order", log_order, {}),
        ("interleaved", interleaved, {}),
        ("shuffled", shuffled, {}),
        ("unreduced", interleaved, {"reduce": False, "allow_error": True}),
        ("hard_delete", interleaved, {"soft_delete": False}),
        ("hard_delete_shuffled", shuffled, {"soft_delete": False}),
        ("versioned", interleaved, {"versioned": True}),
        ("open_shuffled", shuffled, {"guard": False}),
    ]

    report["arms"] = {}
    for name, order, options in arms:
        warehouse, totals, error, batch_count = load(
            order, args.batch, primary_keys, **options)
        reference = None
        if not options:
            reference, _ = in_memory(order, primary_keys)
        tables, agrees = grade(warehouse, source, reference)
        report["arms"][name] = {
            "batches": batch_count,
            "error": error,
            "counters": totals,
            "counts": warehouse.counts(),
            "tables": tables,
            "agrees_with_memory": agrees,
            "all_tables_exact": all(v["exact"] for v in tables.values()),
            "within_run_fingerprint": warehouse.fingerprint(),
            "last_batch": warehouse.last_batch(),
        }
        warehouse.con.close()

    # Idempotency. The same batches applied twice into one warehouse have to leave the
    # identical table, batch column and all, or the word does not mean anything.
    con2 = duckdb.connect()
    warehouse = wh.Warehouse(con2, build_targets())
    warehouse.create()
    batch_list = wh.batches(interleaved, args.batch)
    for i, batch in enumerate(batch_list, start=1):
        warehouse.apply_batch(batch, i, merge_keys=primary_keys)
    once = warehouse.fingerprint()
    for i, batch in enumerate(batch_list, start=1):
        warehouse.apply_batch(batch, i, merge_keys=primary_keys)
    twice = warehouse.fingerprint()
    tables, _ = grade(warehouse, source, None)
    report["replay"] = {
        "batches": len(batch_list),
        "within_run_fingerprint_after_one_pass": once,
        "within_run_fingerprint_after_two_passes": twice,
        "identical": once == twice,
        "all_tables_exact": all(v["exact"] for v in tables.values()),
        "ledger": warehouse.last_batch(),
    }
    warehouse.con.close()

    # Batch size. A boundary decides whether two changes to one row meet in the reduction
    # or in the merge's guard. Both paths have to give the same warehouse.
    report["batch_sizes"] = {}
    for size in (1, 37, 250, len(interleaved)):
        warehouse, totals, error, count = load(interleaved, size, primary_keys)
        tables, _ = grade(warehouse, source, None)
        report["batch_sizes"][size] = {
            "batches": count,
            "within_run_fingerprint_ignoring_batch_column":
                warehouse.fingerprint(include_batch=False),
            "all_tables_exact": all(v["exact"] for v in tables.values()),
            "superseded_inside_a_batch": totals.get("superseded", 0),
        }
        warehouse.con.close()

    if args.markdown:
        print(markdown(report))
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


ARM_ORDER = ("log_order", "interleaved", "shuffled", "unreduced", "hard_delete",
             "hard_delete_shuffled", "versioned", "open_shuffled")


def markdown(report):
    """The tables the README publishes, printed by the thing that measured them."""
    out = ["| arm | rows | live | soft deleted | missing | extra | four tables |",
           "|---|---|---|---|---|---|---|"]
    for arm in ARM_ORDER:
        body = report["arms"][arm]
        counts = body["counts"]
        total = sum(c["rows"] for c in counts.values())
        live = sum(c["live"] for c in counts.values())
        gone = sum(c["soft_deleted"] for c in counts.values())
        missing = sum(t["missing"] for t in body["tables"].values())
        extra = sum(t["extra"] for t in body["tables"].values())
        verdict = "exact" if body["all_tables_exact"] else "**wrong**"
        if body["error"]:
            verdict = "**refused**"
        out.append("| `{}` | {:,} | {:,} | {:,} | {:,} | {:,} | {} |".format(
            arm, total, live, gone, missing, extra, verdict))

    out.append("")
    out.append("| batch size | batches | collapsed inside a batch | warehouse |")
    out.append("|---|---|---|---|")
    first = None
    for size, body in sorted(report["batch_sizes"].items(), key=lambda kv: int(kv[0])):
        fp = body["within_run_fingerprint_ignoring_batch_column"]
        first = fp if first is None else first
        out.append("| {:,} | {:,} | {:,} | {} |".format(
            int(size), body["batches"], body["superseded_inside_a_batch"],
            "identical" if fp == first else "**differs**"))
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
