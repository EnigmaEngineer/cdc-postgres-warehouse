"""Run a real change stream through the consumer and check what comes out.

The question the topic layout measurement left open. `shop.order_item` has a composite
primary key, so Debezium keys its records by both columns and the lines of one order are
hashed independently. Setting `message.key.columns` to `shop.order_item:order_id` puts them
back together. The README said the choice was per order ordering or per row compaction and
did not make it.

This makes it, by running both.

Every arm reads the same change stream out of one slot. What differs is the record key and
the order the records are delivered in. The answer for each arm is the applied state
against the source table, read from the same database a second later.

    log_order            the log's own order, which no consumer ever sees
    interleaved          six partitions, drained in an arbitrary interleaving
    interleaved_open     the same, ordering guard removed
    shuffled             no order at all, guard on
    shuffled_open        no order at all, guard off
    compacted            what a consumer bootstrapping from a compacted topic gets

Run it with the server already up. Bring-up and work go in one script, because the server
does not survive past the shell that started it in some sandboxes.

    python3 -m scripts.consumer_probe --steps 4000 --seed 11 --partitions 6
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc import consumer  # noqa: E402
from cdc import decode  # noqa: E402
from cdc import delivery  # noqa: E402
from cdc import events  # noqa: E402
from cdc import pg  # noqa: E402
from cdc import topics  # noqa: E402
from load import drive  # noqa: E402
from load import workload  # noqa: E402
from warehouse import merge as wh  # noqa: E402

SLOT = "consumer_probe"
PREFIX = wh.TOPIC_PREFIX

# The columns compared against the source, in the order the comparison uses. Every column
# of every table, including the timestamps, because leaving one out means the arm that
# corrupts exactly that column passes.
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=80)
    ap.add_argument("--dsn", default=pg.DEFAULT_DSN)
    ap.add_argument("--markdown", action="store_true")
    args = ap.parse_args()

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    con = pg.connect(args.dsn)
    cur = con.cursor()
    # DateStyle and TimeZone decide how a timestamp prints, and test_decoding renders in
    # this same backend. Pinning them means the comparison is not measuring a session
    # setting that happened to match.
    cur.execute("set timezone to 'UTC'")
    cur.execute("set datestyle to 'ISO, MDY'")

    report = {"settings": vars(args)}

    drive.reset_schema(cur, os.path.join(here, "db", "schema.sql"))
    pg.drop_slot_if_exists(cur, SLOT)
    pg.create_slot(cur, SLOT, "test_decoding")

    report["workload"] = drive.run_workload(cur, args.seed, args.steps,
                                           args.customers, args.products)

    raw = pg.peek_changes_with_lsn(cur, SLOT)
    rows = [(lsn, decode.parse_row(data)) for lsn, data in raw]
    rows = [(lsn, r) for lsn, r in rows if r is not None]
    report["stream"] = {
        "decoded_lines": len(raw),
        "row_changes": len(rows),
        "distinct_positions": len({lsn for lsn, _ in rows}),
        "positions_shared_by_more_than_one_change":
            len(rows) - len({lsn for lsn, _ in rows}),
        "monotonic": all(a[0] <= b[0] for a, b in zip(rows, rows[1:])),
    }

    primary_keys = {}
    for table in workload.TABLES:
        primary_keys["shop." + table] = tuple(
            pg.primary_key_columns(cur, "shop." + table))
    report["primary_keys"] = {k: list(v) for k, v in primary_keys.items()}

    source = {t: drive.truth(cur, t, wh.COMPARED_COLUMNS[t]) for t in workload.TABLES}
    report["source_rows"] = {t: len(v) for t, v in source.items()}

    cur.execute("select order_id, line_no from shop.order_item order by 1, 2")
    surviving_items = [{"order_id": r[0], "line_no": r[1]} for r in cur.fetchall()]

    report["strategies"] = {}
    for name, override in KEY_STRATEGIES.items():
        key_columns = {
            t: events.key_columns_for(t, primary_keys, override) for t in primary_keys
        }
        evs = events.stream(rows, key_columns, PREFIX)
        arms, routed = build_arms(evs, args.partitions, args.seed, primary_keys)

        strategy = {
            "key_columns": {k: list(v) for k, v in key_columns.items()},
            "records": len(evs),
            "tombstones": sum(1 for e in evs if e.tombstone),
            "key_reuse": events.key_reuse(evs, primary_keys),
            "spread": delivery.partition_spread(routed),
            # Over the order_item rows the workload leaves behind, which is the same
            # population the topic layout measurement used. Recomputing it on a different
            # denominator would put two split rates in one repo that disagree for a reason
            # nobody would find.
            "order_item_fanout": topics.split_rate(
                surviving_items, key_columns["shop.order_item"], "order_id",
                args.partitions),
            "arms": {},
        }
        for arm_name, ordered, guard, on_tombstone, merge in arms:
            result = consumer.apply_events(ordered, guard=guard,
                                           on_tombstone=on_tombstone,
                                           merge_keys=merge)
            per_table = {}
            for table in workload.TABLES:
                topic = topics.topic_name(PREFIX, "shop", table)
                applied = consumer.as_row_set(result.rows, topic, wh.COMPARED_COLUMNS[table])
                per_table[table] = consumer.compare(applied, source[table])
            strategy["arms"][arm_name] = {
                "counters": result.counters,
                "delivery": delivery.disorder(ordered),
                "tables": per_table,
                "all_tables_exact": all(v["exact"] for v in per_table.values()),
            }
        report["strategies"][name] = strategy

    # The tombstone arm with the guard on, both keys, is the comparison the day is about.
    # Worked out here rather than beside a table, because a number derived from published
    # numbers and typed into a document is a number nothing re-runs.
    report["verdict"] = {
        k: {
            "order_item_missing_interleaved":
                v["arms"]["interleaved"]["tables"]["order_item"]["missing"],
            "order_item_missing_tombstone_ignored":
                v["arms"]["interleaved_tombstone_ignored"]["tables"]["order_item"]["missing"],
            "order_item_missing_compacted":
                v["arms"]["compacted"]["tables"]["order_item"]["missing"],
            "order_item_missing_key_as_row":
                v["arms"]["interleaved_key_as_row"]["tables"]["order_item"]["missing"],
            "rows_removed_by_tombstones_interleaved":
                v["arms"]["interleaved"]["counters"]["rows_removed_by_tombstones"],
            "stale_dropped_interleaved":
                v["arms"]["interleaved"]["counters"]["stale_dropped"],
            "stale_dropped_shuffled":
                v["arms"]["shuffled"]["counters"]["stale_dropped"],
        }
        for k, v in report["strategies"].items()
    }

    pg.drop_slot_if_exists(cur, SLOT)
    con.close()

    if args.markdown:
        print(markdown(report))
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


ARM_ORDER = ("log_order", "interleaved", "interleaved_open", "compacted",
             "interleaved_tombstone_ignored", "interleaved_key_as_row",
             "shuffled", "shuffled_open")


def markdown(report):
    """The two tables the README publishes, printed by the thing that measured them.

    Typing a row of this into a document by hand is how a figure ends up with nothing
    behind it that could ever contradict it.
    """
    out = []
    for strategy in ("row", "order"):
        body = report["strategies"][strategy]
        out.append("`{}` key, {} of {} record keys carry more than one row".format(
            strategy, body["key_reuse"]["keys_carrying_more_than_one_row"],
            body["key_reuse"]["keys"]))
        out.append("")
        out.append("| delivery | order_item rows | missing | extra | all four tables |")
        out.append("|---|---|---|---|---|")
        for arm in ARM_ORDER:
            t = body["arms"][arm]["tables"]["order_item"]
            out.append("| `{}` | {} | {} | {} | {} |".format(
                arm, t["applied_rows"], t["missing"], t["extra"],
                "exact" if body["arms"][arm]["all_tables_exact"] else "**wrong**"))
        out.append("")

    out.append("| key | delivery | pairs out of order | under one key | dropped as stale |")
    out.append("|---|---|---|---|---|")
    for strategy in ("row", "order"):
        for arm in ("log_order", "interleaved", "shuffled"):
            result = report["strategies"][strategy]["arms"][arm]
            d = result["delivery"]
            out.append("| `{}` | `{}` | {:,} | {:,} | {:,} |".format(
                strategy, arm, d["inversions"], d["same_key_inversions"],
                result["counters"]["stale_dropped"]))
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
